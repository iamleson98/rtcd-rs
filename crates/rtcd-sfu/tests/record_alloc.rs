//! The packet path of a recorded stream allocates nothing more than that of one that is not
//! recorded. This is a test binary of its own because it replaces the global allocator with one
//! that counts.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use rtcd_sfu::record::{RecordItem, record_channel};
use support::{Layer, World};

// Per thread, so that tests running side by side do not count each other's allocations. `const`
// cells without a destructor are safe to touch from inside the allocator.
thread_local! {
    /// Whether the allocations of this thread are being counted.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    /// What the test has asked to be measured.
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

fn note(size: usize) {
    if COUNTING.get() {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        BYTES.set(BYTES.get() + size as u64);
    }
}

// SAFETY: every call is passed on to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Counts the allocations of the engine while it takes datagrams (the packet path: decrypt,
/// forward, write to the receivers), once the test says it is measuring.
fn around(kind: &'static str, on: bool) {
    COUNTING.set(on && kind == "datagram" && MEASURING.get());
}

#[derive(Clone, Copy)]
enum Traffic {
    /// Two listeners hear a speaker.
    Voice,
    /// A speaker alone in its call: no one receives what it sends.
    VoiceUnheard,
    /// A camera and a screen share, on both layers, with a viewer.
    Video,
    /// Two speakers and a listener.
    TwoSpeakers,
}

/// The allocations and bytes of the engine's packet path for `traffic`, and the packets the
/// recording was handed (none when it is not recording).
fn measure(traffic: Traffic, queue: Option<usize>) -> (u64, u64, usize) {
    let mut w = World::new();
    w.around_engine = around;
    let mut b = None;
    let (a, listeners) = match traffic {
        Traffic::TwoSpeakers => {
            let a = w.join("alice", "call1");
            b = Some(w.join("bob", "call1"));
            (a, vec![w.join("carol", "call1")])
        }
        Traffic::Voice => {
            let a = w.join("alice", "call1");
            (a, vec![w.join("bob", "call1"), w.join("carol", "call1")])
        }
        Traffic::VoiceUnheard => (w.join("alice", "call1"), vec![]),
        Traffic::Video => {
            let a = w.join_with_camera("alice", "call1");
            let b = w.join("bob", "call1");
            w.share_screen(a);
            (a, vec![b])
        }
    };
    let mut recording = queue.map(|capacity| {
        let (sink, rx) = record_channel(capacity);
        w.engine
            .record_start(w.now, "group", "call1", "rec1", w.now, sink);
        rx
    });
    let send = |w: &mut World, frames: usize| match traffic {
        Traffic::TwoSpeakers => {
            for f in 0..frames {
                let level = if f % 2 == 0 { 20 } else { 70 };
                w.send_audio(a, level);
                w.send_audio(b.expect("two speakers"), level);
                w.run_for(std::time::Duration::from_millis(20));
            }
        }
        Traffic::Voice | Traffic::VoiceUnheard => {
            w.speak(a, frames, |f| if f % 2 == 0 { 20 } else { 70 })
        }
        Traffic::Video => {
            w.stream_video(
                a,
                false,
                frames as f64 / 30.0,
                30,
                &[Layer::Low, Layer::High],
            );
            w.stream_video(
                a,
                true,
                frames as f64 / 30.0,
                30,
                &[Layer::Low, Layer::High],
            );
        }
    };
    // Everything set up and warm: the buffers have grown to the size they stay at, and the
    // recording has taken its streams.
    send(&mut w, 150);
    let _ = listeners;

    ALLOCATIONS.set(0);
    BYTES.set(0);
    MEASURING.set(true);
    send(&mut w, 150);
    MEASURING.set(false);
    let measured = (ALLOCATIONS.get(), BYTES.get());
    let recorded_packets = recording.as_mut().map_or(0, |rx| {
        std::iter::from_fn(|| rx.try_recv().ok())
            .filter(|i| matches!(i, RecordItem::Packet(_)))
            .count()
    });
    (measured.0, measured.1, recorded_packets)
}

const ROOMY: usize = 1 << 16;

fn same_as_unrecorded(traffic: Traffic, least: usize) {
    same_with_queue(traffic, ROOMY, least);
}

/// The packet path with a queue of `capacity` items, against the same traffic not recorded.
fn same_with_queue(traffic: Traffic, capacity: usize, least: usize) {
    let plain = measure(traffic, None);
    let recorded = measure(traffic, Some(capacity));
    assert!(
        recorded.2 >= least,
        "the recording was handed {} packets, expected at least {least}",
        recorded.2
    );
    println!(
        "allocations on the packet path: {} for {} bytes unrecorded, {} for {} bytes recorded ({} packets)",
        plain.0, plain.1, recorded.0, recorded.1, recorded.2
    );
    assert_eq!(
        (recorded.0, recorded.1),
        (plain.0, plain.1),
        "allocations and bytes of the packet path, recorded against not recorded ({} packets recorded)",
        recorded.2
    );
}

#[test]
fn a_recorded_voice_costs_the_packet_path_no_allocation() {
    same_as_unrecorded(Traffic::Voice, 150);
}

#[test]
fn a_recorded_voice_nobody_hears_costs_the_packet_path_no_allocation() {
    same_as_unrecorded(Traffic::VoiceUnheard, 150);
}

#[test]
fn a_recorded_camera_and_screen_cost_the_packet_path_no_allocation() {
    // 150 frames of two packets on the large layer of each of the two streams.
    same_as_unrecorded(Traffic::Video, 150 * 2 * 2 - 40);
}

/// A queue that is full for good: every packet is left out, and the events that cannot be queued
/// wait. None of that allocates on the packet path.
#[test]
fn a_queue_of_one_item_that_is_always_full_costs_no_allocation() {
    // The beginning fills it; the stream's open waits for room that never comes.
    same_with_queue(Traffic::Voice, 1, 0);
    same_with_queue(Traffic::VoiceUnheard, 1, 0);
}

#[test]
fn a_full_queue_with_no_event_waiting_costs_no_allocation() {
    // The beginning and the open fill it; each packet finds it full.
    same_with_queue(Traffic::Voice, 2, 0);
}

#[test]
fn events_waiting_for_room_while_packets_arrive_cost_no_allocation() {
    // The second speaker's open waits behind the first's, and packets of both are left out.
    same_with_queue(Traffic::TwoSpeakers, 2, 0);
    same_with_queue(Traffic::Video, 3, 0);
}
