//! Voice activity from the audio level header extension (RFC 6464).
//!
//! The level is in -dBov, so a louder frame has a smaller number; what matters is not the level but
//! how much it varies: speech moves between loud and quiet within a second, a steady hum does not.
//! The monitor keeps the last `sample_size` levels and compares their standard deviation against
//! two thresholds, the higher to start voice and the lower to stop it, and holds voice on for at
//! least `activation_duration` so a pause does not make the indicator flicker.

use std::time::{Duration, Instant};

/// Tuning of a [`Monitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VadConfig {
    pub sample_size: usize,
    pub activation_duration: Duration,
    pub activation_threshold: u8,
    pub deactivation_threshold: u8,
}

impl Default for VadConfig {
    fn default() -> Self {
        Self {
            sample_size: 50,
            activation_duration: Duration::from_secs(2),
            activation_threshold: 10,
            deactivation_threshold: 4,
        }
    }
}

/// One Opus frame on the 48 kHz RTP clock.
const OPUS_FRAME_SAMPLES: i32 = 960;
/// The audio level of digital silence.
const SILENCE_LEVEL: u8 = 127;

/// A voice activity detector for one microphone.
#[derive(Debug)]
pub struct Monitor {
    cfg: VadConfig,
    levels: Vec<u8>,
    next: usize,
    voice: bool,
    activated_at: Option<Instant>,
    last_timestamp: Option<u32>,
}

impl Monitor {
    pub fn new(cfg: VadConfig) -> Self {
        assert!(
            cfg.sample_size > 1,
            "a deviation needs at least two samples"
        );
        Self {
            cfg,
            levels: Vec::with_capacity(cfg.sample_size),
            next: 0,
            voice: false,
            activated_at: None,
            last_timestamp: None,
        }
    }

    /// Whether voice is on.
    pub fn voice(&self) -> bool {
        self.voice
    }

    /// Records the level of a packet. A sender using discontinuous transmission leaves silent
    /// frames out, sending one packet every 400 ms instead of every 20 ms, so the frames missing
    /// before this packet (by its RTP timestamp) count as silence; without them the sample would
    /// stretch over seconds of silence and keep voice on. Returns the new state when it changed.
    pub fn push_packet(&mut self, level: u8, timestamp: u32, now: Instant) -> Option<bool> {
        let mut change = None;
        if let Some(last) = self.last_timestamp {
            let diff = timestamp.wrapping_sub(last) as i32;
            // A repeated or reordered packet leaves no frames missing.
            if diff > 0 {
                let missing =
                    ((diff / OPUS_FRAME_SAMPLES) - 1).clamp(0, self.cfg.sample_size as i32);
                for _ in 0..missing {
                    change = self.push_level(SILENCE_LEVEL, now).or(change);
                }
            }
        }
        self.last_timestamp = Some(timestamp);
        self.push_level(level, now).or(change)
    }

    /// Records one level. Returns the new state when it changed.
    pub fn push_level(&mut self, level: u8, now: Instant) -> Option<bool> {
        if self.levels.len() < self.cfg.sample_size {
            // Nothing is decided until the sample is full.
            self.levels.push(level);
            return None;
        }
        self.levels[self.next] = level;
        self.next = (self.next + 1) % self.cfg.sample_size;

        let deviation = std_dev(&self.levels);
        let wanted = if !self.voice && deviation > self.cfg.activation_threshold {
            true
        } else if self.voice && deviation < self.cfg.deactivation_threshold {
            // Held on for at least the activation duration.
            if self
                .activated_at
                .is_some_and(|at| now.saturating_duration_since(at) < self.cfg.activation_duration)
            {
                return None;
            }
            false
        } else {
            return None;
        };
        if wanted {
            self.activated_at = Some(now);
        }
        self.voice = wanted;
        Some(wanted)
    }

    /// Forgets everything and turns voice off (the participant muted). Returns whether voice was on.
    pub fn reset(&mut self) -> bool {
        let was_on = self.voice;
        self.levels.clear();
        self.next = 0;
        self.voice = false;
        self.activated_at = None;
        self.last_timestamp = None;
        was_on
    }
}

/// The sample standard deviation (Bessel's correction) around the rounded mean, rounded to an
/// integer, as the thresholds are integers.
fn std_dev(levels: &[u8]) -> u8 {
    let mean = (levels.iter().map(|&l| f64::from(l)).sum::<f64>() / levels.len() as f64).round();
    let squares: f64 = levels.iter().map(|&l| (f64::from(l) - mean).powi(2)).sum();
    (squares / (levels.len() - 1) as f64).sqrt().round() as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> VadConfig {
        VadConfig {
            sample_size: 10,
            activation_duration: Duration::from_millis(250),
            activation_threshold: 10,
            deactivation_threshold: 5,
        }
    }

    /// Alternating loud and quiet frames: speech.
    fn speech(m: &mut Monitor, now: Instant, frames: usize) -> Option<bool> {
        let mut last = None;
        for i in 0..frames {
            last = m.push_level(if i % 2 == 0 { 20 } else { 70 }, now).or(last);
        }
        last
    }

    #[test]
    fn nothing_is_decided_until_the_sample_is_full() {
        let mut m = Monitor::new(cfg());
        let t = Instant::now();
        assert_eq!(speech(&mut m, t, 10), None);
        assert!(!m.voice());
        assert_eq!(speech(&mut m, t, 1), Some(true), "the 11th frame decides");
    }

    #[test]
    fn voice_starts_on_variation_and_holds_for_the_activation_duration() {
        let mut m = Monitor::new(cfg());
        let t = Instant::now();
        assert_eq!(speech(&mut m, t, 12), Some(true));
        // A steady level would stop voice, but not before the hold has passed.
        for _ in 0..10 {
            assert_eq!(m.push_level(60, t + Duration::from_millis(100)), None);
        }
        assert!(m.voice());
        assert_eq!(
            m.push_level(60, t + Duration::from_millis(300)),
            Some(false)
        );
        assert!(!m.voice());
    }

    #[test]
    fn a_steady_level_never_starts_voice() {
        let mut m = Monitor::new(cfg());
        let t = Instant::now();
        for _ in 0..100 {
            assert_eq!(m.push_level(45, t), None);
        }
    }

    #[test]
    fn frames_a_dtx_sender_left_out_count_as_silence() {
        let mut m = Monitor::new(VadConfig::default());
        let t = Instant::now();
        // Speech, then a packet every 400 ms (20 frames apart): the gaps are silence, which has
        // a level of 127 against speech around 30, so the deviation stays high only while talking.
        let mut ts = 0u32;
        for i in 0..60 {
            m.push_packet(if i % 2 == 0 { 20 } else { 70 }, ts, t);
            ts += 960;
        }
        assert!(m.voice());
        let mut changed = None;
        for _ in 0..10 {
            ts += 960 * 20;
            changed = m
                .push_packet(127, ts, t + Duration::from_secs(5))
                .or(changed);
        }
        assert_eq!(
            changed,
            Some(false),
            "ten seconds of DTX silence turn voice off"
        );
    }

    #[test]
    fn a_reordered_packet_adds_no_silence() {
        let mut m = Monitor::new(VadConfig::default());
        let t = Instant::now();
        m.push_packet(30, 10_000, t);
        m.push_packet(30, 9_000, t);
        assert_eq!(m.levels.len(), 2);
    }

    #[test]
    fn reset_reports_whether_voice_was_on() {
        let mut m = Monitor::new(cfg());
        let t = Instant::now();
        speech(&mut m, t, 12);
        assert!(m.reset());
        assert!(!m.reset());
        assert!(m.levels.is_empty());
    }

    #[test]
    fn the_deviation_matches_the_go_rounding() {
        // Mean 45, deviations of 25: sqrt(10 * 625 / 9) = 26.4.
        let levels = [20, 70, 20, 70, 20, 70, 20, 70, 20, 70];
        assert_eq!(std_dev(&levels), 26);
        assert_eq!(std_dev(&[5; 10]), 0);
    }
}
