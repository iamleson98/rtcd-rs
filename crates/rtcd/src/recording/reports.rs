//! What rtcd tells the server about its recordings, over the clients' websockets.
//!
//! Every app node of the server connects with the same client id, and a report is useful to any of
//! them: the server keeps what it learns in shared storage. So a report goes to any live connection
//! of the client that asked for the recording, and if none is connected it waits, in order, for the
//! next one. Reports are few (a file every ten minutes per stream, one at the end), so the queue is
//! small and bounded; past the bound the oldest is dropped.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use rtcd_proto::ClientMessage;
use tracing::warn;

use crate::metrics::Metrics;
use crate::service::ConnHandle;

/// Reports kept per client while it has no connection.
pub const MAX_QUEUED: usize = 4096;

#[derive(Debug, Default)]
struct Inner {
    conns: HashMap<String, ConnHandle>,
    queues: HashMap<String, VecDeque<ClientMessage>>,
}

/// The connections reports may use, and the reports waiting for one.
#[derive(Debug)]
pub struct Reports {
    inner: Mutex<Inner>,
    metrics: Arc<Metrics>,
}

impl Reports {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            metrics,
        }
    }

    /// A connection opened: what waited for its client goes out on it.
    pub fn attach(&self, conn_id: &str, handle: ConnHandle) {
        let client = handle.client_id.clone();
        let mut inner = self.inner.lock();
        inner.conns.insert(conn_id.to_owned(), handle);
        self.flush(&mut inner, &client);
    }

    pub fn detach(&self, conn_id: &str) {
        self.inner.lock().conns.remove(conn_id);
    }

    /// Sends `msg` to `client` on any of its connections, or keeps it until there is one.
    pub fn send(&self, client: &str, msg: ClientMessage) {
        let mut inner = self.inner.lock();
        let queue = inner.queues.entry(client.to_owned()).or_default();
        if queue.len() >= MAX_QUEUED {
            queue.pop_front();
            warn!(
                client,
                "too many reports wait for a connection; dropped the oldest"
            );
        }
        queue.push_back(msg);
        self.flush(&mut inner, client);
    }

    /// How many reports wait for `client`.
    pub fn queued(&self, client: &str) -> usize {
        self.inner
            .lock()
            .queues
            .get(client)
            .map_or(0, VecDeque::len)
    }

    fn flush(&self, inner: &mut Inner, client: &str) {
        let Inner { conns, queues } = inner;
        let Some(queue) = queues.get_mut(client) else {
            return;
        };
        while let Some(msg) = queue.front() {
            let sent = conns
                .values()
                .filter(|h| h.client_id == client)
                .any(|h| h.send(msg));
            if !sent {
                return;
            }
            self.metrics.ws_message(client, msg.type_name(), "out");
            queue.pop_front();
        }
        queues.remove(client);
    }
}

#[cfg(test)]
mod tests {
    use rtcd_proto::RecordEnded;
    use tokio::sync::mpsc;

    use super::*;
    use crate::service::Outbound;

    fn ended(id: &str) -> ClientMessage {
        ClientMessage::RecordEnded(RecordEnded {
            recording_id: id.into(),
            reason: "stopped".into(),
            ..RecordEnded::default()
        })
    }

    fn conn(client: &str) -> (ConnHandle, mpsc::Receiver<Outbound>) {
        let (tx, rx) = mpsc::channel(8);
        (ConnHandle::new(client.into(), tx), rx)
    }

    fn id_of(o: Outbound) -> String {
        let Outbound::Binary(b) = o else {
            panic!("not a message")
        };
        let ClientMessage::RecordEnded(e) = ClientMessage::unpack(&b).unwrap() else {
            panic!("not a record_ended")
        };
        e.recording_id
    }

    #[test]
    fn reports_wait_for_a_connection_and_go_in_order() {
        let r = Reports::new(Arc::new(Metrics::new()));
        r.send("mm", ended("a"));
        r.send("mm", ended("b"));
        assert_eq!(r.queued("mm"), 2);
        let (h, mut rx) = conn("mm");
        r.attach("c1", h);
        assert_eq!(r.queued("mm"), 0);
        assert_eq!(id_of(rx.try_recv().unwrap()), "a");
        assert_eq!(id_of(rx.try_recv().unwrap()), "b");
        r.send("mm", ended("c"));
        assert_eq!(id_of(rx.try_recv().unwrap()), "c");
    }

    #[test]
    fn a_report_goes_to_a_connection_of_its_own_client_only() {
        let r = Reports::new(Arc::new(Metrics::new()));
        let (other, mut other_rx) = conn("other");
        r.attach("c-other", other);
        r.send("mm", ended("a"));
        assert!(other_rx.try_recv().is_err());
        assert_eq!(r.queued("mm"), 1);
        let (h, mut rx) = conn("mm");
        r.attach("c-mm", h);
        assert_eq!(id_of(rx.try_recv().unwrap()), "a");
    }

    #[test]
    fn a_connection_that_is_gone_is_passed_over_for_another() {
        let r = Reports::new(Arc::new(Metrics::new()));
        let (dead, dead_rx) = conn("mm");
        let (live, mut live_rx) = conn("mm");
        r.attach("dead", dead);
        r.attach("live", live);
        drop(dead_rx);
        for n in 0..3 {
            r.send("mm", ended(&format!("r{n}")));
        }
        for n in 0..3 {
            assert_eq!(id_of(live_rx.try_recv().unwrap()), format!("r{n}"));
        }
        assert_eq!(r.queued("mm"), 0);
        // With the only live one detached, they wait again.
        r.detach("live");
        r.send("mm", ended("late"));
        assert_eq!(r.queued("mm"), 1);
    }

    #[test]
    fn the_queue_is_bounded_and_drops_the_oldest() {
        let r = Reports::new(Arc::new(Metrics::new()));
        for n in 0..MAX_QUEUED + 5 {
            r.send("mm", ended(&n.to_string()));
        }
        assert_eq!(r.queued("mm"), MAX_QUEUED);
        let (h, mut rx) = conn("mm");
        // The connection takes 8 and refuses the rest until it is read; the oldest kept is 5.
        r.attach("c", h);
        assert_eq!(id_of(rx.try_recv().unwrap()), "5");
    }
}
