//! The data channel every participant opens ("calls-dc"): the signaling lock, SDP once the
//! connection is up, the client's quality reports, and a ping for liveness.

use std::time::Instant;

use bytes::BytesMut;
use rtcd_proto::dc::DcMessage;
use tracing::{debug, warn};

use super::Engine;
use super::session::{DcState, SessionKey};
use super::signaling::Channel;

/// Frames kept for a channel that is not open yet.
const MAX_QUEUED: usize = 64;

impl Engine {
    /// Sends a frame to the participant, or holds it until the channel opens.
    pub(super) fn dc_send(&mut self, key: SessionKey, msg: DcMessage, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let Some(id) = s.dc.as_ref().map(|d| d.id) else {
            if s.dc_queue.len() < MAX_QUEUED {
                s.dc_queue.push_back(msg);
            }
            return;
        };
        let frame = msg.encode();
        let sent =
            s.pc.data_channel(id)
                .map(|mut dc| dc.send(now, BytesMut::from(&frame[..])));
        match sent {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                warn!(session = %s.cfg.session_id, %err, "data channel send failed");
                self.metrics.error(&s.cfg.group_id, "dc");
            }
            None => debug!(session = %s.cfg.session_id, "data channel is gone"),
        }
    }

    /// The participant's data channel opened: from now on tracks may be pushed to it.
    pub(super) fn on_dc_open(&mut self, key: SessionKey, id: usize, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.dc.is_some() {
            // Any further channel is answered on itself but carries no signaling of ours.
            return;
        }
        s.dc = Some(DcState { id });
        s.down.ready = true;
        let group = s.cfg.group_id.clone();
        self.metrics
            .data_channel_open_time(&group, now.saturating_duration_since(s.created_at));
        let queued: Vec<_> = s.dc_queue.drain(..).collect();
        for msg in queued {
            self.dc_send(key, msg, now);
        }
        self.dc_send(
            key,
            DcMessage::Features(rtcd_proto::dc::FEATURE_RECORD_LAYER),
            now,
        );
        self.attach_existing_tracks(key, now);
        self.mark_dirty(key);
    }

    pub(super) fn on_dc_closed(&mut self, key: SessionKey, id: usize) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.dc.as_ref().is_some_and(|d| d.id == id) {
            s.dc = None;
        }
    }

    /// A message from the participant on the data channel `id`.
    pub(super) fn on_dc_message(
        &mut self,
        key: SessionKey,
        id: usize,
        is_string: bool,
        data: BytesMut,
        now: Instant,
    ) {
        // Older clients (mobile) ping with the text "ping" and expect the text "pong".
        if is_string {
            if &data[..] == b"ping"
                && let Some(s) = self.sessions.get_mut(key)
                && let Some(Err(err)) =
                    s.pc.data_channel(id)
                        .map(|mut dc| dc.send_text(now, "pong"))
            {
                debug!(session = %s.cfg.session_id, %err, "pong failed");
            }
            return;
        }
        let msg = match DcMessage::decode(&data) {
            Ok(m) => m,
            Err(err) => {
                warn!(session = %self.session_id(key), %err, "unreadable data channel frame");
                self.metrics.error(&self.group_id(key), "dc");
                return;
            }
        };
        let group = self.group_id(key);
        match msg {
            DcMessage::Ping => self.dc_send(key, DcMessage::Pong, now),
            DcMessage::Pong
            | DcMessage::MediaMap(_)
            | DcMessage::Lock(Some(_))
            | DcMessage::Features(_) => {}
            DcMessage::Sdp(json) => self.remote_description(key, &json, Channel::Dc, now),
            DcMessage::SharpScreen(on) => {
                if let Some(s) = self.sessions.get_mut(key) {
                    debug!(session = %s.cfg.session_id, on, "the receiver wants the sharp screen layer");
                    s.down.screen_level.pin_high(on);
                }
                self.review_screen_level(key, now, false);
            }
            DcMessage::LossRate(v) => self.metrics.client_loss_rate(&group, v),
            DcMessage::RoundTripTime(v) => self.metrics.client_rtt(&group, v),
            DcMessage::Jitter(v) => self.metrics.client_jitter(&group, v),
            DcMessage::Lock(None) => {
                let Some(s) = self.sessions.get_mut(key) else {
                    return;
                };
                let granted = s.lock.client_lock(now);
                debug!(session = %s.cfg.session_id, granted, "lock requested");
                self.dc_send(key, DcMessage::Lock(Some(granted)), now);
            }
            DcMessage::Unlock => {
                let Some(s) = self.sessions.get_mut(key) else {
                    return;
                };
                match s.lock.client_unlock(now) {
                    Some(held) => self.metrics.signaling_lock_held_time(&group, held),
                    None => debug!(session = %s.cfg.session_id, "unlock without the lock"),
                }
            }
        }
        // An unlock may let a waiting offer go out.
        self.mark_dirty(key);
    }
}
