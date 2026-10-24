package rtc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/iamleson98/sitename/server/public/shared/mlog"
)

// Receivers choose whose camera they get, and at which size. Forwarding
// every camera to every receiver costs each one a stream per participant
// (a class of 30 is ~30 streams per phone); a receiver showing the board
// wants the teacher's camera only, a gallery the tiles on screen. Cameras
// are sent as two simulcast layers, small (tiles) and full (a large view):
// a receiver gets the full one unless it asks for the small one, and
// whichever there is when the sender sends only one, or while the sender
// has paused the full one. A receiver that never says keeps getting every
// camera in full (the session recorder).

const (
	// cameraStallAfter is how long a full layer may go without a packet,
	// while the small one flows, before its receivers move to the small one.
	cameraStallAfter = 2 * time.Second
	// cameraWatchEvery is how often a simulcast camera's layers are checked.
	cameraWatchEvery = time.Second
)

// maxVideoSubscriptions bounds one receiver's wanted set.
const maxVideoSubscriptions = 100

type videoSubscription struct {
	SessionIDs []string `json:"session_ids"`
	// Low is the senders, among SessionIDs, wanted at the small layer.
	Low []string `json:"low"`
}

// wantsVideoFrom reports whether the receiver wants the sender's camera.
func (s *session) wantsVideoFrom(senderID string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.wantedVideo == nil || s.wantedVideo[senderID]
}

// cameraSource is the out track (and its source) of the sender's camera
// that this receiver should get: the layer it asked for if the sender
// sends it, else the other one; the small one while the full one is
// paused. A camera sent without simulcast is stored under the default
// level and serves everyone.
func (s *session) cameraSource(sender *session) (*webrtc.TrackLocalStaticRTP, *webrtc.TrackRemote) {
	mimeType := ScreenTrackMimeTypeDefault
	if sender.supportsAV1() && s.supportsAV1() {
		mimeType = webrtc.MimeTypeAV1
	}
	s.mut.RLock()
	levels := []string{SimulcastLevelHigh, SimulcastLevelLow}
	if s.lowVideo[sender.cfg.SessionID] || sender.cameraHighStalled.Load() {
		levels = []string{SimulcastLevelLow, SimulcastLevelHigh}
	}
	s.mut.RUnlock()

	sender.mut.RLock()
	defer sender.mut.RUnlock()
	for _, level := range levels {
		idx := getTrackIndex(mimeType, level)
		if out, remote := sender.outVideoTracks[idx], sender.remoteVideoTracks[idx]; len(out) > 0 && remote != nil {
			return pickRandom(out), remote
		}
	}
	return nil, nil
}

// cameraChange is what turns the camera track a receiver has into the one
// it should have (a track is known by its layer, rid): remove the current
// one, add the new one, or both to switch layers.
func cameraChange(have webrtc.TrackLocal, serve *webrtc.TrackLocalStaticRTP) (remove, add bool) {
	switch {
	case have == nil:
		return false, serve != nil
	case serve == nil:
		return true, false
	case have.RID() != serve.RID():
		return true, true
	default:
		return false, false
	}
}

// syncCamera makes what this receiver gets of the sender's camera match
// what it wants: nothing, or the layer cameraSource picks. Switching layers
// removes one track and adds the other; both carry the same track id, so
// the receiver's tile keeps showing the same person. The caller holds the
// call lock.
func (s *session) syncCamera(sender *session) error {
	var serve *webrtc.TrackLocalStaticRTP
	var remote *webrtc.TrackRemote
	if s.wantsVideoFrom(sender.cfg.SessionID) {
		serve, remote = s.cameraSource(sender)
	}
	s.mut.RLock()
	have := s.videoIn[sender.cfg.SessionID]
	s.mut.RUnlock()

	remove, add := cameraChange(have, serve)
	if remove {
		select {
		case s.tracksCh <- trackActionContext{action: trackActionRemove, localTrack: have}:
			s.mut.Lock()
			delete(s.videoIn, sender.cfg.SessionID)
			s.mut.Unlock()
		default:
			return fmt.Errorf("failed to remove video track: channel is full")
		}
	}
	if add && !s.queueVideo(serve, remote, sender) {
		return fmt.Errorf("failed to add video track: channel is full")
	}
	return nil
}

// markCameraPacket notes that a packet of this sender's camera layer came in.
func (s *session) markCameraPacket(rid string, now time.Time) {
	if rid == SimulcastLevelHigh {
		s.cameraHighAt.Store(now.UnixNano())
	} else {
		s.cameraLowAt.Store(now.UnixNano())
	}
}

// updateCameraHighStalled decides whether this sender's full camera layer
// is paused: no packet for cameraStallAfter while the small layer still
// flows (a browser drops its top layer when its upload is short). When
// neither flows the camera is off, and nothing changes. It reports whether
// the state changed.
func (s *session) updateCameraHighStalled(now time.Time) bool {
	highIdle := now.Sub(time.Unix(0, s.cameraHighAt.Load()))
	lowIdle := now.Sub(time.Unix(0, s.cameraLowAt.Load()))
	stalled := s.cameraHighStalled.Load()
	switch {
	case highIdle < cameraWatchEvery:
		stalled = false
	case highIdle >= cameraStallAfter && lowIdle < cameraWatchEvery:
		stalled = true
	}
	return s.cameraHighStalled.Swap(stalled) != stalled
}

// watchCameraLayers moves the receivers of a simulcast camera between its
// layers as its full one pauses and resumes, until done (the full layer's
// track ended).
func (s *Server) watchCameraLayers(call *call, sender *session, done <-chan struct{}) {
	ticker := time.NewTicker(cameraWatchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-sender.closeCh:
			return
		case now := <-ticker.C:
			if !sender.updateCameraHighStalled(now) {
				continue
			}
			s.log.Debug("camera full layer changed", mlog.String("sessionID", sender.cfg.SessionID),
				mlog.Bool("stalled", sender.cameraHighStalled.Load()))
			call.mut.Lock()
			call.iterSessions(func(ss *session) {
				if ss == sender || !ss.tracksInitDone.Load() {
					return
				}
				if err := ss.syncCamera(sender); err != nil {
					s.log.Error(err.Error(), mlog.String("sessionID", ss.cfg.SessionID))
				}
			})
			call.mut.Unlock()
		}
	}
}

// queueVideo queues a sender's camera track for this receiver and records
// it, so a later change knows to remove it.
func (s *session) queueVideo(track *webrtc.TrackLocalStaticRTP, remote *webrtc.TrackRemote, sender *session) bool {
	select {
	case s.tracksCh <- trackActionContext{action: trackActionAdd, localTrack: track, remoteTrack: remote, senderSession: sender}:
	default:
		return false
	}
	s.mut.Lock()
	s.videoIn[sender.cfg.SessionID] = track
	s.mut.Unlock()
	return true
}

// setVideoSubscription applies a receiver's wanted set: the cameras it no
// longer wants are removed, those it now wants are added, those it wants
// at another size switch layers. Holding the call lock keeps cameras from
// being published meanwhile.
func (s *Server) setVideoSubscription(call *call, us *session, data []byte) error {
	var sub videoSubscription
	if err := json.Unmarshal(data, &sub); err != nil {
		return fmt.Errorf("failed to unmarshal video subscription: %w", err)
	}
	if len(sub.SessionIDs) > maxVideoSubscriptions {
		return fmt.Errorf("video subscription too large: %d sessions", len(sub.SessionIDs))
	}
	wanted := make(map[string]bool, len(sub.SessionIDs))
	for _, id := range sub.SessionIDs {
		wanted[id] = true
	}
	low := make(map[string]bool, len(sub.Low))
	for _, id := range sub.Low {
		if wanted[id] {
			low[id] = true
		}
	}

	call.mut.Lock()
	defer call.mut.Unlock()

	us.mut.Lock()
	us.wantedVideo = wanted
	us.lowVideo = low
	us.mut.Unlock()

	// A receiver still connecting gets its cameras from handleTracks, which
	// applies the wanted set.
	if !us.tracksInitDone.Load() {
		return nil
	}

	call.iterSessions(func(ss *session) {
		if ss == us {
			return
		}
		if err := us.syncCamera(ss); err != nil {
			s.log.Error(err.Error())
		}
	})
	return nil
}
