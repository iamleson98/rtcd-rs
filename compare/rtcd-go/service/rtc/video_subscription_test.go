package rtc

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func cameraTrack(t *testing.T, sessionID, rid string) *webrtc.TrackLocalStaticRTP {
	t.Helper()
	tr, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		genTrackID(trackTypeVideo, sessionID), sessionID, webrtc.WithRTPStreamID(rid))
	require.NoError(t, err)
	return tr
}

// sender is a session sending its camera at the given simulcast levels
// ("" for a camera without simulcast).
func sender(t *testing.T, id string, rids ...string) *session {
	s := &session{
		cfg:               SessionConfig{SessionID: id},
		outVideoTracks:    map[string][]*webrtc.TrackLocalStaticRTP{},
		remoteVideoTracks: map[string]*webrtc.TrackRemote{},
	}
	for _, rid := range rids {
		level := rid
		if level == "" {
			level = SimulcastLevelDefault
		}
		idx := getTrackIndex(webrtc.MimeTypeVP8, level)
		s.outVideoTracks[idx] = []*webrtc.TrackLocalStaticRTP{cameraTrack(t, id, rid)}
		s.remoteVideoTracks[idx] = &webrtc.TrackRemote{}
	}
	return s
}

func TestCameraSource(t *testing.T) {
	receiver := &session{}
	both := sender(t, "teacher", SimulcastLevelLow, SimulcastLevelHigh)
	onlyLow := sender(t, "a", SimulcastLevelLow)
	single := sender(t, "b", "")
	none := sender(t, "c")

	rid := func(s *session) string {
		track, remote := receiver.cameraSource(s)
		if track == nil {
			require.Nil(t, remote)
			return "none"
		}
		require.NotNil(t, remote)
		return track.RID()
	}

	require.Equal(t, SimulcastLevelHigh, rid(both), "the full layer unless asked otherwise")
	receiver.lowVideo = map[string]bool{"teacher": true, "a": true, "b": true}
	require.Equal(t, SimulcastLevelLow, rid(both), "the small layer when asked")

	receiver.lowVideo = nil
	require.Equal(t, SimulcastLevelLow, rid(onlyLow), "the layer there is, until the other arrives")
	require.Equal(t, "", rid(single), "a camera without simulcast serves everyone")
	receiver.lowVideo = map[string]bool{"b": true}
	require.Equal(t, "", rid(single))
	require.Equal(t, "none", rid(none))

	both.cameraHighStalled.Store(true)
	require.Equal(t, SimulcastLevelLow, rid(both), "the small layer while the full one is paused")
}

func TestUpdateCameraHighStalled(t *testing.T) {
	s := &session{}
	t0 := time.Unix(1_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	s.markCameraPacket(SimulcastLevelLow, t0)
	s.markCameraPacket(SimulcastLevelHigh, t0)
	require.False(t, s.updateCameraHighStalled(at(time.Second)), "both flowing")

	// The full layer pauses while the small one flows.
	s.markCameraPacket(SimulcastLevelLow, at(2*time.Second))
	require.False(t, s.updateCameraHighStalled(at(2*time.Second-time.Millisecond)), "not for long yet")
	require.True(t, s.updateCameraHighStalled(at(2*time.Second)))
	require.True(t, s.cameraHighStalled.Load())

	// The camera turns off: neither flows, nothing changes.
	require.False(t, s.updateCameraHighStalled(at(time.Minute)))
	require.True(t, s.cameraHighStalled.Load())

	// The full layer resumes.
	s.markCameraPacket(SimulcastLevelHigh, at(time.Minute))
	require.True(t, s.updateCameraHighStalled(at(time.Minute+time.Second/2)))
	require.False(t, s.cameraHighStalled.Load())

	// Off with both layers stopping together: not a pause.
	require.False(t, s.updateCameraHighStalled(at(2*time.Minute)))
	require.False(t, s.cameraHighStalled.Load())
}

func TestCameraChange(t *testing.T) {
	low := cameraTrack(t, "s", SimulcastLevelLow)
	high := cameraTrack(t, "s", SimulcastLevelHigh)
	otherHigh := cameraTrack(t, "s", SimulcastLevelHigh)

	for name, tc := range map[string]struct {
		have        webrtc.TrackLocal
		serve       *webrtc.TrackLocalStaticRTP
		remove, add bool
	}{
		"newly wanted":        {nil, high, false, true},
		"no longer wanted":    {high, nil, true, false},
		"to the small layer":  {high, low, true, true},
		"to the full layer":   {low, high, true, true},
		"same layer":          {high, otherHigh, false, false},
		"nothing either side": {nil, nil, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			remove, add := cameraChange(tc.have, tc.serve)
			require.Equal(t, tc.remove, remove, "remove")
			require.Equal(t, tc.add, add, "add")
		})
	}
}

func TestSyncCamera(t *testing.T) {
	teacher := sender(t, "teacher", SimulcastLevelLow, SimulcastLevelHigh)
	receiver := &session{
		cfg:      SessionConfig{SessionID: "student"},
		tracksCh: make(chan trackActionContext, 10),
		videoIn:  map[string]webrtc.TrackLocal{},
	}
	actions := func() []string {
		var out []string
		for len(receiver.tracksCh) > 0 {
			a := <-receiver.tracksCh
			out = append(out, map[trackAction]string{trackActionAdd: "add ", trackActionRemove: "remove "}[a.action]+a.localTrack.RID())
		}
		return out
	}
	require.NoError(t, receiver.syncCamera(teacher))
	require.Equal(t, []string{"add h"}, actions())

	receiver.wantedVideo = map[string]bool{"teacher": true}
	receiver.lowVideo = map[string]bool{"teacher": true}
	require.NoError(t, receiver.syncCamera(teacher))
	require.Equal(t, []string{"remove h", "add l"}, actions())
	require.Equal(t, SimulcastLevelLow, receiver.videoIn["teacher"].RID())

	require.NoError(t, receiver.syncCamera(teacher))
	require.Empty(t, actions(), "already right")

	receiver.wantedVideo = map[string]bool{}
	require.NoError(t, receiver.syncCamera(teacher))
	require.Equal(t, []string{"remove l"}, actions())
	require.Empty(t, receiver.videoIn)
}

func TestWantsVideoFrom(t *testing.T) {
	s := &session{}
	require.True(t, s.wantsVideoFrom("anyone"), "receivers that never said get every camera")
	s.wantedVideo = map[string]bool{"teacher": true}
	require.True(t, s.wantsVideoFrom("teacher"))
	require.False(t, s.wantsVideoFrom("student"))
	s.wantedVideo = map[string]bool{}
	require.False(t, s.wantsVideoFrom("teacher"), "an empty set means no camera at all")
}
