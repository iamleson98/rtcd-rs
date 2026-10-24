package rtc

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/mattermost/rtcd/service/perf"
)

func counterValue(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

func TestMediaBytes(t *testing.T) {
	metrics := perf.NewMetrics("rtcd", nil)
	mb := newMediaBytes(metrics)
	mb.expect(1111, trackTypeVideo)
	mb.expect(2222, trackTypeVoice)

	i, err := mb.NewInterceptor("")
	require.NoError(t, err)

	var written int
	base := interceptor.RTPWriterFunc(func(h *rtp.Header, p []byte, _ interceptor.Attributes) (int, error) {
		written++
		return h.MarshalSize() + len(p), nil
	})

	send := func(ssrc uint32, payload int) {
		w := i.BindLocalStream(&interceptor.StreamInfo{SSRC: ssrc}, base)
		_, err := w.Write(&rtp.Header{Version: 2, SSRC: ssrc}, make([]byte, payload), nil)
		require.NoError(t, err)
	}

	send(1111, 1000)
	send(1111, 500)
	send(2222, 80)
	send(3333, 999) // not registered: forwarded, not counted

	const header = 12
	require.Equal(t, 4, written)
	require.InDelta(t, 2*header+1500, counterValue(t, metrics.RTPBytesSent.WithLabelValues("video")), 0)
	require.InDelta(t, header+80, counterValue(t, metrics.RTPBytesSent.WithLabelValues("voice")), 0)
	require.InDelta(t, 0, counterValue(t, metrics.RTPBytesSent.WithLabelValues("screen")), 0)

	i.UnbindLocalStream(&interceptor.StreamInfo{SSRC: 1111})
	_, ok := mb.types.Load(uint32(1111))
	require.False(t, ok, "an unbound stream is forgotten")
}

// A sender's SSRC read right after AddTrack, before any negotiation, is the
// one its packets leave with: the registration the session relies on.
func TestMediaBytesOnPeerConnection(t *testing.T) {
	metrics := perf.NewMetrics("rtcd", nil)
	mb := newMediaBytes(metrics)

	var registry interceptor.Registry
	registry.Add(mb)

	var engine webrtc.MediaEngine
	require.NoError(t, engine.RegisterDefaultCodecs())
	sender, err := webrtc.NewAPI(
		webrtc.WithMediaEngine(&engine),
		webrtc.WithInterceptorRegistry(&registry),
	).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer sender.Close()

	receiver, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer receiver.Close()

	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video_s_1", "s")
	require.NoError(t, err)
	rtpSender, err := sender.AddTrack(track)
	require.NoError(t, err)
	mb.expect(uint32(rtpSender.GetParameters().Encodings[0].SSRC), trackTypeVideo)

	offer, err := sender.CreateOffer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(sender)
	require.NoError(t, sender.SetLocalDescription(offer))
	<-gathered
	require.NoError(t, receiver.SetRemoteDescription(*sender.LocalDescription()))
	answer, err := receiver.CreateAnswer(nil)
	require.NoError(t, err)
	gathered = webrtc.GatheringCompletePromise(receiver)
	require.NoError(t, receiver.SetLocalDescription(answer))
	<-gathered
	require.NoError(t, sender.SetRemoteDescription(*receiver.LocalDescription()))

	require.Eventually(t, func() bool {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96}, Payload: make([]byte, 100)})
		return counterValue(t, metrics.RTPBytesSent.WithLabelValues("video")) >= 112
	}, 10*time.Second, 20*time.Millisecond)
}
