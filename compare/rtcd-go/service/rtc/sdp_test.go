package rtc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const firefoxOffer = "v=0\r\n" +
	"o=mozilla...THIS_IS_SDPARTA-99.0 1 0 IN IP4 0.0.0.0\r\n" +
	"s=-\r\n" +
	"t=0 0\r\n" +
	"a=group:BUNDLE 0 1 2\r\n" +
	"m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=mid:0\r\n" +
	"m=video 0 UDP/TLS/RTP/SAVPF 120\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=bundle-only\r\n" +
	"a=mid:1\r\n" +
	"m=application 0 UDP/DTLS/SCTP webrtc-datachannel\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=bundle-only\r\n" +
	"a=mid:2\r\n" +
	"a=sctp-port:5000\r\n"

func TestAcceptBundleOnlyData(t *testing.T) {
	t.Run("a bundle-only data channel section becomes a plain one", func(t *testing.T) {
		got := acceptBundleOnlyData(firefoxOffer)
		require.Equal(t, "v=0\r\n"+
			"o=mozilla...THIS_IS_SDPARTA-99.0 1 0 IN IP4 0.0.0.0\r\n"+
			"s=-\r\n"+
			"t=0 0\r\n"+
			"a=group:BUNDLE 0 1 2\r\n"+
			"m=audio 9 UDP/TLS/RTP/SAVPF 109\r\n"+
			"c=IN IP4 0.0.0.0\r\n"+
			"a=mid:0\r\n"+
			// A bundle-only media section is something pion already handles: it stays as it was.
			"m=video 0 UDP/TLS/RTP/SAVPF 120\r\n"+
			"c=IN IP4 0.0.0.0\r\n"+
			"a=bundle-only\r\n"+
			"a=mid:1\r\n"+
			"m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"+
			"c=IN IP4 0.0.0.0\r\n"+
			"a=mid:2\r\n"+
			"a=sctp-port:5000\r\n", got)
	})

	t.Run("a rejected data channel section stays rejected", func(t *testing.T) {
		offer := "v=0\r\nm=application 0 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:2\r\n"
		require.Equal(t, offer, acceptBundleOnlyData(offer))
	})

	t.Run("an offer without one is returned as it is", func(t *testing.T) {
		offer := "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:2\r\n"
		require.Equal(t, offer, acceptBundleOnlyData(offer))
		require.Equal(t, "", acceptBundleOnlyData(""))
	})

	t.Run("bundle-only on another section does not touch the data channel one", func(t *testing.T) {
		offer := "v=0\r\nm=video 0 UDP/TLS/RTP/SAVPF 120\r\na=bundle-only\r\n" +
			"m=application 0 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:2\r\n"
		require.Equal(t, offer, acceptBundleOnlyData(offer))
	})

	t.Run("bare line feeds work too", func(t *testing.T) {
		got := acceptBundleOnlyData("v=0\nm=application 0 UDP/DTLS/SCTP webrtc-datachannel\na=bundle-only\na=mid:2\n")
		require.Equal(t, "v=0\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\na=mid:2\n", got)
	})
}
