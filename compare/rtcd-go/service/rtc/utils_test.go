package rtc

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateAddrsPairs(t *testing.T) {
	t.Run("nil/empty inputs", func(t *testing.T) {
		pairs, err := generateAddrsPairs(nil, nil, "", false)
		require.NoError(t, err)
		require.Empty(t, pairs)

		pairs, err = generateAddrsPairs([]netip.Addr{}, map[netip.Addr]string{}, "", false)
		require.NoError(t, err)
		require.Empty(t, pairs)
	})

	t.Run("no public addresses", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "",
			netip.MustParseAddr("10.1.1.1"):  "",
		}, "", false)
		require.NoError(t, err)
		require.Equal(t, []string{"127.0.0.1/127.0.0.1", "10.1.1.1/10.1.1.1"}, pairs)
	})

	t.Run("full NAT mapping", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{}, "1.1.1.1/127.0.0.1,1.1.1.1/10.1.1.1", false)
		require.NoError(t, err)
		require.Equal(t, []string{"1.1.1.1/127.0.0.1", "1.1.1.1/10.1.1.1"}, pairs)
	})

	t.Run("no public addresses with override", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "",
			netip.MustParseAddr("10.1.1.1"):  "",
		}, "1.1.1.1", false)
		require.NoError(t, err)
		require.Equal(t, []string{"127.0.0.1/127.0.0.1", "1.1.1.1/10.1.1.1"}, pairs)
	})

	t.Run("single public address for multiple local addrs, no override", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "",
			netip.MustParseAddr("10.1.1.1"):  "1.1.1.1",
		}, "", false)
		require.NoError(t, err)
		require.Equal(t, []string{"127.0.0.1/127.0.0.1", "1.1.1.1/10.1.1.1"}, pairs)
	})

	t.Run("single local/public address map, no override", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "",
			netip.MustParseAddr("10.1.1.1"):  "1.1.1.1",
		}, "", false)
		require.NoError(t, err)
		require.Equal(t, []string{"127.0.0.1/127.0.0.1", "1.1.1.1/10.1.1.1"}, pairs)
	})

	t.Run("multiple public addresses, no override", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "1.1.1.1",
			netip.MustParseAddr("10.1.1.1"):  "1.1.1.2",
		}, "", false)
		require.NoError(t, err)
		require.Equal(t, []string{"1.1.1.1/127.0.0.1", "1.1.1.2/10.1.1.1"}, pairs)
	})

	// This is not a case that would happen in the application because the
	// override would prevent us from finding public IPs.
	t.Run("multiple public addresses, with override", func(t *testing.T) {
		pairs, err := generateAddrsPairs([]netip.Addr{
			netip.MustParseAddr("127.0.0.1"),
			netip.MustParseAddr("10.1.1.1"),
		}, map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "1.1.1.1",
			netip.MustParseAddr("10.1.1.1"):  "1.1.1.2",
		}, "8.8.8.8", false)
		require.NoError(t, err)
		require.Equal(t, []string{"127.0.0.1/127.0.0.1", "8.8.8.8/10.1.1.1"}, pairs)
	})
}

func TestIsValidTrackID(t *testing.T) {
	tcs := []struct {
		name   string
		input  string
		result bool
	}{
		{
			name:   "empty",
			input:  "",
			result: false,
		},
		{
			name:   "not enough fields",
			input:  "screen_id",
			result: false,
		},
		{
			name:   "too many fields",
			input:  "screen_id_id_id",
			result: false,
		},
		{
			name:   "invalid track type",
			input:  "ai_id_id",
			result: false,
		},
		{
			name:   "valid screen",
			input:  "screen_id_id",
			result: true,
		},
		{
			name:   "valid voice",
			input:  "voice_id_id",
			result: true,
		},
		{
			name:   "valid video",
			input:  "video_id_id",
			result: true,
		},
		{
			name:   "valid screen audio",
			input:  "screen-audio_id_id",
			result: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.result, isValidTrackID(tc.input))
		})
	}
}

func TestGetExternalAddrMapFromHostOverride(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := getExternalAddrMapFromHostOverride("", nil)
		require.Empty(t, m)
	})

	t.Run("single host", func(t *testing.T) {
		m := getExternalAddrMapFromHostOverride("10.0.0.1", nil)
		require.Equal(t, map[string]bool{
			"10.0.0.1": true,
		}, m)
	})

	t.Run("mapping", func(t *testing.T) {
		m := getExternalAddrMapFromHostOverride("10.0.0.1/127.0.0.1,10.0.0.3/127.0.0.2,10.0.0.2/127.0.0.3", nil)
		require.Equal(t, map[string]bool{
			"10.0.0.1": true,
			"10.0.0.2": true,
			"10.0.0.3": true,
		}, m)
	})

	t.Run("mixed mapping", func(t *testing.T) {
		m := getExternalAddrMapFromHostOverride("10.0.0.1/127.0.0.1,127.0.0.2/127.0.0.2,10.0.0.2/127.0.0.3", nil)
		require.Equal(t, map[string]bool{
			"10.0.0.1": true,
			"10.0.0.2": true,
		}, m)
	})

	t.Run("public address map", func(t *testing.T) {
		m := getExternalAddrMapFromHostOverride("", map[netip.Addr]string{
			netip.MustParseAddr("127.0.0.1"): "10.0.0.1",
		})
		require.Equal(t, map[string]bool{
			"10.0.0.1": true,
		}, m)
	})
}

func TestWithOpusDTX(t *testing.T) {
	sdp := strings.Join([]string{
		"v=0",
		"m=audio 9 UDP/TLS/RTP/SAVPF 111 63",
		"a=rtpmap:111 opus/48000/2",
		"a=fmtp:111 minptime=10;useinbandfec=1",
		"a=rtpmap:63 red/48000/2",
		"a=fmtp:63 111/111",
		"m=audio 9 UDP/TLS/RTP/SAVPF 109",
		"a=rtpmap:109 OPUS/48000/2",
		"a=fmtp:109 minptime=10;usedtx=0",
		"m=video 9 UDP/TLS/RTP/SAVPF 96",
		"a=rtpmap:96 VP8/90000",
		"",
	}, "\r\n")

	got := strings.Split(withOpusDTX(sdp), "\r\n")
	require.Equal(t, "a=fmtp:111 minptime=10;useinbandfec=1;usedtx=1", got[3])
	require.Equal(t, "a=fmtp:63 111/111", got[5], "not Opus")
	require.Equal(t, "a=fmtp:109 minptime=10;usedtx=0", got[8], "an explicit choice is kept")
	require.Len(t, got, 12)
	require.Equal(t, withOpusDTX(sdp), withOpusDTX(withOpusDTX(sdp)))
}
