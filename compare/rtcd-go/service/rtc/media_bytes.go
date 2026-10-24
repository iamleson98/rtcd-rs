package rtc

import (
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

// mediaBytes counts the RTP bytes (header and payload, not the SRTP tag or
// UDP/IP headers) that leave for one peer connection, by track type. Every
// receiver of a track gets its own copy, so these are the bytes the node
// sends, the figure a traffic quota is spent on.
//
// A sender's SSRC is known as soon as its track is added, but the interceptor
// only sees an opaque stream id, so the session registers each SSRC with its
// track type (expect) and the interceptor looks it up when the stream binds.
type mediaBytes struct {
	metrics Metrics
	types   sync.Map // uint32 SSRC -> trackType
}

func newMediaBytes(m Metrics) *mediaBytes {
	return &mediaBytes{metrics: m}
}

func (b *mediaBytes) expect(ssrc uint32, tt trackType) {
	b.types.Store(ssrc, tt)
}

// NewInterceptor implements interceptor.Factory.
func (b *mediaBytes) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &mediaBytesInterceptor{bytes: b}, nil
}

type mediaBytesInterceptor struct {
	interceptor.NoOp
	bytes *mediaBytes
}

func (i *mediaBytesInterceptor) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	v, ok := i.bytes.types.Load(info.SSRC)
	if !ok {
		return writer
	}
	tt := string(v.(trackType))

	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
		n, err := writer.Write(header, payload, attributes)
		if err == nil {
			i.bytes.metrics.AddRTPBytesSent(tt, header.MarshalSize()+len(payload))
		}
		return n, err
	})
}

func (i *mediaBytesInterceptor) UnbindLocalStream(info *interceptor.StreamInfo) {
	i.bytes.types.Delete(info.SSRC)
}
