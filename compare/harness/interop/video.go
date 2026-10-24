package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattermost/rtcd/service/rtc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// videoSender sends a single-layer VP8 stream, as the product's own load tester does: pion's
// simulcast sender describes itself with both SSRCs and rids, unlike a browser, and not even the
// Go rtcd probes it. Simulcast is covered by the Rust tests with browser-style SDP and by real
// Chrome. Every payload ends with a marker so a receiver can tell the stream's packets.
type videoSender struct {
	streamID string
	trx      *webrtc.RTPTransceiver
	track    *webrtc.TrackLocalStaticRTP
	mu       sync.Mutex
	seq      uint16
	ts       uint32
	pid      uint8
	// keyframe is set when the SFU asked for one; the next frame is then a keyframe, as an encoder
	// does.
	keyframe atomic.Bool
}

func (p *peer) addVideo(kind string) (*videoSender, error) {
	v := &videoSender{streamID: kind + "-stream-" + p.name}
	t, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, kind+"-"+p.name, v.streamID)
	if err != nil {
		return nil, err
	}
	v.track = t
	trx, err := p.pc.AddTransceiverFromTrack(t, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	if err != nil {
		return nil, err
	}
	v.trx = trx
	sender := trx.Sender() // removing the track clears it from the transceiver
	go func() {
		for {
			pkts, _, err := sender.ReadRTCP()
			if err != nil {
				return
			}
			for _, pkt := range pkts {
				switch pkt.(type) {
				case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
					v.keyframe.Store(true)
				}
			}
		}
	}()
	return v, nil
}

func (v *videoSender) frame(key bool, packets int) {
	if v.keyframe.Swap(false) {
		key = true
	}
	v.mu.Lock()
	v.pid++
	v.ts += 3000
	pid, ts := v.pid&0x7f, v.ts
	v.mu.Unlock()
	for n := 0; n < packets; n++ {
		desc := byte(0x80)
		if n == 0 {
			desc |= 0x10
		}
		payload := []byte{desc, 0x80, pid}
		if n == 0 {
			tag := byte(1)
			if key {
				tag = 0
			}
			payload = append(payload, tag)
		}
		payload = append(payload, 1, 0xaa, 0xbb, 0xcc)
		v.mu.Lock()
		seq := v.seq
		v.seq++
		v.mu.Unlock()
		_ = v.track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: seq, Timestamp: ts, Marker: n == packets-1}, Payload: payload})
	}
}

// stream sends frames at 30 per second for d, with a keyframe every keyEvery frames.
func (v *videoSender) stream(d time.Duration, keyEvery int) {
	end := time.Now().Add(d)
	for f := 0; time.Now().Before(end); f++ {
		v.frame(f%keyEvery == 0, 2)
		time.Sleep(33 * time.Millisecond)
	}
}

func (p *peer) announce(t rtc.MessageType, key, streamID string) {
	data, _ := json.Marshal(map[string]string{key: streamID})
	p.sig.send(p.session, t, data)
}

func (p *peer) subscribe(ids, low []string) {
	data, _ := json.Marshal(map[string]any{"session_ids": ids, "low": low})
	p.sig.send(p.session, rtc.VideoSubscribeMessage, data)
}

func (p *peer) describeVideo(prefix string) string {
	st := p.track(prefix)
	if st == nil {
		return prefix + ": none"
	}
	return fmt.Sprintf("%s: %d packets, gaps=%d", prefix, st.packets.Load(), st.gaps.Load())
}
