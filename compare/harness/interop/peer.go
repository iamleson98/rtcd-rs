package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattermost/rtcd/service"
	"github.com/mattermost/rtcd/service/rtc"
	"github.com/mattermost/rtcd/service/rtc/dc"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// signaler carries a participant's messages to rtcd, as the Mattermost server does.
type signaler struct {
	cl     *service.Client
	callID string
}

func (s *signaler) send(session string, t rtc.MessageType, data []byte) {
	_ = s.cl.Send(service.ClientMessage{Type: service.ClientMessageRTC, Data: rtc.Message{
		GroupID: "default", UserID: "user-" + session, SessionID: session, CallID: s.callID, Type: t, Data: data,
	}})
}

// peer is one participant: a pion peer connection that behaves like the lms-web call client.
type peer struct {
	name, session string
	sig           *signaler
	pc            *webrtc.PeerConnection
	dcCh          *webrtc.DataChannel

	mu         sync.Mutex
	lockedCh   chan bool
	dcAnswered atomic.Bool
	remoteSet  bool
	iceQueue   []webrtc.ICECandidateInit
	offers     atomic.Int64 // offers received from the SFU
	negotiate  chan struct{}

	audio      *webrtc.TrackLocalStaticRTP
	audioSend  *webrtc.RTPSender
	audioSSRC  uint32
	seq        uint16
	closed     atomic.Bool
	voiceEvent []bool
	voiceMsgs  []rtc.Message // the envelopes of those events

	recvMu sync.Mutex
	recv   map[string]*trackStats // by track id
}

type trackStats struct {
	streamID string
	ssrc     uint32
	rid      string
	packets  atomic.Int64
	lastSeq  uint16
	gaps     atomic.Int64
	keyframe atomic.Int64
	layers   [3]atomic.Int64 // payload marker 1 = low, 2 = high
}

func newPeer(name, callID string, cl *service.Client) (*peer, error) {
	p := &peer{name: name, session: "session-" + name, sig: &signaler{cl: cl, callID: callID}, lockedCh: make(chan bool, 8), recv: map[string]*trackStats{}, negotiate: make(chan struct{}, 1)}

	var m webrtc.MediaEngine
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	for _, ext := range []string{"urn:ietf:params:rtp-hdrext:sdes:mid", "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id", "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id"} {
		if err := m.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: ext}, webrtc.RTPCodecTypeVideo); err != nil {
			return nil, err
		}
	}
	if err := m.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: "urn:ietf:params:rtp-hdrext:ssrc-audio-level"}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(&m, i); err != nil {
		return nil, err
	}
	var s webrtc.SettingEngine
	s.EnableSCTPZeroChecksum(true)
	s.SetIncludeLoopbackCandidate(true)
	s.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})
	api := webrtc.NewAPI(webrtc.WithMediaEngine(&m), webrtc.WithInterceptorRegistry(i), webrtc.WithSettingEngine(s))
	pc, err := api.NewPeerConnection(webrtc.Configuration{SDPSemantics: webrtc.SDPSemanticsUnifiedPlan})
	if err != nil {
		return nil, err
	}
	p.pc = pc

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		data, _ := json.Marshal(c.ToJSON())
		p.sig.send(p.session, rtc.ICEMessage, data)
	})
	pc.OnTrack(p.onTrack)
	pc.OnNegotiationNeeded(func() {
		select {
		case p.negotiate <- struct{}{}:
		default:
		}
	})

	p.audio, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, "mic-"+name, "stream-"+name)
	if err != nil {
		return nil, err
	}
	p.audioSend, err = pc.AddTrack(p.audio)
	if err != nil {
		return nil, err
	}
	go func() { // keep the sender's RTCP drained
		buf := make([]byte, 1500)
		for {
			if _, _, err := p.audioSend.Read(buf); err != nil {
				return
			}
		}
	}()
	p.audioSSRC = uint32(p.audioSend.GetParameters().Encodings[0].SSRC)

	p.dcCh, err = pc.CreateDataChannel("calls-dc", nil)
	if err != nil {
		return nil, err
	}
	p.dcCh.OnMessage(p.onDC)
	go p.negotiator()
	return p, nil
}

func (p *peer) onTrack(tr *webrtc.TrackRemote, recv *webrtc.RTPReceiver) {
	st := &trackStats{streamID: tr.StreamID(), ssrc: uint32(tr.SSRC()), rid: tr.RID()}
	p.recvMu.Lock()
	p.recv[tr.ID()] = st
	p.recvMu.Unlock()
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := recv.Read(buf); err != nil {
				return
			}
		}
	}()
	first := true
	for {
		pkt, _, err := tr.ReadRTP()
		if err != nil {
			return
		}
		if !first && pkt.SequenceNumber != st.lastSeq+1 {
			st.gaps.Add(1)
		}
		first = false
		st.lastSeq = pkt.SequenceNumber
		st.packets.Add(1)
		if n := len(pkt.Payload); n >= 4 && tr.Kind() == webrtc.RTPCodecTypeVideo {
			if m := pkt.Payload[n-4]; m >= 1 && m <= 2 {
				st.layers[m].Add(1)
			}
		}
	}
}

func (p *peer) received(prefix string) int64 {
	p.recvMu.Lock()
	defer p.recvMu.Unlock()
	var n int64
	for id, st := range p.recv {
		if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
			n += st.packets.Load()
		}
	}
	return n
}

func (p *peer) track(prefix string) *trackStats {
	p.recvMu.Lock()
	defer p.recvMu.Unlock()
	for id, st := range p.recv {
		if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
			return st
		}
	}
	return nil
}

// join starts the session at rtcd and sends the first offer through the service.
func (p *peer) join(cl *service.Client, channel string) error {
	if err := cl.Send(service.ClientMessage{Type: service.ClientMessageJoin, Data: map[string]any{
		"callID": p.sig.callID, "userID": "user-" + p.session, "sessionID": p.session, "channelID": channel, "av1Support": false, "dcSignaling": true,
	}}); err != nil {
		return err
	}
	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	data, _ := json.Marshal(offer)
	p.sig.send(p.session, rtc.SDPMessage, data)
	return nil
}

// onSignal handles a message rtcd sent for this session.
func (p *peer) onSignal(m rtc.Message) {
	switch m.Type {
	case rtc.ICEMessage:
		var wrapper struct {
			Candidate webrtc.ICECandidateInit `json:"candidate"`
		}
		if json.Unmarshal(m.Data, &wrapper) != nil || wrapper.Candidate.Candidate == "" {
			return
		}
		p.mu.Lock()
		if !p.remoteSet {
			p.iceQueue = append(p.iceQueue, wrapper.Candidate)
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		_ = p.pc.AddICECandidate(wrapper.Candidate)
	case rtc.SDPMessage:
		var d webrtc.SessionDescription
		if json.Unmarshal(m.Data, &d) == nil {
			p.onDescription(d, false)
		}
	case rtc.VoiceOnMessage:
		p.mu.Lock()
		p.voiceEvent = append(p.voiceEvent, true)
		p.voiceMsgs = append(p.voiceMsgs, m)
		p.mu.Unlock()
	case rtc.VoiceOffMessage:
		p.mu.Lock()
		p.voiceEvent = append(p.voiceEvent, false)
		p.voiceMsgs = append(p.voiceMsgs, m)
		p.mu.Unlock()
	}
}

func (p *peer) onDescription(d webrtc.SessionDescription, overDC bool) {
	if err := p.pc.SetRemoteDescription(d); err != nil {
		fmt.Printf("  [%s] SetRemoteDescription(%s): %v\n", p.name, d.Type, err)
		return
	}
	p.mu.Lock()
	p.remoteSet = true
	queued := p.iceQueue
	p.iceQueue = nil
	p.mu.Unlock()
	for _, c := range queued {
		_ = p.pc.AddICECandidate(c)
	}
	switch d.Type {
	case webrtc.SDPTypeAnswer:
		if p.dcAnswered.Swap(true) {
			p.unlock()
		}
	case webrtc.SDPTypeOffer:
		p.offers.Add(1)
		answer, err := p.pc.CreateAnswer(nil)
		if err != nil {
			return
		}
		if err := p.pc.SetLocalDescription(answer); err != nil {
			return
		}
		data, _ := json.Marshal(answer)
		if p.dcCh.ReadyState() == webrtc.DataChannelStateOpen {
			p.sendDC(dc.MessageTypeSDP, data)
		} else {
			p.sig.send(p.session, rtc.SDPMessage, data)
		}
	}
}

func (p *peer) sendDC(t dc.MessageType, payload any) {
	msg, err := dc.EncodeMessage(t, payload)
	if err != nil {
		return
	}
	_ = p.dcCh.Send(msg)
}

func (p *peer) onDC(msg webrtc.DataChannelMessage) {
	mt, payload, err := dc.DecodeMessage(msg.Data)
	if err != nil {
		return
	}
	switch mt {
	case dc.MessageTypeSDP:
		var d webrtc.SessionDescription
		if json.Unmarshal(payload.([]byte), &d) == nil {
			p.onDescription(d, true)
		}
	case dc.MessageTypeLock:
		if b, ok := payload.(bool); ok {
			select {
			case p.lockedCh <- b:
			default:
			}
		}
	}
}

func (p *peer) unlock() { p.sendDC(dc.MessageTypeUnlock, nil) }

// negotiator offers when the connection needs it, holding rtcd's signaling lock meanwhile.
func (p *peer) negotiator() {
	for range p.negotiate {
		if p.pc.SignalingState() != webrtc.SignalingStateStable || p.pc.CurrentRemoteDescription() == nil {
			continue
		}
		if p.dcCh.ReadyState() != webrtc.DataChannelStateOpen {
			continue
		}
		got := false
		for i := 0; i < 50 && !got; i++ {
			p.sendDC(dc.MessageTypeLock, nil)
			select {
			case got = <-p.lockedCh:
			case <-time.After(3 * time.Second):
			}
			if !got {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if !got {
			continue
		}
		offer, err := p.pc.CreateOffer(nil)
		if err != nil {
			p.unlock()
			continue
		}
		if err := p.pc.SetLocalDescription(offer); err != nil {
			p.unlock()
			continue
		}
		data, _ := json.Marshal(offer)
		p.sendDC(dc.MessageTypeSDP, data)
	}
}

func (p *peer) ready() bool {
	return p.pc.ConnectionState() == webrtc.PeerConnectionStateConnected && p.dcCh.ReadyState() == webrtc.DataChannelStateOpen && p.dcAnswered.Load()
}

// speak sends frames of 20 ms audio; alternating levels read as speech.
func (p *peer) speak(frames int) {
	ext := 0
	for _, e := range p.audioSend.GetParameters().HeaderExtensions {
		if e.URI == "urn:ietf:params:rtp-hdrext:ssrc-audio-level" {
			ext = e.ID
		}
	}
	for f := 0; f < frames; f++ {
		level := byte(20)
		if f%2 == 1 {
			level = 70
		}
		pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: p.seq, Timestamp: uint32(p.seq) * 960, SSRC: p.audioSSRC}, Payload: []byte{0xfc, 1, 2, 3, 4}}
		if ext > 0 {
			_ = pkt.Header.SetExtension(uint8(ext), []byte{level})
		}
		p.seq++
		_ = p.audio.WriteRTP(pkt)
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *peer) close() {
	p.closed.Store(true)
	_ = p.pc.Close()
}

var _ = bytes.NewReader
var _ = zlib.NewReader

func audioLevelExt(p *peer) int {
	for _, e := range p.audioSend.GetParameters().HeaderExtensions {
		if e.URI == "urn:ietf:params:rtp-hdrext:ssrc-audio-level" {
			return e.ID
		}
	}
	return 0
}

// sendVoice sends one 20 ms frame; alternating levels read as speech.
func (p *peer) sendVoice(ext, frame int) {
	level := byte(20)
	if frame%2 == 1 {
		level = 70
	}
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: p.seq, Timestamp: uint32(p.seq) * 960, SSRC: p.audioSSRC}, Payload: []byte{0xfc, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39}}
	if ext > 0 {
		_ = pkt.Header.SetExtension(uint8(ext), []byte{level})
	}
	p.seq++
	_ = p.audio.WriteRTP(pkt)
}

func (p *peer) resetCounters() {
	p.recvMu.Lock()
	defer p.recvMu.Unlock()
	for _, st := range p.recv {
		st.packets.Store(0)
		st.gaps.Store(0)
	}
}
