// Command golden writes wire vectors produced by the original Go rtcd, for the Rust port to decode
// (and, where the format is deterministic, to reproduce byte for byte).
//
// It is built by gen.sh against the Go sources named by RTCD_GO_SRC and writes into the directory
// given as its only argument. Every file is JSON: {"vectors": [{"name": ..., "hex": ..., ...}]}.
//
// msgpack map iteration order is random in Go, so vectors holding a map with more than one key are
// written several times: a decoder has to accept every order.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mattermost/rtcd/service"
	"github.com/mattermost/rtcd/service/rtc"
	"github.com/mattermost/rtcd/service/rtc/dc"
	"github.com/pion/webrtc/v4"
	"golang.org/x/crypto/bcrypt"
)

type vector map[string]any

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: golden <out dir>")
		os.Exit(2)
	}
	out := os.Args[1]
	write(out, "client_messages.json", clientMessages())
	write(out, "dc_messages.json", dcMessages())
	write(out, "session_config.json", sessionConfigs())
	write(out, "bcrypt.json", bcryptVectors())
	write(out, "turn.json", turnVectors())
}

func write(dir, name string, vectors []vector) {
	data, err := json.MarshalIndent(map[string]any{"vectors": vectors}, "", "  ")
	must(err)
	must(os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0o644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func packed(cm service.ClientMessage) string {
	data, err := cm.Pack()
	must(err)
	return hex.EncodeToString(data)
}

// clientMessages covers every message the Mattermost side and the SFU exchange over the websocket.
func clientMessages() []vector {
	var vs []vector
	add := func(name string, cm service.ClientMessage, expect any) {
		vs = append(vs, vector{"name": name, "hex": packed(cm), "type": cm.Type, "data": expect})
	}

	// Maps with several keys: repeat to catch every iteration order.
	for i := 0; i < 8; i++ {
		add(fmt.Sprintf("hello/%d", i),
			service.ClientMessage{Type: service.ClientMessageHello, Data: map[string]string{"clientID": "client-a", "connID": "connection-id-0123456789ab"}},
			map[string]string{"clientID": "client-a", "connID": "connection-id-0123456789ab"})
		add(fmt.Sprintf("join/%d", i),
			service.ClientMessage{Type: service.ClientMessageJoin, Data: map[string]any{
				"callID": "call1", "userID": "user1", "sessionID": "session1", "channelID": "channel1",
				"av1Support": false, "dcSignaling": true,
			}},
			map[string]any{"callID": "call1", "userID": "user1", "sessionID": "session1", "channelID": "channel1", "av1Support": false, "dcSignaling": true})
	}
	add("join/av1",
		service.ClientMessage{Type: service.ClientMessageJoin, Data: map[string]any{
			"callID": "c", "userID": "u", "sessionID": "s", "channelID": "ch", "av1Support": true, "dcSignaling": false,
		}},
		map[string]any{"callID": "c", "userID": "u", "sessionID": "s", "channelID": "ch", "av1Support": true, "dcSignaling": false})
	add("leave", service.ClientMessage{Type: service.ClientMessageLeave, Data: map[string]string{"sessionID": "session1"}},
		map[string]string{"sessionID": "session1"})
	add("reconnect", service.ClientMessage{Type: service.ClientMessageReconnect, Data: map[string]string{"sessionID": "session1"}},
		map[string]string{"sessionID": "session1"})
	add("close", service.ClientMessage{Type: service.ClientMessageClose, Data: map[string]string{"sessionID": "session1"}},
		map[string]string{"sessionID": "session1"})

	types := []struct {
		name string
		t    rtc.MessageType
	}{
		{"ice", rtc.ICEMessage}, {"sdp", rtc.SDPMessage}, {"mute", rtc.MuteMessage}, {"unmute", rtc.UnmuteMessage},
		{"screen_on", rtc.ScreenOnMessage}, {"screen_off", rtc.ScreenOffMessage}, {"voice_on", rtc.VoiceOnMessage},
		{"voice_off", rtc.VoiceOffMessage}, {"video_on", rtc.VideoOnMessage}, {"video_off", rtc.VideoOffMessage},
		{"video_subscribe", rtc.VideoSubscribeMessage},
	}
	for _, tc := range types {
		m := rtc.Message{GroupID: "default", UserID: "user1", SessionID: "session1", CallID: "call1", Type: tc.t, Data: []byte(`{"k":"v"}`)}
		add("rtc/"+tc.name, service.ClientMessage{Type: service.ClientMessageRTC, Data: m}, messageJSON(m))
	}
	empty := rtc.Message{GroupID: "default", UserID: "user1", SessionID: "session1", CallID: "call1", Type: rtc.MuteMessage}
	add("rtc/no_data", service.ClientMessage{Type: service.ClientMessageRTC, Data: empty}, messageJSON(empty))
	vad := rtc.Message{GroupID: "g", UserID: "u", SessionID: "s", CallID: "c", Type: rtc.VoiceOnMessage}
	add("vad/on", service.ClientMessage{Type: service.ClientMessageVAD, Data: vad}, messageJSON(vad))
	big := make([]byte, 70000)
	for i := range big {
		big[i] = byte(i * 7)
	}
	large := rtc.Message{GroupID: "g", UserID: "u", SessionID: "s", CallID: "c", Type: rtc.SDPMessage, Data: big}
	v := vector{"name": "rtc/large_sdp", "hex": packed(service.ClientMessage{Type: service.ClientMessageRTC, Data: large}), "type": "rtc"}
	v["data"] = messageJSON(large)
	vs = append(vs, v)

	// The ICE payload the SFU sends (rtc.newICEMessage): JSON of {"type":"candidate","candidate": ICECandidateInit}.
	sdpMid, sdpIdx, ufrag := "0", uint16(0), "abcd"
	cand := webrtc.ICECandidateInit{Candidate: "candidate:1 1 udp 2130706431 10.0.0.1 8443 typ host", SDPMid: &sdpMid, SDPMLineIndex: &sdpIdx, UsernameFragment: &ufrag}
	ice, err := json.Marshal(map[string]any{"type": "candidate", "candidate": cand})
	must(err)
	vs = append(vs, vector{"name": "ice_payload", "json": string(ice)})
	return vs
}

func messageJSON(m rtc.Message) map[string]any {
	return map[string]any{
		"group_id": m.GroupID, "user_id": m.UserID, "session_id": m.SessionID, "call_id": m.CallID,
		"type": int(m.Type), "data_hex": hex.EncodeToString(m.Data),
	}
}

// dcMessages covers the data channel framing: a msgpack type byte, then an optional payload.
func dcMessages() []vector {
	var vs []vector
	add := func(name string, mt dc.MessageType, payload any, expect any) {
		data, err := dc.EncodeMessage(mt, payload)
		must(err)
		vs = append(vs, vector{"name": name, "type": int(mt), "hex": hex.EncodeToString(data), "payload": expect})
	}
	add("ping", dc.MessageTypePing, nil, nil)
	add("pong", dc.MessageTypePong, nil, nil)
	add("lock_request", dc.MessageTypeLock, nil, nil)
	add("lock_true", dc.MessageTypeLock, true, true)
	add("lock_false", dc.MessageTypeLock, false, false)
	add("unlock", dc.MessageTypeUnlock, nil, nil)
	add("loss_rate", dc.MessageTypeLossRate, 0.0125, 0.0125)
	add("rtt", dc.MessageTypeRoundTripTime, 0.042, 0.042)
	add("jitter", dc.MessageTypeJitter, 0.003, 0.003)
	add("media_map", dc.MessageTypeMediaMap, dc.MediaMap{"0": {Type: "voice", SenderID: "s1"}}, map[string]any{"0": map[string]string{"type": "voice", "sender_id": "s1"}})

	// SDP payloads are zlib compressed; zlib output is deterministic for one Go version, but the Rust
	// side only has to inflate it, so the vector also records the plain text.
	plain := `{"type":"offer","sdp":"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\n"}`
	data, err := dc.EncodeMessage(dc.MessageTypeSDP, []byte(plain))
	must(err)
	vs = append(vs, vector{"name": "sdp", "type": int(dc.MessageTypeSDP), "hex": hex.EncodeToString(data), "payload": plain})
	return vs
}

// sessionConfigs are the JSON bodies of GET /calls/{id}/sessions[/{id}].
func sessionConfigs() []vector {
	cfg := rtc.SessionConfig{GroupID: "g", CallID: "c", UserID: "u", SessionID: "s", Props: rtc.SessionProps{"channelID": "ch", "av1Support": false, "dcSignaling": true}}
	one, err := json.Marshal(cfg)
	must(err)
	many, err := json.Marshal([]rtc.SessionConfig{cfg, {GroupID: "g", CallID: "c", UserID: "u2", SessionID: "s2"}})
	must(err)
	return []vector{
		{"name": "one", "json": string(one)},
		{"name": "many", "json": string(many)},
	}
}

func bcryptVectors() []vector {
	var vs []vector
	for _, key := range []string{"0123456789abcdef0123456789abcdef", "a-much-longer-key-0123456789abcdef-0123456789abcdef"} {
		h, err := bcrypt.GenerateFromPassword([]byte(key), bcrypt.DefaultCost)
		must(err)
		vs = append(vs, vector{"name": key, "key": key, "hash": string(h)})
	}
	return vs
}

// turnVectors are short-lived TURN credentials as rtc.GenTURNConfigs makes them. The expiry is part
// of the username, so the vector records what Go produced and the Rust side recomputes the password.
func turnVectors() []vector {
	var vs []vector
	for _, secret := range []string{"static-secret", "another secret with spaces"} {
		cfgs, err := rtc.GenTURNConfigs(rtc.ICEServers{{URLs: []string{"turn:turn.example.com:3478"}}}, "session1", secret, 60)
		must(err)
		vs = append(vs, vector{"name": secret, "secret": secret, "username": cfgs[0].Username, "credential": cfgs[0].Credential})
	}
	return vs
}
