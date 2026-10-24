package rtc

import (
	"encoding/json"
	"fmt"

	"github.com/pion/webrtc/v4"
)

type MessageType int

const (
	ICEMessage MessageType = iota + 1
	SDPMessage
	MuteMessage
	UnmuteMessage
	ScreenOnMessage
	ScreenOffMessage
	VoiceOnMessage
	VoiceOffMessage
	VideoOnMessage
	VideoOffMessage
	// VideoSubscribeMessage tells which cameras a receiver wants
	// (videoSubscription); until it sends one, it gets all of them.
	VideoSubscribeMessage
)

type Message struct {
	GroupID   string      `msgpack:"group_id"`
	UserID    string      `msgpack:"user_id"`
	SessionID string      `msgpack:"session_id"`
	CallID    string      `msgpack:"call_id"`
	Type      MessageType `msgpack:"type"`
	Data      []byte      `msgpack:"data,omitempty"`
}

func (m *Message) IsValid() error {
	if m.SessionID == "" {
		return fmt.Errorf("invalid SessionID value: should not be empty")
	}
	if m.Type == 0 {
		return fmt.Errorf("invalid Type value")
	}

	return nil
}

func newMessage(s *session, msgType MessageType, data []byte) Message {
	return Message{
		GroupID:   s.cfg.GroupID,
		UserID:    s.cfg.UserID,
		SessionID: s.cfg.SessionID,
		CallID:    s.cfg.CallID,
		Type:      msgType,
		Data:      data,
	}
}

func newICEMessage(s *session, c *webrtc.ICECandidate) (Message, error) {
	data := make(map[string]interface{})
	data["type"] = "candidate"
	data["candidate"] = c.ToJSON()
	js, err := json.Marshal(data)
	if err != nil {
		return Message{}, err
	}
	return newMessage(s, ICEMessage, js), nil
}
