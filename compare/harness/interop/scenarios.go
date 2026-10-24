package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mattermost/rtcd/service"
	"github.com/mattermost/rtcd/service/rtc"
)

func scenarioLookup(c *conn) error {
	v, err := c.cl.GetVersionInfo()
	if err != nil {
		return fmt.Errorf("version: %w", err)
	}
	check(v.BuildVersion != "" || v.GoOS != "", "GET /version decodes (%s %s/%s)", v.BuildVersion, v.GoOS, v.GoArch)
	_, code, _ := c.cl.GetSessions("no-such-call")
	check(code == 404, "an unknown call is 404 (got %d)", code)
	a, err := c.join("lookup-a", "call-lookup", nil)
	if err != nil {
		return err
	}
	defer a.close()
	cfg, code, err := c.cl.GetSession("call-lookup", a.session)
	check(err == nil && code == 200 && cfg.SessionID == a.session && cfg.CallID == "call-lookup", "GET session returns the session (code %d, %+v)", code, cfg)
	check(cfg.Props.DCSignaling() && cfg.Props.ChannelID() == "channel-call-lookup", "the session carries its properties (%v)", cfg.Props)
	cfgs, code, err := c.cl.GetSessions("call-lookup")
	check(err == nil && code == 200 && len(cfgs) == 1, "GET sessions lists the call (code %d, %d sessions)", code, len(cfgs))
	return nil
}

func scenarioVoice(c *conn) error {
	a, err := c.join("va", "call-voice", nil)
	if err != nil {
		return err
	}
	defer a.close()
	b, err := c.join("vb", "call-voice", nil)
	if err != nil {
		return err
	}
	defer b.close()

	var wg sync.WaitGroup
	for _, p := range []*peer{a, b} {
		wg.Add(1)
		go func() { defer wg.Done(); p.speak(150) }()
	}
	wg.Wait()
	heardB := waitFor(5*time.Second, func() bool { return b.received("voice_session-va") >= 100 })
	heardA := waitFor(5*time.Second, func() bool { return a.received("voice_session-vb") >= 100 })
	check(heardB, "b hears a: %d packets", b.received("voice_session-va"))
	check(heardA, "a hears b: %d packets", a.received("voice_session-vb"))
	check(a.received("voice_session-va") == 0, "nobody hears themselves")
	if st := b.track("voice_session-va"); st != nil {
		check(st.streamID == "session-va", "the stream id is the sender's session id (%q)", st.streamID)
		check(st.gaps.Load() == 0, "no sequence gaps on loopback (%d)", st.gaps.Load())
	}
	a.mu.Lock()
	ev := append([]bool(nil), a.voiceEvent...)
	msgs := append([]rtc.Message(nil), a.voiceMsgs...)
	a.mu.Unlock()
	check(len(ev) > 0 && ev[0], "voice activity was reported to the client: %v", ev)
	// The LMS server publishes these fields to the class (userID) and finds the call by session.
	if len(msgs) > 0 {
		m := msgs[0]
		check(m.UserID == "user-"+a.session && m.SessionID == a.session && m.CallID == a.sig.callID,
			"the voice event names the user, session and call (%q %q %q)", m.UserID, m.SessionID, m.CallID)
		check(m.GroupID != "", "the voice event carries a group id (%q)", m.GroupID)
		fmt.Printf("  INFO  voice event envelope: group=%q type=%d data=%q\n", m.GroupID, m.Type, m.Data)
	}

	// Muting: the SFU stops forwarding.
	a.sig.send(a.session, rtc.MuteMessage, nil)
	time.Sleep(200 * time.Millisecond)
	before := b.received("voice_session-va")
	a.speak(50)
	time.Sleep(200 * time.Millisecond)
	check(b.received("voice_session-va") == before, "a muted microphone is not forwarded")

	_ = c.cl.Send(leave(a.session))
	check(waitFor(5*time.Second, func() bool { return c.wasClosed(a.session) }), "leaving is answered with a close message")
	return nil
}

func scenarioCamera(c *conn) error {
	var cam *videoSender
	a, err := c.join("ca", "call-camera", func(p *peer) error {
		var err error
		cam, err = p.addVideo("camera")
		return err
	})
	if err != nil {
		return err
	}
	defer a.close()
	if os.Getenv("INTEROP_DEBUG") != "" {
		fmt.Println("---- local\n" + a.pc.CurrentLocalDescription().SDP + "---- remote\n" + a.pc.CurrentRemoteDescription().SDP)
		fmt.Printf("---- sender extensions: %+v\n", cam.trx.Sender().GetParameters().HeaderExtensions)
	}
	a.announce(rtc.VideoOnMessage, "videoStreamID", cam.streamID)
	b, err := c.join("cb", "call-camera", nil)
	if err != nil {
		return err
	}
	defer b.close()

	cam.stream(3*time.Second, 30)
	check(waitFor(3*time.Second, func() bool { return b.received("video_session-ca") > 60 }), "b receives a's camera (%s)", b.describeVideo("video_session-ca"))
	st := b.track("video_session-ca")
	if st != nil {
		check(st.streamID == "session-ca", "the stream id is the sender's session id (%q)", st.streamID)
		check(st.gaps.Load() == 0, "no sequence gaps on loopback (%d)", st.gaps.Load())
	}

	// A single-layer sender serves a receiver that asks for the small layer, too.
	b.subscribe([]string{a.session}, []string{a.session})
	time.Sleep(500 * time.Millisecond)
	n := b.received("video_session-ca")
	cam.stream(2*time.Second, 30)
	check(b.received("video_session-ca") > n+30, "a receiver asking for the small layer still gets a camera that has only one")

	b.subscribe(nil, nil)
	time.Sleep(time.Second)
	n = b.received("video_session-ca")
	cam.stream(time.Second, 30)
	check(b.received("video_session-ca") == n, "an empty subscription stops the camera")
	return nil
}

func scenarioScreen(c *conn) error {
	a, err := c.join("sa", "call-screen", nil)
	if err != nil {
		return err
	}
	defer a.close()
	b, err := c.join("sb", "call-screen", nil)
	if err != nil {
		return err
	}
	defer b.close()

	// Announce, then add the transceiver; the client offers under the signaling lock.
	var share *videoSender
	share, err = a.addVideo("screen")
	if err != nil {
		return err
	}
	a.announce(rtc.ScreenOnMessage, "screenStreamID", share.streamID)
	time.Sleep(time.Second)
	share.stream(3*time.Second, 30)
	check(waitFor(3*time.Second, func() bool { return b.received("screen_session-sa") > 40 }), "b receives a's screen share (%s)", b.describeVideo("screen_session-sa"))
	check(a.received("screen_") == 0, "the sharer is not sent its own screen")

	a.sig.send(a.session, rtc.ScreenOffMessage, nil)
	_ = a.pc.RemoveTrack(share.trx.Sender())
	time.Sleep(time.Second)
	n := b.received("screen_session-sa")
	share.stream(time.Second, 30)
	check(b.received("screen_session-sa") == n, "nothing arrives after the share ends")
	return nil
}

func scenarioReconnect(c *conn) error {
	a, err := c.join("ra", "call-reconnect", nil)
	if err != nil {
		return err
	}
	defer a.close()
	second, err := dial(false)
	if err != nil {
		return err
	}
	// The original connection stays up; the client moves the session to the new one.
	second.mu.Lock()
	second.peers[a.session] = a
	second.mu.Unlock()
	if err := second.cl.Send(reconnect(a.session)); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	a.mu.Lock()
	a.voiceEvent = nil
	a.mu.Unlock()
	a.speak(150)
	ok := waitFor(3*time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return len(a.voiceEvent) > 0
	})
	check(ok, "after a reconnect the session's messages arrive")
	_ = second.cl.Close()
	return nil
}

func leave(session string) service.ClientMessage {
	return service.ClientMessage{Type: service.ClientMessageLeave, Data: map[string]string{"sessionID": session}}
}

func reconnect(session string) service.ClientMessage {
	return service.ClientMessage{Type: service.ClientMessageReconnect, Data: map[string]string{"sessionID": session}}
}
