// Command interop runs scenarios against a running rtcd, Go or Rust, using the original Go service
// client for the control plane and pion as the participants' WebRTC stack. The same scenarios must
// pass against both.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mattermost/rtcd/service"
	"github.com/mattermost/rtcd/service/rtc"
)

var (
	urlFlag  = flag.String("url", "http://127.0.0.1:8045", "rtcd API URL")
	clientID = flag.String("client", "interop", "client id to register")
	authKey  = flag.String("key", "0123456789abcdef0123456789abcdef", "client auth key (32+ characters)")
	only     = flag.String("scenario", "all", "scenario to run: all, lookup, voice, camera, screen, reconnect")
)

// conn is one websocket connection to rtcd and the participants whose messages come back on it.
type conn struct {
	cl     *service.Client
	mu     sync.Mutex
	peers  map[string]*peer
	closed map[string]bool
	// onRTC, when set, takes every rtc message instead of the peers (the browser bridge's).
	onRTC func(rtc.Message)
}

func dial(register bool) (*conn, error) {
	cl, err := service.NewClient(service.ClientConfig{URL: *urlFlag, ClientID: *clientID, AuthKey: *authKey, ReconnectInterval: time.Second})
	if err != nil {
		return nil, err
	}
	if register {
		if err := cl.Register(*clientID, *authKey); err != nil && !strings.Contains(err.Error(), "already registered") {
			return nil, fmt.Errorf("register: %w", err)
		}
	}
	if err := cl.Connect(); err != nil {
		return nil, err
	}
	c := &conn{cl: cl, peers: map[string]*peer{}, closed: map[string]bool{}}
	hello := make(chan struct{})
	var once sync.Once
	go func() {
		for msg := range cl.ReceiveCh() {
			switch msg.Type {
			case service.ClientMessageHello:
				once.Do(func() { close(hello) })
			case service.ClientMessageRTC, service.ClientMessageVAD:
				m, ok := msg.Data.(rtc.Message)
				if !ok {
					continue
				}
				if c.onRTC != nil {
					c.onRTC(m)
					continue
				}
				c.mu.Lock()
				p := c.peers[m.SessionID]
				c.mu.Unlock()
				if p != nil {
					p.onSignal(m)
				}
			case service.ClientMessageClose:
				if d, ok := msg.Data.(map[string]string); ok {
					c.mu.Lock()
					c.closed[d["sessionID"]] = true
					c.mu.Unlock()
				}
			}
		}
	}()
	select {
	case <-hello:
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("no hello from rtcd")
	}
	return c, nil
}

func (c *conn) wasClosed(session string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed[session]
}

// join creates a participant and brings it to a connected call.
func (c *conn) join(name, callID string, setup func(*peer) error) (*peer, error) {
	p, err := newPeer(name, callID, c.cl)
	if err != nil {
		return nil, err
	}
	if setup != nil {
		if err := setup(p); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	c.peers[p.session] = p
	c.mu.Unlock()
	if err := p.join(c.cl, "channel-"+callID); err != nil {
		return nil, err
	}
	if !waitFor(15*time.Second, p.ready) {
		return nil, fmt.Errorf("%s never became ready (connection %s, dc %s)", name, p.pc.ConnectionState(), p.dcCh.ReadyState())
	}
	return p, nil
}

func waitFor(d time.Duration, cond func() bool) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

var failures int

func check(ok bool, format string, args ...any) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		failures++
	}
	fmt.Printf("  %s  %s\n", status, fmt.Sprintf(format, args...))
}

type scenario struct {
	name string
	run  func(*conn) error
}

func flag_int(name string, def int, usage string) *int {
	return flag.Int(name, def, usage)
}

func main() {
	flag.Parse()
	if *loadCalls > 0 {
		os.Exit(runLoad())
	}
	if *browserBundle != "" {
		os.Exit(runBrowser())
	}
	c, err := dial(true)
	if err != nil {
		fmt.Println("cannot connect:", err)
		os.Exit(2)
	}
	scenarios := []scenario{
		{"lookup", scenarioLookup},
		{"voice", scenarioVoice},
		{"camera", scenarioCamera},
		{"screen", scenarioScreen},
		{"reconnect", scenarioReconnect},
	}
	for _, s := range scenarios {
		if *only != "all" && *only != s.name {
			continue
		}
		fmt.Printf("== %s\n", s.name)
		start := time.Now()
		if err := s.run(c); err != nil {
			fmt.Printf("  FAIL  %v\n", err)
			failures++
		}
		fmt.Printf("  (%.1fs)\n", time.Since(start).Seconds())
	}
	if failures > 0 {
		fmt.Printf("%d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("all checks passed")
}
