package main

// The browser test: real Chrome frames run the LMS web app's own CallClient (compat/browser), and this
// file plays the part of the LMS server between them and rtcd: it accepts the custom_calls_* websocket
// actions, relays them to rtcd with the original Go service client the way
// server/channels/calls does, and sends rtcd's answers back as `signal` events.

import (
	"bytes"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mattermost/rtcd/service"
	"github.com/mattermost/rtcd/service/rtc"
)

var (
	browserBundle = flag.String("browser", "", "run the Chrome test with this harness bundle (see compare/harness/browser)")
	browserBin    = flag.String("browser-bin", "", "the browser binary for -browser (default: the usual place for -browser-kind)")
	noSandbox     = flag.Bool("no-sandbox", false, "start Chrome with --no-sandbox (containers, CI)")
	synthetic     = flag.Bool("synthetic", false, "capture a canvas and a tone instead of the browser's fake devices (Firefox and Safari have none)")
	browserKind   = flag.String("browser-kind", "chrome", "chrome, firefox or safari")
)

//go:embed page.html frame.html
var pages embed.FS

func defaultBin(kind string) string {
	if *browserBin != "" {
		return *browserBin
	}
	if runtime.GOOS == "darwin" {
		return map[string]string{
			"chrome":  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"firefox": "/Applications/Firefox.app/Contents/MacOS/firefox",
		}[kind]
	}
	return map[string]string{"chrome": "google-chrome", "firefox": "firefox"}[kind]
}

// bridgePeer is one browser connection and its SFU session.
type bridgePeer struct {
	ws      *websocket.Conn
	wmu     sync.Mutex
	name    string
	session string
}

func (b *bridgePeer) event(name string, data any) {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	_ = b.ws.WriteJSON(map[string]any{"event": name, "data": data})
}

type bridge struct {
	cl     *service.Client
	callID string
	mu     sync.Mutex
	peers  map[string]*bridgePeer // by session
	result chan []byte
}

func (br *bridge) fromRTCD(m rtc.Message) {
	br.mu.Lock()
	b := br.peers[m.SessionID]
	br.mu.Unlock()
	if b == nil || m.Data == nil {
		return
	}
	switch m.Type {
	case rtc.SDPMessage, rtc.ICEMessage:
		b.event("custom_calls_signal", map[string]any{"data": string(m.Data), "connID": m.SessionID})
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (br *bridge) serveWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	name := r.URL.Query().Get("name")
	b := &bridgePeer{ws: ws, name: name, session: "browser-" + name}
	br.mu.Lock()
	br.peers[b.session] = b
	br.mu.Unlock()
	defer func() {
		_ = br.cl.Send(service.ClientMessage{Type: service.ClientMessageLeave, Data: map[string]string{"sessionID": b.session}})
		br.mu.Lock()
		delete(br.peers, b.session)
		br.mu.Unlock()
		_ = ws.Close()
	}()
	for {
		var in struct {
			Action string         `json:"action"`
			Data   map[string]any `json:"data"`
		}
		if err := ws.ReadJSON(&in); err != nil {
			return
		}
		br.handle(b, in.Action, in.Data)
	}
}

func str(data map[string]any, key string) []byte {
	if s, ok := data[key].(string); ok {
		return []byte(s)
	}
	return nil
}

// handle is the part of server/channels/calls HandleClientMessage that reaches rtcd.
func (br *bridge) handle(b *bridgePeer, action string, data map[string]any) {
	send := func(t rtc.MessageType, payload []byte) {
		_ = br.cl.Send(service.ClientMessage{Type: service.ClientMessageRTC, Data: rtc.Message{
			GroupID: "default", UserID: "user-" + b.name, SessionID: b.session, CallID: br.callID, Type: t, Data: payload,
		}})
	}
	switch action {
	case "custom_calls_join":
		_ = br.cl.Send(service.ClientMessage{Type: service.ClientMessageJoin, Data: map[string]any{
			"callID": br.callID, "userID": "user-" + b.name, "sessionID": b.session, "channelID": "channel-" + br.callID,
			"av1Support": false, "dcSignaling": true,
		}})
		b.event("custom_calls_join", map[string]any{"connID": b.session, "iceServers": []any{}})
	case "custom_calls_sdp":
		payload := str(data, "data")
		send(rtc.SDPMessage, payload)
	case "custom_calls_ice":
		send(rtc.ICEMessage, str(data, "data"))
	case "custom_calls_mute":
		send(rtc.MuteMessage, nil)
	case "custom_calls_unmute":
		send(rtc.UnmuteMessage, nil)
	case "custom_calls_video_on":
		send(rtc.VideoOnMessage, str(data, "data"))
	case "custom_calls_video_off":
		send(rtc.VideoOffMessage, nil)
	case "custom_calls_screen_on":
		send(rtc.ScreenOnMessage, str(data, "data"))
	case "custom_calls_screen_off":
		send(rtc.ScreenOffMessage, nil)
	case "custom_calls_video_subscribe":
		send(rtc.VideoSubscribeMessage, str(data, "data"))
	case "custom_calls_leave":
		_ = br.cl.Send(service.ClientMessage{Type: service.ClientMessageLeave, Data: map[string]string{"sessionID": b.session}})
	}
}

// snapshot is what a participant's page reports (harness.ts report()).
type snapshot struct {
	Name       string   `json:"name"`
	Role       string   `json:"role"`
	Status     string   `json:"status"`
	Error      *string  `json:"error"`
	Connection string   `json:"connection"`
	Cameras    []string `json:"cameras"`
	RemoteSDP  string   `json:"remoteSdp"`
	LocalSDP   string   `json:"localSdp"`
	AudioIn    int      `json:"audioIn"`
	VideoIn    []struct {
		Packets, Frames, Width, Height, Lost int
		Codec                                string
		Keyframes, Plis, Nacks               int
		Last                                 float64
	} `json:"videoIn"`
	VideoOut []struct {
		Rid       string
		Packets   int
		Frames    int
		Width     int
		Codec     string
		Plis      int
		Keyframes int
	} `json:"videoOut"`
}

type browserResult struct {
	JoinMs []float64    `json:"joinMs"`
	Snaps  [][]snapshot `json:"snaps"` // by phase, then participant
	Errors []string     `json:"errors"`
}

// currentWidth is the width of the video stream that received a packet most recently: browsers keep
// the statistics of the streams a receiver was fed before, and only the current one counts.
func (s snapshot) currentWidth() int {
	width, last := 0, -1.0
	for _, v := range s.VideoIn {
		if v.Frames > 0 && v.Last > last {
			width, last = v.Width, v.Last
		}
	}
	return width
}

func (s snapshot) videoFrames() (frames, width int) {
	for _, v := range s.VideoIn {
		frames += v.Frames
		if v.Width > width {
			width = v.Width
		}
	}
	return
}

func runBrowser() int {
	c, err := dial(true)
	if err != nil {
		fmt.Println("cannot connect:", err)
		return 2
	}
	br := &bridge{cl: c.cl, callID: fmt.Sprintf("browser-%d", time.Now().UnixNano()), peers: map[string]*bridgePeer{}, result: make(chan []byte, 1)}
	c.onRTC = br.fromRTCD

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", br.serveWS)
	mux.HandleFunc("/harness.js", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, *browserBundle) })
	mux.HandleFunc("/log", func(w http.ResponseWriter, r *http.Request) { fmt.Println("  page:", r.URL.Query().Get("m")) })
	mux.HandleFunc("/result", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case br.result <- body:
		default:
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := "page.html"
		if r.URL.Path == "/p.html" {
			name = "frame.html"
		}
		data, _ := pages.ReadFile(name)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return 2
	}
	go func() { _ = http.Serve(ln, mux) }()

	url := "http://" + ln.Addr().String() + "/"
	if *synthetic {
		url += "?synthetic=1"
	}
	stop, err := launchBrowser(url)
	if err != nil {
		fmt.Println("cannot start the browser:", err)
		return 2
	}
	defer stop()

	var raw []byte
	select {
	case raw = <-br.result:
	case <-time.After(3 * time.Minute):
		fmt.Println("  FAIL  no result from the browser")
		return 1
	}
	var res browserResult
	if err := json.Unmarshal(raw, &res); err != nil {
		fmt.Println("  FAIL  bad result:", err)
		return 1
	}
	judgeBrowser(res)
	if failures > 0 {
		fmt.Printf("%d check(s) failed\n", failures)
		return 1
	}
	fmt.Println("all checks passed")
	return 0
}

func judgeBrowser(res browserResult) {
	fmt.Println("== browser (the LMS call client in Chrome)")
	for _, e := range res.Errors {
		check(false, "page error: %s", e)
	}
	if len(res.Snaps) < 3 {
		check(false, "the page reported %d phases, want 3", len(res.Snaps))
		return
	}
	by := func(phase int, name string) snapshot {
		for _, s := range res.Snaps[phase] {
			if s.Name == name {
				return s
			}
		}
		return snapshot{Name: name}
	}
	for _, s := range res.Snaps[0] {
		check(s.Status == "connected" && s.Connection == "connected" && s.Error == nil,
			"%s is connected (status %s, connection %s)", s.Name, s.Status, s.Connection)
		check(s.AudioIn > 200, "%s hears the others (%d audio packets)", s.Name, s.AudioIn)
	}
	maxJoin := 0.0
	for _, j := range res.JoinMs {
		if j > maxJoin {
			maxJoin = j
		}
	}
	fmt.Printf("  INFO  join time: slowest %.0f ms of %d\n", maxJoin, len(res.JoinMs))

	teacher := by(0, "teacher")
	layers := map[string]int{}
	for _, o := range teacher.VideoOut {
		layers[o.Rid] += o.Frames
	}
	check(layers["l"] > 0 && layers["h"] > 0, "the teacher's camera sends both simulcast layers (%v)", layers)

	full := by(0, "viewer")
	if os.Getenv("BROWSER_DUMP_SDP") != "" {
		fmt.Printf("---- viewer remote (SFU offer)\n%s\n---- viewer local (answer)\n%s\n", full.RemoteSDP, full.LocalSDP)
	}
	fmt.Printf("  INFO  viewer video in: %+v\n  INFO  teacher video out: %+v\n", full.VideoIn, teacher.VideoOut)
	frames, _ := full.videoFrames()
	width := full.currentWidth()
	check(frames > 20 && width >= 600, "a viewer gets the teacher's camera at the large layer (%d frames, %d px wide)", frames, width)
	check(len(full.Cameras) == 1 && full.Cameras[0] == "browser-teacher", "the viewer's tile is the teacher's session (%v)", full.Cameras)

	small := by(0, "small")
	frames, _ = small.videoFrames()
	width = small.currentWidth()
	check(frames > 20 && width > 0 && width <= 330, "a viewer that asked for the small layer gets it (%d frames, %d px wide)", frames, width)

	// A client that never names the cameras it wants gets every camera (the server default).
	f, _ := by(0, "speaker").videoFrames()
	check(f > 20, "the speaker, who named no cameras, gets the teacher's (%d frames)", f)
	// Until its video_subscribe arrives the server's default applies (every camera), so a few frames
	// can get through; once it has, nothing more may.
	f, _ = by(0, "listener").videoFrames()
	late, _ := by(2, "listener").videoFrames()
	check(f <= 30 && late == f && len(by(0, "listener").Cameras) == 0,
		"the listener asked for no cameras and receives none afterwards (%d frames, %d at the end)", f, late)

	// The teacher's camera goes off and on again: no renegotiation, the picture comes back.
	off1, _ := by(1, "viewer").videoFrames()
	on2, _ := by(2, "viewer").videoFrames()
	check(on2-off1 > 15, "the picture returns after the camera is switched off and on (%d new frames)", on2-off1)
	check(by(2, "viewer").AudioIn > by(1, "viewer").AudioIn+50, "audio keeps flowing while the speaker mutes and unmutes")
}

// launchBrowser opens url in the chosen browser and returns what stops it again. Only the processes
// started here are stopped: the person's own browser may be running.
func launchBrowser(url string) (func(), error) {
	switch *browserKind {
	case "chrome":
		profile, _ := os.MkdirTemp("", "chrome-profile")
		args := []string{
			"--headless=new", "--user-data-dir=" + profile, "--no-first-run", "--no-default-browser-check",
			"--use-fake-device-for-media-stream", "--use-fake-ui-for-media-stream",
			"--autoplay-policy=no-user-gesture-required", "--disable-background-timer-throttling",
			"--disable-renderer-backgrounding", "--disable-backgrounding-occluded-windows",
		}
		if *noSandbox {
			args = append(args, "--no-sandbox")
		}
		return spawn(defaultBin("chrome"), append(args, url), profile)
	case "firefox":
		profile, _ := os.MkdirTemp("", "firefox-profile")
		prefs := `user_pref("media.navigator.permission.disabled", true);
user_pref("media.navigator.streams.fake", true);
user_pref("media.peerconnection.ice.loopback", true);
user_pref("media.peerconnection.ice.obfuscate_host_addresses", false);
user_pref("media.autoplay.default", 0);
user_pref("media.autoplay.blocking_policy", 0);
user_pref("datareporting.policy.dataSubmissionEnabled", false);
user_pref("browser.shell.checkDefaultBrowser", false);
`
		if err := os.WriteFile(profile+"/user.js", []byte(prefs), 0o644); err != nil {
			return nil, err
		}
		return spawn(defaultBin("firefox"), []string{"-headless", "-no-remote", "-profile", profile, url}, profile)
	case "safari":
		return safariSession(url)
	}
	return nil, fmt.Errorf("unknown browser kind %q", *browserKind)
}

func spawn(bin string, args []string, profile string) (func(), error) {
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = os.RemoveAll(profile)
	}, nil
}

// safariSession drives Safari through safaridriver (Safari's Develop > Allow Remote Automation, or
// `safaridriver --enable`): the session gets its own window, which closing the session removes.
func safariSession(url string) (func(), error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	driver := exec.Command("safaridriver", "-p", fmt.Sprint(port))
	driver.Stdout, driver.Stderr = io.Discard, io.Discard
	if err := driver.Start(); err != nil {
		return nil, err
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	call := func(method, path string, body any) (map[string]any, error) {
		var rd io.Reader
		if body != nil {
			data, _ := json.Marshal(body)
			rd = bytes.NewReader(data)
		}
		req, _ := http.NewRequest(method, base+path, rd)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if resp.StatusCode >= 400 {
			return out, fmt.Errorf("safaridriver %s %s: %d %v", method, path, resp.StatusCode, out["value"])
		}
		return out, nil
	}
	kill := func() { _ = driver.Process.Kill(); _, _ = driver.Process.Wait() }
	var session string
	for i := 0; i < 50; i++ {
		out, err := call("POST", "/session", map[string]any{"capabilities": map[string]any{"alwaysMatch": map[string]any{"browserName": "safari"}}})
		if err == nil {
			if v, ok := out["value"].(map[string]any); ok {
				session, _ = v["sessionId"].(string)
			}
			break
		}
		if i == 49 {
			kill()
			return nil, err
		}
		time.Sleep(200 * time.Millisecond)
	}
	if session == "" {
		kill()
		return nil, fmt.Errorf("safaridriver gave no session")
	}
	if _, err := call("POST", "/session/"+session+"/url", map[string]any{"url": url}); err != nil {
		_, _ = call("DELETE", "/session/"+session, nil)
		kill()
		return nil, err
	}
	return func() { _, _ = call("DELETE", "/session/"+session, nil); kill() }, nil
}
