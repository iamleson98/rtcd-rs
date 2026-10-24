package main

import (
	"fmt"
	"math/rand"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattermost/rtcd/service/rtc"
)

// A class: one teacher sending a camera and a voice to a roomful of students, a few of whom speak
// and some of whom watch the camera, as in the product (docs/calls/LIVE-ROOM-VIDEO.md). The server
// does its real work for everyone; the cost to compare is the server process's CPU and memory.
var (
	loadCalls    = flag_int("calls", 0, "load mode: number of classes (0 runs the scenarios instead)")
	loadStudents = flag_int("students", 29, "students per class")
	loadSpeakers = flag_int("speakers", 3, "students per class who speak")
	loadViewers  = flag_int("viewers", 6, "students per class who watch the teacher's camera")
	loadSeconds  = flag_int("seconds", 30, "seconds to measure")
	loadPID      = flag_int("pid", 0, "pid of the server, to measure its CPU and memory")
	loadJoins    = flag_int("join-concurrency", 6, "participants joining at once")
)

type participant struct {
	p       *peer
	class   int
	role    string // teacher, speaker, viewer, listener
	joinSec float64
}

type loadStats struct {
	mu       sync.Mutex
	joinSecs []float64
	failures []string
	expected int64
}

func runLoad() int {
	conns := make([]*conn, 0, *loadCalls)
	for i := 0; i < *loadCalls; i++ {
		c, err := dial(i == 0)
		if err != nil {
			fmt.Println("cannot connect:", err)
			return 2
		}
		conns = append(conns, c)
	}

	var stats loadStats
	var all []*participant
	var allMu sync.Mutex
	sem := make(chan struct{}, *loadJoins)
	var wg sync.WaitGroup
	joinAll := func(c *conn, class int) {
		defer wg.Done()
		call := fmt.Sprintf("class-%d", class)
		// The teacher first, then the students, as in a class.
		var teacher *participant
		var cam *videoSender
		{
			sem <- struct{}{}
			start := time.Now()
			p, err := c.join(fmt.Sprintf("t%d", class), call, func(p *peer) error {
				var err error
				cam, err = p.addVideo("camera")
				return err
			})
			<-sem
			if err != nil {
				stats.fail(fmt.Sprintf("teacher %d: %v", class, err))
				return
			}
			p.announce(rtc.VideoOnMessage, "videoStreamID", cam.streamID)
			teacher = &participant{p: p, class: class, role: "teacher", joinSec: time.Since(start).Seconds()}
			stats.addJoin(teacher.joinSec)
			allMu.Lock()
			all = append(all, teacher)
			allMu.Unlock()
		}
		var inner sync.WaitGroup
		for s := 0; s < *loadStudents; s++ {
			role := "listener"
			switch {
			case s < *loadSpeakers:
				role = "speaker"
			case s < *loadSpeakers+*loadViewers:
				role = "viewer"
			}
			inner.Add(1)
			sem <- struct{}{}
			go func(s int, role string) {
				defer inner.Done()
				defer func() { <-sem }()
				start := time.Now()
				p, err := c.join(fmt.Sprintf("s%d-%d", class, s), call, nil)
				if err != nil {
					stats.fail(fmt.Sprintf("student %d-%d: %v", class, s, err))
					return
				}
				// A student names the cameras it shows: the teacher's, or none.
				if role == "viewer" {
					p.subscribe([]string{teacher.p.session}, nil)
				} else {
					p.subscribe(nil, nil)
				}
				pp := &participant{p: p, class: class, role: role, joinSec: time.Since(start).Seconds()}
				stats.addJoin(pp.joinSec)
				allMu.Lock()
				all = append(all, pp)
				allMu.Unlock()
			}(s, role)
		}
		inner.Wait()
		// The teacher's camera runs for the whole measurement.
		go func() {
			t := time.NewTicker(33 * time.Millisecond)
			defer t.Stop()
			f := 0
			for range t.C {
				if teacher.p.closed.Load() {
					return
				}
				cam.frame(f%60 == 0, 3)
				f++
			}
		}()
	}
	for i, c := range conns {
		wg.Add(1)
		go joinAll(c, i)
	}
	wg.Wait()
	fmt.Printf("joined %d participants (%d failures)\n", len(all), len(stats.failures))

	// Voices: the teacher and the speakers send one packet every 20 ms.
	stop := make(chan struct{})
	var voices sync.WaitGroup
	for _, pt := range all {
		if pt.role != "teacher" && pt.role != "speaker" {
			continue
		}
		voices.Add(1)
		go func(p *peer) {
			defer voices.Done()
			t := time.NewTicker(20 * time.Millisecond)
			defer t.Stop()
			ext := audioLevelExt(p)
			for f := 0; ; f++ {
				select {
				case <-stop:
					return
				case <-t.C:
					p.sendVoice(ext, f)
				}
			}
		}(pt.p)
	}
	time.Sleep(5 * time.Second) // let media settle
	for _, pt := range all {
		pt.p.resetCounters()
	}
	cpu0, rss0 := sample(*loadPID)
	t0 := time.Now()
	time.Sleep(time.Duration(*loadSeconds) * time.Second)
	elapsed := time.Since(t0).Seconds()
	cpu1, rss1 := sample(*loadPID)
	close(stop)
	voices.Wait()

	// What each receiver should have got: every sender's voice, and the camera if it watches.
	senders := int64(1 + *loadSpeakers)
	var got, want, camGot, camWant int64
	var worst float64
	for _, pt := range all {
		var voiceGot int64
		pt.p.recvMu.Lock()
		for id, st := range pt.p.recv {
			if strings.HasPrefix(id, "voice_") {
				voiceGot += st.packets.Load()
			}
		}
		pt.p.recvMu.Unlock()
		own := int64(0)
		if pt.role == "teacher" || pt.role == "speaker" {
			own = 1
		}
		expected := int64(float64(senders-own) * elapsed * 50)
		got += voiceGot
		want += expected
		if expected > 0 {
			if loss := 1 - float64(voiceGot)/float64(expected); loss > worst {
				worst = loss
			}
		}
		if pt.role == "viewer" {
			camGot += pt.p.received("video_")
			camWant += int64(elapsed * 30 * 3)
		}
	}
	fmt.Printf("\nparticipants   %d in %d classes (%d speakers + teacher, %d watching the camera)\n", len(all), *loadCalls, *loadSpeakers, *loadViewers)
	fmt.Printf("join time      p50 %.2fs  p95 %.2fs  failures %d\n", stats.percentile(0.5), stats.percentile(0.95), len(stats.failures))
	fmt.Printf("voice delivery %.2f%% of %d packets (worst receiver %.2f%% lost)\n", 100*float64(got)/float64(max(want, 1)), want, 100*worst)
	if camWant > 0 {
		fmt.Printf("camera         %.2f%% of %d packets to the viewers\n", 100*float64(camGot)/float64(camWant), camWant)
	}
	if *loadPID != 0 {
		fmt.Printf("server cpu     %.1f%% of one core over %.0fs\n", 100*(cpu1-cpu0)/elapsed, elapsed)
		fmt.Printf("server memory  %.0f MiB resident (%.0f MiB at the start of the window, %d participants)\n", rss1, rss0, len(all))
	}
	for _, f := range stats.failures {
		fmt.Println("failure:", f)
	}
	for _, pt := range all {
		pt.p.close()
	}
	return 0
}

func (s *loadStats) fail(msg string) {
	s.mu.Lock()
	s.failures = append(s.failures, msg)
	s.mu.Unlock()
}

func (s *loadStats) addJoin(sec float64) {
	s.mu.Lock()
	s.joinSecs = append(s.joinSecs, sec)
	s.mu.Unlock()
}

func (s *loadStats) percentile(q float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.joinSecs) == 0 {
		return 0
	}
	v := append([]float64(nil), s.joinSecs...)
	sort.Float64s(v)
	return v[int(q*float64(len(v)-1))]
}

// sample returns a process's total CPU seconds and resident MiB, from ps.
func sample(pid int) (cpu float64, rssMiB float64) {
	if pid == 0 {
		return 0, 0
	}
	out, err := exec.Command("ps", "-o", "rss=,cputime=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, 0
	}
	f := strings.Fields(string(out))
	if len(f) < 2 {
		return 0, 0
	}
	kb, _ := strconv.ParseFloat(f[0], 64)
	var secs float64
	for _, part := range strings.Split(f[1], ":") {
		v, _ := strconv.ParseFloat(part, 64)
		secs = secs*60 + v
	}
	return secs, kb / 1024
}

var _ = rand.Int
var _ atomic.Int64
