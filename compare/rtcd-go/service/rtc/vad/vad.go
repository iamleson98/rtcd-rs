package vad

import (
	"fmt"
	"time"

	"github.com/mattermost/rtcd/service/rtc/stat"
)

const (
	defaultVoiceLevelsSampleSize      = 50
	defaultVoiceActivationThreshold   = 10
	defaultVoiceDeactivationThreshold = 4
	defaultActivationDuration         = 2 * time.Second

	// opusFrameSamples is one 20 ms Opus frame on the 48 kHz RTP clock.
	opusFrameSamples = 960
	// silenceLevel is the audio level (-dBov) of digital silence.
	silenceLevel = 127
)

type VoiceCB func(voice bool)

type Monitor struct {
	cfg MonitorConfig

	voiceLevelsSample    []uint8
	voiceLevelsSamplePtr int
	lastActivationTime   time.Time
	voiceState           bool
	cb                   VoiceCB

	// lastTimestamp is the RTP timestamp of the newest packet pushed.
	lastTimestamp uint32
	hasTimestamp  bool
}

type MonitorConfig struct {
	VoiceLevelsSampleSize      int
	ActivationDuration         time.Duration
	VoiceActivationThreshold   int
	VoiceDeactivationThreshold int
}

func (c MonitorConfig) SetDefaults() MonitorConfig {
	if c.VoiceLevelsSampleSize == 0 {
		c.VoiceLevelsSampleSize = defaultVoiceLevelsSampleSize
	}

	if c.ActivationDuration == 0 {
		c.ActivationDuration = defaultActivationDuration
	}

	if c.VoiceActivationThreshold == 0 {
		c.VoiceActivationThreshold = defaultVoiceActivationThreshold
	}

	if c.VoiceDeactivationThreshold == 0 {
		c.VoiceDeactivationThreshold = defaultVoiceDeactivationThreshold
	}

	return c
}

func (c MonitorConfig) IsValid() error {
	if c.VoiceLevelsSampleSize <= 0 {
		return fmt.Errorf("VoiceLevelsSampleSize should be > 0")
	}

	if c.ActivationDuration <= 0 {
		return fmt.Errorf("ActivationDuration should be > 0")
	}

	if c.VoiceActivationThreshold <= 0 {
		return fmt.Errorf("VoiceActivationThreshold should be > 0")
	}

	if c.VoiceDeactivationThreshold <= 0 {
		return fmt.Errorf("VoiceDeactivationThreshold should be > 0")
	}

	return nil
}

func NewMonitor(cfg MonitorConfig, cb VoiceCB) (*Monitor, error) {
	if err := cfg.IsValid(); err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}

	if cb == nil {
		return nil, fmt.Errorf("voice event callback is required")
	}

	return &Monitor{
		cfg:               cfg,
		voiceLevelsSample: make([]uint8, 0, cfg.VoiceLevelsSampleSize),
		cb:                cb,
	}, nil
}

// PushPacketAudioLevel records a voice packet's audio level. A sender using
// discontinuous transmission (DTX) leaves silent frames out, sending one
// packet every 400 ms instead of every 20 ms, so the frames missing before
// this packet (by its RTP timestamp) count as silence. Without them the
// sample would stretch over seconds of silence and keep voice on.
func (m *Monitor) PushPacketAudioLevel(level uint8, timestamp uint32) {
	if m.hasTimestamp {
		diff := int32(timestamp - m.lastTimestamp)
		if diff <= 0 {
			// Reordered or repeated: no frames are missing.
			m.PushAudioLevel(level)
			return
		}
		for missing := min(int(diff)/opusFrameSamples-1, m.cfg.VoiceLevelsSampleSize); missing > 0; missing-- {
			m.PushAudioLevel(silenceLevel)
		}
	}
	m.lastTimestamp, m.hasTimestamp = timestamp, true
	m.PushAudioLevel(level)
}

func (m *Monitor) PushAudioLevel(level uint8) {
	if len(m.voiceLevelsSample) < m.cfg.VoiceLevelsSampleSize {
		m.voiceLevelsSample = append(m.voiceLevelsSample, level)
		return
	}

	m.voiceLevelsSample[m.voiceLevelsSamplePtr] = level
	if m.voiceLevelsSamplePtr == (m.cfg.VoiceLevelsSampleSize - 1) {
		m.voiceLevelsSamplePtr = 0
	} else {
		m.voiceLevelsSamplePtr++
	}

	avg := stat.Avg(m.voiceLevelsSample)
	dev := stat.StdDev(m.voiceLevelsSample, avg)

	var newState bool
	if !m.voiceState && int(dev) > m.cfg.VoiceActivationThreshold {
		newState = true
		m.lastActivationTime = time.Now()
	} else if m.voiceState && int(dev) < m.cfg.VoiceDeactivationThreshold {
		newState = false
	} else {
		return
	}

	if newState == m.voiceState || (!newState && time.Since(m.lastActivationTime) < m.cfg.ActivationDuration) {
		return
	}

	m.cb(newState)
	m.voiceState = newState
}

func (m *Monitor) Reset() {
	m.voiceLevelsSamplePtr = 0
	m.voiceLevelsSample = m.voiceLevelsSample[:0]
	m.lastActivationTime = time.Time{}
	m.voiceState = false
	m.hasTimestamp = false
	m.cb(false)
}
