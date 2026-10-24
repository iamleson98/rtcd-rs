package rtc

import "time"

// dropLogInterval is how often a track that is dropping packets says so.
const dropLogInterval = time.Second

// dropLog reports a stream of dropped packets as one line per interval
// with the count, instead of one line per packet. A receiver that cannot
// keep up drops thousands of packets a second, and formatting and writing
// a log line for each took more CPU than forwarding them, on a server that
// was already out of it. One dropLog per forwarded track; not safe for
// concurrent use (the track's read loop is its only caller).
type dropLog struct {
	last    time.Time
	dropped int
}

// drop records one dropped packet. It returns true, with the number
// dropped since the last report, when it is time to log.
func (d *dropLog) drop(now time.Time) (int, bool) {
	d.dropped++
	if now.Sub(d.last) < dropLogInterval {
		return 0, false
	}
	n := d.dropped
	d.last, d.dropped = now, 0
	return n, true
}
