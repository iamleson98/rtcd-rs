package rtc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDropLog(t *testing.T) {
	var d dropLog
	start := time.Now()

	n, ok := d.drop(start)
	require.True(t, ok, "the first drop is reported at once")
	require.Equal(t, 1, n)

	for i := range 999 {
		_, ok = d.drop(start.Add(time.Duration(i) * time.Millisecond))
		require.False(t, ok, "drops within the interval are counted, not reported")
	}

	n, ok = d.drop(start.Add(dropLogInterval))
	require.True(t, ok, "one report per interval")
	require.Equal(t, 1000, n, "with every drop since the last report")

	_, ok = d.drop(start.Add(dropLogInterval + time.Millisecond))
	require.False(t, ok)
}
