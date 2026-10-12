package backoff

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestFullJitterBounds(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	base, max := time.Second, time.Minute
	for attempt := 1; attempt <= 100; attempt++ {
		ceiling := max
		if attempt < 8 && base<<(attempt-1) < max {
			ceiling = base << (attempt - 1)
		}
		var hi time.Duration
		for range 2000 {
			d := FullJitter(attempt, base, max, r)
			if d < 0 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, ceiling)
			}
			hi = maxDur(hi, d)
		}
		// The distribution should actually reach toward its ceiling.
		if hi < ceiling*9/10 {
			t.Fatalf("attempt %d: max observed %v, expected near %v", attempt, hi, ceiling)
		}
	}
}

func TestFullJitterNeverOverflows(t *testing.T) {
	if d := FullJitter(1_000_000, time.Hour, 6*time.Hour, nil); d < 0 || d > 6*time.Hour {
		t.Fatalf("got %v", d)
	}
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
