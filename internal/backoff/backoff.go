// Package backoff computes retry delays.
package backoff

import (
	"math/rand/v2"
	"time"
)

// FullJitter returns a random delay in [0, min(max, base*2^(attempt-1))].
//
// Full jitter (rather than a fixed exponential schedule) spreads retries out so
// that thousands of deliveries failing at once against a recovering endpoint
// don't all come back at the same instant.
// See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/.
func FullJitter(attempt int, base, max time.Duration, r *rand.Rand) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := max
	if shift := attempt - 1; shift < 62 {
		if d := base << shift; d > 0 && d < max {
			ceiling = d
		}
	}
	if r == nil {
		return time.Duration(rand.Int64N(int64(ceiling) + 1))
	}
	return time.Duration(r.Int64N(int64(ceiling) + 1))
}
