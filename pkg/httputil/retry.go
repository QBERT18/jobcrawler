package httputil

import (
	"context"
	"math/rand"
	"time"
)

// RetryDo calls fn up to attempts times, sleeping between retries with
// exponential backoff and up to 20 % random jitter.
//
// Backoff schedule (base delay = delay param):
//
//	attempt 1 → delay * 1  ± jitter
//	attempt 2 → delay * 2  ± jitter
//	attempt 3 → delay * 4  ± jitter
//
// Returns the last error if all attempts fail. Returns immediately
// with nil if fn succeeds. Respects context cancellation between retries.
func RetryDo(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	var err error

	for i := range attempts {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		err = fn()
		if err == nil {
			return nil
		}

		if i == attempts-1 {
			break // last attempt — don't sleep
		}

		backoff := delay * (1 << i) // 1×, 2×, 4×, 8×, …
		jitter := time.Duration(rand.Int63n(int64(backoff) / 5)) //nolint:gosec
		sleep := backoff + jitter

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}

	return err
}