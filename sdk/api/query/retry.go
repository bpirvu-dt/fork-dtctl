package query

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

// RateLimitError preserves the first HTTP 429 together with the server's
// Retry-After instruction. It wraps the ordinary typed Query/API error.
type RateLimitError struct {
	err        error
	retryAfter time.Duration
}

func (e *RateLimitError) Error() string { return e.err.Error() }
func (e *RateLimitError) Unwrap() error { return e.err }

// RetryAfter returns the minimum delay requested by the server. A missing or
// invalid header is represented by zero.
func (e *RateLimitError) RetryAfter() time.Duration { return e.retryAfter }

// RetryAfter returns a server-supplied retry delay from an error chain.
func RetryAfter(err error) (time.Duration, bool) {
	var rateLimited *RateLimitError
	if !errors.As(err, &rateLimited) {
		return 0, false
	}
	return rateLimited.retryAfter, true
}

func wrapRateLimit(resp *resty.Response, err error) error {
	if resp == nil || resp.StatusCode() != http.StatusTooManyRequests || err == nil {
		return err
	}
	return &RateLimitError{err: err, retryAfter: parseRetryAfter(resp.Header().Get("Retry-After"), time.Now())}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}
