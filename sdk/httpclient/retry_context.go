package httpclient

import "context"

type suppressRateLimitRetryKey struct{}

// WithoutRateLimitRetry marks a request so the first HTTP 429 is returned to
// the caller. Other retry behavior, including the session client's OAuth 401
// refresh, remains enabled.
func WithoutRateLimitRetry(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, suppressRateLimitRetryKey{}, true)
}

// RateLimitRetrySuppressed reports whether a request context asks the shared
// client to surface the first HTTP 429 instead of retrying it internally.
func RateLimitRetrySuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(suppressRateLimitRetryKey{}).(bool)
	return value
}
