package query

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestReplayHandlerSurfacesFirst429AndPreservesRetryAfter(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(minimalParseTree))
	}))
	defer server.Close()
	client, err := httpclient.New(server.URL,
		httpclient.WithToken("dt0c01.synthetic"),
		httpclient.WithRetry(3, time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(client).WithFirstRateLimitResponse()
	_, err = handler.Parse(context.Background(), ParseRequest{Query: "fetch logs"})
	var rateLimit *RateLimitError
	if !errors.As(err, &rateLimit) {
		t.Fatalf("error = %T %v, want RateLimitError", err, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want first 429 surfaced without retry", calls.Load())
	}
	if delay, ok := RetryAfter(err); !ok || delay != 7*time.Second {
		t.Fatalf("Retry-After = %s, %v; want 7s", delay, ok)
	}
}

func TestNormalHandlerKeepsShared429RetryBehavior(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(minimalParseTree))
	}))
	defer server.Close()
	client, err := httpclient.New(server.URL,
		httpclient.WithToken("dt0c01.synthetic"),
		httpclient.WithRetry(3, time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewHandler(client).Parse(context.Background(), ParseRequest{Query: "fetch logs"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("HTTP calls = %d, want ordinary shared-client retry", calls.Load())
	}
}

func TestReplayRateLimitMarkerDoesNotDisableSessionOAuthRefresh(t *testing.T) {
	var calls, refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call > 2 && r.Header.Get("Authorization") != "Bearer fresh-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(minimalParseTree))
	}))
	defer server.Close()
	client, err := session.NewClient(server.URL, "expired-token")
	if err != nil {
		t.Fatal(err)
	}
	client.EnableTokenRefresh(func(string) (string, error) {
		refreshes.Add(1)
		return "fresh-token", nil
	})
	handler := NewHandler(httpclient.Wrap(client.HTTP())).WithFirstRateLimitResponse()
	for attempt := 0; attempt < 6; attempt++ {
		if _, err := handler.Parse(context.Background(), ParseRequest{Query: "fetch logs"}); err != nil {
			t.Fatalf("sequential parse %d: %v", attempt+1, err)
		}
	}
	if calls.Load() != 7 || refreshes.Load() != 1 {
		t.Fatalf("calls=%d refreshes=%d, want six successes plus one 401 and one refresh", calls.Load(), refreshes.Load())
	}
}
