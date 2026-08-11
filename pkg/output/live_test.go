package output

import (
	"bytes"
	"context"
	"testing"
)

// TestFetchAndPrint_NilDataReturnsNil verifies that fetchAndPrint returns nil
// without clearing the screen or calling printer.Print when the fetcher returns
// (nil, nil) — the signal used by the DQL executor to indicate context cancellation.
func TestFetchAndPrint_NilDataReturnsNil(t *testing.T) {
	printCalled := false
	printer := &recordingPrinter{onPrint: func(data interface{}) error {
		printCalled = true
		return nil
	}}

	var buf bytes.Buffer
	p := &LivePrinter{
		printer:  printer,
		interval: DefaultLiveInterval,
		writer:   &buf,
	}

	fetcher := func(_ context.Context) (interface{}, error) {
		return nil, nil // simulates context-cancelled path
	}

	err := p.fetchAndPrint(context.Background(), fetcher)
	if err != nil {
		t.Fatalf("fetchAndPrint returned unexpected error: %v", err)
	}
	if printCalled {
		t.Error("printer.Print should not be called when fetcher returns nil data")
	}
	if buf.Len() != 0 {
		t.Errorf("no output expected for nil data, got: %q", buf.String())
	}
}

// TestFetchAndPrint_FetcherError propagates the error from the fetcher without
// touching the printer or writing any output.
func TestFetchAndPrint_FetcherError(t *testing.T) {
	printCalled := false
	printer := &recordingPrinter{onPrint: func(data interface{}) error {
		printCalled = true
		return nil
	}}

	var buf bytes.Buffer
	p := &LivePrinter{
		printer:  printer,
		interval: DefaultLiveInterval,
		writer:   &buf,
	}

	wantErr := context.Canceled
	fetcher := func(_ context.Context) (interface{}, error) {
		return nil, wantErr
	}

	err := p.fetchAndPrint(context.Background(), fetcher)
	if err != wantErr {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if printCalled {
		t.Error("printer.Print should not be called when fetcher returns an error")
	}
}

func TestRunLiveScheduledSkipsNoDataAttemptAndStopsAfterTerminalResult(t *testing.T) {
	printed := 0
	printer := &recordingPrinter{onPrint: func(data interface{}) error {
		printed++
		return nil
	}}
	var output bytes.Buffer
	live := &LivePrinter{printer: printer, interval: 5, writer: &output}
	fetches := 0
	completed := false
	fetcher := func(context.Context) (interface{}, error) {
		fetches++
		if fetches == 1 {
			return nil, nil
		}
		completed = true
		return map[string]interface{}{"records": []interface{}{}}, nil
	}
	waits := 0
	waiter := func(context.Context) error {
		waits++
		return nil
	}
	if err := live.RunLiveScheduled(context.Background(), fetcher, waiter, func() bool { return completed }); err != nil {
		t.Fatal(err)
	}
	if fetches != 2 || waits != 1 || printed != 1 {
		t.Fatalf("fetches=%d waits=%d printed=%d, want 2/1/1", fetches, waits, printed)
	}
	if !bytes.Contains(output.Bytes(), []byte("Live mode completed.")) {
		t.Fatalf("missing clean terminal completion output: %q", output.String())
	}
}

// recordingPrinter is a minimal Printer implementation for testing.
type recordingPrinter struct {
	onPrint func(data interface{}) error
}

func (r *recordingPrinter) Print(data interface{}) error     { return r.onPrint(data) }
func (r *recordingPrinter) PrintList(data interface{}) error { return nil }
