package exec

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
)

func TestDavisSnapshotCoverageInspectorUsesExactBoundedReadOnlyRequest(t *testing.T) {
	dataEnd := mustReplayTestTime("2026-06-14T12:00:00.123456789Z")
	observedAt := mustReplayTestTime("2026-08-14T11:00:00Z")
	oldest := "2026-06-14T02:00:00.000000000Z"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/platform/storage/query/v1/query:execute" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request sdkquery.ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		wantQuery := `fetch dt.davis.problems.snapshots,
  from:toTimestamp("1970-01-01T00:00:00.000Z"),
  to:toTimestamp("2026-06-14T12:00:00.123456789Z")
| summarize oldest_snapshot=min(timestamp)`
		if request.Query != wantQuery || request.RequestTimeoutMilliseconds != 5000 || request.MaxResultRecords != 1 ||
			request.DefaultScanLimitGbytes != 1 || request.DefaultSamplingRatio != 1 || request.FetchTimeoutSeconds != 5 ||
			request.PollingPromiseSeconds != 5 || request.Locale != "en_US" || request.Timezone != "UTC" {
			t.Errorf("coverage request = %#v", request)
		}
		for _, forbidden := range []string{"event.id", "bucket", "tenant", "environment", "context-under-test", "create ", "update ", "delete "} {
			if strings.Contains(strings.ToLower(request.Query), forbidden) {
				t.Errorf("coverage query contains forbidden %q: %q", forbidden, request.Query)
			}
		}
		if got := r.Header.Get("dt-client-context"); !strings.Contains(got, "replay-davis-snapshot-coverage") {
			t.Errorf("dt-client-context = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{"oldest_snapshot": oldest}}}})
	}))
	defer server.Close()

	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	inspector := NewDavisSnapshotCoverageInspector(transport).(*grailDavisSnapshotCoverageInspector)
	inspector.now = func() time.Time { return observedAt }
	coverage, err := inspector.Inspect(context.Background(), "synthetic-session", dataEnd)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || !coverage.OldestSnapshot.Equal(mustReplayTestTime(oldest)) || !coverage.ObservedAt.Equal(observedAt) {
		t.Fatalf("requests=%d coverage=%#v", requests.Load(), coverage)
	}
}

func TestDavisSnapshotCoverageInspectorRejectsEveryAmbiguousResponse(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		failure DavisSnapshotCoverageFailure
	}{
		{"empty records", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[]}}`, DavisCoverageEmpty},
		{"null timestamp", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{"oldest_snapshot":null}]}}`, DavisCoverageMalformed},
		{"missing timestamp", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{}]}}`, DavisCoverageMalformed},
		{"wrong type", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{"oldest_snapshot":42}]}}`, DavisCoverageMalformed},
		{"duplicate records", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{"oldest_snapshot":"2026-01-01T00:00:00Z"},{"oldest_snapshot":"2026-01-01T00:00:00Z"}]}}`, DavisCoverageMalformed},
		{"extra returned field", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{"oldest_snapshot":"2026-01-01T00:00:00Z","event.id":"forbidden"}]}}`, DavisCoverageMalformed},
		{"malformed JSON", http.StatusOK, `{"state":`, DavisCoverageUnavailable},
		{"asynchronous", http.StatusAccepted, `{"state":"RUNNING","requestToken":"synthetic-request"}`, DavisCoverageAsync},
		{"scan limited", http.StatusOK, `{"state":"SUCCEEDED","result":{"records":[{"oldest_snapshot":"2026-01-01T00:00:00Z"}],"metadata":{"grail":{"notifications":[{"notificationType":"SCAN_LIMIT","message":"synthetic"}]}}}}`, DavisCoverageScanLimited},
		{"unauthorized", http.StatusUnauthorized, `{"error":{"message":"synthetic"}}`, DavisCoverageUnauthorized},
		{"forbidden", http.StatusForbidden, `{"error":{"message":"synthetic"}}`, DavisCoverageForbidden},
		{"service unavailable", http.StatusServiceUnavailable, `{"error":{"message":"synthetic"}}`, DavisCoverageUnavailable},
		{"rate limited", http.StatusTooManyRequests, `{"error":{"message":"synthetic"}}`, DavisCoverageRateLimited},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if test.status == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "1")
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewDavisSnapshotCoverageInspector(transport).Inspect(context.Background(), "synthetic-session", mustReplayTestTime("2026-06-14T12:00:00Z"))
			var coverageErr *DavisSnapshotCoverageError
			if !errors.As(err, &coverageErr) || coverageErr.Failure != test.failure || strings.Contains(err.Error(), test.body) {
				t.Fatalf("error = %T %v, want sanitized %s", err, err, test.failure)
			}
		})
	}
}

func TestDavisSnapshotCoverageInspectorTimeoutIsFiveSecondBounded(t *testing.T) {
	requestDone := make(chan struct{})
	transport, err := client.NewForTesting("https://example.invalid", "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	transport.HTTP().SetTransport(coverageRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		close(requestDone)
		return nil, r.Context().Err()
	}))
	inspector := &grailDavisSnapshotCoverageInspector{
		handler: sdkquery.NewHandler(httpclient.Wrap(transport.HTTP())), timeout: 25 * time.Millisecond, now: time.Now,
	}
	started := time.Now()
	_, err = inspector.Inspect(context.Background(), "synthetic-session", mustReplayTestTime("2026-06-14T12:00:00Z"))
	var coverageErr *DavisSnapshotCoverageError
	if !errors.As(err, &coverageErr) || coverageErr.Failure != DavisCoverageTimeout || time.Since(started) > time.Second {
		t.Fatalf("timeout error = %T %v", err, err)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not observe timeout cancellation")
	}
}

type coverageRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f coverageRoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type recordingDavisCoverageInspector struct {
	coverage DavisSnapshotCoverage
	err      error
	calls    atomic.Int32
}

func (i *recordingDavisCoverageInspector) Inspect(context.Context, string, time.Time) (DavisSnapshotCoverage, error) {
	i.calls.Add(1)
	return i.coverage, i.err
}

func TestDavisSnapshotCoverageMemoLifecycleAndCompleteKey(t *testing.T) {
	base := DavisSnapshotCoverageKey{
		SessionID: "synthetic-session", EnvironmentIdentity: "synthetic-environment",
		PrincipalIdentity: "synthetic-principal", Table: execreplay.DavisProblemsSnapshotTable,
		ProbeUpperBound: mustReplayTestTime("2026-06-14T12:00:00Z"),
	}
	inspector := &recordingDavisCoverageInspector{coverage: DavisSnapshotCoverage{
		OldestSnapshot: mustReplayTestTime("2026-06-01T00:00:00Z"), ObservedAt: mustReplayTestTime("2026-08-14T12:00:00Z"),
	}}
	provider := NewMemoizedDavisSnapshotCoverageProvider(inspector)
	if _, reuse, err := provider.Coverage(context.Background(), base); err != nil || reuse != DavisSnapshotCoverageMiss {
		t.Fatalf("first lookup reuse=%s err=%v", reuse, err)
	}
	if _, reuse, err := provider.Coverage(context.Background(), base); err != nil || reuse != DavisSnapshotCoverageHit {
		t.Fatalf("second lookup reuse=%s err=%v", reuse, err)
	}
	changes := []func(DavisSnapshotCoverageKey) DavisSnapshotCoverageKey{
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.SessionID += "-new"; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.EnvironmentIdentity += "-new"; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.PrincipalIdentity += "-new"; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.Table += "-new"; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey {
			k.ProbeUpperBound = k.ProbeUpperBound.Add(time.Second)
			return k
		},
	}
	for _, change := range changes {
		if _, reuse, err := provider.Coverage(context.Background(), change(base)); err != nil || reuse != DavisSnapshotCoverageMiss {
			t.Fatalf("changed-key lookup reuse=%s err=%v", reuse, err)
		}
	}
	if got := inspector.calls.Load(); got != int32(1+len(changes)) {
		t.Fatalf("inspector calls = %d", got)
	}

	newInvocation := NewMemoizedDavisSnapshotCoverageProvider(inspector)
	if _, reuse, err := newInvocation.Coverage(context.Background(), base); err != nil || reuse != DavisSnapshotCoverageMiss {
		t.Fatalf("new invocation reuse=%s err=%v", reuse, err)
	}
	if inspector.calls.Load() != int32(2+len(changes)) {
		t.Fatalf("new invocation did not inspect afresh: %d", inspector.calls.Load())
	}
}

func TestDavisSnapshotCoverageMemoReusesFailClosedFailure(t *testing.T) {
	observedAt := mustReplayTestTime("2026-08-14T12:00:00Z")
	inspector := &recordingDavisCoverageInspector{
		coverage: DavisSnapshotCoverage{ObservedAt: observedAt},
		err:      &DavisSnapshotCoverageError{Failure: DavisCoverageMalformed, ObservedAt: observedAt},
	}
	provider := NewMemoizedDavisSnapshotCoverageProvider(inspector)
	key := DavisSnapshotCoverageKey{
		SessionID: "synthetic-session", EnvironmentIdentity: "synthetic-environment", PrincipalIdentity: "synthetic-principal",
		Table: execreplay.DavisProblemsSnapshotTable, ProbeUpperBound: mustReplayTestTime("2026-06-14T12:00:00Z"),
	}
	for attempt, wantReuse := range []DavisSnapshotCoverageReuse{DavisSnapshotCoverageMiss, DavisSnapshotCoverageHit} {
		coverage, reuse, err := provider.Coverage(context.Background(), key)
		if err == nil || reuse != wantReuse || !coverage.ObservedAt.Equal(observedAt) {
			t.Fatalf("attempt %d coverage=%#v reuse=%s err=%v", attempt+1, coverage, reuse, err)
		}
	}
	if inspector.calls.Load() != 1 {
		t.Fatalf("failed inspection calls = %d", inspector.calls.Load())
	}
}

func TestDavisSnapshotCoverageMemoRejectsIncompleteKeysWithoutInspecting(t *testing.T) {
	base := DavisSnapshotCoverageKey{
		SessionID: "synthetic-session", EnvironmentIdentity: "synthetic-environment",
		PrincipalIdentity: "synthetic-principal", Table: execreplay.DavisProblemsSnapshotTable,
		ProbeUpperBound: mustReplayTestTime("2026-06-14T12:00:00Z"),
	}
	inspector := &recordingDavisCoverageInspector{}
	provider := NewMemoizedDavisSnapshotCoverageProvider(inspector)
	incomplete := []func(DavisSnapshotCoverageKey) DavisSnapshotCoverageKey{
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.SessionID = ""; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.EnvironmentIdentity = ""; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.PrincipalIdentity = ""; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.Table = ""; return k },
		func(k DavisSnapshotCoverageKey) DavisSnapshotCoverageKey { k.ProbeUpperBound = time.Time{}; return k },
	}
	for index, mutate := range incomplete {
		if _, reuse, err := provider.Coverage(context.Background(), mutate(base)); err == nil || reuse != DavisSnapshotCoverageMiss {
			t.Fatalf("incomplete key %d reuse=%s err=%v", index, reuse, err)
		}
	}
	if inspector.calls.Load() != 0 {
		t.Fatalf("incomplete keys made %d inspector calls", inspector.calls.Load())
	}
}
