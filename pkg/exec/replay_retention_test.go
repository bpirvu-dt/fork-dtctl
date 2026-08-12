package exec

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

type replayRetentionInspectorFunc func(context.Context) (RetentionInspection, error)

func (f replayRetentionInspectorFunc) Inspect(ctx context.Context) (RetentionInspection, error) {
	return f(ctx)
}

func replayVerifiedRetentionInspector() RetentionInspector {
	return replayRetentionInspectorFunc(func(context.Context) (RetentionInspection, error) {
		tables := make(map[string]RetentionTableBounds)
		for _, table := range []string{"logs", "spans", "events", "bizevents", "metrics", "dt.system.events"} {
			tables[table] = RetentionTableBounds{
				Table: table, BucketCount: 1,
				MinimumRetentionDays: replayMaximumRetentionDays,
				MaximumRetentionDays: replayMaximumRetentionDays,
			}
		}
		return RetentionInspection{Tables: tables, HistoricalResolutionVerified: true}, nil
	})
}

type recordingRetentionInspector struct {
	inspection RetentionInspection
	err        error
	calls      atomic.Int32
}

func (i *recordingRetentionInspector) Inspect(context.Context) (RetentionInspection, error) {
	i.calls.Add(1)
	return i.inspection, i.err
}

func TestGrailRetentionInspectorUsesFixedBoundedReadOnlyQuery(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/platform/storage/query/v1/query:execute" {
			t.Errorf("retention request = %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request sdkquery.ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Query != replayRetentionInspectionDQL || request.RequestTimeoutMilliseconds != replayRetentionInspectionTimeout.Milliseconds() || request.PollingPromiseSeconds != 5 ||
			request.MaxResultRecords != 100 || request.DefaultScanLimitGbytes != 1 || request.DefaultSamplingRatio != 1 ||
			request.Locale != "en_US" || request.Timezone != "UTC" {
			t.Errorf("retention request = %#v", request)
		}
		lower := strings.ToLower(request.Query)
		for _, forbidden := range []string{"create ", "update ", "delete ", "fields name", "bucket.name"} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("retention query contains mutating or identifying surface %q: %q", forbidden, request.Query)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{
			{"dt.system.table": "logs", "bucket_count": "2", "minimum_retention_days": "35", "maximum_retention_days": "90"},
			{"dt.system.table": "metrics", "bucket_count": float64(1), "minimum_retention_days": float64(180), "maximum_retention_days": float64(462)},
		}}})
	}))
	defer server.Close()

	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := NewGrailRetentionInspector(transport).Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || len(inspection.Tables) != 2 || inspection.Tables["logs"].MinimumRetentionDays != 35 || inspection.HistoricalResolutionVerified {
		t.Fatalf("requests=%d inspection=%#v", requests.Load(), inspection)
	}
}

func TestGrailRetentionInspectorLatencyIsBounded(t *testing.T) {
	requestDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(requestDone)
	}))
	defer server.Close()
	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	inspector := &grailRetentionInspector{
		handler: sdkquery.NewHandler(httpclient.Wrap(transport.HTTP())),
		timeout: 25 * time.Millisecond,
	}
	started := time.Now()
	if _, err := inspector.Inspect(context.Background()); err == nil {
		t.Fatal("blocked inspection unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("inspection elapsed %s, want a bounded failure", elapsed)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("inspection cancellation did not reach the request")
	}
}

func TestRetentionNoticesUseKnownBoundsAndAdmitResolutionLimits(t *testing.T) {
	hostNow := mustReplayTestTime("2026-08-11T12:00:00Z")
	inspection := RetentionInspection{Tables: map[string]RetentionTableBounds{
		"logs":    {Table: "logs", BucketCount: 2, MinimumRetentionDays: 35, MaximumRetentionDays: 90},
		"metrics": {Table: "metrics", BucketCount: 1, MinimumRetentionDays: 180, MaximumRetentionDays: 462},
	}}
	record := []execreplay.SourceCompilation{{Source: execreplay.SourceDescriptor{Ordinal: 0, Class: execreplay.SourceRecord, Name: "logs"}}}
	metric := []execreplay.SourceCompilation{{Source: execreplay.SourceDescriptor{Ordinal: 0, Class: execreplay.SourceMetric, Name: "timeseries"}}}

	if got := retentionNotices(record, hostNow.Add(-34*24*time.Hour), hostNow, inspection, nil); len(got) != 0 {
		t.Fatalf("recent record notices = %#v", got)
	}
	got := retentionNotices(record, hostNow.Add(-36*24*time.Hour), hostNow, inspection, nil)
	if len(got) != 1 || got[0].Code != execreplay.NoticeRetentionBoundary || !strings.Contains(got[0].Message, "35 days") {
		t.Fatalf("known-boundary notices = %#v", got)
	}
	got = retentionNotices(metric, hostNow.Add(-24*time.Hour), hostNow, inspection, nil)
	if len(got) != 1 || got[0].Code != execreplay.NoticeHistoricalResolutionUnverified || !strings.Contains(got[0].Message, "not verified") {
		t.Fatalf("metric-resolution notices = %#v", got)
	}
	got = retentionNotices(record, hostNow.Add(-24*time.Hour), hostNow, RetentionInspection{}, errors.New("synthetic inspection failure"))
	if len(got) != 1 || got[0].Code != execreplay.NoticeRetentionNotVerified || !strings.Contains(got[0].Message, "not verified") {
		t.Fatalf("inspection-failure notices = %#v", got)
	}
}

func TestRetentionInspectionRejectsUntrustedMetadata(t *testing.T) {
	tests := []struct {
		name   string
		record map[string]interface{}
	}{
		{
			name: "unexpected table family",
			record: map[string]interface{}{
				"dt.system.table": "synthetic.table", "bucket_count": "1",
				"minimum_retention_days": "35", "maximum_retention_days": "90",
			},
		},
		{
			name: "fractional retention",
			record: map[string]interface{}{
				"dt.system.table": "logs", "bucket_count": "1",
				"minimum_retention_days": 1.5, "maximum_retention_days": "90",
			},
		},
		{
			name: "missing bound",
			record: map[string]interface{}{
				"dt.system.table": "logs", "bucket_count": "1", "minimum_retention_days": "35",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseRetentionBounds(tt.record); err == nil {
				t.Fatalf("parseRetentionBounds(%#v) unexpectedly succeeded", tt.record)
			}
		})
	}
	invalid := RetentionInspection{Tables: map[string]RetentionTableBounds{
		"logs": {Table: "logs", BucketCount: 1, MinimumRetentionDays: 90, MaximumRetentionDays: 35},
	}}
	if err := validateRetentionInspection(invalid); err == nil {
		t.Fatal("reversed retention bounds unexpectedly passed validation")
	}
}

func TestReplayRetentionInspectionWarnsOncePerExecutorAndNeverBlocks(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(
		t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		mustReplayTestTime("2026-06-01T00:00:00Z"), replayRecordVirtual, replayRecordDataEnd, nil,
	)
	inspector := &recordingRetentionInspector{inspection: RetentionInspection{Tables: map[string]RetentionTableBounds{
		"logs": {Table: "logs", BucketCount: 2, MinimumRetentionDays: 35, MaximumRetentionDays: replayMaximumRetentionDays},
	}, HistoricalResolutionVerified: true}}
	fixture.executor.preparer.(*ReplayQueryPreparer).config.RetentionInspector = inspector

	stderr := captureReplayExecutorStderr(t, func() {
		for attempt := 0; attempt < 2; attempt++ {
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{})
			if err != nil || result == nil || result.Response == nil {
				t.Fatalf("attempt %d result=%#v err=%v", attempt+1, result, err)
			}
		}
	})
	if inspector.calls.Load() != 1 {
		t.Fatalf("retention inspections = %d, want one per executor", inspector.calls.Load())
	}
	if strings.Count(stderr, "shortest current logs retention setting") != 1 {
		t.Fatalf("full-disclosure retention warnings = %q", stderr)
	}
	if _, executes, _ := api.counts(); executes != 2 {
		t.Fatalf("query executions = %d, want two successful attempts", executes)
	}
}

func TestRestrictedRetentionWarningsReachOnlyProvenance(t *testing.T) {
	tests := []struct {
		name      string
		dataStart time.Time
		inspector *recordingRetentionInspector
		code      execreplay.NoticeCode
		message   string
	}{
		{
			name: "known boundary", dataStart: mustReplayTestTime("2026-06-01T00:00:00Z"),
			inspector: &recordingRetentionInspector{inspection: RetentionInspection{Tables: map[string]RetentionTableBounds{
				"logs": {Table: "logs", BucketCount: 2, MinimumRetentionDays: 35, MaximumRetentionDays: replayMaximumRetentionDays},
			}, HistoricalResolutionVerified: true}},
			code: execreplay.NoticeRetentionBoundary, message: "shortest current logs retention setting",
		},
		{
			name: "inspection failure", dataStart: replayRecordDataStart,
			inspector: &recordingRetentionInspector{err: errors.New("synthetic inspection failure")},
			code:      execreplay.NoticeRetentionNotVerified, message: "not verified",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newReplayMockAPI(t)
			fixture := newReplayExecutorFixture(
				t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
				tt.dataStart, replayRecordVirtual, replayRecordDataEnd, nil,
			)
			fixture.executor.preparer.(*ReplayQueryPreparer).config.RetentionInspector = tt.inspector
			var result *DQLExecutionResult
			var runErr error
			stderr := captureReplayExecutorStderr(t, func() {
				result, runErr = fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{})
			})
			if runErr != nil || result == nil || result.Response == nil {
				t.Fatalf("result=%#v err=%v", result, runErr)
			}
			if stderr != "" || ReplayOutputMetadata(result.Replay) != nil {
				t.Fatalf("restricted warning leaked: stderr=%q metadata=%#v", stderr, ReplayOutputMetadata(result.Replay))
			}
			raw, err := os.ReadFile(fixture.started.ProvenancePath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), string(tt.code)) || !strings.Contains(string(raw), tt.message) {
				t.Fatalf("restricted provenance missing warning: %s", raw)
			}
			if tt.inspector.calls.Load() != 1 {
				t.Fatalf("retention inspections = %d, want one", tt.inspector.calls.Load())
			}
			if _, executes, _ := api.counts(); executes != 1 {
				t.Fatalf("query executions = %d, warning must not block", executes)
			}
		})
	}
}

func TestReplayMetricWarnsWhenHistoricalResolutionCannotBeInspected(t *testing.T) {
	api := newReplayMetricMockAPI(t)
	fixture := newReplayExecutorFixture(
		t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, nil,
	)
	inspector := &recordingRetentionInspector{inspection: RetentionInspection{Tables: map[string]RetentionTableBounds{
		"metrics": {Table: "metrics", BucketCount: 1, MinimumRetentionDays: 180, MaximumRetentionDays: 462},
	}}}
	fixture.executor.preparer.(*ReplayQueryPreparer).config.RetentionInspector = inspector
	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
	if err != nil || result == nil || result.Replay == nil || result.Replay.Output == nil {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	joined := strings.Join(result.Replay.Output.Warnings, "\n")
	if !strings.Contains(joined, "Historical metric resolution was not verified") {
		t.Fatalf("metric warnings = %q", joined)
	}
	if inspector.calls.Load() != 1 {
		t.Fatalf("retention inspections = %d, want one", inspector.calls.Load())
	}
}

func TestReplayRetentionInspectionSkipsExecutionFreeModes(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(
		t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil,
	)
	inspector := &recordingRetentionInspector{err: errors.New("must not be called")}
	fixture.executor.preparer.(*ReplayQueryPreparer).config.RetentionInspector = inspector

	if _, err := fixture.executor.ExplainReplayWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.VerifyReplayCompatibilityWithContext(context.Background(), replayRecordOriginal, DQLVerifyOptions{}, true); err != nil {
		t.Fatal(err)
	}
	if inspector.calls.Load() != 0 {
		t.Fatalf("execution-free retention inspections = %d, want zero", inspector.calls.Load())
	}
	if _, executes, _ := api.counts(); executes != 0 {
		t.Fatalf("execution-free query executions = %d, want zero", executes)
	}
}
