package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	"github.com/dynatrace-oss/dtctl/pkg/client"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const (
	replayRecordOriginal         = `fetch logs, from:toTimestamp("2026-08-10T10:45:02.718012207Z"), to:toTimestamp("2026-08-10T11:05:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	replayRecordEffective        = `fetch logs, from:toTimestamp("2026-08-10T10:50:02.718012207Z"), to:toTimestamp("2026-08-10T10:55:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	replayLoopOriginal           = `fetch logs, from:toTimestamp("2026-08-09T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
	replayPermanentQuery         = `fetch logs, timeframe:"2026-06-14T09:00:00Z/2026-06-14T10:00:00Z"`
	replayUnknownQuery           = `fetch logs, from:-1d@d`
	replayMetricOriginal         = `timeseries metric_value=avg(dt.host.cpu.usage), from:toTimestamp("2026-08-09T08:36:06Z"), to:toTimestamp("2026-08-10T12:50:06Z")`
	replayMetricEffective        = `timeseries metric_value=avg(dt.host.cpu.usage), from:toTimestamp("2026-08-09T10:36:06.000000000Z"), to:toTimestamp("2026-08-10T10:50:06.000000000Z")`
	replayDavisOriginal          = `fetch dt.davis.problems, from:toTimestamp("2026-06-14T09:00:00.000Z"), to:toTimestamp("2026-06-14T10:00:00.000Z")`
	replayDavisTimeframeOriginal = `fetch dt.davis.problems, timeframe:"2026-06-14T09:00:00Z/2026-06-14T10:00:00Z"`
	replayDavisEffective         = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")
| sort timestamp desc
| dedup event.id
| filter event.start < toTimestamp("2026-06-14T10:00:00.000000000Z") and coalesce(event.end, toTimestamp("2026-06-14T10:00:00.000000000Z")) >= toTimestamp("2026-06-14T09:00:00.000000000Z")`
)

var (
	replayRecordDataStart      = mustReplayTestTime("2026-08-10T10:50:02.718012207Z")
	replayRecordVirtual        = mustReplayTestTime("2026-08-10T10:55:02.718012207Z")
	replayRecordDataEnd        = mustReplayTestTime("2026-08-10T11:05:02.718012207Z")
	replayLoopDataStart        = mustReplayTestTime("2026-08-09T10:55:03Z")
	replayLoopDataEnd          = mustReplayTestTime("2026-08-10T10:55:03Z")
	replayMetricDataStart      = mustReplayTestTime("2026-08-09T10:36:06Z")
	replayMetricDataEnd        = mustReplayTestTime("2026-08-10T10:50:06Z")
	replayTestEnvironmentHash  = strings.Repeat("c", 64)
	replayTestContextInputHash = strings.Repeat("d", 64)
)

type replayExecutorFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *replayExecutorFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *replayExecutorFakeClock) Add(value time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(value)
	c.mu.Unlock()
}

type replayMockAPI struct {
	t                        *testing.T
	originalBody             json.RawMessage
	validationBody           json.RawMessage
	parseStatus              int
	effectiveStatus          int
	executeStatus            int
	pollStatus               int
	executeResponse          sdkquery.Response
	coverageResponse         sdkquery.Response
	coverageStatus           int
	coverageRawBody          []byte
	async                    bool
	disableDynamicValidation bool
	onPoll                   func()
	isOriginal               func(string) bool
	beforeExecuteResponse    func()
	beforeCoverageResponse   func()
	retryAfter               string
	remoteErrorMessage       string
	pollErrorMessage         string
	parseErrorMessage        string

	mu               sync.Mutex
	parseRequests    []sdkquery.ParseRequest
	executeRequests  []sdkquery.ExecuteRequest
	coverageRequests []sdkquery.ExecuteRequest
	pollCalls        int
	requestOrder     []string
}

func newReplayMockAPI(t *testing.T) *replayMockAPI {
	return &replayMockAPI{
		t:              t,
		originalBody:   replayFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		executeResponse: sdkquery.Response{
			State: "SUCCEEDED",
			Result: &sdkquery.Result{
				Records:  []map[string]interface{}{{"matched": float64(1)}},
				Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{CanonicalQuery: replayRecordEffective}},
			},
		},
		coverageResponse: sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{
			"oldest_snapshot": "2026-06-14T02:00:00.000000000Z",
		}}}},
	}
}

func newReplayMetricMockAPI(t *testing.T) *replayMockAPI {
	t.Helper()
	api := newReplayMockAPI(t)
	api.originalBody = replayFixtureBody(t, "phase0b/fixtures/metrics/01-automatic/parse.json")
	api.validationBody = replayFixtureBody(t, "phase0b/fixtures/metrics/01-automatic/validation-parse.json")
	api.isOriginal = func(query string) bool { return query == replayMetricOriginal }
	points := make([]interface{}, 147)
	for index := range points {
		points[index] = float64(index)
	}
	api.executeResponse = sdkquery.Response{
		State: "SUCCEEDED",
		Result: &sdkquery.Result{
			Records: []map[string]interface{}{{
				"timeframe": map[string]interface{}{
					"start": "2026-08-09T10:30:00Z", "end": "2026-08-10T11:00:00Z",
				},
				"interval": "600000000000", "metric_value": points,
			}},
			Metadata: &sdkquery.Metadata{
				Grail:   &sdkquery.GrailMetadata{CanonicalQuery: replayMetricEffective},
				Metrics: []sdkquery.MetricInfo{{MetricKey: "dt.host.cpu.usage", FieldName: "metric_value", Aggregation: "avg"}},
			},
		},
	}
	return api
}

func newReplayDavisMockAPI(t *testing.T) *replayMockAPI {
	t.Helper()
	api := newReplayMockAPI(t)
	api.originalBody = replayFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/original/parse.json")
	api.validationBody = replayFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/effective/parse.json")
	api.isOriginal = func(query string) bool { return query == replayDavisOriginal }
	api.executeResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
		Records:  []map[string]interface{}{{"mapped": true}},
		Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{CanonicalQuery: replayDavisEffective}},
	}}
	return api
}

func (a *replayMockAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/platform/storage/query/v1/query:parse":
		var request sdkquery.ParseRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode parse request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.parseRequests = append(a.parseRequests, request)
		isOriginal := strings.Contains(request.Query, `2026-08-10T10:45:02.718012207Z`)
		if a.isOriginal != nil {
			isOriginal = a.isOriginal(request.Query)
		}
		status := a.effectiveStatus
		body := a.validationBody
		if isOriginal {
			status, body = a.parseStatus, a.originalBody
		}
		if isOriginal {
			a.requestOrder = append(a.requestOrder, "original_parse")
		} else {
			a.requestOrder = append(a.requestOrder, "effective_parse")
		}
		a.mu.Unlock()
		if status != 0 && status != http.StatusOK {
			if a.retryAfter != "" {
				w.Header().Set("Retry-After", a.retryAfter)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if a.parseErrorMessage == "" {
				_, _ = w.Write([]byte(`{"error":{"message":"query request failed"}}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "query failed", "details": map[string]any{"errorType": "REMOTE_ERROR", "errorMessage": a.parseErrorMessage},
			}})
			return
		}
		if !a.disableDynamicValidation {
			body = dynamicValidationBody(a.t, body, request.Query)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	case "/platform/storage/query/v1/query:execute":
		var request sdkquery.ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode execute request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.Contains(request.Query, "| summarize oldest_snapshot=min(timestamp)") {
			a.mu.Lock()
			a.coverageRequests = append(a.coverageRequests, request)
			a.requestOrder = append(a.requestOrder, "coverage")
			a.mu.Unlock()
			if a.beforeCoverageResponse != nil {
				a.beforeCoverageResponse()
			}
			w.Header().Set("Content-Type", "application/json")
			if a.coverageStatus != 0 && a.coverageStatus != http.StatusOK {
				if a.retryAfter != "" {
					w.Header().Set("Retry-After", a.retryAfter)
				}
				w.WriteHeader(a.coverageStatus)
				_, _ = w.Write([]byte(`{"error":{"message":"synthetic coverage failure"}}`))
				return
			}
			if a.coverageRawBody != nil {
				_, _ = w.Write(a.coverageRawBody)
				return
			}
			_ = json.NewEncoder(w).Encode(a.coverageResponse)
			return
		}
		a.mu.Lock()
		a.executeRequests = append(a.executeRequests, request)
		a.requestOrder = append(a.requestOrder, "main_execute")
		a.mu.Unlock()
		if a.beforeExecuteResponse != nil {
			a.beforeExecuteResponse()
		}
		if a.executeStatus != 0 && a.executeStatus != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			if a.retryAfter != "" {
				w.Header().Set("Retry-After", a.retryAfter)
			}
			w.WriteHeader(a.executeStatus)
			message := a.remoteErrorMessage
			if message == "" {
				message = "remote failure"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "query failed", "details": map[string]any{"errorType": "REMOTE_ERROR", "errorMessage": message},
			}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if a.async {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "RUNNING", RequestToken: "synthetic-request-token"})
			return
		}
		_ = json.NewEncoder(w).Encode(a.executeResponse)
	case "/platform/storage/query/v1/query:poll":
		a.mu.Lock()
		a.pollCalls++
		a.mu.Unlock()
		if a.onPoll != nil {
			a.onPoll()
		}
		w.Header().Set("Content-Type", "application/json")
		if a.pollStatus != 0 && a.pollStatus != http.StatusOK {
			w.WriteHeader(a.pollStatus)
			message := a.pollErrorMessage
			if message == "" {
				message = "poll failed"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "query failed", "details": map[string]any{"errorType": "REMOTE_ERROR", "errorMessage": message},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(a.executeResponse)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *replayMockAPI) counts() (int, int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.parseRequests), len(a.executeRequests), a.pollCalls
}

func (a *replayMockAPI) queries() ([]sdkquery.ParseRequest, []sdkquery.ExecuteRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]sdkquery.ParseRequest(nil), a.parseRequests...), append([]sdkquery.ExecuteRequest(nil), a.executeRequests...)
}

func (a *replayMockAPI) coverage() ([]sdkquery.ExecuteRequest, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]sdkquery.ExecuteRequest(nil), a.coverageRequests...), append([]string(nil), a.requestOrder...)
}

type replayExecutorFixture struct {
	executor *DQLExecutor
	store    *session.ReplayStateStore
	clock    *replayExecutorFakeClock
	locator  session.ReplayLocator
	started  session.ReplaySession
	api      *replayMockAPI
}

type replayTestOrder struct {
	mu     sync.Mutex
	values []string
}

func (o *replayTestOrder) add(value string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.values = append(o.values, value)
	o.mu.Unlock()
}

func (o *replayTestOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.values...)
}

type replayTestSink struct {
	mu             sync.Mutex
	preflightErr   error
	appendFailures map[int]error
	preflightCalls int
	appendCalls    int
	records        []session.ReplayProvenanceRecord
	order          *replayTestOrder
}

func (s *replayTestSink) Preflight(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preflightCalls++
	return s.preflightErr
}

func (s *replayTestSink) Append(_ context.Context, record session.ReplayProvenanceRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendCalls++
	s.order.add("append:" + record.Event)
	if err := s.appendFailures[s.appendCalls]; err != nil {
		return err
	}
	s.records = append(s.records, record)
	return nil
}

func (s *replayTestSink) snapshot() (int, int, []session.ReplayProvenanceRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preflightCalls, s.appendCalls, append([]session.ReplayProvenanceRecord(nil), s.records...)
}

type replayTrackingStore struct {
	session.ReplayStore
	order        *replayTestOrder
	mu           sync.Mutex
	dispositions []session.CompletionDisposition
}

type replayFailingCompletionStore struct {
	session.ReplayStore
	err error
}

func (s *replayFailingCompletionStore) MarkCompleted(session.ReplayLocator, string, time.Time) (session.ReplaySession, session.CompletionDisposition, error) {
	return session.ReplaySession{}, "", s.err
}

func (s *replayTrackingStore) MarkCompleted(locator session.ReplayLocator, id string, now time.Time) (session.ReplaySession, session.CompletionDisposition, error) {
	s.order.add("mark-completed")
	state, disposition, err := s.ReplayStore.MarkCompleted(locator, id, now)
	s.mu.Lock()
	s.dispositions = append(s.dispositions, disposition)
	s.mu.Unlock()
	return state, disposition, err
}

func (s *replayTrackingStore) completionDispositions() []session.CompletionDisposition {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]session.CompletionDisposition(nil), s.dispositions...)
}

func newReplayExecutorFixture(t *testing.T, api *replayMockAPI, clockMode, disclosure string, dataStart, virtualStart, dataEnd time.Time, sinkFactory func(string) session.ProvenanceSink) replayExecutorFixture {
	t.Helper()
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	clock := &replayExecutorFakeClock{now: mustReplayTestTime("2026-08-11T12:00:00Z")}
	stateDir := filepath.Join(t.TempDir(), "replay")
	locator := session.ReplayLocator{ContextKey: session.ContextKey(strings.Repeat("a", 64)), ContextIdentityHash: strings.Repeat("b", 64)}
	provenancePath := ""
	if disclosure == session.ReplayDisclosureRestricted {
		provenancePath = filepath.Join(stateDir, "provenance.jsonl")
	}
	store := session.NewReplayStateStore(stateDir, clock)
	started, err := store.Start(session.ReplayStartRequest{
		Locator: locator, ContextName: "synthetic-replay", EnvironmentHash: replayTestEnvironmentHash, ContextInputHash: replayTestContextInputHash,
		Config: session.ResolvedReplayConfig{
			DataStart: dataStart, DataEnd: dataEnd, VirtualStart: virtualStart,
			ClockMode: clockMode, Disclosure: disclosure, ProvenancePath: provenancePath,
			ValueSources: session.ReplayValueSources{
				DataStart: session.ReplayValueFromContext, DataEnd: session.ReplayValueFromContext,
				VirtualStart: session.ReplayValueFromContext, ClockMode: session.ReplayValueFromContext,
				Disclosure: session.ReplayValueFromContext, ProvenancePath: session.ReplayValueFromDefault,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sinkFactory == nil {
		sinkFactory = func(path string) session.ProvenanceSink { return session.NewFileProvenanceSink(path, stateDir) }
	}
	preparer, err := NewReplayQueryPreparer(ReplayPreparerConfig{
		Store: store, Clock: clock, Locator: locator, ContextName: "synthetic-replay",
		ExpectedContextInputHash: replayTestContextInputHash, ExpectedEnvironmentHash: replayTestEnvironmentHash,
		EnvironmentID: replayTestEnvironmentHash, ClientIdentity: "synthetic-principal",
		FallbackDisclosure: disclosure, FallbackProvenancePath: provenancePath,
		SourcePolicy: execreplay.Milestone1SourcePolicy(), RetentionInspector: replayVerifiedRetentionInspector(),
		DavisCoverage: NewMemoizedDavisSnapshotCoverageProvider(NewDavisSnapshotCoverageInspector(transport)), SinkFactory: sinkFactory,
	})
	if err != nil {
		t.Fatal(err)
	}
	return replayExecutorFixture{
		executor: NewDQLExecutor(transport).WithQueryPreparer(preparer), store: store,
		clock: clock, locator: locator, started: started, api: api,
	}
}

func (f replayExecutorFixture) useStore(store session.ReplayStore) {
	f.executor.preparer.(*ReplayQueryPreparer).config.Store = store
}

func restartReplayExecutorFixture(f replayExecutorFixture) (session.ReplaySession, error) {
	state, err := f.store.Status(f.locator)
	if err != nil {
		return session.ReplaySession{}, err
	}
	return f.store.Start(session.ReplayStartRequest{
		Locator: f.locator, ContextName: state.ContextName,
		EnvironmentHash: state.EnvironmentHash, ContextInputHash: state.ContextInputHash,
		Restart: true,
		Config: session.ResolvedReplayConfig{
			DataStart: state.DataStart, DataEnd: state.DataEnd, VirtualStart: state.VirtualStart,
			ClockMode: state.ClockMode, Disclosure: state.Disclosure, ProvenancePath: state.ProvenancePath,
			ValueSources: state.ValueSources,
		},
	})
}

func TestDQLExecutorReplayPipelineMemoAndEffectiveExecution(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
	opts := DQLExecuteOptions{AgentMode: true, Locale: "en_US", Timezone: "UTC", ParserOptions: sdkquery.QueryOptions{"mode": "strict"}}
	var memoBytes []byte
	for attempt := 0; attempt < 2; attempt++ {
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt+1, err)
		}
		if result.Response == nil || result.Replay == nil || !result.Replay.VirtualNow.Equal(replayRecordVirtual) {
			t.Fatalf("attempt %d result = %#v", attempt+1, result)
		}
		provider := fixture.executor.originalASTs.(*MemoizedOriginalASTProvider)
		provider.mu.Lock()
		entryCount := len(provider.entries)
		if entryCount != 1 {
			provider.mu.Unlock()
			t.Fatalf("attempt %d memo entries = %d, want 1", attempt+1, entryCount)
		}
		var view OriginalASTView
		for _, entry := range provider.entries {
			view = entry.view
		}
		provider.mu.Unlock()
		currentBytes := view.Bytes()
		currentAST, err := view.AST()
		if err != nil {
			t.Fatal(err)
		}
		structuralBytes, err := json.Marshal(currentAST)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(currentBytes, structuralBytes) {
			t.Fatalf("attempt %d memo bytes and structure disagree", attempt+1)
		}
		if attempt == 0 {
			memoBytes = append([]byte(nil), currentBytes...)
		} else if !bytes.Equal(currentBytes, memoBytes) {
			t.Fatalf("attempt %d changed the memoized original AST", attempt+1)
		}
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 3 || executeCalls != 2 {
		t.Fatalf("parse=%d execute=%d, want one original + two effective parses and two executions", parseCalls, executeCalls)
	}
	parses, executions := api.queries()
	if parses[0].Query != replayRecordOriginal || parses[1].Query != replayRecordEffective || executions[0].Query != replayRecordEffective {
		t.Fatalf("unexpected pipeline queries: parse=%q/%q execute=%q", parses[0].Query, parses[1].Query, executions[0].Query)
	}
	if parses[0].Locale != "en_US" || parses[0].Timezone != "UTC" || parses[0].QueryOptions["mode"] != "strict" {
		t.Fatalf("original parse lost complete-key inputs: %#v", parses[0])
	}

	// A byte change is a distinct original-parse key even when its semantics
	// and token positions remain compatible.
	if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal+" ", opts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, _ = api.counts()
	if parseCalls != 5 || executeCalls != 3 {
		t.Fatalf("after changed bytes parse=%d execute=%d, want 5 and 3", parseCalls, executeCalls)
	}

	// A separate top-level executor owns an empty memo.
	secondPreparer, err := NewReplayQueryPreparer(fixture.executor.preparer.(*ReplayQueryPreparer).config)
	if err != nil {
		t.Fatal(err)
	}
	second := NewDQLExecutor(fixture.executor.client).WithQueryPreparer(secondPreparer)
	if _, err := second.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, _ = api.counts()
	if parseCalls != 7 || executeCalls != 4 {
		t.Fatalf("second invocation parse=%d execute=%d, want fresh original+effective parse", parseCalls, executeCalls)
	}
}

func TestDQLExecutorDavisProblemsMappingCoverageParseAuditExecuteOrder(t *testing.T) {
	tests := []struct {
		name         string
		original     string
		originalBody json.RawMessage
	}{
		{"from and to", replayDavisOriginal, replayFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/original/parse.json")},
		{"timeframe", replayDavisTimeframeOriginal, replayFixtureWithSourceToken(t, "phase0/fixtures/07-fetch-explicit-timeframe/parse.json", "logs", execreplay.DavisProblemsView)},
	}
	for _, test := range tests {
		var fullParses []sdkquery.ParseRequest
		var fullExecutions, fullCoverage []sdkquery.ExecuteRequest
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api := newReplayDavisMockAPI(t)
				api.originalBody = test.originalBody
				api.isOriginal = func(query string) bool { return query == test.original }
				var sink *replayTestSink
				var sinkFactory func(string) session.ProvenanceSink
				if disclosure == session.ReplayDisclosureRestricted {
					sink = &replayTestSink{}
					sinkFactory = func(string) session.ProvenanceSink { return sink }
				}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
					mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)

				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.original, DQLExecuteOptions{AgentMode: true})
				if err != nil {
					t.Fatal(err)
				}
				parseCalls, executeCalls, _ := api.counts()
				coverageRequests, order := api.coverage()
				if parseCalls != 2 || executeCalls != 1 || len(coverageRequests) != 1 ||
					!equalStrings(order, []string{"original_parse", "coverage", "effective_parse", "main_execute"}) {
					t.Fatalf("parse=%d coverage=%d execute=%d order=%v", parseCalls, len(coverageRequests), executeCalls, order)
				}
				parses, executions := api.queries()
				if len(parses) != 2 || parses[0].Query != test.original || parses[1].Query != replayDavisEffective ||
					len(executions) != 1 || executions[0].Query != replayDavisEffective {
					t.Fatalf("queries: parses=%#v executes=%#v", parses, executions)
				}
				if !strings.Contains(coverageRequests[0].Query, `to:toTimestamp("2026-06-14T12:00:00.000000000Z")`) {
					t.Fatalf("coverage upper bound = %q", coverageRequests[0].Query)
				}
				if disclosure == session.ReplayDisclosureFull {
					fullParses = append([]sdkquery.ParseRequest(nil), parses...)
					fullExecutions = append([]sdkquery.ExecuteRequest(nil), executions...)
					fullCoverage = append([]sdkquery.ExecuteRequest(nil), coverageRequests...)
				} else if !reflect.DeepEqual(parses, fullParses) ||
					!reflect.DeepEqual(executions, fullExecutions) || !reflect.DeepEqual(coverageRequests, fullCoverage) {
					t.Fatalf("restricted requests differ from full: parses=%#v/full=%#v executes=%#v/full=%#v coverage=%#v/full=%#v",
						parses, fullParses, executions, fullExecutions, coverageRequests, fullCoverage)
				}
				if result == nil || result.Replay == nil || result.Replay.Output == nil {
					t.Fatalf("result = %#v", result)
				}
				metadata := result.Replay.Output
				if len(metadata.Sources) != 1 || metadata.Sources[0].DavisProblemsMapping == nil ||
					metadata.Sources[0].DavisProblemsMapping.LogicalF != "2026-06-14T09:00:00Z" ||
					metadata.Sources[0].DavisProblemsMapping.PhysicalW != "2026-06-14T03:00:00Z" ||
					metadata.DavisSnapshotCoverage == nil || !metadata.DavisSnapshotCoverage.Verified ||
					metadata.DavisSnapshotCoverage.Reuse != string(DavisSnapshotCoverageMiss) || !metadata.DavisMappingsAudited {
					t.Fatalf("mapped metadata = %#v", metadata)
				}
				if !containsReplayNotice(metadata.Warnings, "Mapped dt.davis.problems") {
					t.Fatalf("mapping notification missing from metadata: %#v", metadata.Warnings)
				}
				if sink != nil {
					preflights, appends, records := sink.snapshot()
					if preflights != 1 || appends != 2 || len(records) != 2 || records[0].Event != "query_notice" || records[1].Event != "query_execution" {
						t.Fatalf("restricted provenance preflights=%d appends=%d records=%#v", preflights, appends, records)
					}
					if !strings.Contains(fmt.Sprint(records[0].Fields["notices"]), "Mapped dt.davis.problems") ||
						records[1].Fields["original_dql"] != test.original ||
						records[1].Fields["effective_dql"] != replayDavisEffective ||
						records[1].Fields["grail_canonical_effective_dql"] != replayDavisEffective {
						t.Fatalf("restricted mapped provenance = %#v", records)
					}
					coverage, ok := records[1].Fields["davis_snapshot_coverage"].(map[string]any)
					if !ok || coverage["status"] != "verified" || coverage["coverage_verified"] != true ||
						coverage["oldest_snapshot"] == "" || coverage["observed_at"] == "" ||
						fmt.Sprint(coverage["reuse"]) != string(DavisSnapshotCoverageMiss) {
						t.Fatalf("restricted coverage provenance = %#v", coverage)
					}
					sources, ok := records[1].Fields["sources"].([]map[string]any)
					if !ok || len(sources) != 1 {
						t.Fatalf("restricted source provenance = %#v", records[1].Fields["sources"])
					}
					mapping, ok := sources[0]["davis_problems_mapping"].(map[string]any)
					if !ok || mapping["warmup_clamped"] != false {
						t.Fatalf("restricted mapping provenance = %#v", sources[0])
					}
					logical := mapping["logical_view_range"].(map[string]string)
					physical := mapping["physical_snapshot_range"].(map[string]string)
					if logical["f"] != "2026-06-14T09:00:00Z" || logical["t"] != "2026-06-14T10:00:00Z" ||
						physical["w"] != "2026-06-14T03:00:00Z" || physical["t"] != "2026-06-14T10:00:00Z" {
						t.Fatalf("restricted F/T/W provenance logical=%#v physical=%#v", logical, physical)
					}
					audit, ok := records[1].Fields["audit"].(map[string]any)
					if !ok || audit["ok"] != true || audit["davis_mappings_audited"] != true {
						t.Fatalf("restricted audit provenance = %#v", audit)
					}
				}
			})
		}
	}
}

func TestDQLExecutorDavisCoverageFailuresAreFailClosedBeforeEffectiveParse(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*replayMockAPI)
		want   string
	}{
		{"request failure", func(api *replayMockAPI) { api.coverageStatus = http.StatusServiceUnavailable }, execreplay.DavisCoverageInspectionFailedMessage},
		{"empty result", func(api *replayMockAPI) {
			api.coverageResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{}}}
		}, execreplay.DavisCoverageInspectionFailedMessage},
		{"malformed result", func(api *replayMockAPI) {
			api.coverageResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{"oldest_snapshot": 42}}}}
		}, execreplay.DavisCoverageInspectionFailedMessage},
		{"scan-limit notification", func(api *replayMockAPI) {
			api.coverageResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
				Records: []map[string]interface{}{{"oldest_snapshot": "2026-06-14T02:00:00Z"}},
				Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{Notifications: []sdkquery.Notification{{
					NotificationType: "SCAN_LIMIT_GBYTES", Message: "synthetic",
				}}}},
			}}
		}, execreplay.DavisCoverageInspectionFailedMessage},
		{"asynchronous result", func(api *replayMockAPI) {
			api.coverageResponse = sdkquery.Response{State: "RUNNING", RequestToken: "synthetic-request"}
		}, execreplay.DavisCoverageInspectionFailedMessage},
		{"insufficient result", func(api *replayMockAPI) {
			api.coverageResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{
				"oldest_snapshot": "2026-06-14T03:00:00.000000001Z",
			}}}}
		}, execreplay.DavisCoverageInsufficientMessage},
	}
	for _, test := range tests {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api := newReplayDavisMockAPI(t)
				test.mutate(api)
				var sink *replayTestSink
				var sinkFactory func(string) session.ProvenanceSink
				if disclosure == session.ReplayDisclosureRestricted {
					sink = &replayTestSink{}
					sinkFactory = func(string) session.ProvenanceSink { return sink }
				}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
					mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
				if disclosure == session.ReplayDisclosureFull {
					if result != nil || err == nil || !strings.HasSuffix(err.Error(), test.want) ||
						!strings.Contains(err.Error(), "dt.davis.problems.snapshots") || !strings.Contains(err.Error(), "dedup event.id") {
						t.Fatalf("result=%#v error=%v", result, err)
					}
				} else {
					if result != nil || err == nil || err.Error() != restrictedPreparationMessage {
						t.Fatalf("result=%#v restricted error=%v", result, err)
					}
					preflights, appends, records := sink.snapshot()
					if preflights != 1 || appends != 1 || len(records) != 1 || records[0].Event != "query_pre_execution" ||
						!strings.HasSuffix(fmt.Sprint(records[0].Fields["detail"]), test.want) {
						t.Fatalf("restricted coverage provenance preflights=%d appends=%d records=%#v", preflights, appends, records)
					}
				}
				parseCalls, executeCalls, _ := api.counts()
				coverageRequests, _ := api.coverage()
				if parseCalls != 1 || len(coverageRequests) != 1 || executeCalls != 0 {
					t.Fatalf("parse=%d coverage=%d main execute=%d", parseCalls, len(coverageRequests), executeCalls)
				}
			})
		}
	}
}

func TestDQLExecutorRestrictedDavisEffectiveParseFailureIsGenericAndPrivate(t *testing.T) {
	api := newReplayDavisMockAPI(t)
	api.effectiveStatus = http.StatusBadRequest
	api.parseErrorMessage = `generated source dt.davis.problems.snapshots at toTimestamp("2026-06-14T03:00:00.000000000Z")`
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
		mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"),
		func(string) session.ProvenanceSink { return sink })

	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
	if result != nil || err == nil || err.Error() != restrictedPreparationMessage || strings.Contains(err.Error(), execreplay.DavisProblemsSnapshotTable) {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	parseCalls, executeCalls, _ := api.counts()
	coverageRequests, _ := api.coverage()
	if parseCalls != 2 || len(coverageRequests) != 1 || executeCalls != 0 {
		t.Fatalf("parse=%d coverage=%d main execute=%d", parseCalls, len(coverageRequests), executeCalls)
	}
	preflights, appends, records := sink.snapshot()
	if preflights != 1 || appends != 1 || len(records) != 1 || records[0].Event != "query_pre_execution" ||
		!strings.Contains(fmt.Sprint(records[0].Fields["detail"]), execreplay.DavisProblemsSnapshotTable) {
		t.Fatalf("restricted effective-parse provenance preflights=%d appends=%d records=%#v", preflights, appends, records)
	}
}

func TestDQLExecutorDavisCoverageMemoReusesWithinInvocationAndMissesAfterRestart(t *testing.T) {
	api := newReplayDavisMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), nil)
	opts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, opts)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt+1, err)
		}
		wantReuse := string(DavisSnapshotCoverageMiss)
		if attempt == 1 {
			wantReuse = string(DavisSnapshotCoverageHit)
		}
		if result.Replay.Output.DavisSnapshotCoverage.Reuse != wantReuse {
			t.Fatalf("attempt %d reuse = %q", attempt+1, result.Replay.Output.DavisSnapshotCoverage.Reuse)
		}
	}
	parseCalls, executeCalls, _ := api.counts()
	coverageRequests, _ := api.coverage()
	if parseCalls != 3 || executeCalls != 2 || len(coverageRequests) != 1 {
		t.Fatalf("before restart parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
	}
	if _, err := restartReplayExecutorFixture(fixture); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, opts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, _ = api.counts()
	coverageRequests, _ = api.coverage()
	if parseCalls != 4 || executeCalls != 3 || len(coverageRequests) != 2 {
		t.Fatalf("after restart parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
	}
}

func TestDQLExecutorDavisExplainAndVerifyNeverConsultPopulatedCoverageMemo(t *testing.T) {
	api := newReplayDavisMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), nil)
	if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true}); err != nil {
		t.Fatal(err)
	}
	explanation, err := fixture.executor.ExplainReplayWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	if explanation.CoverageVerified == nil || *explanation.CoverageVerified ||
		explanation.CoverageMessage != execreplay.DavisCoverageNotVerifiedMessage || explanation.EffectiveDQL != replayDavisEffective ||
		len(explanation.Sources) != 1 || explanation.Sources[0].DavisMapping == nil {
		t.Fatalf("explanation = %#v", explanation)
	}
	verification, err := fixture.executor.VerifyReplayCompatibilityWithContext(context.Background(), replayDavisOriginal, DQLVerifyOptions{Timezone: "UTC"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if verification == nil || verification.CoverageVerified == nil || *verification.CoverageVerified ||
		verification.CoverageMessage != execreplay.DavisCoverageNotVerifiedMessage || verification.EffectiveQuery != replayDavisEffective ||
		!verification.CompilerSupported || !verification.EffectiveQueryValid {
		t.Fatalf("verification = %#v", verification)
	}
	parseCalls, executeCalls, _ := api.counts()
	coverageRequests, _ := api.coverage()
	if parseCalls != 4 || executeCalls != 1 || len(coverageRequests) != 1 {
		t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
	}
}

func TestDQLExecutorDavisMappedAttemptsRecomputeRangesWithOneCoverageProbe(t *testing.T) {
	for _, mode := range []ReplayExecutionMode{ReplayExecutionWait, ReplayExecutionLive} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(string(mode)+"/"+disclosure, func(t *testing.T) {
				api := newReplayDavisMockAPI(t)
				var sink *replayTestSink
				var sinkFactory func(string) session.ProvenanceSink
				if disclosure == session.ReplayDisclosureRestricted {
					sink = &replayTestSink{}
					sinkFactory = func(string) session.ProvenanceSink { return sink }
				}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, disclosure,
					mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T09:30:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)
				opts := DQLExecuteOptions{AgentMode: true, ReplayMode: mode}
				first, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, opts)
				if err != nil {
					t.Fatal(err)
				}
				fixture.clock.Add(5 * time.Minute)
				second, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, opts)
				if err != nil {
					t.Fatal(err)
				}
				left := first.Replay.Output.Sources[0].DavisProblemsMapping
				right := second.Replay.Output.Sources[0].DavisProblemsMapping
				if left.LogicalT == right.LogicalT || left.PhysicalT == right.PhysicalT || left.LogicalF != right.LogicalF || left.PhysicalW != right.PhysicalW {
					t.Fatalf("attempt ranges: first=%#v second=%#v", left, right)
				}
				if disclosure == session.ReplayDisclosureRestricted {
					if ReplayOutputMetadata(first.Replay) != nil || ReplayOutputMetadata(second.Replay) != nil {
						t.Fatal("restricted mapped attempt exposed ordinary replay metadata")
					}
					preflights, appends, records := sink.snapshot()
					noticeRecords := 0
					var coverageReuse []string
					for _, record := range records {
						if record.Event == "query_notice" && strings.Contains(fmt.Sprint(record.Fields["notices"]), "Mapped dt.davis.problems") {
							noticeRecords++
						}
						if record.Event == "query_execution" {
							coverage, ok := record.Fields["davis_snapshot_coverage"].(map[string]any)
							if !ok {
								t.Fatalf("restricted execution omitted coverage provenance: %#v", record.Fields)
							}
							coverageReuse = append(coverageReuse, fmt.Sprint(coverage["reuse"]))
						}
					}
					if preflights != 2 || appends != 4 || len(records) != 4 || noticeRecords != 2 ||
						!equalStrings(coverageReuse, []string{string(DavisSnapshotCoverageMiss), string(DavisSnapshotCoverageHit)}) {
						t.Fatalf("restricted attempts preflights=%d appends=%d notice-records=%d reuse=%v records=%#v", preflights, appends, noticeRecords, coverageReuse, records)
					}
				} else if !containsReplayNotice(first.Replay.Output.Warnings, "Mapped dt.davis.problems") ||
					!containsReplayNotice(second.Replay.Output.Warnings, "Mapped dt.davis.problems") {
					t.Fatalf("full mapped attempts did not each announce mapping: first=%#v second=%#v", first.Replay.Output.Warnings, second.Replay.Output.Warnings)
				}
				parseCalls, executeCalls, _ := api.counts()
				coverageRequests, _ := api.coverage()
				if parseCalls != 3 || executeCalls != 2 || len(coverageRequests) != 1 {
					t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
				}
				parses, executions := api.queries()
				if len(parses) != 3 || parses[0].Query != replayDavisOriginal || len(executions) != 2 ||
					parses[1].Query != executions[0].Query || parses[2].Query != executions[1].Query || executions[0].Query == executions[1].Query {
					t.Fatalf("attempt queries were not independently parsed and executed: parses=%#v executions=%#v", parses, executions)
				}
				wantDQL := func(logicalF, physicalW, timeT string) string {
					return fmt.Sprintf(`fetch dt.davis.problems.snapshots, from:toTimestamp("%s.000000000Z"), to:toTimestamp("%s.000000000Z")
| sort timestamp desc
| dedup event.id
| filter event.start < toTimestamp("%s.000000000Z") and coalesce(event.end, toTimestamp("%s.000000000Z")) >= toTimestamp("%s.000000000Z")`, physicalW, timeT, timeT, timeT, logicalF)
				}
				for index, attempt := range []struct {
					mapping                             *output.DavisProblemsMappingMetadata
					logicalF, logicalT, physicalW, want string
				}{
					{left, "2026-06-14T09:00:00Z", "2026-06-14T09:30:00Z", "2026-06-14T03:00:00Z", wantDQL("2026-06-14T09:00:00", "2026-06-14T03:00:00", "2026-06-14T09:30:00")},
					{right, "2026-06-14T09:00:00Z", "2026-06-14T09:35:00Z", "2026-06-14T03:00:00Z", wantDQL("2026-06-14T09:00:00", "2026-06-14T03:00:00", "2026-06-14T09:35:00")},
				} {
					if attempt.mapping.LogicalF != attempt.logicalF || attempt.mapping.LogicalT != attempt.logicalT ||
						attempt.mapping.PhysicalW != attempt.physicalW || attempt.mapping.PhysicalT != attempt.logicalT ||
						parses[index+1].Query != attempt.want || executions[index].Query != attempt.want {
						t.Fatalf("attempt %d mapping=%#v parse=%q execute=%q want=%q", index+1, attempt.mapping, parses[index+1].Query, executions[index].Query, attempt.want)
					}
				}
			})
		}
	}
}

func TestDQLExecutorDavisWarmupClampWarnsOnlyAfterCoveragePasses(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		t.Run(disclosure, func(t *testing.T) {
			api := newReplayDavisMockAPI(t)
			var sink *replayTestSink
			var sinkFactory func(string) session.ProvenanceSink
			if disclosure == session.ReplayDisclosureRestricted {
				sink = &replayTestSink{}
				sinkFactory = func(string) session.ProvenanceSink { return sink }
			}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
				mustReplayTestTime("2026-06-14T08:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
			if err != nil {
				t.Fatal(err)
			}
			mapping := result.Replay.Output.Sources[0].DavisProblemsMapping
			if mapping == nil || !mapping.WarmupClamped || mapping.PhysicalW != "2026-06-14T08:00:00Z" {
				t.Fatalf("mapping = %#v", mapping)
			}
			warnings := strings.Join(result.Replay.Output.Warnings, "\n")
			if !strings.Contains(warnings, "documented six-hour refresh cadence") || !strings.Contains(warnings, "less than six hours of warm-up") {
				t.Fatalf("clamp warning = %q", warnings)
			}
			if disclosure == session.ReplayDisclosureRestricted {
				if ReplayOutputMetadata(result.Replay) != nil {
					t.Fatal("restricted clamp warning reached ordinary replay metadata")
				}
				_, _, records := sink.snapshot()
				if len(records) != 2 || !strings.Contains(fmt.Sprint(records[0].Fields["notices"]), "less than six hours of warm-up") {
					t.Fatalf("restricted clamp provenance = %#v", records)
				}
				sources, ok := records[1].Fields["sources"].([]map[string]any)
				if !ok || len(sources) != 1 {
					t.Fatalf("restricted clamp sources = %#v", records[1].Fields["sources"])
				}
				privateMapping, ok := sources[0]["davis_problems_mapping"].(map[string]any)
				if !ok || privateMapping["warmup_clamped"] != true {
					t.Fatalf("restricted clamp mapping = %#v", sources[0])
				}
				physical, ok := privateMapping["physical_snapshot_range"].(map[string]string)
				if !ok || physical["w"] != "2026-06-14T08:00:00Z" || physical["t"] != "2026-06-14T10:00:00Z" {
					t.Fatalf("restricted clamped physical range = %#v", privateMapping["physical_snapshot_range"])
				}
			}
		})
	}
}

func TestDavisMappedPreparationProvenanceContainsExactCoverageAndMappingAudit(t *testing.T) {
	api := newReplayDavisMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull,
		mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), nil)
	parseHandler := fixture.executor.sdkHandler("").WithFirstRateLimitResponse()
	prepared, err := fixture.executor.preparer.Prepare(context.Background(), PrepareInput{
		OriginalQuery: replayDavisOriginal, Mode: ReplayExecutionOneShot,
		Options: DQLExecuteOptions{Timezone: "UTC"}, OriginalASTs: fixture.executor.originalASTs,
		Parse: func(ctx context.Context, request sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
			return parseHandler.Parse(ctx, request)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	record := provenanceRecord("query_execution", prepared.provenance, nil)
	if record.Fields["original_dql"] != replayDavisOriginal || record.Fields["effective_dql"] != replayDavisEffective {
		t.Fatalf("query provenance = %#v", record.Fields)
	}
	coverage, ok := record.Fields["davis_snapshot_coverage"].(map[string]any)
	if !ok || coverage["status"] != "verified" || coverage["coverage_verified"] != true ||
		coverage["oldest_snapshot"] == "" || coverage["observed_at"] == "" || coverage["reuse"] != DavisSnapshotCoverageMiss {
		t.Fatalf("coverage provenance = %#v", record.Fields["davis_snapshot_coverage"])
	}
	sources, ok := record.Fields["sources"].([]map[string]any)
	if !ok || len(sources) != 1 {
		t.Fatalf("sources provenance = %#v", record.Fields["sources"])
	}
	mapping, ok := sources[0]["davis_problems_mapping"].(map[string]any)
	if !ok || mapping["original_view"] != execreplay.DavisProblemsView ||
		mapping["effective_snapshot_table"] != execreplay.DavisProblemsSnapshotTable || mapping["warmup_clamped"] != false {
		t.Fatalf("mapping provenance = %#v", sources[0])
	}
	logical := mapping["logical_view_range"].(map[string]string)
	physical := mapping["physical_snapshot_range"].(map[string]string)
	if logical["f"] != "2026-06-14T09:00:00Z" || logical["t"] != "2026-06-14T10:00:00Z" ||
		physical["w"] != "2026-06-14T03:00:00Z" || physical["t"] != "2026-06-14T10:00:00Z" {
		t.Fatalf("F/T/W provenance logical=%#v physical=%#v", logical, physical)
	}
	audit := record.Fields["audit"].(map[string]any)
	if audit["davis_mappings_audited"] != true {
		t.Fatalf("audit provenance = %#v", audit)
	}
}

func TestDQLExecutorDavisValidationTamperingAlwaysPreventsMainExecute(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"snapshot token", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "dt.davis.problems.snapshots"`), []byte(`"canonicalString": "dt.davis.events.snapshots"`), 1)
		}},
		{"stage order", func(body []byte) []byte {
			body = bytes.Replace(body, []byte(`"canonicalString": "sort"`), []byte(`"canonicalString": "synthetic-stage"`), 1)
			body = bytes.Replace(body, []byte(`"canonicalString": "dedup"`), []byte(`"canonicalString": "sort"`), 1)
			return bytes.Replace(body, []byte(`"canonicalString": "synthetic-stage"`), []byte(`"canonicalString": "dedup"`), 1)
		}},
		{"identity", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "event.id"`), []byte(`"canonicalString": "event.kind"`), 1)
		}},
		{"sort direction", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "desc"`), []byte(`"canonicalString": "asc"`), 1)
		}},
		{"upper operator", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "\u003c"`), []byte(`"canonicalString": "\u003c="`), 1)
		}},
		{"coalesce", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "coalesce"`), []byte(`"canonicalString": "max"`), 1)
		}},
		{"inclusive lower operator", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`"canonicalString": "\u003e="`), []byte(`"canonicalString": "\u003e"`), 1)
		}},
		{"physical W", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`\"2026-06-14T03:00:00.000000000Z\"`), []byte(`\"2026-06-14T03:00:00.000000001Z\"`), 1)
		}},
		{"logical F", func(body []byte) []byte {
			return bytes.Replace(body, []byte(`\"2026-06-14T09:00:00.000000000Z\"`), []byte(`\"2026-06-14T09:00:00.000000001Z\"`), 1)
		}},
	}
	for _, test := range tests {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api := newReplayDavisMockAPI(t)
				api.disableDynamicValidation = true
				api.validationBody = test.mutate(append([]byte(nil), api.validationBody...))
				var sink *replayTestSink
				var sinkFactory func(string) session.ProvenanceSink
				if disclosure == session.ReplayDisclosureRestricted {
					sink = &replayTestSink{}
					sinkFactory = func(string) session.ProvenanceSink { return sink }
				}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
					mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)
				_, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
				var auditErr *execreplay.ReplayError
				if !errors.As(err, &auditErr) || auditErr.Code != execreplay.ErrorAudit {
					t.Fatalf("error = %T %v", err, err)
				}
				if disclosure == session.ReplayDisclosureRestricted {
					if err.Error() != restrictedPreparationMessage {
						t.Fatalf("restricted audit error = %q", err)
					}
					preflights, appends, records := sink.snapshot()
					if preflights != 1 || appends != 1 || len(records) != 1 || records[0].Event != "query_pre_execution" ||
						records[0].Fields["detail"] != auditErr.Error() {
						t.Fatalf("restricted audit provenance preflights=%d appends=%d records=%#v", preflights, appends, records)
					}
					audit, ok := records[0].Fields["audit"].(map[string]any)
					if !ok || audit["ok"] != false {
						t.Fatalf("restricted audit result = %#v", records[0].Fields["audit"])
					}
				}
				parseCalls, executeCalls, _ := api.counts()
				coverageRequests, _ := api.coverage()
				if parseCalls != 2 || len(coverageRequests) != 1 || executeCalls != 0 {
					t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
				}
			})
		}
	}
}

func containsReplayNotice(values []string, fragment string) bool {
	for _, value := range values {
		if strings.Contains(value, fragment) {
			return true
		}
	}
	return false
}

func TestDQLExecutorWithoutReplayMakesNoParseCallAndPreservesQuery(t *testing.T) {
	api := newReplayMockAPI(t)
	server := httptest.NewServer(api)
	defer server.Close()
	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewDQLExecutor(transport).ExecuteQueryWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{})
	if err != nil || result == nil {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	parseCalls, executeCalls, _ := api.counts()
	_, executions := api.queries()
	if parseCalls != 0 || executeCalls != 1 || executions[0].Query != replayRecordOriginal || strings.Contains(executions[0].Query, "dt.system.buckets") {
		t.Fatalf("parse=%d execute=%d query=%q", parseCalls, executeCalls, executions[0].Query)
	}
}

func TestDQLExecutorReplayFreshVirtualTimePerExecutionButNotDuringPolling(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
	opts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait}
	if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Add(5 * time.Minute)
	if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, _ := api.counts()
	_, executions := api.queries()
	if parseCalls != 3 || executeCalls != 2 || executions[0].Query == executions[1].Query {
		t.Fatalf("parse=%d execute=%d queries equal=%v", parseCalls, executeCalls, executions[0].Query == executions[1].Query)
	}

	asyncAPI := newReplayMockAPI(t)
	asyncAPI.async = true
	asyncFixture := newReplayExecutorFixture(t, asyncAPI, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
	asyncAPI.onPoll = func() { asyncFixture.clock.Add(5 * time.Minute) }
	if _, err := asyncFixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, pollCalls := asyncAPI.counts()
	_, executions = asyncAPI.queries()
	if parseCalls != 2 || executeCalls != 1 || pollCalls != 1 || !strings.Contains(executions[0].Query, "10:55:02.718012207Z") {
		t.Fatalf("parse=%d execute=%d poll=%d query=%q", parseCalls, executeCalls, pollCalls, executions[0].Query)
	}

	liveAPI := newReplayMockAPI(t)
	liveFixture := newReplayExecutorFixture(t, liveAPI, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
	liveOpts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionLive}
	if _, err := liveFixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, liveOpts); err != nil {
		t.Fatal(err)
	}
	liveFixture.clock.Add(5 * time.Minute)
	if _, err := liveFixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, liveOpts); err != nil {
		t.Fatal(err)
	}
	parseCalls, executeCalls, _ = liveAPI.counts()
	_, executions = liveAPI.queries()
	if parseCalls != 3 || executeCalls != 2 || executions[0].Query == executions[1].Query {
		t.Fatalf("live parse=%d execute=%d queries equal=%v", parseCalls, executeCalls, executions[0].Query == executions[1].Query)
	}
}

func TestDQLExecutorReplayTemporaryNonOverlapRecomputesAndThenExecutes(t *testing.T) {
	api := newReplayMockAPI(t)
	api.originalBody = replayFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/parse.json")
	api.validationBody = replayFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/validation-parse.json")
	api.isOriginal = func(query string) bool { return query == replayLoopOriginal }
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayLoopDataStart, replayLoopDataStart, replayLoopDataEnd, nil)
	preparer := fixture.executor.preparer.(*ReplayQueryPreparer)
	var waits []time.Duration
	preparer.config.WaitFunc = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		fixture.clock.Add(delay)
		return nil
	}
	opts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait}

	first, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayLoopOriginal, opts)
	if first != nil || !ReplayTemporaryNonOverlap(err) || err.Error() != fullTemporaryNonOverlapMessage {
		t.Fatalf("first result=%#v err=%v, want temporary no-overlap", first, err)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 1 || executeCalls != 0 {
		t.Fatalf("temporary attempt parse=%d execute=%d, want original parse only", parseCalls, executeCalls)
	}
	info, ok := ReplayErrorInfo(err)
	if !ok || !info.VirtualNow.Equal(replayLoopDataStart) {
		t.Fatalf("temporary attempt info = %#v, %v", info, ok)
	}
	if waitErr := fixture.executor.WaitForNextAttempt(context.Background(), 5*time.Minute, &info, err); waitErr != nil {
		t.Fatal(waitErr)
	}
	second, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayLoopOriginal, opts)
	if err != nil || second == nil || second.Response == nil {
		t.Fatalf("second result=%#v err=%v", second, err)
	}
	parseCalls, executeCalls, _ = api.counts()
	if parseCalls != 2 || executeCalls != 1 {
		t.Fatalf("after overlap parse=%d execute=%d, want one original, one effective, one execute", parseCalls, executeCalls)
	}
	if len(waits) != 1 || waits[0] != 5*time.Minute {
		t.Fatalf("waits = %v, want one normal-cadence wait", waits)
	}
}

func TestDQLExecutorReplayNonOverlapHardCasesNeverExecute(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fixture   string
		clockMode string
		mode      ReplayExecutionMode
		start     time.Time
		virtual   time.Time
		end       time.Time
		wantClass execreplay.OverlapClassification
	}{
		{
			name: "one shot temporary range", query: replayLoopOriginal,
			fixture:   "phase0b/fixtures/records/logs/00-discovery/parse.json",
			clockMode: session.ReplayClockRealtime, mode: ReplayExecutionOneShot,
			start: replayLoopDataStart, virtual: replayLoopDataStart, end: replayLoopDataEnd,
			wantClass: execreplay.OverlapTemporary,
		},
		{
			name: "manual wait temporary range", query: replayLoopOriginal,
			fixture:   "phase0b/fixtures/records/logs/00-discovery/parse.json",
			clockMode: session.ReplayClockManual, mode: ReplayExecutionWait,
			start: replayLoopDataStart, virtual: replayLoopDataStart, end: replayLoopDataEnd,
			wantClass: execreplay.OverlapTemporary,
		},
		{
			name: "permanent absolute range", query: replayPermanentQuery,
			fixture:   "phase0/fixtures/07-fetch-explicit-timeframe/parse.json",
			clockMode: session.ReplayClockRealtime, mode: ReplayExecutionWait,
			start: replayLoopDataStart, virtual: replayLoopDataStart, end: replayLoopDataEnd,
			wantClass: execreplay.OverlapPermanent,
		},
		{
			name: "unknown future alignment", query: replayUnknownQuery,
			fixture:   "phase0/fixtures/03-fetch-aligned-duration/parse.json",
			clockMode: session.ReplayClockRealtime, mode: ReplayExecutionLive,
			start: replayLoopDataStart, virtual: replayLoopDataStart, end: replayLoopDataEnd,
			wantClass: execreplay.OverlapUnknown,
		},
		{
			name: "terminal non-overlap remains permanent", query: replayPermanentQuery,
			fixture:   "phase0/fixtures/07-fetch-explicit-timeframe/parse.json",
			clockMode: session.ReplayClockManual, mode: ReplayExecutionWait,
			start: replayLoopDataStart, virtual: replayLoopDataEnd, end: replayLoopDataEnd,
			wantClass: execreplay.OverlapPermanent,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newReplayMockAPI(t)
			api.originalBody = replayFixtureBody(t, test.fixture)
			api.isOriginal = func(query string) bool { return query == test.query }
			fixture := newReplayExecutorFixture(t, api, test.clockMode, session.ReplayDisclosureFull, test.start, test.virtual, test.end, nil)
			_, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.query, DQLExecuteOptions{AgentMode: true, ReplayMode: test.mode})
			var nonOverlap *execreplay.NonOverlapError
			if err == nil || !errors.As(err, &nonOverlap) || nonOverlap.Classification != test.wantClass {
				t.Fatalf("error = %T %v, classification=%v; want %s", err, err, nonOverlap, test.wantClass)
			}
			if ReplayTemporaryNonOverlap(err) {
				t.Fatal("hard non-overlap was marked retryable")
			}
			parseCalls, executeCalls, _ := api.counts()
			if parseCalls != 1 || executeCalls != 0 {
				t.Fatalf("parse=%d execute=%d, want one original parse and no execute", parseCalls, executeCalls)
			}
		})
	}
}

func TestDQLExecutorReplayRestrictedHardNonOverlapIsGenericAndRecorded(t *testing.T) {
	api := newReplayMockAPI(t)
	api.originalBody = replayFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/parse.json")
	api.isOriginal = func(query string) bool { return query == replayLoopOriginal }
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayLoopDataStart, replayLoopDataStart, replayLoopDataEnd, func(string) session.ProvenanceSink { return sink })
	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayLoopOriginal, DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait})
	if result != nil || err == nil || err.Error() != restrictedNoDataMessage {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	parseCalls, executeCalls, _ := api.counts()
	_, appends, records := sink.snapshot()
	if parseCalls != 1 || executeCalls != 0 || appends != 1 || len(records) != 1 || !strings.Contains(records[0].Fields["detail"].(string), "overlap") {
		t.Fatalf("parse=%d execute=%d appends=%d records=%#v", parseCalls, executeCalls, appends, records)
	}
}

func TestDQLExecutorReplayCadenceAndTerminalScheduling(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd.Add(-2*time.Minute), replayRecordDataEnd, nil)
	if err := fixture.executor.ValidateReplayCadence(MinReplayExecutionInterval - time.Nanosecond); err == nil {
		t.Fatal("expected faster-than-five-second cadence rejection")
	}
	if parseCalls, executeCalls, _ := api.counts(); parseCalls != 0 || executeCalls != 0 {
		t.Fatalf("cadence rejection parse=%d execute=%d, want zero", parseCalls, executeCalls)
	}

	preparer := fixture.executor.preparer.(*ReplayQueryPreparer)
	var waits []time.Duration
	preparer.config.WaitFunc = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		fixture.clock.Add(delay)
		return nil
	}
	opts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait}
	first, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Replay.Terminal {
		t.Fatal("first attempt unexpectedly terminal")
	}
	if err := fixture.executor.WaitForNextAttempt(context.Background(), 5*time.Minute, first.Replay, nil); err != nil {
		t.Fatal(err)
	}
	if len(waits) != 1 || waits[0] != 2*time.Minute {
		t.Fatalf("terminal wait = %v, want 2m", waits)
	}
	terminal, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
	if err != nil || terminal == nil || terminal.Replay == nil || !terminal.Replay.Terminal {
		t.Fatalf("terminal result=%#v err=%v", terminal, err)
	}
	state, err := fixture.store.Status(fixture.locator)
	if err != nil || state.Status != session.ReplayStatusCompleted {
		t.Fatalf("terminal state=%#v err=%v", state, err)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 3 || executeCalls != 2 {
		t.Fatalf("parse=%d execute=%d, want one original plus two effective and two executions", parseCalls, executeCalls)
	}
}

func TestDQLExecutorReplayPreExecutionFailuresNeverExecute(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*replayMockAPI)
	}{
		{"original parse failure", func(api *replayMockAPI) { api.parseStatus = http.StatusInternalServerError }},
		{"effective parse failure", func(api *replayMockAPI) { api.effectiveStatus = http.StatusInternalServerError }},
		{"original AST adaptation failure", func(api *replayMockAPI) {
			api.originalBody = json.RawMessage(`{"nodeType":"FUTURE","isOptional":false}`)
			api.disableDynamicValidation = true
		}},
		{"validation audit failure", func(api *replayMockAPI) {
			api.validationBody = api.originalBody
			api.disableDynamicValidation = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newReplayMockAPI(t)
			test.configure(api)
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
			if _, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true}); err == nil {
				t.Fatal("expected preparation failure")
			}
			_, executeCalls, _ := api.counts()
			if executeCalls != 0 {
				t.Fatalf("execute calls = %d, want zero", executeCalls)
			}
		})
	}
}

func TestDQLExecutorReplaySourceSpecificRejectionsNeverExecute(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		fixture string
		want    string
	}{
		{
			name: "metric shift", query: "timeseries value=avg(metric.key), shift:-7d",
			fixture: "phase0/fixtures/12-timeseries-shift/parse.json", want: "shift",
		},
		{
			name:    "current topology enrichment",
			query:   `fetch logs, from:toTimestamp("2026-08-10T09:54:05Z"), to:toTimestamp("2026-08-10T10:54:05Z") | limit 1 | append [ smartscapeNodes "*" | limit 1 ] | summarize count()`,
			fixture: "phase0b/fixtures/current-state-rejection/01-smartscape-nodes/historical-context/parse.json", want: "current",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newReplayMockAPI(t)
			api.originalBody = replayFixtureBody(t, test.fixture)
			api.isOriginal = func(query string) bool { return query == test.query }
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.query, DQLExecuteOptions{AgentMode: true})
			if result != nil || err == nil || !strings.Contains(strings.ToLower(err.Error()), test.want) {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			parseCalls, executeCalls, _ := api.counts()
			if parseCalls != 1 || executeCalls != 0 {
				t.Fatalf("parse=%d execute=%d", parseCalls, executeCalls)
			}
		})
	}
}

func TestDQLExecutorReplayRestrictedPreflightAndPreparationFailures(t *testing.T) {
	t.Run("preflight fails before parse", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{preflightErr: errors.New("synthetic sink unavailable")}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPreflightSinkMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		preflight, appends, _ := sink.snapshot()
		parseCalls, executeCalls, _ := api.counts()
		if preflight != 1 || appends != 0 || parseCalls != 0 || executeCalls != 0 {
			t.Fatalf("preflight=%d appends=%d parse=%d execute=%d", preflight, appends, parseCalls, executeCalls)
		}
	})

	t.Run("preparation detail reaches provenance and generic output", func(t *testing.T) {
		api := newReplayMockAPI(t)
		api.effectiveStatus = http.StatusServiceUnavailable
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPreparationMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		_, appends, records := sink.snapshot()
		parseCalls, executeCalls, _ := api.counts()
		if appends != 1 || len(records) != 1 || parseCalls != 2 || executeCalls != 0 {
			t.Fatalf("appends=%d records=%d parse=%d execute=%d", appends, len(records), parseCalls, executeCalls)
		}
		fields := records[0].Fields
		if fields["original_dql"] != replayRecordOriginal || fields["effective_dql"] != replayRecordEffective || !strings.Contains(fields["detail"].(string), "query request failed") {
			t.Fatalf("incomplete preparation provenance: %#v", fields)
		}
	})

	t.Run("stored restricted route remains authoritative during drift", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		preparer := fixture.executor.preparer.(*ReplayQueryPreparer)
		preparer.config.ExpectedContextInputHash = strings.Repeat("e", 64)
		preparer.config.FallbackDisclosure = session.ReplayDisclosureFull
		preparer.config.FallbackProvenancePath = ""
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedReadinessMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		preflight, appends, records := sink.snapshot()
		parseCalls, executeCalls, _ := api.counts()
		if preflight != 1 || appends != 1 || len(records) != 1 || parseCalls != 0 || executeCalls != 0 {
			t.Fatalf("preflight=%d appends=%d records=%d parse=%d execute=%d", preflight, appends, len(records), parseCalls, executeCalls)
		}
		if !strings.Contains(records[0].Fields["detail"].(string), "context changed") {
			t.Fatalf("drift detail missing from private provenance: %#v", records[0])
		}
	})

	t.Run("notice append failure prevents execute", func(t *testing.T) {
		const query = `timeseries metric_value=avg(dt.host.cpu.usage), interval:1d, from:toTimestamp("2026-08-08T19:53:06Z"), to:toTimestamp("2026-08-10T12:48:06Z")`
		api := newReplayMockAPI(t)
		api.originalBody = replayFixtureBody(t, "phase0b/fixtures/metrics/05-explicit-1d/parse.json")
		api.validationBody = replayFixtureBody(t, "phase0b/fixtures/metrics/05-explicit-1d/validation-parse.json")
		api.isOriginal = func(value string) bool { return value == query }
		sink := &replayTestSink{appendFailures: map[int]error{1: errors.New("synthetic notice append failure")}}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
			mustReplayTestTime("2026-08-08T00:00:00Z"), mustReplayTestTime("2026-08-10T13:00:00Z"), mustReplayTestTime("2026-08-11T00:00:00Z"),
			func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), query, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPreflightSinkMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		parseCalls, executeCalls, _ := api.counts()
		_, appends, _ := sink.snapshot()
		if parseCalls != 2 || executeCalls != 0 || appends != 1 {
			t.Fatalf("parse=%d execute=%d appends=%d", parseCalls, executeCalls, appends)
		}
	})
}

func TestDQLExecutorReplayRestrictedPostExecutionOrderingAndFailures(t *testing.T) {
	t.Run("returned telemetry passes through unchanged", func(t *testing.T) {
		api := newReplayMockAPI(t)
		api.executeResponse.Result.Records = []map[string]interface{}{{
			"replay": "telemetry-replay", "virtual": "telemetry-virtual",
			"session": "telemetry-session", "clock": "telemetry-clock",
			"interval": "telemetry-interval", "effective": "telemetry-effective",
			"marker": "returned-telemetry-marker",
		}}
		expected, err := json.Marshal(api.executeResponse)
		if err != nil {
			t.Fatal(err)
		}
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if err != nil || result == nil || result.Response == nil {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		actual, err := json.Marshal(result.Response)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != string(expected) {
			t.Fatalf("returned telemetry changed:\n got %s\nwant %s", actual, expected)
		}
		_, _, records := sink.snapshot()
		encodedRecords, err := json.Marshal(records)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encodedRecords), "returned-telemetry-marker") {
			t.Fatalf("returned telemetry leaked into provenance: %s", encodedRecords)
		}
	})

	t.Run("execution append failure suppresses result", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{appendFailures: map[int]error{1: errors.New("synthetic append failure")}}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPostExecutionSinkMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		_, appends, records := sink.snapshot()
		_, executeCalls, _ := api.counts()
		if appends != 1 || len(records) != 0 || executeCalls != 1 {
			t.Fatalf("appends=%d records=%d execute=%d", appends, len(records), executeCalls)
		}
	})

	t.Run("terminal execution append precedes completion", func(t *testing.T) {
		api := newReplayMockAPI(t)
		order := &replayTestOrder{}
		sink := &replayTestSink{order: order}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		tracking := &replayTrackingStore{ReplayStore: fixture.store, order: order}
		fixture.useStore(tracking)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if err != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionRecorded {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		if got := order.snapshot(); !equalStrings(got, []string{"append:query_execution", "mark-completed", "append:query_completion"}) {
			t.Fatalf("terminal ordering = %v", got)
		}
		_, _, records := sink.snapshot()
		if len(records) != 2 || records[0].Fields["original_dql"] != replayRecordOriginal || records[0].Fields["effective_dql"] == "" || records[0].Fields["audit"] == nil || records[1].Fields["completion_disposition"] != session.CompletionRecorded {
			t.Fatalf("terminal provenance = %#v", records)
		}
	})

	t.Run("terminal execution append failure leaves terminal ready", func(t *testing.T) {
		api := newReplayMockAPI(t)
		order := &replayTestOrder{}
		sink := &replayTestSink{order: order, appendFailures: map[int]error{1: errors.New("synthetic append failure")}}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		tracking := &replayTrackingStore{ReplayStore: fixture.store, order: order}
		fixture.useStore(tracking)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPostExecutionSinkMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		if stateErr != nil || state.Status != session.ReplayStatusTerminalReady || len(tracking.completionDispositions()) != 0 {
			t.Fatalf("state=%#v stateErr=%v dispositions=%v", state, stateErr, tracking.completionDispositions())
		}
		if got := order.snapshot(); !equalStrings(got, []string{"append:query_execution"}) {
			t.Fatalf("terminal failure ordering = %v", got)
		}
	})

	t.Run("completion append failure suppresses result without rollback", func(t *testing.T) {
		api := newReplayMockAPI(t)
		order := &replayTestOrder{}
		sink := &replayTestSink{order: order, appendFailures: map[int]error{2: errors.New("synthetic disposition failure")}}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		tracking := &replayTrackingStore{ReplayStore: fixture.store, order: order}
		fixture.useStore(tracking)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedPostExecutionSinkMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		if stateErr != nil || state.Status != session.ReplayStatusCompleted || state.CompletedAt == nil {
			t.Fatalf("completion was rolled back: state=%#v err=%v", state, stateErr)
		}
		if got := order.snapshot(); !equalStrings(got, []string{"append:query_execution", "mark-completed", "append:query_completion"}) {
			t.Fatalf("terminal failure ordering = %v", got)
		}
	})
}

func TestDQLExecutorReplayTerminalFailureReplacementAndConcurrency(t *testing.T) {
	t.Run("remote failure remains retryable without state write", func(t *testing.T) {
		api := newReplayMockAPI(t)
		api.executeStatus = http.StatusServiceUnavailable
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)
		opts := DQLExecuteOptions{AgentMode: true}
		if result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); result != nil || err == nil {
			t.Fatalf("result=%#v err=%v, want remote failure", result, err)
		} else if ReplayLoopHardFailure(err) {
			t.Fatalf("transient remote 503 was classified as a hard loop failure: %v", err)
		}
		state, err := fixture.store.Status(fixture.locator)
		if err != nil || state.Status != session.ReplayStatusTerminalReady || state.CompletedAt != nil {
			t.Fatalf("failed terminal state=%#v err=%v", state, err)
		}
		api.executeStatus = 0
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
		if err != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionRecorded {
			t.Fatalf("retry result=%#v err=%v", result, err)
		}
		parseCalls, executeCalls, _ := api.counts()
		if parseCalls != 3 || executeCalls != 2 {
			t.Fatalf("retry parse=%d execute=%d, want memoized original and fresh effective parse", parseCalls, executeCalls)
		}
	})

	t.Run("completion write failure leaves terminal ready for retry", func(t *testing.T) {
		api := newReplayMockAPI(t)
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)
		fixture.useStore(&replayFailingCompletionStore{ReplayStore: fixture.store, err: errors.New("synthetic completion write failure")})
		opts := DQLExecuteOptions{AgentMode: true}
		if result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts); result != nil || err == nil || !strings.Contains(err.Error(), "completion write failure") {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, err := fixture.store.Status(fixture.locator)
		if err != nil || state.Status != session.ReplayStatusTerminalReady || state.CompletedAt != nil {
			t.Fatalf("failed finalization state=%#v err=%v", state, err)
		}
		fixture.useStore(fixture.store)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
		if err != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionRecorded {
			t.Fatalf("retry result=%#v err=%v", result, err)
		}
	})

	t.Run("restricted completion write failure is generic and recorded", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		fixture.useStore(&replayFailingCompletionStore{ReplayStore: fixture.store, err: errors.New("synthetic completion write failure")})
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedFinalizationMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		_, appends, records := sink.snapshot()
		if stateErr != nil || state.Status != session.ReplayStatusTerminalReady || appends != 2 || len(records) != 2 || !strings.Contains(records[1].Fields["detail"].(string), "completion write failure") {
			t.Fatalf("state=%#v stateErr=%v appends=%d records=%#v", state, stateErr, appends, records)
		}
	})

	t.Run("replacement preserves result and replacement state", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		type restartResult struct {
			state session.ReplaySession
			err   error
		}
		restarted := make(chan restartResult, 1)
		api.beforeExecuteResponse = func() {
			state, restartErr := restartReplayExecutorFixture(fixture)
			restarted <- restartResult{state: state, err: restartErr}
		}
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		replacement := <-restarted
		if replacement.err != nil {
			t.Fatal(replacement.err)
		}
		if err != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionSessionReplaced {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		if stateErr != nil || state.SessionID != replacement.state.SessionID || state.Status == session.ReplayStatusCompleted {
			t.Fatalf("replacement changed: state=%#v err=%v", state, stateErr)
		}
		_, _, records := sink.snapshot()
		if len(records) != 2 || records[1].Fields["completion_disposition"] != session.CompletionSessionReplaced {
			t.Fatalf("replacement provenance = %#v", records)
		}
	})

	t.Run("stopped session preserves result and stopped state", func(t *testing.T) {
		api := newReplayMockAPI(t)
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
		stopped := make(chan error, 1)
		api.beforeExecuteResponse = func() {
			_, stopErr := fixture.store.Stop(fixture.locator)
			stopped <- stopErr
		}
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		if stopErr := <-stopped; stopErr != nil {
			t.Fatal(stopErr)
		}
		if err != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionSessionReplaced {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		if stateErr != nil || state.Status != session.ReplayStatusStopped || state.CompletedAt != nil {
			t.Fatalf("stopped state changed: state=%#v err=%v", state, stateErr)
		}
		_, _, records := sink.snapshot()
		if len(records) != 2 || records[1].Fields["completion_disposition"] != session.CompletionSessionReplaced {
			t.Fatalf("stopped disposition provenance = %#v", records)
		}
	})

	t.Run("full disclosure reports replacement note", func(t *testing.T) {
		api := newReplayMockAPI(t)
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)
		restarted := make(chan error, 1)
		api.beforeExecuteResponse = func() {
			_, restartErr := restartReplayExecutorFixture(fixture)
			restarted <- restartErr
		}
		var result *DQLExecutionResult
		var executeErr error
		stderr := captureReplayExecutorStderr(t, func() {
			result, executeErr = fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
		})
		if restartErr := <-restarted; restartErr != nil {
			t.Fatal(restartErr)
		}
		if executeErr != nil || result == nil || result.Replay.CompletionDisposition != session.CompletionSessionReplaced {
			t.Fatalf("result=%#v err=%v", result, executeErr)
		}
		if !strings.Contains(stderr, "session changed before terminal completion") {
			t.Fatalf("replacement note missing: %q", stderr)
		}
	})

	t.Run("duplicate terminal reads complete once", func(t *testing.T) {
		api := newReplayMockAPI(t)
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		api.beforeExecuteResponse = func() {
			entered <- struct{}{}
			<-release
		}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)
		tracking := &replayTrackingStore{ReplayStore: fixture.store}
		fixture.useStore(tracking)
		results := make(chan *DQLExecutionResult, 2)
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
				results <- result
				errs <- err
			}()
		}
		for range 2 {
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("terminal executions did not both reach the read-only request")
			}
		}
		close(release)
		seen := map[session.CompletionDisposition]int{}
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			result := <-results
			if result == nil || result.Response == nil {
				t.Fatalf("missing concurrent terminal result: %#v", result)
			}
			seen[result.Replay.CompletionDisposition]++
		}
		if seen[session.CompletionRecorded] != 1 || seen[session.CompletionAlreadyCompleted] != 1 {
			t.Fatalf("completion dispositions = %v", seen)
		}
		state, err := fixture.store.Status(fixture.locator)
		if err != nil || state.Status != session.ReplayStatusCompleted || state.CompletedAt == nil || state.Revision != fixture.started.Revision+1 {
			t.Fatalf("concurrent terminal state=%#v err=%v", state, err)
		}
		parseCalls, executeCalls, _ := api.counts()
		if parseCalls != 3 || executeCalls != 2 {
			t.Fatalf("parse=%d execute=%d, want one coalesced original and two effective parses", parseCalls, executeCalls)
		}
	})
}

func TestDQLExecutorReplayMetricResultContractGatesTerminalOutput(t *testing.T) {
	t.Run("valid natural buckets permit completion", func(t *testing.T) {
		api := newReplayMetricMockAPI(t)
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, nil)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
		if err != nil || result == nil || result.Response == nil || result.Replay.CompletionDisposition != session.CompletionRecorded {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		_, executions := api.queries()
		if len(executions) != 1 || executions[0].Query != replayMetricEffective {
			t.Fatalf("execute queries = %#v", executions)
		}
		state, stateErr := fixture.store.Status(fixture.locator)
		if stateErr != nil || state.Status != session.ReplayStatusCompleted {
			t.Fatalf("state=%#v err=%v", state, stateErr)
		}
	})

	tests := []struct {
		name   string
		mutate func(*sdkquery.Response)
	}{
		{
			name: "unknown natural interval",
			mutate: func(response *sdkquery.Response) {
				delete(response.Result.Records[0], "interval")
			},
		},
		{
			name: "non-intersecting bucket",
			mutate: func(response *sdkquery.Response) {
				response.Result.Records[0]["timeframe"] = map[string]interface{}{
					"start": "2026-08-09T10:10:00Z", "end": "2026-08-09T10:20:00Z",
				}
				response.Result.Records[0]["metric_value"] = []interface{}{float64(1)}
			},
		},
		{
			name: "second spilling bucket",
			mutate: func(response *sdkquery.Response) {
				response.Result.Records[0]["timeframe"] = map[string]interface{}{
					"start": "2026-08-09T10:20:00Z", "end": "2026-08-09T10:50:00Z",
				}
				response.Result.Records[0]["metric_value"] = []interface{}{float64(1), float64(2), float64(3)}
			},
		},
		{
			name: "spill longer than one interval",
			mutate: func(response *sdkquery.Response) {
				response.Result.Records[0]["timeframe"] = map[string]interface{}{
					"start": "2026-08-09T10:10:00Z", "end": "2026-08-09T10:40:00Z",
				}
				response.Result.Records[0]["metric_value"] = []interface{}{float64(1), float64(2), float64(3)}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newReplayMetricMockAPI(t)
			test.mutate(&api.executeResponse)
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, nil)
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
			if result != nil || err == nil {
				t.Fatalf("result=%#v err=%v, want validation failure and no output", result, err)
			}
			var contractErr *execreplay.ResultContractError
			if !errors.As(err, &contractErr) && !strings.Contains(err.Error(), "metric result") {
				t.Fatalf("error = %T %v, want result-contract failure", err, err)
			}
			parseCalls, executeCalls, _ := api.counts()
			if parseCalls != 2 || executeCalls != 1 {
				t.Fatalf("parse=%d execute=%d, want two parses and exactly one execute", parseCalls, executeCalls)
			}
			state, stateErr := fixture.store.Status(fixture.locator)
			if stateErr != nil || state.Status != session.ReplayStatusTerminalReady || state.CompletedAt != nil {
				t.Fatalf("contract failure changed state: %#v err=%v", state, stateErr)
			}
		})
	}

	t.Run("restricted contract failure is generic and recorded", func(t *testing.T) {
		api := newReplayMetricMockAPI(t)
		delete(api.executeResponse.Result.Records[0], "interval")
		sink := &replayTestSink{}
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, func(string) session.ProvenanceSink { return sink })
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
		if result != nil || err == nil || err.Error() != restrictedValidationMessage {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		parseCalls, executeCalls, _ := api.counts()
		state, stateErr := fixture.store.Status(fixture.locator)
		_, appends, records := sink.snapshot()
		if parseCalls != 2 || executeCalls != 1 || stateErr != nil || state.Status != session.ReplayStatusTerminalReady || appends != 1 || len(records) != 1 || !strings.Contains(records[0].Fields["detail"].(string), "natural interval") {
			t.Fatalf("parse=%d execute=%d state=%#v stateErr=%v appends=%d records=%#v", parseCalls, executeCalls, state, stateErr, appends, records)
		}
	})
}

func TestDQLExecutorRestrictedMetricProvenanceContainsCompleteExecutionFacts(t *testing.T) {
	api := newReplayMetricMockAPI(t)
	api.executeResponse.Result.Metadata.Grail.Notifications = []sdkquery.Notification{{
		Severity: "WARNING", NotificationType: "SYNTHETIC_NOTICE", Message: "synthetic query notification",
	}}
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, func(string) session.ProvenanceSink { return sink })
	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
	if err != nil || result == nil || result.Replay == nil || result.Replay.CompletionDisposition != session.CompletionRecorded ||
		result.Replay.Output == nil || result.Replay.Output.State != session.ReplayStatusCompleted {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	_, appends, records := sink.snapshot()
	if appends != 2 || len(records) != 2 {
		t.Fatalf("appends=%d records=%#v", appends, records)
	}
	fields := records[0].Fields
	for _, key := range []string{"outcome", "original_dql", "effective_dql", "grail_canonical_effective_dql", "session", "sources", "notices", "query_notifications", "audit", "validated_result_contracts"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("execution provenance omits %q: %#v", key, fields)
		}
	}
	notifications, ok := fields["query_notifications"].([]map[string]any)
	if !ok || len(notifications) != 1 || notifications[0]["type"] != "SYNTHETIC_NOTICE" || notifications[0]["message"] != "synthetic query notification" {
		t.Fatalf("query notifications = %#v", fields["query_notifications"])
	}
	sessionFields, ok := fields["session"].(map[string]any)
	if !ok {
		t.Fatalf("session provenance = %#v", fields["session"])
	}
	for _, key := range []string{"id", "state", "started_at", "clock_mode", "anchor_host", "anchor_virtual", "virtual_now", "data_start", "data_end", "visible_data_start", "visible_data_end"} {
		if sessionFields[key] == nil || sessionFields[key] == "" {
			t.Fatalf("session provenance omits %q: %#v", key, sessionFields)
		}
	}
	sources, ok := fields["sources"].([]map[string]any)
	if !ok || len(sources) != 1 {
		t.Fatalf("source provenance = %#v", fields["sources"])
	}
	for _, key := range []string{"class", "requested_range", "logical_effective_range", "physical_range", "boundary_policy", "overlap", "physical_pending", "actual_natural_interval_ns", "maximum_lower_spill_ns", "maximum_upper_spill_ns"} {
		if _, ok := sources[0][key]; !ok {
			t.Fatalf("source provenance omits %q: %#v", key, sources[0])
		}
	}
	validated, ok := fields["validated_result_contracts"].([]map[string]any)
	if !ok || len(validated) != 1 || validated[0]["natural_interval_ns"] == nil || validated[0]["physical_range"] == nil || validated[0]["lower_spill_ns"] == nil || validated[0]["upper_spill_ns"] == nil {
		t.Fatalf("validated metric provenance = %#v", fields["validated_result_contracts"])
	}
	if records[1].Fields["completion_disposition"] != session.CompletionRecorded {
		t.Fatalf("completion provenance = %#v", records[1])
	}
	goldenRecord := records[0]
	goldenRecord.SessionID = "0123456789abcdef0123456789abcdef"
	sessionFields["id"] = goldenRecord.SessionID
	encoded, err := json.Marshal(goldenRecord)
	if err != nil {
		t.Fatal(err)
	}
	testutil.AssertGolden(t, "replay/restricted-provenance-jsonl", string(encoded)+"\n")
}

func TestDQLExecutorReplayRateLimitAndRestrictedRemoteMapping(t *testing.T) {
	t.Run("first 429 preserves retry after", func(t *testing.T) {
		api := newReplayMockAPI(t)
		api.executeStatus = http.StatusTooManyRequests
		api.retryAfter = "7"
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
		preparer := fixture.executor.preparer.(*ReplayQueryPreparer)
		var waited time.Duration
		preparer.config.WaitFunc = func(_ context.Context, delay time.Duration) error {
			waited = delay
			fixture.clock.Add(delay)
			return nil
		}
		opts := DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionWait}
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
		if result != nil || err == nil || ReplayRetryAfter(err) != 7*time.Second {
			t.Fatalf("result=%#v err=%v retry-after=%s", result, err, ReplayRetryAfter(err))
		}
		if ReplayLoopHardFailure(err) {
			t.Fatalf("rate limit was classified as a hard loop failure: %v", err)
		}
		info, ok := ReplayErrorInfo(err)
		if !ok {
			t.Fatalf("rate-limit error lost scheduling info: %v", err)
		}
		if waitErr := fixture.executor.WaitForNextAttempt(context.Background(), 5*time.Second, &info, err); waitErr != nil {
			t.Fatal(waitErr)
		}
		if waited != 7*time.Second {
			t.Fatalf("waited %s, want Retry-After 7s", waited)
		}
		api.executeStatus = 0
		result, err = fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, opts)
		if err != nil || result == nil {
			t.Fatalf("retry result=%#v err=%v", result, err)
		}
		parseCalls, executeCalls, _ := api.counts()
		if parseCalls != 3 || executeCalls != 2 {
			t.Fatalf("parse=%d execute=%d after retry", parseCalls, executeCalls)
		}
	})

	t.Run("429 without retry after remains retryable at normal cadence", func(t *testing.T) {
		api := newReplayMockAPI(t)
		api.parseStatus = http.StatusTooManyRequests
		fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
		result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true, ReplayMode: ReplayExecutionLive})
		if result != nil || err == nil || ReplayRetryAfter(err) != 0 || ReplayLoopHardFailure(err) {
			t.Fatalf("result=%#v err=%v retry-after=%s hard=%t", result, err, ReplayRetryAfter(err), ReplayLoopHardFailure(err))
		}
		parseCalls, executeCalls, _ := api.counts()
		if parseCalls != 1 || executeCalls != 0 {
			t.Fatalf("parse=%d execute=%d, want one failed parse and no execute", parseCalls, executeCalls)
		}
	})

	for _, remoteMessage := range []string{"ordinary remote failure", "remote session unavailable"} {
		t.Run(remoteMessage, func(t *testing.T) {
			api := newReplayMockAPI(t)
			api.executeStatus = http.StatusBadRequest
			api.remoteErrorMessage = remoteMessage
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
			if result != nil || err == nil || !strings.Contains(err.Error(), remoteMessage) || err.Error() == restrictedRemoteExecutionMessage {
				t.Fatalf("result=%#v err=%v, want normal remote error passthrough", result, err)
			}
			if !ReplayLoopHardFailure(err) {
				t.Fatalf("remote 400 was not classified as a hard loop failure: %v", err)
			}
			_, _, records := sink.snapshot()
			if len(records) != 1 || !strings.Contains(records[0].Fields["detail"].(string), remoteMessage) {
				t.Fatalf("remote provenance = %#v", records)
			}
		})
	}

	for name, remoteMessage := range map[string]string{
		"snapshot token":             "source dt.davis.problems.snapshots failed",
		"generated bound":            `generated bound toTimestamp("2026-06-14T03:00:00.000000000Z") failed`,
		"normalized generated bound": `generated bound TOTIMESTAMP ( "2026-06-14T03:00:00+00:00" ) failed`,
		"named generated bound":      `generated bound toTimestamp(value: "2026-06-14T03:00:00Z") failed`,
		"standalone generated bound": `snapshot read starts at 2026-06-14T03:00:00Z`,
		"inserted stage":             "inserted stage dedup event.id failed",
		"inserted event token":       "unknown field event.end",
		"inserted sort token":        "unknown command sort",
		"inserted time token":        "unknown field timestamp",
		"inserted dedup token":       "unknown command dedup",
		"inserted filter token":      "unknown command filter",
		"inserted function":          "unknown function coalesce",
		"effective query":            replayDavisEffective,
	} {
		t.Run("mapped "+name, func(t *testing.T) {
			api := newReplayDavisMockAPI(t)
			api.executeStatus = http.StatusBadRequest
			api.remoteErrorMessage = remoteMessage
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
				mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"),
				func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
			if result != nil || err == nil || err.Error() != restrictedRemoteExecutionMessage || strings.Contains(err.Error(), remoteMessage) {
				t.Fatalf("result=%#v err=%v, want mapped remote fallback", result, err)
			}
			_, _, records := sink.snapshot()
			if len(records) != 2 || records[1].Event != "query_execution" || !strings.Contains(fmt.Sprint(records[1].Fields["detail"]), remoteMessage) {
				t.Fatalf("mapped remote provenance = %#v", records)
			}
			parseCalls, executeCalls, _ := api.counts()
			coverageRequests, _ := api.coverage()
			if parseCalls != 2 || executeCalls != 1 || len(coverageRequests) != 1 {
				t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
			}
		})
	}

	for name, remoteMessage := range map[string]string{
		"snapshot token":                   "source dt.davis.problems.snapshots failed",
		"normalized generated bound":       `invalid toTimestamp ( "2026-06-14T03:00:00Z" )`,
		"named normalized generated bound": `invalid TOTIMESTAMP ( VALUE : "2026-06-14T03:00:00+00:00" )`,
		"standalone generated bound":       `snapshot read starts at 2026-06-14T03:00:00Z`,
		"inserted token":                   "unknown field event.start",
		"inserted operator":                "unknown function coalesce",
	} {
		t.Run("mapped polling "+name, func(t *testing.T) {
			api := newReplayDavisMockAPI(t)
			api.async = true
			api.pollStatus = http.StatusBadGateway
			api.pollErrorMessage = remoteMessage
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
				mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"),
				func(string) session.ProvenanceSink { return sink })

			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayDavisOriginal, DQLExecuteOptions{AgentMode: true})
			if result != nil || err == nil || err.Error() != restrictedRemoteExecutionMessage || strings.Contains(err.Error(), api.pollErrorMessage) {
				t.Fatalf("result=%#v err=%v, want mapped polling fallback", result, err)
			}
			_, _, records := sink.snapshot()
			recordedDetail := ""
			if len(records) == 2 {
				recordedDetail = strings.ReplaceAll(fmt.Sprint(records[1].Fields["detail"]), `\"`, `"`)
			}
			if len(records) != 2 || records[1].Event != "query_execution" ||
				!strings.Contains(recordedDetail, api.pollErrorMessage) {
				t.Fatalf("mapped polling provenance = %#v", records)
			}
			parseCalls, executeCalls, pollCalls := api.counts()
			coverageRequests, _ := api.coverage()
			if parseCalls != 2 || executeCalls != 1 || pollCalls != 1 || len(coverageRequests) != 1 {
				t.Fatalf("parse=%d coverage=%d execute=%d poll=%d", parseCalls, len(coverageRequests), executeCalls, pollCalls)
			}
		})
	}

	info := ReplayExecutionInfo{Active: true, Disclosure: session.ReplayDisclosureRestricted}
	generated := newReplayAttemptError(replayErrorRemote, errors.New("effective request failed"), info, false, 0, true)
	if generated.Error() != restrictedRemoteExecutionMessage {
		t.Fatalf("generated restricted-word remote error = %q", generated)
	}
	remotePoll := httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"remote session unavailable"}`)
	passthrough := newReplayAttemptError(replayErrorRemote, remotePoll, info, false, 0, true)
	if passthrough.Error() != remotePoll.Error() {
		t.Fatalf("remote poll content was rewritten: got %q want %q", passthrough, remotePoll)
	}
	generatedWrapper := fmt.Errorf("effective request failed: %w", remotePoll)
	masked := newReplayAttemptError(replayErrorRemote, generatedWrapper, info, false, 0, true)
	if masked.Error() != restrictedRemoteExecutionMessage {
		t.Fatalf("dtctl-generated restricted wrapper was not masked: %q", masked)
	}

	mappedInfo := ReplayExecutionInfo{
		Active: true, Disclosure: session.ReplayDisclosureRestricted,
		OriginalQuery: replayDavisOriginal, EffectiveQuery: replayDavisEffective,
		Output: &output.ReplayMetadata{Sources: []output.ReplaySourceMetadata{{
			DavisProblemsMapping: &output.DavisProblemsMappingMetadata{Eligible: true},
		}}},
	}
	verbatimOriginal := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: replayDavisOriginal}
	if got := newReplayAttemptError(replayErrorRemote, verbatimOriginal, mappedInfo, false, 0, true); got.Error() != verbatimOriginal.Error() {
		t.Fatalf("verbatim original query was masked: got %q want %q", got, verbatimOriginal)
	}
	for name, detail := range map[string]error{
		"poll snapshot token":                 httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"dt.davis.problems.snapshots"}`),
		"poll generated timestamp":            httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"toTimestamp(\"2026-06-14T03:00:00.000000000Z\")"}`),
		"poll normalized generated timestamp": httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"TOTIMESTAMP ( \"2026-06-14T03:00:00+00:00\" )"}`),
		"poll standalone generated timestamp": httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"snapshot read starts at 2026-06-14T03:00:00Z"}`),
		"poll inserted token":                 httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", `{"error":"event.id"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if got := newReplayAttemptError(replayErrorRemote, detail, mappedInfo, false, 0, true); got.Error() != restrictedRemoteExecutionMessage {
				t.Fatalf("mapped polling detail was not masked: %q", got)
			}
		})
	}
	collidingBound := &sdkquery.QueryError{
		StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR",
		Message: `invalid bound toTimestamp ( "2026-06-14T09:00:00Z" )`,
	}
	if got := newReplayAttemptError(replayErrorRemote, collidingBound, mappedInfo, false, 0, true); got.Error() != restrictedRemoteExecutionMessage {
		t.Fatalf("user/generated timestamp collision was not masked: %q", got)
	}
	for name, message := range map[string]string{
		"logical F": "invalid bound 2026-06-14T09:00:00Z",
		"upper T":   "invalid bound 2026-06-14T10:00:00+00:00",
	} {
		t.Run("standalone user-generated collision "+name, func(t *testing.T) {
			detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: message}
			if got := newReplayAttemptError(replayErrorRemote, detail, mappedInfo, false, 0, true); got.Error() != restrictedRemoteExecutionMessage {
				t.Fatalf("standalone user/generated timestamp collision was not masked: %q", got)
			}
		})
	}
	for _, lexeme := range []string{"sort", "timestamp", "dedup", "event.id", "filter", "event.start", "coalesce", "event.end"} {
		t.Run("user-generated collision "+lexeme, func(t *testing.T) {
			userAuthored := mappedInfo
			userAuthored.OriginalQuery += " | fields " + lexeme
			detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: "unknown field " + lexeme}
			if got := newReplayAttemptError(replayErrorRemote, detail, userAuthored, false, 0, true); got.Error() != restrictedRemoteExecutionMessage {
				t.Fatalf("user/generated mapping lexeme collision was not masked: %q", got)
			}
		})
	}
	boundaryControl := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: "unknown field event.identity"}
	if got := newReplayAttemptError(replayErrorRemote, boundaryControl, mappedInfo, false, 0, true); got.Error() != boundaryControl.Error() {
		t.Fatalf("mapping-token boundary control was masked: got %q want %q", got, boundaryControl)
	}
}

func TestDQLExecutorFullDisclosureRoutesCompilerNoticesOnce(t *testing.T) {
	executor := &DQLExecutor{}
	prepared := PreparedQuery{
		Disclosure: session.ReplayDisclosureFull,
		Compilation: execreplay.CompileResult{Notices: []execreplay.Notice{
			{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeDavisWarmup,
				Message: "synthetic compatibility warning",
			},
			{
				Kind: execreplay.NoticeNotification, Code: execreplay.NoticeFixedDayInterval,
				Message: "synthetic per-execution mapping notification", PerExecution: true,
			},
		}},
	}
	stderr := captureReplayExecutorStderr(t, func() {
		executor.printReplayNoticeOnce(prepared, DQLExecuteOptions{})
		executor.printReplayNoticeOnce(prepared, DQLExecuteOptions{})
	})
	if strings.Count(stderr, "synthetic compatibility warning") != 1 || !strings.Contains(stderr, "Warning:") {
		t.Fatalf("compiler notice route = %q", stderr)
	}
	if strings.Count(stderr, "synthetic per-execution mapping notification") != 2 {
		t.Fatalf("mapping notification was not routed per execution: %q", stderr)
	}

	agentExecutor := &DQLExecutor{}
	stderr = captureReplayExecutorStderr(t, func() {
		agentExecutor.printReplayNoticeOnce(prepared, DQLExecuteOptions{AgentMode: true})
	})
	if stderr != "" {
		t.Fatalf("agent warning must wait for the Phase 5 envelope route: %q", stderr)
	}
}

func TestRestrictedReplayDisclosureLeakMatrixMessagesAndNotices(t *testing.T) {
	info := ReplayExecutionInfo{Active: true, Disclosure: session.ReplayDisclosureRestricted}
	surfaces := map[string]string{
		"hard non-overlap":      restrictedMessage(replayErrorNonOverlap, errors.New("replay interval does not overlap"), false, false),
		"temporary non-overlap": restrictedMessage(replayErrorNonOverlap, errors.New("virtual interval pending"), true, false),
		"readiness":             restrictedMessage(replayErrorReadiness, errors.New("session is stopped"), false, false),
		"preparation":           restrictedMessage(replayErrorPrepare, errors.New("effective query unsupported"), false, false),
		"validation":            restrictedMessage(replayErrorValidation, errors.New("interval spill invalid"), false, true),
		"finalization":          restrictedMessage(replayErrorFinalize, errors.New("session completion failed"), false, true),
		"sink preflight":        restrictedMessage(replayErrorSink, errors.New("replay sink unavailable"), false, false),
		"sink post-execution":   restrictedMessage(replayErrorSink, errors.New("replay sink append failed"), false, true),
		"remote fallback":       newReplayAttemptError(replayErrorRemote, errors.New("effective request failed"), info, false, 0, true).Error(),
		"other":                 restrictedMessage("synthetic_other", errors.New("replay detail"), false, false),
	}
	for name, value := range surfaces {
		if containsRestrictedGeneratedWord(value) {
			t.Errorf("restricted %s contains a disclosure word: %q", name, value)
		}
	}

	prepared := PreparedQuery{
		Disclosure: session.ReplayDisclosureRestricted,
		Compilation: execreplay.CompileResult{Notices: []execreplay.Notice{
			{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionBoundary,
				Message: "replay retention boundary virtual interval",
			},
			{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeHistoricalResolutionUnverified,
				Message: "historical resolution was not verified for this replay session",
			},
			{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionNotVerified,
				Message: "Retention was not verified for synthetic.unmapped because the retention inspection covers only the logs, spans, events, bizevents, metrics, and dt.system.events families.",
			},
		}},
	}
	stderr := captureReplayExecutorStderr(t, func() {
		(&DQLExecutor{}).printReplayNoticeOnce(prepared, DQLExecuteOptions{})
	})
	if stderr != "" {
		t.Fatalf("restricted compiler notice reached ordinary stderr: %q", stderr)
	}
}

func TestDQLExecutorReplayNoSessionFailsBeforeParseOrExecute(t *testing.T) {
	api := newReplayMockAPI(t)
	server := httptest.NewServer(api)
	defer server.Close()
	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	clock := &replayExecutorFakeClock{now: time.Now()}
	locator := session.ReplayLocator{ContextKey: session.ContextKey(strings.Repeat("c", 64)), ContextIdentityHash: strings.Repeat("d", 64)}
	preparer, err := NewReplayQueryPreparer(ReplayPreparerConfig{
		Store: session.NewReplayStateStore(filepath.Join(t.TempDir(), "replay"), clock), Clock: clock, Locator: locator, ContextName: "synthetic-replay",
		ExpectedContextInputHash: replayTestContextInputHash, ExpectedEnvironmentHash: replayTestEnvironmentHash,
		EnvironmentID: replayTestEnvironmentHash, ClientIdentity: "synthetic-principal",
		FallbackDisclosure: session.ReplayDisclosureFull, SourcePolicy: execreplay.Milestone1SourcePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDQLExecutor(transport).WithQueryPreparer(preparer).ExecuteQueryDetailedWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
	if err == nil || !strings.Contains(err.Error(), "no replay session is active") {
		t.Fatalf("error = %v", err)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 0 || executeCalls != 0 {
		t.Fatalf("parse=%d execute=%d, want zero", parseCalls, executeCalls)
	}
}

func TestDQLExecutorExplainReplayAuditsWithoutExecutingOrClaimingTerminal(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)
	explanation, err := fixture.executor.ExplainReplayWithContext(context.Background(), replayRecordOriginal, DQLExecuteOptions{AgentMode: true})
	if err != nil {
		t.Fatal(err)
	}
	if explanation.EffectiveDQL == "" || !explanation.Clock.VirtualNow.Equal(replayRecordDataEnd) || len(explanation.Sources) != 1 {
		t.Fatalf("explanation = %#v", explanation)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 2 || executeCalls != 0 {
		t.Fatalf("explain parse=%d execute=%d, want two parses and no execute", parseCalls, executeCalls)
	}
	state, stateErr := fixture.store.Status(fixture.locator)
	if stateErr != nil || state.Status != session.ReplayStatusTerminalReady || state.CompletedAt != nil {
		t.Fatalf("explain claimed terminal: state=%#v err=%v", state, stateErr)
	}
}

func TestDQLExecutorVerifyReplayCompatibilityNeverExecutesOrCompletes(t *testing.T) {
	api := newReplayMockAPI(t)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordDataEnd, replayRecordDataEnd, nil)

	compatibility, err := fixture.executor.VerifyReplayCompatibilityWithContext(
		context.Background(), replayRecordOriginal, DQLVerifyOptions{Timezone: "UTC", Locale: "en_US"}, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if compatibility == nil || !compatibility.FullDisclosure() || !compatibility.OriginalDQLValid ||
		!compatibility.CompilerSupported || !compatibility.EffectiveQueryValid || len(compatibility.UnsupportedConstructs) != 0 {
		t.Fatalf("compatibility = %#v", compatibility)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 2 || executeCalls != 0 {
		t.Fatalf("verify compatibility parse=%d execute=%d, want two parses and no execute", parseCalls, executeCalls)
	}
	state, stateErr := fixture.store.Status(fixture.locator)
	if stateErr != nil || state.Status != session.ReplayStatusTerminalReady || state.CompletedAt != nil {
		t.Fatalf("verification claimed terminal: state=%#v err=%v", state, stateErr)
	}
}

func TestDQLExecutorVerifyReplayCompatibilityNamesUnsupportedConstruct(t *testing.T) {
	api := newReplayMockAPI(t)
	api.originalBody = bytes.Replace(api.originalBody,
		[]byte(`"canonicalString": "filter"`), []byte(`"canonicalString": "unapprovedCommand"`), 1)
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)

	compatibility, err := fixture.executor.VerifyReplayCompatibilityWithContext(context.Background(), replayRecordOriginal, DQLVerifyOptions{Timezone: "UTC"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if compatibility == nil || compatibility.CompilerSupported || compatibility.EffectiveQueryValid ||
		len(compatibility.UnsupportedConstructs) != 1 || compatibility.UnsupportedConstructs[0] != "unapprovedcommand" {
		t.Fatalf("compatibility = %#v", compatibility)
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 1 || executeCalls != 0 {
		t.Fatalf("unsupported compatibility parse=%d execute=%d, want one parse and no execute", parseCalls, executeCalls)
	}
}

func TestDQLExecutorVerifyReplayCompatibilityRestrictedRecordsDetails(t *testing.T) {
	api := newReplayMockAPI(t)
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(
		t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
		replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd,
		func(string) session.ProvenanceSink { return sink },
	)

	compatibility, err := fixture.executor.VerifyReplayCompatibilityWithContext(context.Background(), replayRecordOriginal, DQLVerifyOptions{Timezone: "UTC"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if compatibility == nil || compatibility.FullDisclosure() || !compatibility.CompilerSupported || !compatibility.EffectiveQueryValid {
		t.Fatalf("compatibility = %#v", compatibility)
	}
	preflights, appends, records := sink.snapshot()
	if preflights != 1 || appends != 1 || len(records) != 1 || records[0].Event != "query_verification" {
		t.Fatalf("sink preflights=%d appends=%d records=%#v", preflights, appends, records)
	}
	if records[0].Fields["original_dql"] != replayRecordOriginal || records[0].Fields["effective_dql"] != replayRecordEffective {
		t.Fatalf("query forms = %#v", records[0].Fields)
	}
	verification, ok := records[0].Fields["verification"].(map[string]any)
	if !ok || verification["original_dql_valid"] != true || verification["compiler_supported"] != true || verification["effective_query_valid"] != true {
		t.Fatalf("verification provenance = %#v", records[0].Fields["verification"])
	}
	parseCalls, executeCalls, _ := api.counts()
	if parseCalls != 2 || executeCalls != 0 {
		t.Fatalf("restricted compatibility parse=%d execute=%d, want two parses and no execute", parseCalls, executeCalls)
	}
}

func TestDQLExecutorVerifyReplayCompatibilityRestrictedDavisMapsWithoutCoverage(t *testing.T) {
	api := newReplayDavisMockAPI(t)
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(
		t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
		mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"),
		func(string) session.ProvenanceSink { return sink },
	)

	compatibility, err := fixture.executor.VerifyReplayCompatibilityWithContext(
		context.Background(), replayDavisOriginal, DQLVerifyOptions{Timezone: "UTC"}, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if compatibility == nil || compatibility.FullDisclosure() || !compatibility.CompilerSupported ||
		!compatibility.EffectiveQueryValid || compatibility.EffectiveQuery != replayDavisEffective ||
		compatibility.CoverageVerified == nil || *compatibility.CoverageVerified ||
		compatibility.CoverageMessage != execreplay.DavisCoverageNotVerifiedMessage {
		t.Fatalf("restricted Davis compatibility = %#v", compatibility)
	}
	preflights, appends, records := sink.snapshot()
	if preflights != 1 || appends != 1 || len(records) != 1 || records[0].Event != "query_verification" {
		t.Fatalf("sink preflights=%d appends=%d records=%#v", preflights, appends, records)
	}
	fields := records[0].Fields
	if fields["original_dql"] != replayDavisOriginal || fields["effective_dql"] != replayDavisEffective {
		t.Fatalf("query forms = %#v", fields)
	}
	coverage, ok := fields["davis_snapshot_coverage"].(map[string]any)
	if !ok || coverage["status"] != "not_checked" || coverage["coverage_verified"] != false || coverage["observed_at"] != nil || coverage["reuse"] != nil {
		t.Fatalf("probe-free coverage provenance = %#v", fields["davis_snapshot_coverage"])
	}
	sources, ok := fields["sources"].([]map[string]any)
	if !ok || len(sources) != 1 || sources[0]["davis_problems_mapping"] == nil {
		t.Fatalf("mapping provenance = %#v", fields["sources"])
	}
	mapping := sources[0]["davis_problems_mapping"].(map[string]any)
	logical := mapping["logical_view_range"].(map[string]string)
	physical := mapping["physical_snapshot_range"].(map[string]string)
	if logical["f"] != "2026-06-14T09:00:00Z" || logical["t"] != "2026-06-14T10:00:00Z" ||
		physical["w"] != "2026-06-14T03:00:00Z" || physical["t"] != "2026-06-14T10:00:00Z" ||
		mapping["warmup_clamped"] != false {
		t.Fatalf("probe-free mapping ranges logical=%#v physical=%#v mapping=%#v", logical, physical, mapping)
	}
	audit, ok := fields["audit"].(map[string]any)
	if !ok || audit["ok"] != true || audit["davis_mappings_audited"] != true {
		t.Fatalf("mapping audit provenance = %#v", fields["audit"])
	}
	parseCalls, executeCalls, _ := api.counts()
	coverageRequests, _ := api.coverage()
	if parseCalls != 2 || executeCalls != 0 || len(coverageRequests) != 0 {
		t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
	}
}

func TestDQLExecutorDavisCurrentViewGuidanceUsesUnchangedDisclosureRoutes(t *testing.T) {
	eventsOriginal := strings.Replace(replayRecordOriginal, "fetch logs", "fetch dt.davis.events", 1)
	tests := []struct {
		name       string
		disclosure string
		view       string
		query      string
		api        func(*testing.T) *replayMockAPI
	}{
		{"full events", session.ReplayDisclosureFull, "dt.davis.events", eventsOriginal, newReplayMockAPI},
		{"restricted events", session.ReplayDisclosureRestricted, "dt.davis.events", eventsOriginal, newReplayMockAPI},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := test.api(t)
			api.isOriginal = func(query string) bool { return query == test.query }
			if test.view == "dt.davis.events" {
				api.originalBody = bytes.Replace(api.originalBody,
					[]byte(`"canonicalString": "logs"`), []byte(`"canonicalString": "dt.davis.events"`), 1)
			}
			sink := &replayTestSink{}
			var sinkFactory func(string) session.ProvenanceSink
			if test.disclosure == session.ReplayDisclosureRestricted {
				sinkFactory = func(string) session.ProvenanceSink { return sink }
			}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, test.disclosure,
				mustReplayTestTime("2026-06-14T02:00:00Z"), mustReplayTestTime("2026-06-14T10:00:00Z"), mustReplayTestTime("2026-06-14T12:00:00Z"), sinkFactory)

			_, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.query, DQLExecuteOptions{AgentMode: true})
			if err == nil {
				t.Fatal("current Davis view unexpectedly executed")
			}
			if test.disclosure == session.ReplayDisclosureFull {
				if !strings.Contains(err.Error(), "dt.davis.events.snapshots") || !strings.Contains(err.Error(), "dedup event.id") {
					t.Fatalf("full Davis events guidance = %v", err)
				}
			} else {
				if err.Error() != restrictedPreparationMessage {
					t.Fatalf("restricted error = %q", err)
				}
				_, _, records := sink.snapshot()
				if len(records) != 1 || !strings.Contains(fmt.Sprint(records[0].Fields["detail"]), test.view+".snapshots") ||
					!strings.Contains(fmt.Sprint(records[0].Fields["detail"]), "dedup event.id") {
					t.Fatalf("restricted Davis provenance = %#v", records)
				}
			}
			parseCalls, executeCalls, _ := api.counts()
			coverageRequests, _ := api.coverage()
			if parseCalls != 1 || len(coverageRequests) != 0 || executeCalls != 0 {
				t.Fatalf("parse=%d coverage=%d execute=%d", parseCalls, len(coverageRequests), executeCalls)
			}
		})
	}
}

func TestDQLExecutorDirectDavisSnapshotsRemainOrdinaryAndProbeFree(t *testing.T) {
	tests := []struct {
		name       string
		original   string
		effective  string
		fixtureDir string
	}{
		{
			"problem snapshots",
			`fetch dt.davis.problems.snapshots, from:toTimestamp("2026-08-03T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`,
			`fetch dt.davis.problems.snapshots, from:toTimestamp("2026-08-03T10:55:03.000000000Z"), to:toTimestamp("2026-08-10T10:55:03.000000000Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`,
			"davis-problems-snapshots",
		},
		{
			"event snapshots",
			`fetch dt.davis.events.snapshots, from:toTimestamp("2026-08-03T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`,
			`fetch dt.davis.events.snapshots, from:toTimestamp("2026-08-03T10:55:03.000000000Z"), to:toTimestamp("2026-08-10T10:55:03.000000000Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`,
			"davis-events-snapshots",
		},
	}
	for _, test := range tests {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api := newReplayMockAPI(t)
				api.originalBody = replayFixtureBody(t, "phase0b/fixtures/records/"+test.fixtureDir+"/00-discovery/parse.json")
				api.validationBody = replayFixtureBody(t, "phase0b/fixtures/records/"+test.fixtureDir+"/00-discovery/validation-parse.json")
				api.isOriginal = func(query string) bool { return query == test.original }
				api.executeResponse.Result.Metadata.Grail.CanonicalQuery = test.effective
				var sinkFactory func(string) session.ProvenanceSink
				if disclosure == session.ReplayDisclosureRestricted {
					sink := &replayTestSink{}
					sinkFactory = func(string) session.ProvenanceSink { return sink }
				}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
					mustReplayTestTime("2026-08-03T10:55:03Z"), mustReplayTestTime("2026-08-10T10:55:03Z"), mustReplayTestTime("2026-08-11T10:55:03Z"), sinkFactory)
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.original, DQLExecuteOptions{AgentMode: true})
				if err != nil {
					t.Fatal(err)
				}
				_, executions := api.queries()
				coverageRequests, _ := api.coverage()
				if len(coverageRequests) != 0 || len(executions) != 1 || executions[0].Query != test.effective ||
					result.Replay.Output.DavisSnapshotCoverage != nil || result.Replay.Output.Sources[0].DavisProblemsMapping != nil {
					t.Fatalf("coverage=%d executions=%#v metadata=%#v", len(coverageRequests), executions, result.Replay.Output)
				}
				if disclosure == session.ReplayDisclosureRestricted && ReplayOutputMetadata(result.Replay) != nil {
					t.Fatal("restricted direct snapshot query exposed ordinary replay metadata")
				}
			})
		}
	}
}

func mustReplayTestTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func replayFixtureBody(t *testing.T, relative string) json.RawMessage {
	t.Helper()
	path := filepath.Join("..", "..", "sdk", "api", "query", "testdata", relative)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var capture struct {
		ResponseBody json.RawMessage `json:"response_body"`
	}
	if err := json.Unmarshal(raw, &capture); err == nil && len(capture.ResponseBody) > 0 {
		return capture.ResponseBody
	}
	return raw
}

func replayFixtureWithSourceToken(t *testing.T, relative, oldToken, newToken string) json.RawMessage {
	t.Helper()
	var root sdkquery.ParseResponse
	if err := json.Unmarshal(replayFixtureBody(t, relative), &root); err != nil {
		t.Fatal(err)
	}
	var target *sdkquery.DQLNode
	walkSDKNode(&root, func(node *sdkquery.DQLNode) {
		if target == nil && node.Terminal != nil && node.Terminal.Type == "DATA_OBJECT" && node.Terminal.CanonicalString == oldToken {
			target = node
		}
	})
	if target == nil || target.TokenPosition == nil {
		t.Fatalf("fixture %s has no positioned DATA_OBJECT %q", relative, oldToken)
	}
	oldEnd := target.TokenPosition.End
	delta := len(newToken) - len(oldToken)
	walkSDKNode(&root, func(node *sdkquery.DQLNode) {
		if node.TokenPosition == nil {
			return
		}
		if node.TokenPosition.Start.Index > oldEnd.Index {
			node.TokenPosition.Start.Index += delta
			if node.TokenPosition.Start.Line == oldEnd.Line {
				node.TokenPosition.Start.Column += delta
			}
		}
		if node.TokenPosition.End.Index >= oldEnd.Index {
			node.TokenPosition.End.Index += delta
			if node.TokenPosition.End.Line == oldEnd.Line {
				node.TokenPosition.End.Column += delta
			}
		}
	})
	target.Terminal.CanonicalString = newToken
	encoded, err := json.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func dynamicValidationBody(t *testing.T, raw json.RawMessage, query string) json.RawMessage {
	t.Helper()
	var root sdkquery.ParseResponse
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	from, to, ok := sourceBoundaryStrings(query)
	if !ok {
		return raw
	}
	values := []string{from, to}
	if strings.Contains(query, "| sort timestamp desc") && strings.Contains(query, "| dedup event.id") {
		values = timestampArguments(query)
	}
	stringsSeen := 0
	walkSDKNode(&root, func(node *sdkquery.DQLNode) {
		if !sdkTimestampTerminal(node) || stringsSeen >= len(values) {
			return
		}
		node.Terminal.CanonicalString = `"` + values[stringsSeen] + `"`
		stringsSeen++
	})
	if stringsSeen != len(values) {
		t.Fatalf("validation fixture exposed %d source-boundary strings", stringsSeen)
	}
	encoded, err := json.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func sdkTimestampTerminal(node *sdkquery.DQLNode) bool {
	return node != nil && node.Terminal != nil &&
		(node.Terminal.Type == "STRING" || node.Terminal.Type == "TIMESTAMP_VALUE")
}

func timestampArguments(query string) []string {
	const marker = `toTimestamp("`
	var values []string
	for offset := 0; offset < len(query); {
		start := strings.Index(query[offset:], marker)
		if start < 0 {
			break
		}
		start += offset + len(marker)
		end := strings.Index(query[start:], `")`)
		if end < 0 {
			break
		}
		values = append(values, query[start:start+end])
		offset = start + end + 2
	}
	return values
}

func sourceBoundaryStrings(query string) (string, string, bool) {
	const marker = `toTimestamp("`
	first := strings.Index(query, marker)
	if first < 0 {
		return "", "", false
	}
	first += len(marker)
	firstEnd := strings.Index(query[first:], `")`)
	if firstEnd < 0 {
		return "", "", false
	}
	from := query[first : first+firstEnd]
	secondStart := first + firstEnd + 2
	second := strings.Index(query[secondStart:], marker)
	if second < 0 {
		return "", "", false
	}
	second = secondStart + second + len(marker)
	secondEnd := strings.Index(query[second:], `")`)
	if secondEnd < 0 {
		return "", "", false
	}
	return from, query[second : second+secondEnd], true
}

func walkSDKNode(node *sdkquery.DQLNode, visit func(*sdkquery.DQLNode)) {
	if node == nil {
		return
	}
	visit(node)
	if node.Container != nil {
		for _, child := range node.Container.Children {
			walkSDKNode(child, visit)
		}
	}
	if node.Alternative != nil {
		for _, child := range node.Alternative.Alternatives {
			walkSDKNode(child, visit)
		}
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func captureReplayExecutorStderr(t *testing.T, run func()) string {
	t.Helper()
	old := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	run()
	_ = writer.Close()
	os.Stderr = old
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	return string(data)
}
