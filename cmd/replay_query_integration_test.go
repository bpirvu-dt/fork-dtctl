package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	pkgclient "github.com/dynatrace-oss/dtctl/pkg/client"
	"github.com/dynatrace-oss/dtctl/pkg/config"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const (
	replayCLIRecordOriginal  = `fetch logs, from:toTimestamp("2026-08-10T10:45:02.718012207Z"), to:toTimestamp("2026-08-10T11:05:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	replayCLIRecordEffective = `fetch logs, from:toTimestamp("2026-08-10T10:50:02.718012207Z"), to:toTimestamp("2026-08-10T10:55:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	replayCLILoopOriginal    = `fetch logs, from:toTimestamp("2026-08-09T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
	replayCLIDavisOriginal   = `fetch dt.davis.problems, from:toTimestamp("2026-06-14T09:00:00.000Z"), to:toTimestamp("2026-06-14T10:00:00.000Z")`
	replayCLIDavisEffective  = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")
| sort timestamp desc
| dedup event.id
| filter event.start < toTimestamp("2026-06-14T10:00:00.000000000Z") and coalesce(event.end, toTimestamp("2026-06-14T10:00:00.000000000Z")) >= toTimestamp("2026-06-14T09:00:00.000000000Z")`
)

type replayCLIQueryAPI struct {
	t                *testing.T
	originalQuery    string
	originalBody     json.RawMessage
	validationBody   json.RawMessage
	executeFailures  int
	executeStatus    int
	inspectionStatus int
	coverageStatus   int
	coverageOldest   string
	remoteError      string
	executeResponse  *sdkquery.Response

	mu        sync.Mutex
	parses    int
	executes  int
	inspects  int
	coverages int
	verifies  int
	queries   []string
}

func (a *replayCLIQueryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/platform/storage/query/v1/query:parse":
		var request sdkquery.ParseRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode parse request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.parses++
		a.mu.Unlock()
		body := a.validationBody
		if request.Query == a.originalQuery {
			body = a.originalBody
		} else if len(body) > 0 {
			body = replayCLIQueryDynamicValidation(a.t, body, request.Query)
		}
		if len(body) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
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
		if strings.Contains(request.Query, "fetch dt.system.buckets") {
			a.mu.Lock()
			a.inspects++
			status := a.inspectionStatus
			a.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if status != 0 && status != http.StatusOK {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"message":"current retention metadata unavailable"}}`))
				return
			}
			_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{
				{"dt.system.table": "logs", "bucket_count": "2", "minimum_retention_days": "35", "maximum_retention_days": "90"},
				{"dt.system.table": "spans", "bucket_count": "1", "minimum_retention_days": "10", "maximum_retention_days": "1000"},
				{"dt.system.table": "events", "bucket_count": "1", "minimum_retention_days": "35", "maximum_retention_days": "462"},
				{"dt.system.table": "bizevents", "bucket_count": "1", "minimum_retention_days": "35", "maximum_retention_days": "3657"},
				{"dt.system.table": "metrics", "bucket_count": "1", "minimum_retention_days": "180", "maximum_retention_days": "462"},
				{"dt.system.table": "dt.system.events", "bucket_count": "1", "minimum_retention_days": "35", "maximum_retention_days": "372"},
			}}})
			return
		}
		if strings.Contains(request.Query, "oldest_snapshot=min(timestamp)") {
			a.mu.Lock()
			a.coverages++
			status, oldest := a.coverageStatus, a.coverageOldest
			a.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if status != 0 && status != http.StatusOK {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"message":"synthetic coverage failure"}}`))
				return
			}
			records := []map[string]interface{}{}
			if oldest != "" {
				records = append(records, map[string]interface{}{"oldest_snapshot": oldest})
			}
			_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: records}})
			return
		}
		a.mu.Lock()
		a.executes++
		a.queries = append(a.queries, request.Query)
		shouldFail := a.executeFailures > 0
		if shouldFail {
			a.executeFailures--
		}
		status := a.executeStatus
		a.mu.Unlock()
		if shouldFail {
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			message := a.remoteError
			if message == "" {
				message = "query request failed"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"message": "query failed", "details": map[string]any{"errorType": "REMOTE_ERROR", "errorMessage": message},
			}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		response := sdkquery.Response{
			State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{"matched": float64(1)}}},
		}
		if a.executeResponse != nil {
			response = *a.executeResponse
		}
		_ = json.NewEncoder(w).Encode(response)
	case "/platform/storage/query/v1/query:verify":
		var request sdkquery.VerifyRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode verify request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.verifies++
		a.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sdkquery.VerifyResponse{Valid: true, CanonicalQuery: request.Query})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *replayCLIQueryAPI) counts() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.parses, a.executes
}

func (a *replayCLIQueryAPI) executedQueries() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.queries...)
}

func (a *replayCLIQueryAPI) verifyCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.verifies
}

func (a *replayCLIQueryAPI) inspectionCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inspects
}

func (a *replayCLIQueryAPI) coverageCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.coverages
}

type replayCLIQueryFixture struct {
	clock   *replayCLIFakeClock
	store   *session.ReplayStateStore
	locator session.ReplayLocator
	state   session.ReplaySession
}

func newReplayCLIQueryFixture(t *testing.T, serverURL, disclosure, clockMode string, dataStart, virtualStart, dataEnd time.Time) replayCLIQueryFixture {
	t.Helper()
	t.Setenv("DTCTL_DISABLE_KEYRING", "1")
	t.Setenv(config.EnvTokenStorage, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	stateDir := filepath.Join(dir, "state", "replay")
	raw := &config.ReplayConfig{
		DataStart: dataStart.Format(time.RFC3339Nano), DataEnd: dataEnd.Format(time.RFC3339Nano),
		VirtualStart: virtualStart.Format(time.RFC3339Nano), ClockMode: clockMode,
		Disclosure: disclosure,
	}
	cfg := replayCLIConfig(raw)
	cfg.Contexts[0].Context.Environment = serverURL
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "dt0c01.synthetic"}}
	writeReplayCLIConfig(t, configPath, cfg)
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, stateDir, clock)
	loaded, err := config.LoadFrom(configPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := loaded.CurrentContextObj()
	if err != nil {
		t.Fatal(err)
	}
	source, err := loaded.SourceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	locator := session.ReplayLocator{
		ContextKey:          session.NewReplayContextKey(source, loaded.CurrentContext, ctx.Environment),
		ContextIdentityHash: session.ReplayContextIdentityHash(source, loaded.CurrentContext),
	}
	resolved, err := session.ResolveReplayConfig(ctx.Replay, session.ReplayConfigOverrides{}, stateDir, locator.ContextKey)
	if err != nil {
		t.Fatal(err)
	}
	store := session.NewReplayStateStore(stateDir, clock)
	state, err := store.Start(session.ReplayStartRequest{
		Locator: locator, ContextName: loaded.CurrentContext,
		EnvironmentHash: session.ReplayEnvironmentHash(ctx.Environment), ContextInputHash: session.ReplayContextInputHash(ctx),
		Config: resolved,
	})
	if err != nil {
		t.Fatal(err)
	}
	return replayCLIQueryFixture{clock: clock, store: store, locator: locator, state: state}
}

func setReplayQueryFlags(t *testing.T, live bool, interval time.Duration, explain bool) {
	t.Helper()
	values := map[string]string{
		"live": "false", "interval": "1m", "explain-replay": "false", "file": "", "dql": "",
	}
	for name := range values {
		flag := queryCmd.Flags().Lookup(name)
		values[name] = flag.Value.String()
	}
	t.Cleanup(func() {
		for name, value := range values {
			_ = queryCmd.Flags().Set(name, value)
		}
	})
	_ = queryCmd.Flags().Set("live", map[bool]string{true: "true", false: "false"}[live])
	_ = queryCmd.Flags().Set("interval", interval.String())
	_ = queryCmd.Flags().Set("explain-replay", map[bool]string{true: "true", false: "false"}[explain])
	_ = queryCmd.Flags().Set("file", "")
	_ = queryCmd.Flags().Set("dql", "")
}

func setReplayQueryMetadataFlag(t *testing.T, value string) {
	t.Helper()
	flag := queryCmd.Flags().Lookup("metadata")
	previousValue, previousChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		_ = flag.Value.Set(previousValue)
		flag.Changed = previousChanged
	})
	if err := flag.Value.Set(value); err != nil {
		t.Fatal(err)
	}
	flag.Changed = true
}

func setReplayWaitQueryFlags(t *testing.T, interval time.Duration) {
	t.Helper()
	wanted := map[string]string{
		"for": "any", "timeout": "0", "max-attempts": "2", "initial-delay": "0",
		"min-interval": interval.String(), "max-interval": interval.String(), "backoff-multiplier": "2",
		"quiet": "false", "verbose": "false", "file": "",
	}
	previous := make(map[string]string, len(wanted))
	for name, value := range wanted {
		flag := waitQueryCmd.Flags().Lookup(name)
		previous[name] = flag.Value.String()
		if err := waitQueryCmd.Flags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for name, value := range previous {
			_ = waitQueryCmd.Flags().Set(name, value)
		}
	})
}

func setReplayVerifyQueryFlags(t *testing.T) {
	t.Helper()
	values := map[string]string{
		"file": "", "canonical": "false", "timezone": "UTC", "locale": "", "fail-on-warn": "false", "client-context": "",
	}
	previous := make(map[string]string, len(values))
	for name, value := range values {
		flag := verifyQueryCmd.Flags().Lookup(name)
		previous[name] = flag.Value.String()
		if err := verifyQueryCmd.Flags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for name, value := range previous {
			_ = verifyQueryCmd.Flags().Set(name, value)
		}
	})
}

func TestReplayQueryCLICadenceRejectsBeforeParseOrExecute(t *testing.T) {
	api := &replayCLIQueryAPI{t: t}
	server := httptest.NewServer(api)
	defer server.Close()
	newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	setReplayQueryFlags(t, true, 4*time.Second, false)
	err := queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	if err == nil || !strings.Contains(err.Error(), "supported minimum of 5s") {
		t.Fatalf("error = %v", err)
	}
	if parses, executes := api.counts(); parses != 0 || executes != 0 {
		t.Fatalf("parse=%d execute=%d, want zero", parses, executes)
	}
}

func TestReplayQueryCLIRestrictedCadenceIsGenericAndRecordedBeforeLoop(t *testing.T) {
	api := &replayCLIQueryAPI{t: t}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	setReplayQueryFlags(t, true, 4*time.Second, false)
	err := queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	if err == nil || err.Error() != "The query could not be prepared. It was not executed." {
		t.Fatalf("error = %v", err)
	}
	if parses, executes := api.counts(); parses != 0 || executes != 0 {
		t.Fatalf("parse=%d execute=%d, want zero", parses, executes)
	}
	provenance, readErr := os.ReadFile(fixture.state.ProvenancePath)
	if readErr != nil || !strings.Contains(string(provenance), "supported minimum of 5s") {
		t.Fatalf("provenance=%q err=%v", provenance, readErr)
	}
}

func TestReplayClientIdentityUsesStableNonSecretPrincipal(t *testing.T) {
	const token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJzeW50aGV0aWMtcHJpbmNpcGFsIn0."
	transport, err := pkgclient.New("http://127.0.0.1", token)
	if err != nil {
		t.Fatal(err)
	}
	identity := replayClientIdentity(transport, "synthetic-token-ref")
	if !strings.HasPrefix(identity, "principal-sha256:") || strings.Contains(identity, "synthetic-principal") || strings.Contains(identity, token) {
		t.Fatalf("principal identity is not a non-secret digest: %q", identity)
	}
	if fallback := replayClientIdentity(nil, "synthetic-token-ref"); fallback != "token-ref:synthetic-token-ref" {
		t.Fatalf("fallback identity = %q", fallback)
	}
}

func TestReplayQueryCLIExplainAndRestrictedUnknownFlag(t *testing.T) {
	t.Run("full disclosure explains without execute", func(t *testing.T) {
		api := &replayCLIQueryAPI{
			t: t, originalQuery: replayCLIRecordOriginal,
			originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
			validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		}
		server := httptest.NewServer(api)
		defer server.Close()
		fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual,
			mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
		setReplayQueryFlags(t, false, time.Minute, true)
		var runErr error
		stdout := captureStdout(t, func() { runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal}) })
		if runErr != nil || !strings.Contains(stdout, "Effective DQL:") {
			t.Fatalf("stdout=%q err=%v", stdout, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 0 {
			t.Fatalf("parse=%d execute=%d", parses, executes)
		}
		state, err := fixture.store.Status(fixture.locator)
		if err != nil || state.Status != session.ReplayStatusActive || state.CompletedAt != nil {
			t.Fatalf("explain changed state: %#v err=%v", state, err)
		}
	})

	t.Run("full disclosure agent output is structured", func(t *testing.T) {
		api := &replayCLIQueryAPI{
			t: t, originalQuery: replayCLIRecordOriginal,
			originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
			validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		}
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual,
			mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
		setReplayQueryFlags(t, false, time.Minute, true)
		agentMode = true
		var runErr error
		stdout := captureStdout(t, func() { runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal}) })
		if runErr != nil {
			t.Fatal(runErr)
		}
		var envelope struct {
			OK     bool `json:"ok"`
			Result struct {
				VirtualNow     string `json:"virtual_now"`
				EffectiveQuery string `json:"effective_query"`
			} `json:"result"`
		}
		if err := json.Unmarshal([]byte(stdout), &envelope); err != nil || !envelope.OK || envelope.Result.VirtualNow == "" || envelope.Result.EffectiveQuery == "" {
			t.Fatalf("agent explanation=%q parsed=%#v err=%v", stdout, envelope, err)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 0 {
			t.Fatalf("parse=%d execute=%d", parses, executes)
		}
	})

	t.Run("restricted disclosure is an ordinary unknown flag", func(t *testing.T) {
		api := &replayCLIQueryAPI{t: t}
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
			mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
		setReplayQueryFlags(t, false, time.Minute, true)
		err := queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
		if err == nil || err.Error() != "unknown flag --explain-replay" {
			t.Fatalf("error = %v", err)
		}
		if parses, executes := api.counts(); parses != 0 || executes != 0 {
			t.Fatalf("parse=%d execute=%d", parses, executes)
		}
	})
}

func TestReplayQueryCLIDavisProblemsMappingDisclosureCoverageAndInspection(t *testing.T) {
	newAPI := func(t *testing.T, oldest string) *replayCLIQueryAPI {
		t.Helper()
		return &replayCLIQueryAPI{
			t: t, originalQuery: replayCLIDavisOriginal, coverageOldest: oldest,
			originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/original/parse.json"),
			validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/effective/parse.json"),
		}
	}
	start := mustReplayCLITime("2026-06-14T02:00:00Z")
	virtual := mustReplayCLITime("2026-06-14T10:00:00Z")
	end := mustReplayCLITime("2026-06-14T12:00:00Z")

	t.Run("proven coverage executes exact mapping and announces it", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, false)
		var runErr error
		stdout, stderr := captureReplayQueryStreams(t, func() {
			runErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		})
		if runErr != nil || stdout == "" || !strings.Contains(stderr, "Mapped dt.davis.problems to dt.davis.problems.snapshots") {
			t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 1 || api.coverageCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
		queries := api.executedQueries()
		if len(queries) != 1 || queries[0] != replayCLIDavisEffective {
			t.Fatalf("executed queries = %q", queries)
		}
	})

	t.Run("insufficient coverage retains guidance and prevents effective parse", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00.000000001Z")
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, false)
		err := queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		if err == nil || !strings.HasSuffix(err.Error(), execreplay.DavisCoverageInsufficientMessage) ||
			!strings.Contains(err.Error(), "dt.davis.problems.snapshots") {
			t.Fatalf("error = %v", err)
		}
		if parses, executes := api.counts(); parses != 1 || executes != 0 || api.coverageCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
	})

	t.Run("restricted proven coverage executes exact mapping with private routing", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		server := httptest.NewServer(api)
		defer server.Close()
		fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, false)
		var runErr error
		stdout, stderr := captureReplayQueryStreams(t, func() {
			runErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		})
		if runErr != nil || stdout == "" || stderr != "" || strings.Contains(stdout+stderr, "dt.davis.problems.snapshots") {
			t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 1 || api.coverageCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
		queries := api.executedQueries()
		if len(queries) != 1 || queries[0] != replayCLIDavisEffective {
			t.Fatalf("executed queries = %q", queries)
		}
		provenance, err := os.ReadFile(fixture.state.ProvenancePath)
		if err != nil || !strings.Contains(string(provenance), "dt.davis.problems.snapshots") ||
			!strings.Contains(string(provenance), `"logical_view_range"`) ||
			!strings.Contains(string(provenance), `"physical_snapshot_range"`) ||
			!strings.Contains(string(provenance), `"davis_mappings_audited":true`) {
			t.Fatalf("restricted mapped provenance=%s err=%v", provenance, err)
		}
	})

	t.Run("restricted insufficient coverage is generic and prevents effective parse", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00.000000001Z")
		server := httptest.NewServer(api)
		defer server.Close()
		fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, false)
		err := queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		if err == nil || err.Error() != "The query could not be prepared. It was not executed." {
			t.Fatalf("error = %v", err)
		}
		var rendered strings.Builder
		if printErr := output.PrintError(&rendered, errorToDetail(err)); printErr != nil {
			t.Fatal(printErr)
		}
		testutil.AssertGolden(t, "replay/error-restricted-davis-coverage", rendered.String())
		if parses, executes := api.counts(); parses != 1 || executes != 0 || api.coverageCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
		provenance, readErr := os.ReadFile(fixture.state.ProvenancePath)
		if readErr != nil || !strings.Contains(string(provenance), execreplay.DavisCoverageInsufficientMessage) ||
			!strings.Contains(string(provenance), `"status":"insufficient"`) {
			t.Fatalf("coverage provenance=%s err=%v", provenance, readErr)
		}
	})

	t.Run("restricted mapped remote query text is generic and private", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		api.executeFailures = 1
		api.executeStatus = http.StatusBadRequest
		api.remoteError = `generated source dt.davis.problems.snapshots at toTimestamp("2026-06-14T03:00:00.000000000Z")`
		server := httptest.NewServer(api)
		defer server.Close()
		fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, false)
		err := queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		if err == nil || err.Error() != "The query failed. No result was returned." || strings.Contains(err.Error(), "dt.davis.problems.snapshots") {
			t.Fatalf("error = %v", err)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 1 || api.coverageCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
		provenance, readErr := os.ReadFile(fixture.state.ProvenancePath)
		if readErr != nil || !strings.Contains(string(provenance), "dt.davis.problems.snapshots") {
			t.Fatalf("remote provenance=%s err=%v", provenance, readErr)
		}
	})

	t.Run("explain derives and audits without probing or notification", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual, start, virtual, end)
		setReplayQueryFlags(t, false, time.Minute, true)
		var runErr error
		stdout, stderr := captureReplayQueryStreams(t, func() {
			runErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
		})
		if runErr != nil || !strings.Contains(stdout, execreplay.DavisCoverageNotVerifiedMessage) ||
			!strings.Contains(stdout, replayCLIDavisEffective) || strings.Contains(stderr, "Mapped dt.davis.problems") {
			t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 0 || api.coverageCount() != 0 {
			t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
		}
	})

	t.Run("verify derives and audits without probing", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		server := httptest.NewServer(api)
		defer server.Close()
		newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual, start, virtual, end)
		setReplayVerifyQueryFlags(t)
		var runErr error
		_, stderr := captureReplayQueryStreams(t, func() {
			runErr = verifyQueryCmd.RunE(verifyQueryCmd, []string{replayCLIDavisOriginal})
		})
		if runErr != nil || !strings.Contains(stderr, execreplay.DavisCoverageNotVerifiedMessage) ||
			!strings.Contains(stderr, replayCLIDavisEffective) {
			t.Fatalf("stderr=%q err=%v", stderr, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 0 || api.coverageCount() != 0 || api.verifyCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d verify=%d", parses, api.coverageCount(), executes, api.verifyCount())
		}
	})

	t.Run("restricted verify derives and audits with private detail", func(t *testing.T) {
		api := newAPI(t, "2026-06-14T03:00:00Z")
		server := httptest.NewServer(api)
		defer server.Close()
		fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual, start, virtual, end)
		setReplayVerifyQueryFlags(t)
		var runErr error
		stdout, stderr := captureReplayQueryStreams(t, func() {
			runErr = verifyQueryCmd.RunE(verifyQueryCmd, []string{replayCLIDavisOriginal})
		})
		if runErr != nil || strings.Contains(strings.ToLower(stdout+stderr), "replay") ||
			strings.Contains(stdout+stderr, replayCLIDavisEffective) || strings.Contains(stdout+stderr, execreplay.DavisCoverageNotVerifiedMessage) {
			t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
		}
		if parses, executes := api.counts(); parses != 2 || executes != 0 || api.coverageCount() != 0 || api.verifyCount() != 1 {
			t.Fatalf("parse=%d coverage=%d execute=%d verify=%d", parses, api.coverageCount(), executes, api.verifyCount())
		}
		provenance, err := os.ReadFile(fixture.state.ProvenancePath)
		if err != nil || !strings.Contains(string(provenance), "dt.davis.problems.snapshots") ||
			!strings.Contains(string(provenance), `"status":"not_checked"`) ||
			!strings.Contains(string(provenance), `"davis_mappings_audited":true`) {
			t.Fatalf("restricted verify provenance=%s err=%v", provenance, err)
		}
	})
}

func TestReplayVerifyQueryCLICompatibilityDisclosureAndNoScan(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		t.Run(disclosure, func(t *testing.T) {
			api := &replayCLIQueryAPI{
				t: t, originalQuery: replayCLIRecordOriginal,
				originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
				validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
			}
			server := httptest.NewServer(api)
			defer server.Close()
			fixture := newReplayCLIQueryFixture(t, server.URL, disclosure, session.ReplayClockManual,
				mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
			setReplayVerifyQueryFlags(t)
			var runErr error
			stdout, stderr := captureReplayQueryStreams(t, func() {
				runErr = verifyQueryCmd.RunE(verifyQueryCmd, []string{replayCLIRecordOriginal})
			})
			if runErr != nil {
				t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
			}
			if disclosure == session.ReplayDisclosureFull && !strings.Contains(stderr, "Replay compatibility:") {
				t.Fatalf("full verification omitted compatibility details: stdout=%q stderr=%q", stdout, stderr)
			}
			if disclosure == session.ReplayDisclosureRestricted && strings.Contains(strings.ToLower(stdout+stderr), "replay") {
				t.Fatalf("restricted verification disclosed compatibility details: stdout=%q stderr=%q", stdout, stderr)
			}
			if parses, executes := api.counts(); parses != 2 || executes != 0 || api.verifyCount() != 1 {
				t.Fatalf("verify=%d parse=%d execute=%d, want 1/2/0", api.verifyCount(), parses, executes)
			}
			state, err := fixture.store.Status(fixture.locator)
			if err != nil || state.Status != session.ReplayStatusActive || state.CompletedAt != nil {
				t.Fatalf("verify changed state: %#v err=%v", state, err)
			}
			if disclosure == session.ReplayDisclosureRestricted {
				provenance, err := os.ReadFile(fixture.state.ProvenancePath)
				if err != nil || !strings.Contains(string(provenance), `"event":"query_verification"`) ||
					!strings.Contains(string(provenance), `"compiler_supported":true`) {
					t.Fatalf("provenance=%q err=%v", provenance, err)
				}
			}
		})
	}
}

func TestReplayQueryCLIForeignUnreadableStateStaysSilent(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		t.Run(disclosure, func(t *testing.T) {
			api := &replayCLIQueryAPI{
				t: t, originalQuery: replayCLIRecordOriginal,
				originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
				validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
			}
			server := httptest.NewServer(api)
			defer server.Close()
			fixture := newReplayCLIQueryFixture(t, server.URL, disclosure, session.ReplayClockManual,
				mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
			foreignKey := session.ContextKey(strings.Repeat("b", 64))
			if foreignKey == fixture.locator.ContextKey {
				foreignKey = session.ContextKey(strings.Repeat("c", 64))
			}
			foreignFilename := string(foreignKey) + ".state.json"
			writeReplayUnknownFieldState(t, fixture.store.StatePath(foreignKey), fixture.state, foreignKey)
			setReplayQueryFlags(t, false, time.Minute, false)

			var runErr error
			stdout, stderr := captureReplayQueryStreams(t, func() {
				runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
			})
			if runErr != nil {
				t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
			}
			generated := stdout + stderr
			for _, forbidden := range []string{foreignFilename, "unreadable state", "future_writer_field", "unknown field"} {
				if strings.Contains(generated, forbidden) {
					t.Fatalf("query output leaked diagnostic %q: stdout=%q stderr=%q", forbidden, stdout, stderr)
				}
			}
			if parses, executes := api.counts(); parses != 2 || executes != 1 {
				t.Fatalf("parse=%d execute=%d, want two parses and one execution", parses, executes)
			}
		})
	}
}

func TestReplayQueryCLIOneShotNonOverlapIsHardError(t *testing.T) {
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLILoopOriginal,
		originalBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/parse.json"),
	}
	server := httptest.NewServer(api)
	defer server.Close()
	newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockRealtime,
		mustReplayCLITime("2026-08-09T10:55:03Z"), mustReplayCLITime("2026-08-09T10:55:03Z"), mustReplayCLITime("2026-08-10T10:55:03Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	err := queryCmd.RunE(queryCmd, []string{replayCLILoopOriginal})
	if err == nil || !strings.Contains(err.Error(), "does not overlap") {
		t.Fatalf("error = %v", err)
	}
	if parses, executes := api.counts(); parses != 1 || executes != 0 {
		t.Fatalf("parse=%d execute=%d, want one original parse and no execute", parses, executes)
	}
}

func TestReplayQueryCLIRealtimeWaitAndLiveRetryTemporaryNonOverlap(t *testing.T) {
	tests := []struct {
		mode       string
		disclosure string
		progress   string
	}{
		{"wait", session.ReplayDisclosureFull, "no visible overlap yet"},
		{"live", session.ReplayDisclosureFull, "no visible overlap yet"},
		{"wait", session.ReplayDisclosureRestricted, "no data yet for the requested timeframe; retrying"},
		{"live", session.ReplayDisclosureRestricted, "no data yet for the requested timeframe; retrying"},
	}
	for _, test := range tests {
		t.Run(test.mode+"-"+test.disclosure, func(t *testing.T) {
			api := &replayCLIQueryAPI{
				t: t, originalQuery: replayCLILoopOriginal,
				originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/parse.json"),
				validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/00-discovery/validation-parse.json"),
			}
			server := httptest.NewServer(api)
			defer server.Close()
			dataStart := mustReplayCLITime("2026-08-09T10:55:03Z")
			fixture := newReplayCLIQueryFixture(t, server.URL, test.disclosure, session.ReplayClockRealtime,
				dataStart, dataStart, dataStart.Add(5*time.Minute))
			replayQueryWaitFunc = func(_ context.Context, delay time.Duration) error {
				fixture.clock.Add(delay)
				return nil
			}
			oldFormat := outputFormat
			outputFormat = ""
			t.Cleanup(func() { outputFormat = oldFormat })

			var runErr error
			stdout, stderr := captureReplayQueryStreams(t, func() {
				if test.mode == "wait" {
					setReplayWaitQueryFlags(t, 5*time.Minute)
					runErr = waitQueryCmd.RunE(waitQueryCmd, []string{replayCLILoopOriginal})
					return
				}
				setReplayQueryFlags(t, true, 5*time.Minute, false)
				runErr = queryCmd.RunE(queryCmd, []string{replayCLILoopOriginal})
			})
			if runErr != nil {
				t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
			}
			if !strings.Contains(stderr, test.progress) {
				t.Fatalf("temporary progress missing: stdout=%q stderr=%q", stdout, stderr)
			}
			if parses, executes := api.counts(); parses != 2 || executes != 1 {
				t.Fatalf("parse=%d execute=%d, want original+effective parse and one execution", parses, executes)
			}
			state, err := fixture.store.Status(fixture.locator)
			if err != nil || state.Status != session.ReplayStatusCompleted {
				t.Fatalf("terminal loop state=%#v err=%v", state, err)
			}
			if test.mode == "live" && !strings.Contains(stdout, "Live mode completed.") {
				t.Fatalf("live loop did not exit after terminal result: %q", stdout)
			}
			if test.disclosure == session.ReplayDisclosureRestricted {
				generated := strings.ToLower(stdout + stderr)
				for _, word := range []string{"replay", "virtual", "session", "clock", "interval", "effective"} {
					if strings.Contains(generated, word) {
						t.Fatalf("restricted generated output contains %q: stdout=%q stderr=%q", word, stdout, stderr)
					}
				}
				provenance, err := os.ReadFile(fixture.state.ProvenancePath)
				if err != nil || !replayCLIProvenanceHasQueries(provenance, replayCLILoopOriginal) {
					t.Fatalf("provenance=%q err=%v", provenance, err)
				}
			}
		})
	}
}

func TestReplayQueryCLIHTTPRetryReusesPreparationBeforeNextLiveExecution(t *testing.T) {
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIRecordOriginal, executeFailures: 1,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
	}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockRealtime,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:00:02.718012207Z"))
	replayQueryWaitFunc = func(_ context.Context, delay time.Duration) error {
		fixture.clock.Add(delay)
		return nil
	}
	setReplayQueryFlags(t, true, 5*time.Minute, false)
	var runErr error
	stdout, stderr := captureReplayQueryStreams(t, func() {
		runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	})
	if runErr != nil || stderr != "" || !strings.Contains(stdout, "Live mode completed.") {
		t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
	}
	if parses, executes := api.counts(); parses != 3 || executes != 3 {
		t.Fatalf("parse=%d execute=%d, want one original parse, two effective parses, and one transport retry", parses, executes)
	}
	queries := api.executedQueries()
	if len(queries) != 3 || queries[0] != queries[1] || queries[1] == queries[2] {
		t.Fatalf("execute queries = %#v; transport retry must reuse text while the next live execution recomputes it", queries)
	}
	state, err := fixture.store.Status(fixture.locator)
	if err != nil || state.Status != session.ReplayStatusCompleted {
		t.Fatalf("state=%#v err=%v", state, err)
	}
}

func TestReplayQueryCLIRestrictedSinkFailureUsesGenericErrorBeforeParse(t *testing.T) {
	api := &replayCLIQueryAPI{t: t}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	// A directory at the resolved file path is an unusable sink. The failure is
	// discovered by preflight before any language-service or data request.
	if err := os.Mkdir(fixture.state.ProvenancePath, 0700); err != nil {
		t.Fatal(err)
	}
	setReplayQueryFlags(t, false, time.Minute, false)
	err := queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	if err == nil || err.Error() != "Required local recording is unavailable. The query was not executed." {
		t.Fatalf("error = %v", err)
	}
	if parses, executes := api.counts(); parses != 0 || executes != 0 {
		t.Fatalf("parse=%d execute=%d", parses, executes)
	}
}

func TestReplayQueryCLIRestrictedNoSessionPreflightsAndRecordsBeforeGenericReadiness(t *testing.T) {
	api := &replayCLIQueryAPI{t: t}
	server := httptest.NewServer(api)
	defer server.Close()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	stateDir := filepath.Join(dir, "state", "replay")
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = session.ReplayDisclosureRestricted
	cfg := replayCLIConfig(raw)
	cfg.Contexts[0].Context.Environment = server.URL
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "dt0c01.synthetic"}}
	writeReplayCLIConfig(t, configPath, cfg)
	configureReplayCLI(t, configPath, stateDir, &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)})
	setReplayQueryFlags(t, false, time.Minute, false)
	err := queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	if err == nil || err.Error() != "this context is not ready for queries" {
		t.Fatalf("error = %v", err)
	}
	if parses, executes := api.counts(); parses != 0 || executes != 0 {
		t.Fatalf("parse=%d execute=%d, want zero", parses, executes)
	}
	paths, globErr := filepath.Glob(filepath.Join(stateDir, "*.provenance.jsonl"))
	if globErr != nil || len(paths) != 1 {
		t.Fatalf("provenance paths=%v err=%v", paths, globErr)
	}
	provenance, readErr := os.ReadFile(paths[0])
	if readErr != nil || !strings.Contains(string(provenance), "no replay session is active") {
		t.Fatalf("provenance=%q err=%v", provenance, readErr)
	}
}

func TestReplayQueryCLIRestrictedAndNonReplayAgentResultsAreByteIdentical(t *testing.T) {
	returned := map[string]interface{}{
		"replay": "replay", "virtual": "virtual", "session": "session",
		"clock": "clock", "interval": "interval", "effective": "effective",
	}
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIRecordOriginal,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
			Records: []map[string]interface{}{returned},
			Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
				Query: replayCLIRecordOriginal, CanonicalQuery: replayCLIRecordOriginal,
			}},
		}},
	}
	server := httptest.NewServer(api)
	defer server.Close()
	newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var replayErr error
	replayOutput := captureStdout(t, func() { replayErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal}) })
	if replayErr != nil {
		t.Fatal(replayErr)
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(nil)
	cfg.Contexts[0].Context.Environment = server.URL
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "dt0c01.synthetic"}}
	writeReplayCLIConfig(t, configPath, cfg)
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)})
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var plainErr error
	plainOutput := captureStdout(t, func() { plainErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal}) })
	if plainErr != nil {
		t.Fatal(plainErr)
	}
	if replayOutput != plainOutput {
		t.Fatalf("restricted and non-replay agent outputs differ:\nreplay=%s\nplain=%s", replayOutput, plainOutput)
	}
	returnedJSON, err := json.Marshal(returned)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(replayOutput, string(returnedJSON)) {
		t.Fatalf("returned disclosure-word fixture was not preserved byte-for-byte: envelope=%s record=%s", replayOutput, returnedJSON)
	}
	if parses, executes := api.counts(); parses != 2 || executes != 2 {
		t.Fatalf("parse=%d execute=%d, want replay original+effective parses and one execution on each path", parses, executes)
	}
}

func TestReplayQueryCLIMappedRestrictedAndNonReplayAgentResultsAreByteIdentical(t *testing.T) {
	returned := map[string]interface{}{
		"replay": "replay", "virtual": "virtual", "session": "session",
		"clock": "clock", "interval": "interval", "effective": "effective",
	}
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIDavisOriginal, coverageOldest: "2026-06-14T03:00:00Z",
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/original/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/effective/parse.json"),
		executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
			Records: []map[string]interface{}{returned},
			Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
				Query: replayCLIDavisOriginal, CanonicalQuery: replayCLIDavisOriginal,
			}},
		}},
	}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-06-14T02:00:00Z"), mustReplayCLITime("2026-06-14T10:00:00Z"), mustReplayCLITime("2026-06-14T12:00:00Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var replayErr error
	replayOutput, replayStderr := captureReplayQueryStreams(t, func() {
		replayErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
	})
	if replayErr != nil || replayStderr != "" {
		t.Fatalf("stdout=%q stderr=%q err=%v", replayOutput, replayStderr, replayErr)
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(nil)
	cfg.Contexts[0].Context.Environment = server.URL
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "dt0c01.synthetic"}}
	writeReplayCLIConfig(t, configPath, cfg)
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)})
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var plainErr error
	plainOutput := captureStdout(t, func() { plainErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal}) })
	if plainErr != nil {
		t.Fatal(plainErr)
	}
	if replayOutput != plainOutput {
		t.Fatalf("mapped restricted and non-replay agent outputs differ:\nrestricted=%s\nplain=%s", replayOutput, plainOutput)
	}
	testutil.AssertGolden(t, "replay/agent-restricted-davis-mapping", replayOutput)
	var generated map[string]json.RawMessage
	if err := json.Unmarshal([]byte(replayOutput), &generated); err != nil {
		t.Fatal(err)
	}
	delete(generated, "result")
	generatedJSON, err := json.Marshal(generated)
	if err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "mapped agent envelope outside result", string(generatedJSON))
	if _, exists := generated["replay"]; exists {
		t.Fatalf("mapped restricted envelope exposed replay metadata: %s", replayOutput)
	}
	provenance, err := os.ReadFile(fixture.state.ProvenancePath)
	if err != nil || !strings.Contains(string(provenance), "dt.davis.problems.snapshots") ||
		!strings.Contains(string(provenance), `"davis_mappings_audited":true`) {
		t.Fatalf("mapped provenance=%s err=%v", provenance, err)
	}
	if parses, executes := api.counts(); parses != 2 || executes != 2 || api.coverageCount() != 1 {
		t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
	}
}

func TestReplayQueryCLIDirectSnapshotRestrictedAndNonReplayAgentResultsAreByteIdentical(t *testing.T) {
	const (
		original  = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-08-03T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
		effective = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-08-03T10:55:03.000000000Z"), to:toTimestamp("2026-08-10T10:55:03.000000000Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
	)
	api := &replayCLIQueryAPI{
		t: t, originalQuery: original,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/davis-problems-snapshots/00-discovery/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/davis-problems-snapshots/00-discovery/validation-parse.json"),
		executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
			Records: []map[string]interface{}{{"capture_marker": "synthetic"}},
			Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
				Query: original, CanonicalQuery: original,
			}},
		}},
	}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-08-03T10:55:03Z"), mustReplayCLITime("2026-08-10T10:55:03Z"), mustReplayCLITime("2026-08-11T10:55:03Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var replayErr error
	replayOutput, replayStderr := captureReplayQueryStreams(t, func() {
		replayErr = queryCmd.RunE(queryCmd, []string{original})
	})
	if replayErr != nil || replayStderr != "" {
		t.Fatalf("stdout=%q stderr=%q err=%v", replayOutput, replayStderr, replayErr)
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(nil)
	cfg.Contexts[0].Context.Environment = server.URL
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "dt0c01.synthetic"}}
	writeReplayCLIConfig(t, configPath, cfg)
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)})
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var plainErr error
	plainOutput := captureStdout(t, func() { plainErr = queryCmd.RunE(queryCmd, []string{original}) })
	if plainErr != nil {
		t.Fatal(plainErr)
	}
	if replayOutput != plainOutput {
		t.Fatalf("direct-snapshot restricted and non-replay agent outputs differ:\nrestricted=%s\nplain=%s", replayOutput, plainOutput)
	}
	if strings.Contains(replayOutput, effective) {
		t.Fatalf("direct-snapshot restricted output exposed rewritten query: %s", replayOutput)
	}
	provenance, err := os.ReadFile(fixture.state.ProvenancePath)
	if err != nil || !strings.Contains(string(provenance), "dt.davis.problems.snapshots") ||
		!strings.Contains(string(provenance), `"effective_dql"`) {
		t.Fatalf("direct-snapshot provenance=%s err=%v", provenance, err)
	}
	if parses, executes := api.counts(); parses != 2 || executes != 2 || api.coverageCount() != 0 {
		t.Fatalf("parse=%d coverage=%d execute=%d", parses, api.coverageCount(), executes)
	}
}

func TestReplayQueryCLIMappedRestrictedMetadataKeepsOriginalQuery(t *testing.T) {
	for _, mode := range []struct {
		name            string
		agent           bool
		format          string
		metadata        string
		returnedQuery   string
		expectTimeframe bool
	}{
		{name: "agent-all", agent: true, returnedQuery: replayCLIDavisEffective, expectTimeframe: true},
		{name: "json-all", format: "json", metadata: "all", returnedQuery: replayCLIDavisEffective, expectTimeframe: true},
		{name: "agent-explicit", agent: true, metadata: "query,canonicalQuery"},
		{name: "json-explicit", format: "json", metadata: "query,canonicalQuery"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			api := &replayCLIQueryAPI{
				t: t, originalQuery: replayCLIDavisOriginal, coverageOldest: "2026-06-14T03:00:00Z",
				originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/original/parse.json"),
				validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/davis/problems-view-mapping/effective/parse.json"),
				executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
					Records: []map[string]interface{}{{"capture_marker": "synthetic"}},
					Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
						Query: mode.returnedQuery, CanonicalQuery: replayCLIDavisEffective,
						// This mock exercises pass-through only. The skipped contract test below
						// remains the ship gate for deciding which real server window is correct.
						AnalysisTimeframe: &sdkquery.AnalysisTimeframe{Start: "2026-06-14T03:00:00Z", End: "2026-06-14T10:00:00Z"},
					}},
				}},
			}
			server := httptest.NewServer(api)
			defer server.Close()
			fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
				mustReplayCLITime("2026-06-14T02:00:00Z"), mustReplayCLITime("2026-06-14T10:00:00Z"), mustReplayCLITime("2026-06-14T12:00:00Z"))
			setReplayQueryFlags(t, false, time.Minute, false)
			agentMode, outputFormat = mode.agent, mode.format
			if mode.metadata != "" {
				setReplayQueryMetadataFlag(t, mode.metadata)
			}
			var runErr error
			stdout, stderr := captureReplayQueryStreams(t, func() {
				runErr = queryCmd.RunE(queryCmd, []string{replayCLIDavisOriginal})
			})
			if runErr != nil || stderr != "" {
				t.Fatalf("stdout=%q stderr=%q err=%v", stdout, stderr, runErr)
			}
			var envelope struct {
				Metadata map[string]json.RawMessage `json:"metadata"`
			}
			if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
				t.Fatalf("decode output %q: %v", stdout, err)
			}
			var query string
			if err := json.Unmarshal(envelope.Metadata["query"], &query); err != nil || query != replayCLIDavisOriginal {
				t.Fatalf("ordinary metadata query=%q err=%v metadata=%s", query, err, envelope.Metadata["query"])
			}
			if _, exists := envelope.Metadata["canonicalQuery"]; exists {
				t.Fatalf("ordinary metadata retained mapped canonical query: %s", stdout)
			}
			if mode.expectTimeframe {
				var timeframe sdkquery.AnalysisTimeframe
				if err := json.Unmarshal(envelope.Metadata["analysisTimeframe"], &timeframe); err != nil ||
					timeframe.Start != "2026-06-14T03:00:00Z" || timeframe.End != "2026-06-14T10:00:00Z" {
					t.Fatalf("analysisTimeframe=%#v err=%v", timeframe, err)
				}
			} else if _, exists := envelope.Metadata["analysisTimeframe"]; exists || len(envelope.Metadata) != 1 {
				t.Fatalf("explicit query metadata selected unexpected fields: %s", stdout)
			}
			generatedMetadata := make(map[string]json.RawMessage, len(envelope.Metadata))
			for key, value := range envelope.Metadata {
				generatedMetadata[key] = value
			}
			delete(generatedMetadata, "analysisTimeframe")
			generatedMetadataBytes, err := json.Marshal(generatedMetadata)
			if err != nil {
				t.Fatal(err)
			}
			assertNoRestrictedGeneratedWords(t, "mapped metadata excluding analysisTimeframe", string(generatedMetadataBytes))
			for _, leaked := range []string{"dt.davis.problems.snapshots", "2026-06-14T03:00:00.000000000Z", "sort timestamp desc", "dedup event.id", "coalesce(event.end"} {
				if strings.Contains(string(generatedMetadataBytes), leaked) {
					t.Fatalf("ordinary metadata outside analysisTimeframe contains mapped fragment %q: %s", leaked, generatedMetadataBytes)
				}
			}
			provenance, err := os.ReadFile(fixture.state.ProvenancePath)
			if err != nil || !strings.Contains(string(provenance), `"grail_canonical_effective_dql"`) ||
				!strings.Contains(string(provenance), "dt.davis.problems.snapshots") {
				t.Fatalf("canonical mapped provenance=%s err=%v", provenance, err)
			}
		})
	}
}

func TestReplayQueryCLIMappedAnalysisTimeframeContract(t *testing.T) {
	const (
		logicalStart  = "2026-06-14T09:00:00Z"
		physicalStart = "2026-06-14T03:00:00Z"
		upperBound    = "2026-06-14T10:00:00Z"
	)
	t.Skipf("OPEN ITEM: a sanitized mapped-query Grail capture must pin analysisTimeframe to logical [%s,%s) or physical [%s,%s); a mock cannot choose the server contract", logicalStart, upperBound, physicalStart, upperBound)
}

func TestRestrictedReplayDisclosureLeakMatrixQueryNotifications(t *testing.T) {
	const notification = "replay virtual session clock interval effective"
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIRecordOriginal,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
			Records: []map[string]interface{}{{"matched": float64(1)}},
			Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
				Query: replayCLIRecordEffective, CanonicalQuery: replayCLIRecordEffective,
				Notifications: []sdkquery.Notification{{
					Severity: "WARNING", NotificationType: "SYNTHETIC_RETENTION", Message: notification,
				}},
			}},
		}},
	}
	server := httptest.NewServer(api)
	defer server.Close()
	fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true

	var runErr error
	stdout, stderr := captureReplayQueryStreams(t, func() {
		runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if stderr != "" {
		t.Fatalf("restricted query notification reached stderr: %q", stderr)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode restricted envelope %q: %v", stdout, err)
	}
	delete(envelope, "result") // returned records are deliberately outside the generated-text scan
	generated, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "agent envelope outside result", string(generated))

	provenance, err := os.ReadFile(fixture.state.ProvenancePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(provenance), notification) || !strings.Contains(string(provenance), `"query_notifications"`) {
		t.Fatalf("restricted query notification did not reach provenance: %s", provenance)
	}
	if parses, executes := api.counts(); parses != 2 || executes != 1 {
		t.Fatalf("parse=%d execute=%d, want two parses and one execution", parses, executes)
	}
	if inspections := api.inspectionCount(); inspections != 1 {
		t.Fatalf("retention inspections=%d, want one per invocation", inspections)
	}
}

func TestReplayQueryCLIFullAgentLabelsAllQueryForms(t *testing.T) {
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIRecordOriginal,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
		executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
			Records: []map[string]interface{}{{"matched": float64(1)}},
			Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{
				Query: replayCLIRecordEffective, CanonicalQuery: replayCLIRecordEffective,
				Notifications: []sdkquery.Notification{{
					Severity: "WARNING", NotificationType: "SYNTHETIC_RETENTION",
					Message: "synthetic retention warning",
				}},
			}},
		}},
	}
	server := httptest.NewServer(api)
	defer server.Close()
	newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, session.ReplayClockManual,
		mustReplayCLITime("2026-08-10T10:50:02.718012207Z"), mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"))
	setReplayQueryFlags(t, false, time.Minute, false)
	agentMode = true
	var runErr error
	stdout := captureStdout(t, func() { runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal}) })
	if runErr != nil {
		t.Fatal(runErr)
	}
	var envelope struct {
		Replay *struct {
			Active                       bool   `json:"active"`
			SessionID                    string `json:"session_id"`
			SessionStartedAt             string `json:"session_started_at"`
			ClockMode                    string `json:"clock_mode"`
			VirtualNow                   string `json:"virtual_now"`
			DataStart                    string `json:"data_start"`
			DataEnd                      string `json:"data_end"`
			VisibleEnd                   string `json:"visible_end"`
			State                        string `json:"state"`
			OriginalQuery                string `json:"original_query"`
			EffectiveQuery               string `json:"effective_query"`
			GrailCanonicalEffectiveQuery string `json:"grail_canonical_effective_query"`
			Sources                      []any  `json:"sources"`
			Warnings                     []any  `json:"warnings"`
		} `json:"replay"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode envelope %q: %v", stdout, err)
	}
	got := envelope.Replay
	if got == nil || !got.Active || got.SessionID == "" || got.SessionStartedAt == "" || got.ClockMode != session.ReplayClockManual ||
		got.VirtualNow == "" || got.DataStart == "" || got.DataEnd == "" || got.VisibleEnd == "" || got.State == "" ||
		got.OriginalQuery != replayCLIRecordOriginal || got.EffectiveQuery != replayCLIRecordEffective ||
		got.GrailCanonicalEffectiveQuery != replayCLIRecordEffective || len(got.Sources) != 1 || len(got.Warnings) != 1 ||
		got.Warnings[0] != "synthetic retention warning" {
		t.Fatalf("replay metadata = %#v; envelope=%s", got, stdout)
	}
}

func replayCLIQueryFixtureBody(t *testing.T, relative string) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "sdk", "api", "query", "testdata", relative))
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

func replayCLIQueryDynamicValidation(t *testing.T, raw json.RawMessage, query string) json.RawMessage {
	t.Helper()
	from, to, ok := replayCLIQuerySourceBoundaries(query)
	if !ok {
		return raw
	}
	var root sdkquery.ParseResponse
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	seen := 0
	replayCLIQueryWalkNode(&root, func(node *sdkquery.DQLNode) {
		if node.Terminal == nil || node.Terminal.Type != "STRING" || seen >= 2 {
			return
		}
		if seen == 0 {
			node.Terminal.CanonicalString = `"` + from + `"`
		} else {
			node.Terminal.CanonicalString = `"` + to + `"`
		}
		seen++
	})
	encoded, err := json.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func replayCLIQuerySourceBoundaries(query string) (string, string, bool) {
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

func replayCLIQueryWalkNode(node *sdkquery.DQLNode, visit func(*sdkquery.DQLNode)) {
	if node == nil {
		return
	}
	visit(node)
	if node.Container != nil {
		for _, child := range node.Container.Children {
			replayCLIQueryWalkNode(child, visit)
		}
	}
	if node.Alternative != nil {
		for _, child := range node.Alternative.Alternatives {
			replayCLIQueryWalkNode(child, visit)
		}
	}
}

func captureReplayQueryStreams(t *testing.T, run func()) (string, string) {
	t.Helper()
	oldStdout, oldStderr := os.Stdout, os.Stderr
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = stdoutWriter, stderrWriter
	run()
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	os.Stdout, os.Stderr = oldStdout, oldStderr
	stdoutBytes, err := io.ReadAll(stdoutReader)
	if err != nil {
		t.Fatal(err)
	}
	stderrBytes, err := io.ReadAll(stderrReader)
	if err != nil {
		t.Fatal(err)
	}
	_ = stdoutReader.Close()
	_ = stderrReader.Close()
	return string(stdoutBytes), string(stderrBytes)
}

func replayCLIProvenanceHasQueries(raw []byte, original string) bool {
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record session.ReplayProvenanceRecord
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		originalDQL, _ := record.Fields["original_dql"].(string)
		effectiveDQL, _ := record.Fields["effective_dql"].(string)
		if originalDQL == original && effectiveDQL != "" {
			return true
		}
	}
	return false
}

func mustReplayCLITime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
