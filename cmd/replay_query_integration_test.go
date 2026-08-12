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

	pkgclient "github.com/dynatrace-oss/dtctl/pkg/client"
	"github.com/dynatrace-oss/dtctl/pkg/config"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const (
	replayCLIRecordOriginal = `fetch logs, from:toTimestamp("2026-08-10T10:45:02.718012207Z"), to:toTimestamp("2026-08-10T11:05:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	replayCLILoopOriginal   = `fetch logs, from:toTimestamp("2026-08-09T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
)

type replayCLIQueryAPI struct {
	t               *testing.T
	originalQuery   string
	originalBody    json.RawMessage
	validationBody  json.RawMessage
	executeFailures int
	executeStatus   int

	mu       sync.Mutex
	parses   int
	executes int
	queries  []string
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
			_, _ = w.Write([]byte(`{"error":{"message":"query request failed"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sdkquery.Response{
			State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{"matched": float64(1)}}},
		})
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
	api := &replayCLIQueryAPI{
		t: t, originalQuery: replayCLIRecordOriginal,
		originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
		validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
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
	if parses, executes := api.counts(); parses != 2 || executes != 2 {
		t.Fatalf("parse=%d execute=%d, want replay original+effective parses and one execution on each path", parses, executes)
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
