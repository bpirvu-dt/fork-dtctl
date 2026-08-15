//go:build integration
// +build integration

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	pkgexec "github.com/dynatrace-oss/dtctl/pkg/exec"
	"github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const (
	e2eDavisOriginal  = `fetch dt.davis.problems, from:toTimestamp("2026-06-14T09:00:00.000Z"), to:toTimestamp("2026-06-14T10:00:00.000Z")`
	e2eDavisEffective = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")
| sort timestamp desc
| dedup event.id
| filter event.start < toTimestamp("2026-06-14T10:00:00.000000000Z") and coalesce(event.end, toTimestamp("2026-06-14T10:00:00.000000000Z")) >= toTimestamp("2026-06-14T09:00:00.000000000Z")`
)

type e2eReplayClock struct{ now time.Time }

func (c e2eReplayClock) Now() time.Time { return c.now }

type e2eDavisQueryAPI struct {
	t              *testing.T
	original       []byte
	effective      []byte
	oldestSnapshot string
	tamperAudit    bool

	mu    sync.Mutex
	order []string
}

func (a *e2eDavisQueryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/platform/storage/query/v1/query:parse":
		var request sdkquery.ParseRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode parse request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body := a.effective
		label := "effective_parse"
		if request.Query == e2eDavisOriginal {
			body, label = a.original, "original_parse"
		} else if request.Query != e2eDavisEffective {
			a.t.Errorf("unexpected parse query: %q", request.Query)
		}
		if a.tamperAudit && label == "effective_parse" {
			body = []byte(strings.Replace(string(body), `"canonicalString": "event.id"`, `"canonicalString": "event.kind"`, 1))
		}
		a.add(label)
		_, _ = w.Write(body)
	case "/platform/storage/query/v1/query:execute":
		var request sdkquery.ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			a.t.Errorf("decode execute request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.Contains(request.Query, "oldest_snapshot=min(timestamp)") {
			a.add("coverage")
			_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{
				"oldest_snapshot": a.oldestSnapshot,
			}}}})
			return
		}
		if request.Query != e2eDavisEffective {
			a.t.Errorf("main execute query: %q", request.Query)
		}
		a.add("main_execute")
		_ = json.NewEncoder(w).Encode(sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{Records: []map[string]interface{}{{"mapped": true}}}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *e2eDavisQueryAPI) add(value string) {
	a.mu.Lock()
	a.order = append(a.order, value)
	a.mu.Unlock()
}

func (a *e2eDavisQueryAPI) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.order...)
}

func TestReplayDavisMappingBlackBoxMockServer(t *testing.T) {
	t.Run("coverage proven execution", func(t *testing.T) {
		executor, api := newE2EDavisExecutor(t, "2026-06-14T03:00:00Z", false)
		result, err := executor.ExecuteQueryDetailedWithContext(context.Background(), e2eDavisOriginal, pkgexec.DQLExecuteOptions{AgentMode: true})
		if err != nil {
			t.Fatal(err)
		}
		if result == nil || result.Replay == nil || result.Replay.Output == nil ||
			result.Replay.Output.DavisSnapshotCoverage == nil || !result.Replay.Output.DavisSnapshotCoverage.Verified ||
			result.Replay.Output.Sources[0].DavisProblemsMapping == nil {
			t.Fatalf("mapped result = %#v", result)
		}
		assertE2EOrder(t, api.snapshot(), "original_parse", "coverage", "effective_parse", "main_execute")
	})

	t.Run("insufficient coverage fails before effective parse", func(t *testing.T) {
		executor, api := newE2EDavisExecutor(t, "2026-06-14T03:00:00.000000001Z", false)
		if _, err := executor.ExecuteQueryDetailedWithContext(context.Background(), e2eDavisOriginal, pkgexec.DQLExecuteOptions{AgentMode: true}); err == nil ||
			!strings.HasSuffix(err.Error(), replay.DavisCoverageInsufficientMessage) {
			t.Fatalf("coverage error = %v", err)
		}
		assertE2EOrder(t, api.snapshot(), "original_parse", "coverage")
	})

	t.Run("audit tampering fails before main execute", func(t *testing.T) {
		executor, api := newE2EDavisExecutor(t, "2026-06-14T03:00:00Z", true)
		if _, err := executor.ExecuteQueryDetailedWithContext(context.Background(), e2eDavisOriginal, pkgexec.DQLExecuteOptions{AgentMode: true}); err == nil {
			t.Fatal("tampered validation AST unexpectedly executed")
		}
		assertE2EOrder(t, api.snapshot(), "original_parse", "coverage", "effective_parse")
	})

	t.Run("explain and verify are probe free", func(t *testing.T) {
		executor, api := newE2EDavisExecutor(t, "2026-06-14T03:00:00Z", false)
		explanation, err := executor.ExplainReplayWithContext(context.Background(), e2eDavisOriginal, pkgexec.DQLExecuteOptions{AgentMode: true})
		if err != nil || explanation.CoverageVerified == nil || *explanation.CoverageVerified {
			t.Fatalf("explanation=%#v err=%v", explanation, err)
		}
		assertE2EOrder(t, api.snapshot(), "original_parse", "effective_parse")

		executor, api = newE2EDavisExecutor(t, "2026-06-14T03:00:00Z", false)
		verification, err := executor.VerifyReplayCompatibilityWithContext(context.Background(), e2eDavisOriginal, pkgexec.DQLVerifyOptions{Timezone: "UTC"}, true)
		if err != nil || verification == nil || verification.CoverageVerified == nil || *verification.CoverageVerified {
			t.Fatalf("verification=%#v err=%v", verification, err)
		}
		assertE2EOrder(t, api.snapshot(), "original_parse", "effective_parse")
	})
}

func newE2EDavisExecutor(t *testing.T, oldestSnapshot string, tamperAudit bool) (*pkgexec.DQLExecutor, *e2eDavisQueryAPI) {
	t.Helper()
	readFixture := func(name string) []byte {
		value, err := os.ReadFile(filepath.Join("..", "..", "..", "sdk", "api", "query", "testdata", "phase0b", "fixtures", "davis", "problems-view-mapping", name, "parse.json"))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	api := &e2eDavisQueryAPI{t: t, original: readFixture("original"), effective: readFixture("effective"), oldestSnapshot: oldestSnapshot, tamperAudit: tamperAudit}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	transport, err := client.NewForTesting(server.URL, "dt0c01.synthetic")
	if err != nil {
		t.Fatal(err)
	}
	clock := e2eReplayClock{now: time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)}
	locator := session.ReplayLocator{ContextKey: session.ContextKey(strings.Repeat("a", 64)), ContextIdentityHash: strings.Repeat("b", 64)}
	store := session.NewReplayStateStore(filepath.Join(t.TempDir(), "state"), clock)
	const environmentHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const contextHash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	if _, err := store.Start(session.ReplayStartRequest{
		Locator: locator, ContextName: "synthetic-replay", EnvironmentHash: environmentHash, ContextInputHash: contextHash,
		Config: session.ResolvedReplayConfig{
			DataStart: time.Date(2026, 6, 14, 2, 0, 0, 0, time.UTC), DataEnd: time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC),
			VirtualStart: time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC), ClockMode: session.ReplayClockManual, Disclosure: session.ReplayDisclosureFull,
			ValueSources: session.ReplayValueSources{
				DataStart: session.ReplayValueFromContext, DataEnd: session.ReplayValueFromContext,
				VirtualStart: session.ReplayValueFromContext, ClockMode: session.ReplayValueFromContext,
				Disclosure: session.ReplayValueFromContext, ProvenancePath: session.ReplayValueFromDefault,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	preparer, err := pkgexec.NewReplayQueryPreparer(pkgexec.ReplayPreparerConfig{
		Store: store, Clock: clock, Locator: locator, ContextName: "synthetic-replay",
		ExpectedContextInputHash: contextHash, ExpectedEnvironmentHash: environmentHash,
		EnvironmentID: environmentHash, ClientIdentity: "synthetic-principal", FallbackDisclosure: session.ReplayDisclosureFull,
		SourcePolicy:  replay.Milestone1SourcePolicy(),
		DavisCoverage: pkgexec.NewMemoizedDavisSnapshotCoverageProvider(pkgexec.NewDavisSnapshotCoverageInspector(transport)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return pkgexec.NewDQLExecutor(transport).WithQueryPreparer(preparer), api
}

func assertE2EOrder(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("request order = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("request order = %v, want %v", got, want)
		}
	}
}
