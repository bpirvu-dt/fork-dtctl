package replay

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMilestone1RecordAllowlistUsesRealCorpus(t *testing.T) {
	tests := []struct {
		fixture string
		table   string
		field   string
	}{
		{"records/logs/00-discovery/parse.json", "logs", "timestamp"},
		{"records/spans/00-discovery/parse.json", "spans", "start_time"},
		{"records/events/00-discovery/parse.json", "events", "timestamp"},
		{"records/bizevents/00-discovery/parse.json", "bizevents", "timestamp"},
		{"records/system-events/00-discovery/parse.json", "dt.system.events", "timestamp"},
		{"records/davis-events-snapshots/00-discovery/parse.json", "dt.davis.events.snapshots", "timestamp"},
		{"records/davis-problems-snapshots/00-discovery/parse.json", "dt.davis.problems.snapshots", "timestamp"},
	}
	policy := Milestone1SourcePolicy()
	if len(policy.RecordTables) != len(tests) {
		t.Fatalf("record policy length = %d, want %d", len(policy.RecordTables), len(tests))
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			sources, err := ClassifySources(loadPhase0BFixture(t, tt.fixture), policy)
			if err != nil {
				t.Fatalf("ClassifySources: %v", err)
			}
			if len(sources) != 1 {
				t.Fatalf("sources = %d, want 1", len(sources))
			}
			got := sources[0]
			if got.Class != SourceRecord || got.Name != tt.table || got.RecordTimeField != tt.field || got.BoundaryPolicy != BoundaryExact {
				t.Fatalf("source = %#v", got)
			}
		})
	}
}

func TestMetricAllowlistUsesRealCorpus(t *testing.T) {
	tests := []struct {
		fixture  string
		shape    MetricShape
		interval time.Duration
		auto     bool
	}{
		{"metrics/01-automatic/parse.json", MetricBaseline, 0, true},
		{"metrics/02-explicit-1m/parse.json", MetricBaseline, time.Minute, false},
		{"metrics/03-explicit-5m/parse.json", MetricBaseline, 5 * time.Minute, false},
		{"metrics/04-explicit-1h/parse.json", MetricBaseline, time.Hour, false},
		{"metrics/05-explicit-1d/parse.json", MetricBaseline, 24 * time.Hour, false},
		{"metrics/06a-calendar-vienna-current/parse.json", MetricBaseline, 24 * time.Hour, false},
		{"metrics/07-rate/parse.json", MetricRate, time.Hour, false},
		{"metrics/08-multi-series/parse.json", MetricSingleDimension, 5 * time.Minute, false},
		{"metrics/09-multi-aggregation/parse.json", MetricMultiAggregation, 5 * time.Minute, false},
		{"metrics/10-rollup/parse.json", MetricRollup, time.Hour, false},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			sources, err := ClassifySources(loadPhase0BFixture(t, tt.fixture), Milestone1SourcePolicy())
			if err != nil {
				t.Fatalf("ClassifySources: %v", err)
			}
			if len(sources) != 1 || sources[0].Metric == nil {
				t.Fatalf("sources = %#v", sources)
			}
			metric := sources[0].Metric
			if metric.Shape != tt.shape || metric.AutomaticInterval != tt.auto {
				t.Fatalf("metric = %#v", metric)
			}
			if tt.auto {
				if metric.DeclaredInterval != nil {
					t.Fatalf("automatic interval = %v, want nil", *metric.DeclaredInterval)
				}
			} else if metric.DeclaredInterval == nil || *metric.DeclaredInterval != tt.interval {
				t.Fatalf("declared interval = %v, want %v", metric.DeclaredInterval, tt.interval)
			}
		})
	}
}

func TestNestedSourceFormsClassifyEverySourceIndependently(t *testing.T) {
	for _, command := range []string{"append", "join", "lookup"} {
		t.Run(command, func(t *testing.T) {
			fixture := "nested/" + command + "/parse.json"
			sources, err := ClassifySources(loadPhase0BFixture(t, fixture), Milestone1SourcePolicy())
			if err != nil {
				t.Fatalf("ClassifySources: %v", err)
			}
			if len(sources) != 2 || sources[0].Name != "logs" || sources[1].Name != "spans" {
				t.Fatalf("sources = %#v", sources)
			}
			if sources[0].Path == sources[1].Path {
				t.Fatal("nested sources have the same AST path")
			}
		})
	}
}

func TestSourcePolicyRejectsCurrentStateAndUnknownSources(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		wantCode ErrorCode
	}{
		{"Smartscape nodes", "current-state-rejection/01-smartscape-nodes/historical-context/parse.json", ErrorCurrentState},
		{"Smartscape edges", "current-state-rejection/02-smartscape-edges/historical-context/parse.json", ErrorCurrentState},
		{"traverse", "current-state-rejection/03-traverse/historical-context/parse.json", ErrorCurrentState},
		{"entity source", "current-state-rejection/04-fetch-dt-entity/historical-context/parse.json", ErrorCurrentState},
		{"entityName", "current-state-rejection/05-entity-name/historical-context/parse.json", ErrorCurrentState},
		{"entityAttr", "current-state-rejection/06-entity-attr/historical-context/parse.json", ErrorCurrentState},
		{"classic selector", "current-state-rejection/07-classic-entity-selector/historical-context/parse.json", ErrorCurrentState},
		{"getNodeName", "current-state-rejection/08-get-node-name/historical-context/parse.json", ErrorCurrentState},
		{"getNodeField", "current-state-rejection/09-get-node-field/historical-context/parse.json", ErrorCurrentState},
		{"fieldsSnapshot", "current-state-rejection/10-fields-snapshot/historical-context/parse.json", ErrorCurrentState},
		{"describe", "current-state-rejection/13-describe/historical-context/parse.json", ErrorCurrentState},
		{"unknown table", "retention/current-bucket-metadata/parse.json", ErrorUnsupportedSource},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ClassifySources(loadPhase0BFixture(t, tt.fixture), Milestone1SourcePolicy())
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != tt.wantCode {
				t.Fatalf("error = %T %v, want ReplayError code %s", err, err, tt.wantCode)
			}
		})
	}
}

func TestCurrentStateNegativeControlsRemainAllowed(t *testing.T) {
	tests := []struct {
		fixture string
		class   SourceClass
	}{
		{"current-state-rejection/negative-controls/01-string/parse.json", SourceSynthetic},
		{"current-state-rejection/negative-controls/02-comment/parse.json", SourceSynthetic},
		{"current-state-rejection/negative-controls/03-stored-fields/parse.json", SourceSynthetic},
		{"current-state-rejection/negative-controls/04-historical-entity-id/parse.json", SourceRecord},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			sources, err := ClassifySources(loadPhase0BFixture(t, tt.fixture), Milestone1SourcePolicy())
			if err != nil {
				t.Fatalf("ClassifySources: %v", err)
			}
			if len(sources) != 1 || sources[0].Class != tt.class {
				t.Fatalf("sources = %#v", sources)
			}
		})
	}
}

func TestEveryTimeseriesShiftIsRejected(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/12-timeseries-shift/parse.json")
	_, err := ClassifySources(ast, Milestone1SourcePolicy())
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorShift {
		t.Fatalf("error = %T %v, want shift ReplayError", err, err)
	}
}

func TestUnallowlistedMetricShapeFailsClosed(t *testing.T) {
	ast := loadPhase0BFixture(t, "metrics/01-automatic/parse.json").Clone()
	aggregation := firstTerminal(ast, "TIMESERIES_AGGREGATION")
	if aggregation == nil {
		t.Fatal("fixture has no aggregation terminal")
	}
	aggregation.Canonical = "min"
	_, err := ClassifySources(ast, Milestone1SourcePolicy())
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorUnsupportedForm {
		t.Fatalf("error = %T %v, want unsupported-form ReplayError", err, err)
	}
}

func TestGenuineCalendarMetricIntervalFailsClosed(t *testing.T) {
	ast := loadPhase0BFixture(t, "metrics/05-explicit-1d/parse.json").Clone()
	var duration *Node
	_ = ast.WalkExecutable(func(node *Node) error {
		if duration == nil && node.Role == "DURATION" {
			duration = node
		}
		return nil
	})
	if duration == nil {
		t.Fatal("fixture has no duration")
	}
	duration.Role = "CALENDAR_DURATION"
	numbers := terminalsWithRole(duration, "NUMBER")
	units := terminalsWithRole(duration, "TIME_UNIT")
	if len(numbers) != 1 || len(units) != 1 {
		t.Fatalf("duration terminals = %d numbers, %d units", len(numbers), len(units))
	}
	numbers[0].Canonical = "1"
	units[0].Canonical = "M"
	_, err := ClassifySources(ast, Milestone1SourcePolicy())
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || (replayErr.Code != ErrorTimeframe && replayErr.Code != ErrorASTContract) {
		t.Fatalf("error = %T %v, want calendar rejection", err, err)
	}
}

func TestKnownSemanticRoleInUnexpectedParentFailsClosed(t *testing.T) {
	ast := loadPhase0BFixture(t, "current-state-rejection/negative-controls/04-historical-entity-id/parse.json").Clone()
	identifiers := terminalNodes(ast, "SIMPLE_IDENTIFIER")
	if len(identifiers) == 0 {
		t.Fatal("fixture has no stored-field identifier")
	}
	identifiers[len(identifiers)-1].Role = "DATA_OBJECT"
	_, err := ClassifySources(ast, Milestone1SourcePolicy())
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorASTContract {
		t.Fatalf("error = %T %v, want AST placement rejection", err, err)
	}
}

func TestFutureModelAndMutableLoadFormsFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		command  string
		wantCode ErrorCode
	}{
		{"mutable load", "phase0/fixtures/05-fetch-no-timeframe/parse.json", "load", ErrorCurrentState},
		{"future model source", "phase0/fixtures/13-string-now/parse.json", "futureModel", ErrorUnsupportedForm},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast := loadSDKFixture(t, test.fixture).Clone()
			command := firstTerminal(ast, "COMMAND_NAME")
			if command == nil {
				t.Fatal("fixture has no command")
			}
			command.Canonical = test.command
			_, err := ClassifySources(ast, Milestone1SourcePolicy())
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != test.wantCode || replayErr.Construct == "" {
				t.Fatalf("error = %T %#v, want %s", err, err, test.wantCode)
			}
		})
	}
}

func TestCurrentDavisViewErrorCarriesSnapshotRewrite(t *testing.T) {
	tests := []struct {
		view     string
		snapshot string
	}{
		{"dt.davis.problems", "dt.davis.problems.snapshots"},
		{"dt.davis.events", "dt.davis.events.snapshots"},
	}
	for _, test := range tests {
		t.Run(test.view, func(t *testing.T) {
			ast := loadPhase0BFixture(t, "current-state-rejection/04-fetch-dt-entity/historical-context/parse.json").Clone()
			dataObject := firstTerminal(ast, "DATA_OBJECT")
			if dataObject == nil {
				t.Fatal("fixture has no data object")
			}
			dataObject.Canonical = test.view
			_, err := ClassifySources(ast, Milestone1SourcePolicy())
			var davisErr *DavisCurrentViewError
			if !errors.As(err, &davisErr) {
				t.Fatalf("error = %T %v, want *DavisCurrentViewError", err, err)
			}
			if davisErr.SnapshotTable != test.snapshot || davisErr.IdentityField != "event.id" ||
				!strings.Contains(davisErr.LatestPerIDPattern, "dedup event.id") ||
				!strings.Contains(davisErr.Error(), test.snapshot) || !strings.Contains(davisErr.Error(), "Pattern:") {
				t.Fatalf("Davis error = %#v (%v)", davisErr, davisErr)
			}
		})
	}
}

func loadPhase0BFixture(t *testing.T, relative string) *AST {
	t.Helper()
	return loadSDKFixture(t, filepath.Join("phase0b", "fixtures", relative))
}

func loadSDKFixture(t *testing.T, relative string) *AST {
	t.Helper()
	path := filepath.Join("..", "..", "..", "sdk", "api", "query", "testdata", relative)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body, ok, err := fixtureAST(raw)
	if err != nil || !ok {
		t.Fatalf("fixture %s has no AST: ok=%v err=%v", path, ok, err)
	}
	ast, err := AdaptJSON(body)
	if err != nil {
		t.Fatalf("adapt %s: %v", path, err)
	}
	return ast
}

func firstTerminal(ast *AST, role string) *Node {
	var found *Node
	_ = ast.WalkExecutable(func(node *Node) error {
		if found == nil && node.Kind == NodeTerminal && node.Role == role {
			found = node
		}
		return nil
	})
	return found
}
