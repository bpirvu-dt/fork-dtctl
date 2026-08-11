package replay

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

type compilerGoldenCase struct {
	Name         string `json:"name"`
	ASTFixture   string `json:"ast_fixture"`
	OriginalDQL  string `json:"original_dql"`
	EffectiveDQL string `json:"effective_dql"`
}

func TestCompilerGoldenCorpus(t *testing.T) {
	raw, err := os.ReadFile("testdata/compiler_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []compilerGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 15 {
		t.Fatalf("golden cases = %d, want full transformation matrix", len(cases))
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			input := fixedCompileInput(loadSDKFixture(t, test.ASTFixture), test.OriginalDQL)
			result, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if result.EffectiveDQL != test.EffectiveDQL {
				t.Fatalf("effective DQL:\n got: %s\nwant: %s", result.EffectiveDQL, test.EffectiveDQL)
			}
			if result.Explain.EffectiveDQL != result.EffectiveDQL || !result.AuditRequired {
				t.Fatalf("incomplete compile result: %#v", result)
			}
		})
	}
}

func TestCompileDoesNotMutateReusableAST(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/01-fetch-explicit-now/parse.json")
	before, err := json.Marshal(ast)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Compile(fixedCompileInput(ast, "fetch logs, from:now()-1h"))
	if err != nil {
		t.Fatal(err)
	}
	secondInput := fixedCompileInput(ast, "fetch logs, from:now()-1h")
	secondInput.VirtualNow = mustTime(t, "2026-06-14T11:00:00Z")
	secondInput.VisibleInterval.End = secondInput.VirtualNow
	second, err := Compile(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if first.EffectiveDQL == second.EffectiveDQL {
		t.Fatal("different virtual times emitted identical DQL")
	}
	after, err := json.Marshal(ast)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("compiler mutated the reusable input AST")
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestCompilerUsesGlobalDefaultBeforeServiceDefault(t *testing.T) {
	input := fixedCompileInput(loadSDKFixture(t, "phase0/fixtures/05-fetch-no-timeframe/parse.json"), "fetch logs")
	global := Interval{Start: mustTime(t, "2026-06-14T08:30:00Z"), End: mustTime(t, "2026-06-14T09:30:00Z")}
	input.GlobalDefault = &global
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Sources[0].Requested == nil || result.Sources[0].Requested.Basis != "global default timeframe" ||
		result.Sources[0].Effective == nil || *result.Sources[0].Effective != global {
		t.Fatalf("source = %#v", result.Sources[0])
	}
}

func TestCompilerRejectsUnsupportedOpenAndCalendarForms(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		original string
		code     ErrorCode
	}{
		{"to only", "phase0/fixtures/06-fetch-to-now/parse.json", "fetch logs, to:now()-20m", ErrorTimeframe},
		{"calendar arithmetic", "phase0/fixtures/08-fetch-calendar-arithmetic/parse.json", "fetch logs, from:now()-1M+2w", ErrorTimeframe},
		{"timeseries shift", "phase0/fixtures/12-timeseries-shift/parse.json", "timeseries value=avg(metric.key), shift:-7d", ErrorShift},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Compile(fixedCompileInput(loadSDKFixture(t, test.fixture), test.original))
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != test.code {
				t.Fatalf("error = %T %v, want code %s", err, err, test.code)
			}
		})
	}
}

func TestCompilerAbsoluteOverlapTableUsesRealSourceAST(t *testing.T) {
	const fixture = "phase0b/fixtures/records/logs/01-to-at-t/parse.json"
	const original = `fetch logs, from:toTimestamp("2026-08-10T10:45:02.718012207Z"), to:toTimestamp("2026-08-10T11:05:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	tests := []struct {
		name           string
		replayStart    string
		replayEnd      string
		virtualNow     string
		wantClass      OverlapClassification
		wantEffective  *Interval
		wantNoEmission bool
	}{
		{
			name:        "absolute range inside visible interval",
			replayStart: "2026-08-10T10:00:00Z", replayEnd: "2026-08-10T12:00:00Z", virtualNow: "2026-08-10T11:30:00Z",
			wantClass: OverlapPresent, wantEffective: intervalPtr(mustInterval(t, "2026-08-10T10:45:02.718012207Z", "2026-08-10T11:05:02.718012207Z")),
		},
		{
			name:        "range before data start can never overlap",
			replayStart: "2026-08-10T12:00:00Z", replayEnd: "2026-08-10T16:00:00Z", virtualNow: "2026-08-10T14:00:00Z",
			wantClass: OverlapPermanent, wantNoEmission: true,
		},
		{
			name:        "future range inside replay is temporary",
			replayStart: "2026-08-10T08:00:00Z", replayEnd: "2026-08-10T12:00:00Z", virtualNow: "2026-08-10T10:00:00Z",
			wantClass: OverlapTemporary, wantNoEmission: true,
		},
		{
			name:        "range crossing data start",
			replayStart: "2026-08-10T10:50:00Z", replayEnd: "2026-08-10T12:00:00Z", virtualNow: "2026-08-10T11:30:00Z",
			wantClass: OverlapPresent, wantEffective: intervalPtr(mustInterval(t, "2026-08-10T10:50:00Z", "2026-08-10T11:05:02.718012207Z")),
		},
		{
			name:        "range crossing visible end",
			replayStart: "2026-08-10T10:00:00Z", replayEnd: "2026-08-10T12:00:00Z", virtualNow: "2026-08-10T10:55:00Z",
			wantClass: OverlapPresent, wantEffective: intervalPtr(mustInterval(t, "2026-08-10T10:45:02.718012207Z", "2026-08-10T10:55:00Z")),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := compileInputAt(t, loadSDKFixture(t, fixture), original, test.replayStart, test.replayEnd, test.virtualNow)
			result, err := Compile(input)
			if test.wantClass == OverlapPresent {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var nonOverlap *NonOverlapError
				if !errors.As(err, &nonOverlap) || nonOverlap.Classification != test.wantClass {
					t.Fatalf("error = %T %v, want %s NonOverlapError", err, err, test.wantClass)
				}
			}
			if len(result.Sources) != 1 || result.Sources[0].Overlap.Classification != test.wantClass ||
				!equalTestInterval(result.Sources[0].Effective, test.wantEffective) {
				t.Fatalf("source = %#v, want class %s effective %#v", result.Sources, test.wantClass, test.wantEffective)
			}
			if test.wantNoEmission && result.EffectiveDQL != "" {
				t.Fatalf("non-overlap emitted DQL: %s", result.EffectiveDQL)
			}
		})
	}
}

func TestCompilerEmptyVisibleIntervalClassifiesTemporaryAndUnknown(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		original string
		want     OverlapClassification
	}{
		{"relative lookback becomes visible later", "phase0/fixtures/02-fetch-implicit-duration/parse.json", "fetch logs, from:-1h", OverlapTemporary},
		{"aligned future behavior is unproven", "phase0/fixtures/03-fetch-aligned-duration/parse.json", "fetch logs, from:-1d@d", OverlapUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := fixedCompileInput(loadSDKFixture(t, test.fixture), test.original)
			input.VirtualNow = input.ReplayInterval.Start
			input.VisibleInterval.End = input.VirtualNow
			result, err := Compile(input)
			var nonOverlap *NonOverlapError
			if !errors.As(err, &nonOverlap) || nonOverlap.Classification != test.want {
				t.Fatalf("error = %T %v, want %s", err, err, test.want)
			}
			if result.EffectiveDQL != "" || len(result.Sources) != 1 {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestCompilerAtDataEndMakesRemainingNonOverlapPermanent(t *testing.T) {
	const original = `fetch logs, from:toTimestamp("2026-08-10T10:45:02.718012207Z"), to:toTimestamp("2026-08-10T11:05:02.718012207Z") | filter timestamp == toTimestamp("2026-08-10T10:55:02.718012207Z") | summarize matched=count()`
	input := compileInputAt(t, loadSDKFixture(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"), original,
		"2026-08-10T08:00:00Z", "2026-08-10T10:00:00Z", "2026-08-10T10:00:00Z")
	_, err := Compile(input)
	var nonOverlap *NonOverlapError
	if !errors.As(err, &nonOverlap) || nonOverlap.Classification != OverlapPermanent {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestCompilerNestedSourcesUseIndependentWindows(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/10-nested-append/parse.json").Clone()
	numbers := terminalNodes(ast, "NUMBER")
	if len(numbers) != 1 {
		t.Fatalf("NUMBER nodes = %d, want 1", len(numbers))
	}
	numbers[0].Canonical = "1"
	result, err := Compile(fixedCompileInput(ast, "fetch logs | append [ fetch spans, from:now()-1h ]"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sources) != 2 || result.Sources[0].Effective == nil || result.Sources[1].Effective == nil {
		t.Fatalf("sources = %#v", result.Sources)
	}
	if result.Sources[0].Effective.Start != timeAt(8) || result.Sources[1].Effective.Start != timeAt(9) {
		t.Fatalf("effective windows = %#v, %#v", result.Sources[0].Effective, result.Sources[1].Effective)
	}
}

func TestCompilerMetricContractAndTypedNotices(t *testing.T) {
	const metricDQL = `timeseries metric_value=avg(dt.host.cpu.usage), interval:1d, from:toTimestamp("2026-08-08T19:53:06Z"), to:toTimestamp("2026-08-10T12:48:06Z")`
	metricInput := compileInputAt(t, loadPhase0BFixture(t, "metrics/05-explicit-1d/parse.json"), metricDQL,
		"2026-08-08T00:00:00Z", "2026-08-11T00:00:00Z", "2026-08-10T13:00:00Z")
	metric, err := Compile(metricInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(metric.ResultContracts) != 1 || metric.ResultContracts[0].DeclaredNaturalInterval == nil ||
		*metric.ResultContracts[0].DeclaredNaturalInterval != 24*time.Hour || !metric.Sources[0].PhysicalPending {
		t.Fatalf("metric result = %#v", metric)
	}
	assertNotice(t, metric.Notices, NoticeFixedDayInterval, NoticeNotification)

	const davisDQL = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-08-03T09:55:03Z"), to:toTimestamp("2026-08-10T10:56:03Z") | filter isNotNull(timestamp) | sort timestamp desc | fields observed_timestamp=timestamp | limit 1`
	davisInput := compileInputAt(t, loadPhase0BFixture(t, "records/davis-problems-snapshots/00-discovery/parse.json"), davisDQL,
		"2026-08-03T08:00:00Z", "2026-08-11T00:00:00Z", "2026-08-10T11:00:00Z")
	davisInput.VirtualStart = mustTime(t, "2026-08-03T13:00:00Z")
	davis, err := Compile(davisInput)
	if err != nil {
		t.Fatal(err)
	}
	assertNotice(t, davis.Notices, NoticeDavisWarmup, NoticeWarning)
	davisInput.VirtualStart = mustTime(t, "2026-08-03T14:00:00Z")
	davis, err = Compile(davisInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(davis.Notices) != 0 {
		t.Fatalf("six-hour warm-up notices = %#v", davis.Notices)
	}
}

func TestCompilerBuildsResultContractForEveryAllowlistedMetricForm(t *testing.T) {
	tests := []struct {
		fixture string
		dql     string
	}{
		{"metrics/01-automatic/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), from:toTimestamp("2026-08-09T08:36:06Z"), to:toTimestamp("2026-08-10T12:50:06Z")`},
		{"metrics/02-explicit-1m/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), interval:1m, from:toTimestamp("2026-08-10T08:36:06Z"), to:toTimestamp("2026-08-10T12:51:06Z")`},
		{"metrics/03-explicit-5m/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), interval:5m, from:toTimestamp("2026-08-10T08:10:06Z"), to:toTimestamp("2026-08-10T12:51:06Z")`},
		{"metrics/04-explicit-1h/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), interval:1h, from:toTimestamp("2026-08-10T07:16:06Z"), to:toTimestamp("2026-08-10T12:46:06Z")`},
		{"metrics/05-explicit-1d/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), interval:1d, from:toTimestamp("2026-08-08T19:53:06Z"), to:toTimestamp("2026-08-10T12:48:06Z")`},
		{"metrics/06a-calendar-vienna-current/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), interval:1d, from:toTimestamp("2026-08-08T19:53:06Z"), to:toTimestamp("2026-08-10T12:48:06Z")`},
		{"metrics/07-rate/parse.json", `timeseries metric_value=sum(dt.service.request.count, rate:1s), interval:1h, from:toTimestamp("2026-08-10T05:36:06Z"), to:toTimestamp("2026-08-10T12:46:06Z")`},
		{"metrics/08-multi-series/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage), by:{dt.entity.host}, interval:5m, from:toTimestamp("2026-08-10T08:10:06Z"), to:toTimestamp("2026-08-10T12:51:06Z")`},
		{"metrics/09-multi-aggregation/parse.json", `timeseries {metric_average=avg(dt.host.cpu.usage), metric_maximum=max(dt.host.cpu.usage)}, interval:5m, from:toTimestamp("2026-08-10T08:10:06Z"), to:toTimestamp("2026-08-10T12:51:06Z")`},
		{"metrics/10-rollup/parse.json", `timeseries metric_value=avg(dt.host.cpu.usage, rollup:avg), interval:1h, from:toTimestamp("2026-08-10T05:36:06Z"), to:toTimestamp("2026-08-10T12:46:06Z")`},
	}
	for _, test := range tests {
		t.Run(test.fixture, func(t *testing.T) {
			input := compileInputAt(t, loadPhase0BFixture(t, test.fixture), test.dql,
				"2026-08-10T10:00:00Z", "2026-08-10T12:00:00Z", "2026-08-10T11:00:00Z")
			result, err := Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Sources) != 1 || len(result.ResultContracts) != 1 ||
				result.Sources[0].ResultContract == nil || !result.ResultContracts[0].NaturalIntervalRequired {
				t.Fatalf("result contracts = %#v", result)
			}
		})
	}
}

func compileInputAt(t *testing.T, ast *AST, original, replayStart, replayEnd, virtualNow string) CompileInput {
	t.Helper()
	start := mustTime(t, replayStart)
	end := mustTime(t, replayEnd)
	now := mustTime(t, virtualNow)
	return CompileInput{
		OriginalDQL: original, AST: ast, VirtualNow: now, VirtualStart: start,
		ReplayInterval: Interval{Start: start, End: end}, VisibleInterval: Interval{Start: start, End: now},
		Locale: "en_US", TimezoneName: "UTC", Timezone: time.UTC, SourcePolicy: Milestone1SourcePolicy(),
	}
}

func mustInterval(t *testing.T, start, end string) Interval {
	t.Helper()
	return Interval{Start: mustTime(t, start), End: mustTime(t, end)}
}

func intervalPtr(value Interval) *Interval {
	return &value
}

func equalTestInterval(left, right *Interval) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func terminalNodes(ast *AST, role string) []*Node {
	var out []*Node
	_ = ast.WalkExecutable(func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == role {
			out = append(out, node)
		}
		return nil
	})
	return out
}

func assertNotice(t *testing.T, notices []Notice, code NoticeCode, kind NoticeKind) {
	t.Helper()
	for _, notice := range notices {
		if notice.Code == code && notice.Kind == kind && notice.Message != "" {
			return
		}
	}
	t.Fatalf("notice %s/%s not found in %#v", code, kind, notices)
}

func fixedCompileInput(ast *AST, original string) CompileInput {
	start := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	return CompileInput{
		OriginalDQL: original, AST: ast, VirtualNow: now, VirtualStart: start,
		ReplayInterval:  Interval{Start: start, End: end},
		VisibleInterval: Interval{Start: start, End: now},
		Locale:          "en_US", TimezoneName: "UTC", Timezone: time.UTC,
		SourcePolicy: Milestone1SourcePolicy(),
	}
}
