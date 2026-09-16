package replay

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func topologyFixtureInput(t *testing.T, name string) CompileInput {
	t.Helper()
	query, err := os.ReadFile(filepath.Join("../../../sdk/api/query/testdata/topology/fixtures", name, "original.dql"))
	if err != nil {
		t.Fatal(err)
	}
	input := fixedCompileInput(loadSDKFixture(t, "topology/fixtures/"+name+"/parse.json"), string(query))
	input.ReplayInterval = mustInterval(t, "2026-08-01T10:00:00Z", "2026-08-01T14:00:00Z")
	input.VirtualNow = mustTime(t, "2026-08-01T12:00:00Z")
	input.VirtualStart = input.VirtualNow
	input.VisibleInterval = Interval{Start: input.ReplayInterval.Start, End: input.VirtualNow}
	return input
}

func TestTopologyCapturedCompilerAndAudit(t *testing.T) {
	for _, name := range []string{
		"nodes-bare", "nodes-window", "nodes-two-hour", "nodes-timeframe", "nodes-from-only", "nodes-list", "nodes-wildcard", "nodes-unquoted",
		"edges-bare", "edges-window", "edges-runs-on", "edges-list", "edges-unquoted",
		"traverse-window", "traverse-limit", "traverse-filter", "traverse-fields-add", "traverse-chain",
		"traverse-edges-feeder", "traverse-edge-list", "traverse-params", "traverse-nested", "traverse-different-windows",
	} {
		t.Run(name, func(t *testing.T) {
			input := topologyFixtureInput(t, name)
			result, err := Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := os.ReadFile(filepath.Join("../../../sdk/api/query/testdata/topology/fixtures", name, "effective.dql"))
			if err != nil || result.EffectiveDQL != string(expected) {
				t.Fatalf("effective=%s expected=%s err=%v", result.EffectiveDQL, expected, err)
			}
			for _, source := range result.Sources {
				if source.Source.Class != SourceTopology { // Nested fixtures can include record or synthetic sources.
					continue
				}
				if source.Source.BoundaryPolicy != BoundaryWindowOnly || source.Effective == nil || source.PhysicalRange == nil ||
					*source.Effective != *source.PhysicalRange || source.PhysicalPending || source.ResultContract != nil {
					t.Fatalf("topology contract=%#v", source)
				}
			}
			for _, binding := range result.Traversals {
				feeder := result.Sources[binding.FeederOrdinal]
				if binding.Path == "" || binding.FeederPath != feeder.Source.Path || binding.Effective != *feeder.Effective {
					t.Fatalf("binding=%#v feeder=%#v", binding, feeder)
				}
			}
			validation := loadSDKFixture(t, "topology/fixtures/"+name+"/validation-parse.json")
			audit, err := Audit(AuditInput{ValidationAST: validation, Compilation: result, SourcePolicy: input.SourcePolicy, Timezone: time.UTC})
			if err != nil || !audit.OK {
				t.Fatalf("audit=%#v err=%v", audit, err)
			}
			if name == "traverse-chain" && (len(result.Traversals) != 2 || result.Traversals[0].FeederPath != result.Traversals[1].FeederPath) {
				t.Fatalf("chained bindings=%#v", result.Traversals)
			}
		})
	}
}

func TestTopologyTimeframePrecedenceAndWidth(t *testing.T) {
	global := mustInterval(t, "2026-08-01T10:15:00Z", "2026-08-01T10:45:00Z")
	for _, name := range []string{"nodes-bare", "edges-bare", "nodes-window", "nodes-timeframe", "nodes-from-only"} {
		t.Run(name, func(t *testing.T) {
			input := topologyFixtureInput(t, name)
			result, err := Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			want := mustInterval(t, "2026-08-01T11:00:00Z", "2026-08-01T12:00:00Z")
			if name == "nodes-from-only" {
				want.Start = input.VirtualNow.Add(-5 * time.Minute)
			}
			if strings.HasSuffix(name, "-bare") {
				want.Start = input.VirtualNow.Add(-time.Minute)
			}
			if *result.Sources[0].Effective != want {
				t.Fatalf("window=%v want=%v", *result.Sources[0].Effective, want)
			}
			input.GlobalDefault = &global
			result, err = Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(name, "-bare") {
				want = global
			}
			if *result.Sources[0].Effective != want {
				t.Fatalf("default precedence window=%v want=%v", *result.Sources[0].Effective, want)
			}
		})
	}
	for _, width := range []time.Duration{time.Nanosecond, 5 * time.Second, 20 * time.Second, time.Minute - time.Nanosecond, time.Minute, 2 * time.Hour} {
		t.Run(width.String(), func(t *testing.T) {
			input := topologyFixtureInput(t, "nodes-bare")
			requested := Interval{Start: input.VirtualNow.Add(-width), End: input.VirtualNow}
			input.GlobalDefault = &requested
			result, err := Compile(input)
			if width < time.Minute {
				var rejection *ReplayError
				if !errors.As(err, &rejection) || rejection.Code != ErrorTimeframe || !strings.Contains(err.Error(), "60 seconds") ||
					!strings.Contains(err.Error(), requested.Start.Format(time.RFC3339Nano)) || rejection.PublicMessage != "The query could not be run as written." {
					t.Fatalf("width rejection=%v", err)
				}
				if result.AuditRequired || result.EffectiveDQL != "" {
					t.Fatal("narrow window was emitted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if result.Sources[0].Effective == nil || *result.Sources[0].Effective != requested || result.Explain.Sources[0].Effective == nil {
				t.Fatalf("computed window missing or widened: %#v", result.Sources)
			}
		})
	}
	// A wide request can still leave less than one minute after intersection.
	input := topologyFixtureInput(t, "nodes-window")
	input.VirtualNow = mustTime(t, "2026-08-01T11:00:20Z")
	input.VirtualStart = input.VirtualNow
	input.VisibleInterval.End = input.VirtualNow
	result, err := Compile(input)
	if err == nil || result.Sources[0].Effective == nil || result.Sources[0].Effective.End.Sub(result.Sources[0].Effective.Start) != 20*time.Second {
		t.Fatalf("wide request intersection result=%#v err=%v", result, err)
	}
	input = topologyFixtureInput(t, "nodes-bare")
	input.VirtualNow = input.ReplayInterval.Start.Add(time.Minute)
	input.VirtualStart = input.VirtualNow
	input.VisibleInterval.End = input.VirtualNow
	result, err = Compile(input)
	if err != nil || *result.Sources[0].Effective != input.VisibleInterval {
		t.Fatalf("first minute result=%#v err=%v", result, err)
	}
}

func TestTopologyNonOverlapKeepsExistingClassification(t *testing.T) {
	for _, test := range []struct {
		start, end string
		class      OverlapClassification
	}{
		{"2026-08-01T12:30:00Z", "2026-08-01T13:00:00Z", OverlapTemporary},
		{"2026-08-01T09:00:00Z", "2026-08-01T10:00:00Z", OverlapPermanent},
		{"2026-08-01T14:00:00Z", "2026-08-01T15:00:00Z", OverlapPermanent},
	} {
		input := topologyFixtureInput(t, "edges-bare")
		window := mustInterval(t, test.start, test.end)
		input.GlobalDefault = &window
		result, err := Compile(input)
		var nonOverlap *NonOverlapError
		if !errors.As(err, &nonOverlap) || nonOverlap.Classification != test.class || result.Sources[0].Effective != nil {
			t.Fatalf("window=%v result=%#v err=%v", window, result, err)
		}
	}
}

func TestTopologyRejectedCapturedShapes(t *testing.T) {
	for _, name := range []string{"edges-wildcard", "edges-contains", "edges-pattern", "traverse-after-fetch", "traverse-after-data", "traverse-after-append", "traverse-after-join", "traverse-after-lookup", "traverse-nested-no-feeder"} {
		t.Run(name, func(t *testing.T) {
			result, err := Compile(topologyFixtureInput(t, name))
			var rejection *ReplayError
			if !errors.As(err, &rejection) || rejection.Code != ErrorUnsupportedForm || result.AuditRequired {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			if strings.HasPrefix(name, "edges-") && (!strings.Contains(err.Error(), "calls") || !strings.Contains(err.Error(), "runs_on")) {
				t.Fatalf("edge rejection lacks verified types: %v", err)
			}
			if strings.HasPrefix(name, "traverse-") && rejection.Construct != "traverse" {
				t.Fatalf("feeder rejection lacks command: %v", err)
			}
		})
	}
}

func TestTopologyInvalidTimeframesDoNotUseFallback(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*CompileInput)
	}{
		{"invalid default", func(input *CompileInput) {
			input.GlobalDefault = &Interval{Start: input.VirtualNow, End: input.VirtualNow}
		}},
		{"to without from", func(input *CompileInput) {
			for _, token := range topologyTokens(input.AST, "PARAMETER_KEY") {
				if token.Canonical == "from" {
					token.Canonical = "to"
				}
			}
		}},
		{"malformed timestamp", func(input *CompileInput) {
			for _, token := range topologyTokens(input.AST, "TIME_UNIT") {
				token.Canonical = "unsupported-unit"
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := topologyFixtureInput(t, "nodes-from-only")
			test.mutate(&input)
			result, err := Compile(input)
			if err == nil || result.AuditRequired || result.EffectiveDQL != "" {
				t.Fatalf("invalid input fell back: %#v err=%v", result, err)
			}
		})
	}
	// A syntactically valid, explicit 1ns topology range still records the
	// effective window and uses the topology width error, not the record rule.
	input := topologyFixtureInput(t, "nodes-window")
	for _, token := range topologyTokens(input.AST, "TIMESTAMP_VALUE") {
		if strings.Contains(token.Canonical, "11:00:00") {
			token.Canonical = `"2026-08-01T11:59:59.999999999Z"`
		}
	}
	result, err := Compile(input)
	if err == nil || !strings.Contains(err.Error(), "60 seconds") || len(result.Sources) != 1 || result.Sources[0].Effective == nil || result.Sources[0].Effective.End.Sub(result.Sources[0].Effective.Start) != time.Nanosecond {
		t.Fatalf("explicit nanosecond result=%#v err=%v", result, err)
	}
}
