package replay

import (
	"errors"
	"testing"
	"time"
)

func TestExplainDataContainsClockAndPerSourceFacts(t *testing.T) {
	input := fixedCompileInput(loadSDKFixture(t, "phase0/fixtures/01-fetch-explicit-now/parse.json"), "fetch logs, from:now()-1h")
	input.VirtualStart = time.Time{}
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	clock := result.Explain.Clock
	if clock.VirtualNow != input.VirtualNow || clock.VirtualStart != input.ReplayInterval.Start ||
		clock.ReplayInterval != input.ReplayInterval || clock.VisibleInterval != input.VisibleInterval ||
		clock.Locale != "en_US" || clock.Timezone != "UTC" {
		t.Fatalf("clock explain = %#v", clock)
	}
	if len(result.Explain.Sources) != 1 {
		t.Fatalf("sources = %#v", result.Explain.Sources)
	}
	source := result.Explain.Sources[0]
	if source.Class != SourceRecord || source.Name != "logs" || source.RecordTimeField != "timestamp" ||
		source.Requested == nil || source.Effective == nil || source.Classification != OverlapPresent || source.Proof.Reason == "" {
		t.Fatalf("source explain = %#v", source)
	}
	if result.Explain.EffectiveDQL == "" || result.Explain.EffectiveDQL != result.EffectiveDQL {
		t.Fatalf("effective explain = %q", result.Explain.EffectiveDQL)
	}
}

func TestCompileInputContractFailsClosed(t *testing.T) {
	base := fixedCompileInput(loadSDKFixture(t, "phase0/fixtures/05-fetch-no-timeframe/parse.json"), "fetch logs")
	tests := []struct {
		name   string
		mutate func(*CompileInput)
		code   ErrorCode
	}{
		{"missing AST", func(input *CompileInput) { input.AST = nil }, ErrorASTContract},
		{"empty original", func(input *CompileInput) { input.OriginalDQL = "" }, ErrorASTContract},
		{"invalid replay", func(input *CompileInput) { input.ReplayInterval.End = input.ReplayInterval.Start }, ErrorTimeframe},
		{"now outside replay", func(input *CompileInput) { input.VirtualNow = input.ReplayInterval.End.Add(time.Second) }, ErrorTimeframe},
		{"now before virtual start", func(input *CompileInput) { input.VirtualStart = input.VirtualNow.Add(time.Minute) }, ErrorTimeframe},
		{"visible mismatch", func(input *CompileInput) { input.VisibleInterval.End = input.VisibleInterval.End.Add(-time.Second) }, ErrorTimeframe},
		{"invalid global default", func(input *CompileInput) {
			value := Interval{Start: input.VirtualNow, End: input.VirtualNow}
			input.GlobalDefault = &value
		}, ErrorTimeframe},
		{"empty policy", func(input *CompileInput) { input.SourcePolicy = SourcePolicy{} }, ErrorUnsupportedSource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.mutate(&input)
			_, err := Compile(input)
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != test.code {
				t.Fatalf("error = %T %v, want %s", err, err, test.code)
			}
		})
	}
}

func TestCompileAcceptsOnlyMatchingPrecomputedSourceAnalysis(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/01-fetch-explicit-now/parse.json")
	input := fixedCompileInput(ast, "fetch logs, from:now()-1h")
	descriptors, analysis, err := AnalyzeSourcesWithMapping(ast, input.SourcePolicy, input.DavisMapping)
	if err != nil || len(descriptors) != 1 || analysis == nil {
		t.Fatalf("AnalyzeSourcesWithMapping = %#v, %#v, %v", descriptors, analysis, err)
	}
	input.PrecomputedAnalysis = analysis
	result, err := Compile(input)
	if err != nil || result.EffectiveDQL == "" {
		t.Fatalf("Compile with matching analysis = %#v, %v", result, err)
	}

	t.Run("AST revision", func(t *testing.T) {
		mismatch := input
		mismatch.AST = ast.Clone()
		firstTerminal(mismatch.AST, "DATA_OBJECT").Canonical = "events"
		_, err := Compile(mismatch)
		assertReplayErrorCode(t, err, ErrorASTContract)
	})

	t.Run("source policy", func(t *testing.T) {
		mismatch := input
		mismatch.SourcePolicy = cloneSourcePolicy(input.SourcePolicy)
		mismatch.SourcePolicy.DefaultLookback++
		_, err := Compile(mismatch)
		assertReplayErrorCode(t, err, ErrorAudit)
	})

	t.Run("mapping mode", func(t *testing.T) {
		mismatch := input
		mismatch.DavisMapping = DavisProblemsMappingPolicy{Mode: DavisProblemsMappingInspection}
		_, err := Compile(mismatch)
		assertReplayErrorCode(t, err, ErrorAudit)
	})
}

func TestDavisProblemsMappingCandidateCloneOwnsSpan(t *testing.T) {
	original := DavisProblemsMappingCandidate{Span: &Span{Start: Position{Index: 1}, End: Position{Index: 2}}}
	clone := original.Clone()
	clone.Span.Start.Index = 99
	if original.Span.Start.Index != 1 {
		t.Fatalf("Clone shared its span with the original: %#v", original)
	}
}

func assertReplayErrorCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != want {
		t.Fatalf("error = %T %v, want %s", err, err, want)
	}
}
