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
