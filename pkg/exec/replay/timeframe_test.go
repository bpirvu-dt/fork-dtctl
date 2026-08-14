package replay

import (
	"errors"
	"testing"
	"time"
)

func TestResolveRequestedRangeFromRealTimeExpressionFixtures(t *testing.T) {
	base := time.Date(2026, 6, 14, 10, 30, 0, 0, time.UTC)
	context := timeframeContext{
		VirtualNow:      base,
		ReplayInterval:  intervalAt(8, 12),
		VisibleInterval: Interval{Start: intervalAt(8, 12).Start, End: base},
		Timezone:        time.UTC,
	}
	tests := []struct {
		name      string
		fixture   string
		wantStart time.Time
		wantEnd   time.Time
		fromKind  EndpointDependency
	}{
		{"explicit now", "phase0/fixtures/01-fetch-explicit-now/parse.json", base.Add(-time.Hour), base, EndpointRelative},
		{"implicit now", "phase0/fixtures/02-fetch-implicit-duration/parse.json", base.Add(-time.Hour), base, EndpointRelative},
		{"aligned day", "phase0/fixtures/03-fetch-aligned-duration/parse.json", time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC), base, EndpointUnknown},
		{"alignment only", "phase0/fixtures/04-fetch-alignment-only/parse.json", time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC), base, EndpointUnknown},
		{"missing timeframe", "phase0/fixtures/05-fetch-no-timeframe/parse.json", base.Add(-2 * time.Hour), base, EndpointRelative},
		{"quoted timeframe", "phase0/fixtures/07-fetch-explicit-timeframe/parse.json", time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC), time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC), EndpointAbsolute},
		{"timeseries now", "phase0/fixtures/11-timeseries-from/parse.json", base.Add(-time.Hour), base, EndpointRelative},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := firstSourceAnalysis(t, loadSDKFixture(t, tt.fixture))
			requested, err := resolveRequestedRange(source, context)
			if err != nil {
				t.Fatalf("resolveRequestedRange: %v", err)
			}
			if !requested.Range.Start.Equal(tt.wantStart) || !requested.Range.End.Equal(tt.wantEnd) || requested.From.Dependency != tt.fromKind {
				t.Fatalf("requested = %#v, want %s to %s (%s)", requested, tt.wantStart, tt.wantEnd, tt.fromKind)
			}
		})
	}
}

func TestGlobalDefaultOverridesVerifiedMissingTimeframeDefault(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/05-fetch-no-timeframe/parse.json")
	global := Interval{
		Start: time.Date(2026, 6, 14, 8, 15, 0, 0, time.UTC),
		End:   time.Date(2026, 6, 14, 9, 45, 0, 0, time.UTC),
	}
	context := timeframeContext{VirtualNow: intervalAt(8, 12).End, ReplayInterval: intervalAt(8, 12), VisibleInterval: intervalAt(8, 12), GlobalDefault: &global}
	requested, err := resolveRequestedRange(firstSourceAnalysis(t, ast), context)
	if err != nil {
		t.Fatalf("resolveRequestedRange: %v", err)
	}
	if requested.Basis != "global default timeframe" || requested.From.Dependency != EndpointAbsolute || requested.Range != global {
		t.Fatalf("requested = %#v", requested)
	}
}

func TestUnsupportedTimeframeFormsFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
	}{
		{"to without from", "phase0/fixtures/06-fetch-to-now/parse.json"},
		{"genuine calendar arithmetic", "phase0/fixtures/08-fetch-calendar-arithmetic/parse.json"},
	}
	context := timeframeContext{VirtualNow: intervalAt(8, 12).End, ReplayInterval: intervalAt(8, 12), VisibleInterval: intervalAt(8, 12), Timezone: time.UTC}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := firstSourceAnalysis(t, loadSDKFixture(t, tt.fixture))
			_, err := resolveRequestedRange(source, context)
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
				t.Fatalf("error = %T %v, want timeframe ReplayError", err, err)
			}
		})
	}
}

func TestMalformedQuotedTimeframeFailsClosed(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/07-fetch-explicit-timeframe/parse.json").Clone()
	literal := firstTerminal(ast, "STRING")
	if literal == nil {
		t.Fatal("fixture has no timeframe string")
	}
	literal.Canonical = `"not-a-timeframe"`
	context := timeframeContext{VirtualNow: timeAt(10), ReplayInterval: intervalAt(8, 12), VisibleInterval: intervalAt(8, 10), Timezone: time.UTC}
	_, err := resolveRequestedRange(firstSourceAnalysis(t, ast), context)
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
		t.Fatalf("error = %T %v", err, err)
	}
}

func TestUnverifiedAlignmentFormsFailClosed(t *testing.T) {
	t.Run("unknown alignment operator", func(t *testing.T) {
		ast := loadSDKFixture(t, "phase0/fixtures/04-fetch-alignment-only/parse.json").Clone()
		operators := terminalNodes(ast, "OPERATOR")
		if len(operators) != 1 {
			t.Fatalf("operators = %d, want 1", len(operators))
		}
		operators[0].Canonical = "@w"
		context := timeframeContext{VirtualNow: timeAt(10).Add(30 * time.Minute), ReplayInterval: intervalAt(8, 12), VisibleInterval: intervalAt(8, 10), Timezone: time.UTC}
		_, err := resolveRequestedRange(firstSourceAnalysis(t, ast), context)
		var replayErr *ReplayError
		if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
			t.Fatalf("error = %T %v", err, err)
		}
	})
	t.Run("DST-sensitive timezone", func(t *testing.T) {
		ast := loadSDKFixture(t, "phase0/fixtures/04-fetch-alignment-only/parse.json")
		context := timeframeContext{
			VirtualNow: timeAt(10).Add(30 * time.Minute), ReplayInterval: intervalAt(8, 12),
			VisibleInterval: intervalAt(8, 10), Timezone: time.FixedZone("synthetic-dst-sensitive-zone", 60*60),
		}
		_, err := resolveRequestedRange(firstSourceAnalysis(t, ast), context)
		var replayErr *ReplayError
		if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
			t.Fatalf("error = %T %v", err, err)
		}
	})
	t.Run("fixed offset named UTC", func(t *testing.T) {
		ast := loadSDKFixture(t, "phase0/fixtures/04-fetch-alignment-only/parse.json")
		context := timeframeContext{
			VirtualNow: timeAt(10).Add(30 * time.Minute), ReplayInterval: intervalAt(8, 12),
			VisibleInterval: intervalAt(8, 10), Timezone: time.FixedZone("UTC", 60*60),
		}
		_, err := resolveRequestedRange(firstSourceAnalysis(t, ast), context)
		var replayErr *ReplayError
		if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
			t.Fatalf("error = %T %v", err, err)
		}
	})
}

func TestNonOverlapClassificationUsesFullReplayIntervalForProof(t *testing.T) {
	replay := intervalAt(8, 12)
	visible := Interval{Start: replay.Start, End: timeAt(10)}
	tests := []struct {
		name      string
		requested RequestedRange
		now       time.Time
		want      OverlapClassification
		wantRange Interval
	}{
		{"inside", absoluteRequested(Interval{Start: timeAt(9), End: timeAt(10)}, "test"), timeAt(10), OverlapPresent, Interval{Start: timeAt(9), End: timeAt(10)}},
		{"before replay", absoluteRequested(Interval{Start: timeAt(6), End: timeAt(7)}, "test"), timeAt(10), OverlapPermanent, Interval{}},
		{"absolute future", absoluteRequested(Interval{Start: timeAt(11), End: timeAt(12)}, "test"), timeAt(10), OverlapTemporary, Interval{}},
		{"relative always ahead", relativeRequested(timeAt(10), time.Hour, 2*time.Hour), timeAt(10), OverlapPermanent, Interval{}},
		{"relative never reaches start", relativeRequested(timeAt(10), -10*time.Hour, -9*time.Hour), timeAt(10), OverlapPermanent, Interval{}},
		{"unknown future behavior", unknownRequested(timeAt(11), timeAt(12)), timeAt(10), OverlapUnknown, Interval{}},
		{"empty initial interval later overlaps", relativeRequested(timeAt(8), -time.Hour, 0), timeAt(8), OverlapTemporary, Interval{}},
		{"remaining at data end", absoluteRequested(Interval{Start: timeAt(13), End: timeAt(14)}, "test"), timeAt(12), OverlapPermanent, Interval{}},
		{"lower partial overlap", absoluteRequested(Interval{Start: timeAt(7), End: timeAt(9)}, "test"), timeAt(10), OverlapPresent, Interval{Start: timeAt(8), End: timeAt(9)}},
		{"upper partial overlap", absoluteRequested(Interval{Start: timeAt(9), End: timeAt(13)}, "test"), timeAt(12), OverlapPresent, Interval{Start: timeAt(9), End: timeAt(10)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentVisible := visible
			if tt.now.Equal(timeAt(8)) {
				currentVisible.End = timeAt(8)
			}
			effective, proof := ClassifyOverlap(tt.requested, currentVisible, replay, tt.now)
			if proof.Classification != tt.want || effective != tt.wantRange || proof.Reason == "" {
				t.Fatalf("effective = %#v, proof = %#v, want %s %#v", effective, proof, tt.want, tt.wantRange)
			}
		})
	}
}

func TestEqualAndOneNanosecondRequestedWindowsAreRejected(t *testing.T) {
	for _, end := range []time.Time{timeAt(9), timeAt(9).Add(time.Nanosecond)} {
		_, err := requestedFromEndpoints(
			TimeEndpoint{Value: timeAt(9), Dependency: EndpointAbsolute},
			TimeEndpoint{Value: end, Dependency: EndpointAbsolute},
			"test",
		)
		var replayErr *ReplayError
		if !errors.As(err, &replayErr) || replayErr.Code != ErrorTimeframe {
			t.Fatalf("end = %s, error = %T %v", end, err, err)
		}
	}
}

func firstSourceAnalysis(t *testing.T, ast *AST) *sourceAnalysis {
	t.Helper()
	sources, err := analyzeSources(ast, Milestone1SourcePolicy(), DavisProblemsMappingPolicy{})
	if err != nil {
		t.Fatalf("analyzeSources: %v", err)
	}
	if len(sources) == 0 {
		t.Fatal("fixture has no source")
	}
	return sources[0]
}

func intervalAt(startHour, endHour int) Interval {
	return Interval{Start: timeAt(startHour), End: timeAt(endHour)}
}

func timeAt(hour int) time.Time {
	return time.Date(2026, 6, 14, hour, 0, 0, 0, time.UTC)
}

func relativeRequested(now time.Time, fromOffset, toOffset time.Duration) RequestedRange {
	from := relativeEndpoint(now, fromOffset)
	to := relativeEndpoint(now, toOffset)
	requested, err := requestedFromEndpoints(from, to, "test")
	if err != nil {
		panic(err)
	}
	return requested
}

func unknownRequested(from, to time.Time) RequestedRange {
	return RequestedRange{
		Range: Interval{Start: from, End: to},
		From:  TimeEndpoint{Value: from, Dependency: EndpointUnknown},
		To:    TimeEndpoint{Value: to, Dependency: EndpointUnknown},
		Basis: "test",
	}
}
