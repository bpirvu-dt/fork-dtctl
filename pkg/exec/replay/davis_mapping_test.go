package replay

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	davisMappingOriginal          = `fetch dt.davis.problems, from:toTimestamp("2026-06-14T09:00:00.000Z"), to:toTimestamp("2026-06-14T10:00:00.000Z")`
	davisMappingTimeframeOriginal = `fetch dt.davis.problems, timeframe:"2026-06-14T09:00:00Z/2026-06-14T10:00:00Z"`
	davisMappingEffective         = `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")
| sort timestamp desc
| dedup event.id
| filter event.start < toTimestamp("2026-06-14T10:00:00.000000000Z") and coalesce(event.end, toTimestamp("2026-06-14T10:00:00.000000000Z")) >= toTimestamp("2026-06-14T09:00:00.000000000Z")`
)

func TestDavisProblemsMappingUsesLiveOriginalAndEffectiveFixtures(t *testing.T) {
	originalAST := loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json")
	input := davisMappingCompileInput(t, originalAST, davisMappingOriginal, DavisProblemsMappingExecution)
	result, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if result.EffectiveDQL != davisMappingEffective {
		t.Fatalf("effective DQL:\n%s\nwant:\n%s", result.EffectiveDQL, davisMappingEffective)
	}
	if result.InspectionOnly || len(result.Sources) != 1 || result.Sources[0].DavisMapping == nil {
		t.Fatalf("mapping result = %#v", result)
	}
	mapping := result.Sources[0].DavisMapping
	if !mapping.Logical.F.Equal(mustTime(t, "2026-06-14T09:00:00Z")) ||
		!mapping.Logical.T.Equal(mustTime(t, "2026-06-14T10:00:00Z")) ||
		!mapping.Physical.W.Equal(mustTime(t, "2026-06-14T03:00:00Z")) ||
		!mapping.Physical.T.Equal(mapping.Logical.T) || mapping.WarmupClamped || !mapping.Coverage.Verified {
		t.Fatalf("typed F/T/W mapping = %#v", mapping)
	}

	validationAST := loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json")
	audit, err := Audit(AuditInput{
		ValidationAST: validationAST, Compilation: result,
		SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if !audit.OK || !audit.DavisMappingsAudited || !audit.StructureMatches {
		t.Fatalf("audit = %#v", audit)
	}
}

func TestDavisProblemsMappingRequiresExactOriginalTokenAndFullPolicy(t *testing.T) {
	ast := loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json")
	if _, err := ClassifySources(ast, Milestone1SourcePolicy()); err == nil {
		t.Fatal("disabled mapping accepted the current view")
	}
	descriptors, err := ClassifySourcesWithMapping(ast, Milestone1SourcePolicy(), DavisProblemsMappingPolicy{Mode: DavisProblemsMappingInspection})
	if err != nil || len(descriptors) != 1 || descriptors[0].Class != SourceDavisProblemsView || descriptors[0].DavisProblems == nil {
		t.Fatalf("eligible classification = %#v, %v", descriptors, err)
	}

	variant := ast.Clone()
	dataObject := firstTerminal(variant, "DATA_OBJECT")
	dataObject.Canonical = "DT.DAVIS.PROBLEMS"
	_, err = ClassifySourcesWithMapping(variant, Milestone1SourcePolicy(), DavisProblemsMappingPolicy{Mode: DavisProblemsMappingInspection})
	var current *DavisCurrentViewError
	if !errors.As(err, &current) {
		t.Fatalf("case-variant token error = %T %v, want current-view rejection", err, err)
	}
}

func TestDavisProblemsMappingComposesCommandEndInsertionAndPreservesDownstreamBytes(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		original   string
		wantPrefix string
		wantSuffix string
		wantExact  string
		wantError  ErrorCode
		audit      bool
	}{
		{
			name: "no bounds", fixture: "phase0/fixtures/05-fetch-no-timeframe/parse.json", original: "fetch dt.davis.problems",
			wantPrefix: `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T02:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")`,
		},
		{
			name: "from only", fixture: "phase0/fixtures/01-fetch-explicit-now/parse.json", original: "fetch dt.davis.problems, from:now()-1h",
			wantPrefix: `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:00:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")`,
		},
		{
			name: "to only", fixture: "phase0/fixtures/06-fetch-to-now/parse.json", original: "fetch dt.davis.problems, to:now()-20m",
			wantError: ErrorTimeframe,
		},
		{
			name: "timeframe", fixture: "phase0/fixtures/07-fetch-explicit-timeframe/parse.json", original: davisMappingTimeframeOriginal,
			wantExact: davisMappingEffective, audit: true,
		},
		{
			name: "downstream", fixture: "pipeline/contains/parse.json", original: `fetch dt.davis.problems, from:now()-5m, to:now() | filter contains(content, "error") | limit 1`,
			wantPrefix: `fetch dt.davis.problems.snapshots, from:toTimestamp("2026-06-14T03:55:00.000000000Z"), to:toTimestamp("2026-06-14T10:00:00.000000000Z")`,
			wantSuffix: ` | filter contains(content, "error") | limit 1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast := loadSDKFixture(t, test.fixture).Clone()
			replaceTestSourceToken(ast, "logs", davisProblemsView)
			input := davisMappingCompileInput(t, ast, test.original, DavisProblemsMappingExecution)
			result, err := Compile(input)
			if test.wantError != "" {
				var replayErr *ReplayError
				if !errors.As(err, &replayErr) || replayErr.Code != test.wantError {
					t.Fatalf("error = %T %v, want %s", err, err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantExact != "" && result.EffectiveDQL != test.wantExact {
				t.Fatalf("effective DQL:\n%s\nwant:\n%s", result.EffectiveDQL, test.wantExact)
			}
			if !strings.HasPrefix(result.EffectiveDQL, test.wantPrefix) ||
				(test.wantSuffix != "" && !strings.HasSuffix(result.EffectiveDQL, test.wantSuffix)) ||
				strings.Count(result.EffectiveDQL, "| sort timestamp desc") != 1 ||
				strings.Count(result.EffectiveDQL, "| dedup event.id") != 1 {
				t.Fatalf("effective DQL = %q", result.EffectiveDQL)
			}
			if test.audit {
				audit, auditErr := Audit(AuditInput{
					ValidationAST: loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json"),
					Compilation:   result, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
				})
				if auditErr != nil || !audit.OK || !audit.DavisMappingsAudited {
					t.Fatalf("timeframe audit = %#v, %v", audit, auditErr)
				}
			}
		})
	}
}

func TestDavisProblemsMappingInspectionIsExplicitlyCoverageUnverified(t *testing.T) {
	input := davisMappingCompileInput(t, loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json"), davisMappingOriginal, DavisProblemsMappingInspection)
	input.DavisMapping.Coverage = nil
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	if !result.InspectionOnly || result.Explain.CoverageVerified == nil || *result.Explain.CoverageVerified ||
		result.Explain.CoverageMessage != DavisCoverageNotVerifiedMessage || result.Sources[0].DavisMapping.Coverage.Verified {
		t.Fatalf("inspection result = %#v", result)
	}
}

func TestDavisProblemsMappingFTWClampBoundaries(t *testing.T) {
	f := mustTime(t, "2026-06-14T09:00:00.123456789Z")
	timeT := mustTime(t, "2026-06-14T10:00:00.987654321Z")
	unclampedW := f.Add(-6 * time.Hour)
	tests := []struct {
		name      string
		dataStart time.Time
		wantW     time.Time
		wantClamp bool
	}{
		{"one nanosecond before six hours", unclampedW.Add(-time.Nanosecond), unclampedW, false},
		{"exact six-hour equality", unclampedW, unclampedW, false},
		{"one nanosecond after six hours", unclampedW.Add(time.Nanosecond), unclampedW.Add(time.Nanosecond), true},
		{"F equals data_start", f, f, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast, original := davisAbsoluteRange(t, f, timeT)
			input := davisMappingCompileInput(t, ast, original, DavisProblemsMappingExecution)
			input.ReplayInterval.Start = test.dataStart
			input.VisibleInterval.Start = test.dataStart
			input.VisibleInterval.End = timeT
			input.VirtualNow = timeT
			if input.VirtualStart.Before(test.dataStart) {
				input.VirtualStart = test.dataStart
			}
			input.DavisMapping.Coverage.OldestSnapshot = test.dataStart.Add(-time.Hour)
			result, err := Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			mapping := result.Sources[0].DavisMapping
			if !mapping.Logical.F.Equal(f) || !mapping.Logical.T.Equal(timeT) ||
				!mapping.Physical.W.Equal(test.wantW) || !mapping.Physical.T.Equal(timeT) ||
				mapping.WarmupClamped != test.wantClamp {
				t.Fatalf("mapping = %#v", mapping)
			}
		})
	}
}

func TestDavisProblemsMappingFencingKeepsLogicalAndPhysicalRangesIndependent(t *testing.T) {
	f := mustTime(t, "2026-06-14T09:00:00Z")
	timeT := mustTime(t, "2026-06-14T10:00:00Z")
	ast, original := davisAbsoluteRange(t, f, timeT)
	input := davisMappingCompileInput(t, ast, original, DavisProblemsMappingExecution)
	input.ReplayInterval.Start = mustTime(t, "2026-06-14T09:15:00Z")
	input.VisibleInterval = Interval{Start: input.ReplayInterval.Start, End: mustTime(t, "2026-06-14T09:45:00Z")}
	input.VirtualNow = input.VisibleInterval.End
	input.VirtualStart = input.ReplayInterval.Start
	input.DavisMapping.Coverage.OldestSnapshot = mustTime(t, "2026-06-14T09:00:00Z")
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	mapping := result.Sources[0].DavisMapping
	if !mapping.Logical.F.Equal(input.ReplayInterval.Start) || !mapping.Logical.T.Equal(input.VisibleInterval.End) ||
		!mapping.Physical.W.Equal(input.ReplayInterval.Start) || !mapping.Physical.T.Equal(input.VisibleInterval.End) {
		t.Fatalf("partially fenced mapping = %#v", mapping)
	}
	if !strings.Contains(result.EffectiveDQL, `coalesce(event.end, toTimestamp("2026-06-14T09:45:00.000000000Z")) >= toTimestamp("2026-06-14T09:15:00.000000000Z")`) {
		t.Fatalf("logical filter did not retain independent F/T: %s", result.EffectiveDQL)
	}
}

func TestDavisProblemsMappingTerminalUpperFenceUsesDataEnd(t *testing.T) {
	f := mustTime(t, "2026-06-14T11:30:00.123456789Z")
	requestedT := mustTime(t, "2026-06-14T13:00:00Z")
	dataEnd := mustTime(t, "2026-06-14T12:00:00Z")
	ast, original := davisAbsoluteRange(t, f, requestedT)
	input := davisMappingCompileInput(t, ast, original, DavisProblemsMappingExecution)
	input.VirtualNow = dataEnd
	input.VisibleInterval.End = dataEnd
	input.ReplayInterval.End = dataEnd
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	mapping := result.Sources[0].DavisMapping
	if !mapping.Logical.F.Equal(f) || !mapping.Logical.T.Equal(dataEnd) ||
		!mapping.Physical.W.Equal(f.Add(-davisProblemsWarmup)) || !mapping.Physical.T.Equal(dataEnd) {
		t.Fatalf("terminal mapping = %#v", mapping)
	}
}

func TestDavisProblemsMappingChangesFWithoutChangingClampedW(t *testing.T) {
	timeT := mustTime(t, "2026-06-14T10:00:00Z")
	dataStart := mustTime(t, "2026-06-14T08:00:00Z")
	var results []CompileResult
	for _, f := range []time.Time{mustTime(t, "2026-06-14T09:00:00Z"), mustTime(t, "2026-06-14T09:30:00Z")} {
		ast, original := davisAbsoluteRange(t, f, timeT)
		input := davisMappingCompileInput(t, ast, original, DavisProblemsMappingExecution)
		input.ReplayInterval.Start = dataStart
		input.VisibleInterval.Start = dataStart
		input.DavisMapping.Coverage.OldestSnapshot = dataStart
		result, err := Compile(input)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, result)
	}
	left, right := results[0].Sources[0].DavisMapping, results[1].Sources[0].DavisMapping
	if left.Logical.F.Equal(right.Logical.F) || !left.Physical.W.Equal(dataStart) || !right.Physical.W.Equal(dataStart) ||
		results[0].EffectiveDQL == results[1].EffectiveDQL {
		t.Fatalf("logical and physical ranges were not independent: left=%#v right=%#v", left, right)
	}
}

func TestDavisProblemsMappingMultipleSourcesUseSharedCoverageAndDistinctRanges(t *testing.T) {
	ast := loadSDKFixture(t, "phase0/fixtures/10-nested-append/parse.json").Clone()
	for _, terminal := range terminalNodes(ast, "DATA_OBJECT") {
		terminal.Canonical = davisProblemsView
	}
	terminalNodes(ast, "NUMBER")[0].Canonical = "1"
	input := davisMappingCompileInput(t, ast, "fetch logs | append [ fetch spans, from:now()-1h ]", DavisProblemsMappingExecution)
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sources) != 2 || result.Sources[0].DavisMapping == nil || result.Sources[1].DavisMapping == nil {
		t.Fatalf("sources = %#v", result.Sources)
	}
	left, right := result.Sources[0].DavisMapping, result.Sources[1].DavisMapping
	if left.Logical.F.Equal(right.Logical.F) || left.Coverage != right.Coverage ||
		strings.Count(result.EffectiveDQL, davisProblemsSnapshotTable) != 2 {
		t.Fatalf("mapping ranges/coverage = left %#v right %#v effective %q", left, right, result.EffectiveDQL)
	}
	validation := ast.Clone()
	validationSources, err := analyzeSources(validation, Milestone1SourcePolicy(), DavisProblemsMappingPolicy{Mode: DavisProblemsMappingInspection})
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := len(validationSources) - 1; ordinal >= 0; ordinal-- {
		sequence := mappedValidationCommandSequence(t, *result.Sources[ordinal].DavisMapping)
		if !replaceNodeWithSequence(validation.Root, validationSources[ordinal].node, sequence) {
			t.Fatalf("could not replace mapped source %d in validation AST", ordinal)
		}
	}
	if _, err := Audit(AuditInput{
		ValidationAST: validation, Compilation: result, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	}); err != nil {
		t.Fatalf("multiple mapped sources audit: %v", err)
	}
}

func TestDavisProblemsMappingCoverageGateAcceptsEqualityAndRejectsLaterOldest(t *testing.T) {
	ast := loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json")
	input := davisMappingCompileInput(t, ast, davisMappingOriginal, DavisProblemsMappingExecution)
	input.DavisMapping.Coverage.OldestSnapshot = mustTime(t, "2026-06-14T03:00:00Z")
	if _, err := Compile(input); err != nil {
		t.Fatalf("oldest_snapshot == W rejected: %v", err)
	}
	input.DavisMapping.Coverage.OldestSnapshot = mustTime(t, "2026-06-14T03:00:00.000000001Z")
	result, err := Compile(input)
	var davisErr *DavisCurrentViewError
	if !errors.As(err, &davisErr) || davisErr.CoverageFailure != DavisCoverageInsufficient ||
		!strings.HasSuffix(err.Error(), DavisCoverageInsufficientMessage) {
		t.Fatalf("insufficient coverage error = %T %v", err, err)
	}
	if len(result.Sources) != 1 || result.Sources[0].DavisMapping == nil ||
		result.Sources[0].DavisMapping.Coverage.Verified || result.Sources[0].DavisMapping.Coverage.OldestSnapshot.IsZero() {
		t.Fatalf("failed coverage compilation = %#v", result.Sources)
	}
}

func TestDavisProblemsMappingAuditRejectsEveryTamperedMappingFact(t *testing.T) {
	compilation, err := Compile(davisMappingCompileInput(t,
		loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json"), davisMappingOriginal, DavisProblemsMappingExecution))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		tamper func(*AST)
	}{
		{"snapshot token", func(ast *AST) { firstTerminal(ast, "DATA_OBJECT").Canonical = "dt.davis.events.snapshots" }},
		{"source path", func(ast *AST) { firstSourceAnalysis(t, ast).node.Path += "/tampered" }},
		{"data-object path", func(ast *AST) { firstTerminal(ast, "DATA_OBJECT").Path += "/tampered" }},
		{"stage order", func(ast *AST) {
			commands, _, _ := directCommandSequence(ast.Root, firstSourceAnalysis(t, ast).node)
			terminalsWithRole(commands[1], "COMMAND_NAME")[0].Canonical, terminalsWithRole(commands[2], "COMMAND_NAME")[0].Canonical = "dedup", "sort"
		}},
		{"sort field", func(ast *AST) { firstCanonicalTerminal(ast, "SIMPLE_IDENTIFIER", "timestamp").Canonical = "start_time" }},
		{"sort direction", func(ast *AST) { firstTerminal(ast, "PARAMETER_MODIFIER").Canonical = "asc" }},
		{"dedup identity", func(ast *AST) { firstCanonicalTerminal(ast, "SIMPLE_IDENTIFIER", "event.id").Canonical = "event.kind" }},
		{"lifetime start field", func(ast *AST) {
			firstCanonicalTerminal(ast, "SIMPLE_IDENTIFIER", "event.start").Canonical = "timestamp"
		}},
		{"coalesce", func(ast *AST) { firstCanonicalTerminal(ast, "FUNCTION_NAME", "coalesce").Canonical = "max" }},
		{"upper operator", func(ast *AST) { firstCanonicalTerminal(ast, "OPERATOR", "<").Canonical = "<=" }},
		{"inclusive lower operator", func(ast *AST) { firstCanonicalTerminal(ast, "OPERATOR", ">=").Canonical = ">" }},
		{"physical W", func(ast *AST) { stringTerminals(ast)[0].Canonical = `"2026-06-14T03:00:00.000000001Z"` }},
		{"physical T", func(ast *AST) { stringTerminals(ast)[1].Canonical = `"2026-06-14T09:59:59.999999999Z"` }},
		{"logical upper T", func(ast *AST) { stringTerminals(ast)[2].Canonical = `"2026-06-14T09:59:59.999999999Z"` }},
		{"coalesce T", func(ast *AST) { stringTerminals(ast)[3].Canonical = `"2026-06-14T09:59:59.999999999Z"` }},
		{"logical F", func(ast *AST) { stringTerminals(ast)[4].Canonical = `"2026-06-14T09:00:00.000000001Z"` }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json").Clone()
			test.tamper(validation)
			_, err := Audit(AuditInput{ValidationAST: validation, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC})
			assertAuditError(t, err)
		})
	}
}

func TestDavisProblemsMappingAuditRejectsChangedDownstreamSemantics(t *testing.T) {
	compilation, err := Compile(davisMappingCompileInput(t,
		loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json"), davisMappingOriginal, DavisProblemsMappingExecution))
	if err != nil {
		t.Fatal(err)
	}
	validation := loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json").Clone()
	downstream := loadSDKFixture(t, "pipeline/contains/parse.json")
	commands, _, ok := directCommandSequence(downstream.Root, firstSourceAnalysis(t, downstream).node)
	if !ok || len(commands) < 2 {
		t.Fatal("downstream fixture lacks a pipeline command")
	}
	validation.Root.Children = append(validation.Root.Children, cloneNode(commands[len(commands)-1]))
	_, err = Audit(AuditInput{ValidationAST: validation, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC})
	assertAuditError(t, err)
}

func TestDavisProblemsMappingAuditPreservesUserPipelineAndSeparators(t *testing.T) {
	const original = `fetch logs, from:now()-5m, to:now() | filter contains(content, "error") | limit 1`
	originalAST := loadSDKFixture(t, "pipeline/contains/parse.json").Clone()
	firstTerminal(originalAST, "DATA_OBJECT").Canonical = davisProblemsView
	compilation, err := Compile(davisMappingCompileInput(t, originalAST, original, DavisProblemsMappingExecution))
	if err != nil {
		t.Fatal(err)
	}
	validation := mappedValidationWithUserPipeline(t, compilation)
	if _, err := Audit(AuditInput{
		ValidationAST: validation, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	}); err != nil {
		t.Fatalf("unchanged downstream pipeline audit: %v", err)
	}

	tampered := validation.Clone()
	firstCanonicalTerminal(tampered, "SIMPLE_IDENTIFIER", "content").Canonical = "severity"
	_, err = Audit(AuditInput{
		ValidationAST: tampered, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	assertAuditError(t, err)

	tampered = validation.Clone()
	for index := len(tampered.Root.Children) - 1; index >= 0; index-- {
		if tampered.Root.Children[index].Role != "COMMAND_SEPARATOR" {
			continue
		}
		tampered.Root.Children = append(tampered.Root.Children[:index], tampered.Root.Children[index+1:]...)
		break
	}
	_, err = Audit(AuditInput{
		ValidationAST: tampered, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	assertAuditError(t, err)
}

func mappedValidationWithUserPipeline(t *testing.T, compilation CompileResult) *AST {
	t.Helper()
	if len(compilation.Sources) != 1 || compilation.Sources[0].DavisMapping == nil {
		t.Fatalf("compilation lacks one mapping: %#v", compilation.Sources)
	}
	mapping := compilation.Sources[0].DavisMapping
	validation := loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json").Clone()
	values := stringTerminals(validation)
	want := []time.Time{mapping.Physical.W, mapping.Physical.T, mapping.Logical.T, mapping.Logical.T, mapping.Logical.F}
	if len(values) != len(want) {
		t.Fatalf("effective fixture string terminals = %d, want %d", len(values), len(want))
	}
	for index := range want {
		values[index].Canonical = fmt.Sprintf("%q", want[index].UTC().Format(generatedTimestampLayout))
	}
	pipeline := loadSDKFixture(t, "pipeline/contains/parse.json")
	for _, node := range pipeline.Root.Children[1:] {
		validation.Root.Children = append(validation.Root.Children, cloneNode(node))
	}
	return validation
}

func mappedValidationCommandSequence(t *testing.T, mapping DavisProblemsMappingCompilation) []*Node {
	t.Helper()
	template := loadPhase0BFixture(t, "davis/problems-view-mapping/effective/parse.json").Clone()
	values := stringTerminals(template)
	want := []time.Time{mapping.Physical.W, mapping.Physical.T, mapping.Logical.T, mapping.Logical.T, mapping.Logical.F}
	if len(values) != len(want) {
		t.Fatalf("effective fixture string terminals = %d, want %d", len(values), len(want))
	}
	for index := range want {
		values[index].Canonical = fmt.Sprintf("%q", want[index].UTC().Format(generatedTimestampLayout))
	}
	source := template.Root.Children[0]
	source.Path = mapping.Candidate.SourcePath
	firstTerminal(template, "DATA_OBJECT").Path = mapping.Candidate.DataObjectPath
	sequence := make([]*Node, len(template.Root.Children))
	for index, node := range template.Root.Children {
		sequence[index] = cloneNode(node)
	}
	return sequence
}

func replaceNodeWithSequence(root, target *Node, replacement []*Node) bool {
	if root == nil {
		return false
	}
	for index, child := range root.Children {
		if child == target {
			children := make([]*Node, 0, len(root.Children)-1+len(replacement))
			children = append(children, root.Children[:index]...)
			children = append(children, replacement...)
			children = append(children, root.Children[index+1:]...)
			root.Children = children
			return true
		}
		if replaceNodeWithSequence(child, target, replacement) {
			return true
		}
	}
	for _, kind := range sortedAlternativeKinds(root.Alternatives) {
		if replaceNodeWithSequence(root.Alternatives[kind], target, replacement) {
			return true
		}
	}
	return false
}

func davisAbsoluteRange(t *testing.T, f, timeT time.Time) (*AST, string) {
	t.Helper()
	ast := loadPhase0BFixture(t, "davis/problems-view-mapping/original/parse.json").Clone()
	values := stringTerminals(ast)
	if len(values) != 2 {
		t.Fatalf("original mapping fixture STRING terminals = %d", len(values))
	}
	values[0].Canonical = fmt.Sprintf("%q", f.UTC().Format(time.RFC3339Nano))
	values[1].Canonical = fmt.Sprintf("%q", timeT.UTC().Format(time.RFC3339Nano))
	return ast, davisMappingOriginal
}

func stringTerminals(ast *AST) []*Node {
	return terminalNodes(ast, "STRING")
}

func firstCanonicalTerminal(ast *AST, role, canonical string) *Node {
	for _, terminal := range terminalNodes(ast, role) {
		if terminal.Canonical == canonical {
			return terminal
		}
	}
	panic("test fixture lacks terminal " + role + "=" + canonical)
}

func replaceTestSourceToken(ast *AST, oldToken, newToken string) {
	target := firstCanonicalTerminal(ast, "DATA_OBJECT", oldToken)
	if target.Span == nil {
		panic("test fixture source token has no span")
	}
	oldSpan := *target.Span
	delta := utf16TestLength(newToken) - utf16TestLength(oldToken)
	_ = ast.Walk(func(node *Node) error {
		if node.Span == nil {
			return nil
		}
		if node.Span.Start.Index > oldSpan.End.Index {
			node.Span.Start.Index += delta
			if node.Span.Start.Line == oldSpan.Start.Line {
				node.Span.Start.Column += delta
			}
		}
		if node.Span.End.Index >= oldSpan.End.Index {
			node.Span.End.Index += delta
			if node.Span.End.Line == oldSpan.End.Line {
				node.Span.End.Column += delta
			}
		}
		return nil
	})
	target.Canonical = newToken
}

func davisMappingCompileInput(t *testing.T, ast *AST, original string, mode DavisProblemsMappingMode) CompileInput {
	t.Helper()
	input := fixedCompileInput(ast, original)
	input.VirtualStart = mustTime(t, "2026-06-14T08:00:00Z")
	input.ReplayInterval.Start = mustTime(t, "2026-06-14T02:00:00Z")
	input.VisibleInterval.Start = input.ReplayInterval.Start
	input.DavisMapping = DavisProblemsMappingPolicy{Mode: mode}
	if mode == DavisProblemsMappingExecution {
		input.DavisMapping.Coverage = &DavisSnapshotCoverage{
			OldestSnapshot: mustTime(t, "2026-06-14T02:00:00Z"),
			ObservedAt:     mustTime(t, "2026-08-14T10:00:00Z"),
		}
	}
	return input
}
