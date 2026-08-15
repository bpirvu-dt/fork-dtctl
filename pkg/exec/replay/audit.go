package replay

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AuditPlan is compiled from the immutable original AST and consumed only by
// the validation-AST audit. It contains no raw SDK nodes.
type AuditPlan struct {
	UntouchedSemanticFingerprint string
	DavisProblemsMappings        []DavisProblemsMappingExpectation
}

// AuditInput carries the fresh validation AST supplied by the caller. Audit
// never invokes query:parse itself.
type AuditInput struct {
	ValidationAST *AST
	Compilation   CompileResult
	SourcePolicy  SourcePolicy
	Timezone      *time.Location
}

// AuditResult records the fail-closed pre-execution proofs.
type AuditResult struct {
	OK                   bool
	SourceCount          int
	NoSemanticNow        bool
	AllSourcesBounded    bool
	StructureMatches     bool
	DavisMappingsAudited bool
	Rules                []string
}

// Audit validates the actual effective-query AST against the compile result.
func Audit(input AuditInput) (AuditResult, error) {
	if !input.Compilation.AuditRequired || input.Compilation.EffectiveDQL == "" {
		return AuditResult{}, auditError(nil, "the compilation is not ready for validation")
	}
	if input.ValidationAST == nil || input.ValidationAST.Root == nil {
		return AuditResult{}, auditError(nil, "the validation AST is missing")
	}
	if len(input.SourcePolicy.RecordTables) == 0 {
		return AuditResult{}, auditError(nil, "the validation source policy is empty")
	}
	policy := cloneSourcePolicy(input.SourcePolicy)
	validation := input.ValidationAST.Clone()
	sources, err := analyzeSources(validation, policy, DavisProblemsMappingPolicy{})
	if err != nil {
		return AuditResult{}, err
	}
	result := AuditResult{SourceCount: len(sources)}
	if err := auditNoSemanticNow(validation); err != nil {
		return result, err
	}
	result.NoSemanticNow = true
	clock := input.Compilation.Explain.Clock
	zone := input.Timezone
	if zone == nil {
		zone = time.UTC
	}
	context := timeframeContext{
		VirtualNow: clock.VirtualNow, ReplayInterval: clock.ReplayInterval,
		VisibleInterval: clock.VisibleInterval, Timezone: zone,
		GlobalDefault: cloneInterval(clock.GlobalDefault), DefaultLookback: policy.DefaultLookback,
	}
	generatedCommands, err := auditSources(validation, sources, input.Compilation, context)
	if err != nil {
		return result, err
	}
	result.AllSourcesBounded = true
	result.DavisMappingsAudited = len(input.Compilation.AuditPlan.DavisProblemsMappings) > 0
	fingerprint, err := semanticFingerprintWithMappings(validation, sources, clock.VirtualNow, input.Compilation.AuditPlan.DavisProblemsMappings, generatedCommands)
	if err != nil {
		return result, err
	}
	if fingerprint != input.Compilation.AuditPlan.UntouchedSemanticFingerprint {
		return result, auditError(validation.Root, "the validation AST changed semantics outside the approved time edits")
	}
	result.StructureMatches = true
	result.OK = true
	result.Rules = []string{
		"no semantic real-time now", "every source known and bounded", "no shift or forbidden construct",
		"every source boundary matches the compiled session-derived range", "every source overlaps",
		"untouched executable semantics match the original AST",
	}
	if result.DavisMappingsAudited {
		result.Rules = append(result.Rules,
			"every Davis problems mapping has exact snapshot and lifetime ranges",
			"every Davis problems mapping has exact ordered reconstruction stages",
		)
	}
	return result, nil
}

func auditNoSemanticNow(ast *AST) error {
	return ast.WalkExecutable(func(node *Node) error {
		if node.Kind == NodeContainer && node.Role == "FUNCTION" && strings.EqualFold(ownFunctionName(node), "now") {
			return auditError(node, "a semantic real-time now() remains in the validation AST")
		}
		return nil
	})
}

func auditSources(ast *AST, actual []*sourceAnalysis, compilation CompileResult, context timeframeContext) (map[*Node]struct{}, error) {
	if len(actual) != len(compilation.Sources) {
		return nil, auditError(nil, fmt.Sprintf("the source count changed from %d to %d", len(compilation.Sources), len(actual)))
	}
	mappings := make(map[int]DavisProblemsMappingExpectation, len(compilation.AuditPlan.DavisProblemsMappings))
	for _, expectation := range compilation.AuditPlan.DavisProblemsMappings {
		if _, duplicate := mappings[expectation.SourceOrdinal]; duplicate {
			return nil, auditError(nil, fmt.Sprintf("mapped source %d has duplicate audit expectations", expectation.SourceOrdinal))
		}
		mappings[expectation.SourceOrdinal] = expectation
		if compilation.InspectionOnly == expectation.Coverage.Verified {
			return nil, auditError(nil, fmt.Sprintf("mapped source %d has a coverage marker inconsistent with its operation mode", expectation.SourceOrdinal))
		}
	}
	contracts := make(map[int]ReplayResultContract, len(compilation.ResultContracts))
	for _, contract := range compilation.ResultContracts {
		if _, duplicate := contracts[contract.Source.Ordinal]; duplicate {
			return nil, auditError(nil, fmt.Sprintf("metric source %d has duplicate result contracts", contract.Source.Ordinal))
		}
		contracts[contract.Source.Ordinal] = contract
	}
	metricSources := 0
	generatedCommands := make(map[*Node]struct{})
	mappedSources := 0
	for index, source := range actual {
		expected := compilation.Sources[index]
		if mapping, mapped := mappings[index]; mapped {
			commands, err := auditMappedDavisProblemsSource(ast, source, expected, mapping, context)
			if err != nil {
				return nil, err
			}
			for _, command := range commands {
				generatedCommands[command] = struct{}{}
			}
			mappedSources++
			continue
		}
		if expected.DavisMapping != nil || expected.Source.DavisProblems != nil {
			return nil, auditError(source.node, fmt.Sprintf("mapped source %d has no exact audit expectation", index))
		}
		if source.Ordinal != expected.Source.Ordinal || !sameSourceContract(source.SourceDescriptor, expected.Source) {
			return nil, auditError(source.node, fmt.Sprintf("source %d changed identity or source contract", index))
		}
		if source.Class == SourceSynthetic {
			if expected.Requested != nil || expected.Effective != nil {
				return nil, auditError(source.node, fmt.Sprintf("synthetic source %d has a telemetry range", index))
			}
			continue
		}
		requested, err := resolveRequestedRange(source, context)
		if err != nil {
			return nil, err
		}
		if requested.From.Dependency != EndpointAbsolute || requested.To.Dependency != EndpointAbsolute {
			return nil, auditError(source.node, fmt.Sprintf("source %d retains an implicit or relative real-time boundary", index))
		}
		if expected.Effective == nil || requested.Range != *expected.Effective {
			return nil, auditError(source.node, fmt.Sprintf("source %d boundaries do not match the compiled session-derived range", index))
		}
		intersection, overlaps := requested.Range.Intersect(context.VisibleInterval)
		if !overlaps || intersection != requested.Range {
			return nil, auditError(source.node, fmt.Sprintf("source %d is empty or outside the visible replay interval", index))
		}
		switch source.Class {
		case SourceRecord:
			if source.BoundaryPolicy != BoundaryExact || expected.PhysicalRange == nil || *expected.PhysicalRange != requested.Range {
				return nil, auditError(source.node, fmt.Sprintf("record source %d does not have exact physical boundaries", index))
			}
		case SourceMetric:
			metricSources++
			contract, ok := contracts[index]
			if !ok || contract.Source.Ordinal != index || contract.Source.Name != source.Name || contract.Source.Shape != source.Metric.Shape ||
				contract.BoundaryPolicy != BoundaryMetricBucket || contract.LogicalWindow != requested.Range || !contract.NaturalIntervalRequired {
				return nil, auditError(source.node, fmt.Sprintf("metric source %d lacks a complete post-execution contract", index))
			}
		default:
			return nil, auditError(source.node, fmt.Sprintf("source %d has an unknown class", index))
		}
	}
	if metricSources != len(contracts) {
		return nil, auditError(nil, "the number of metric result contracts does not match the validation sources")
	}
	if mappedSources != len(mappings) {
		return nil, auditError(nil, "the number of mapped Davis problems sources does not match the audit expectations")
	}
	return generatedCommands, nil
}

func auditMappedDavisProblemsSource(ast *AST, actual *sourceAnalysis, expected SourceCompilation, mapping DavisProblemsMappingExpectation, context timeframeContext) ([]*Node, error) {
	if actual.Ordinal != mapping.SourceOrdinal || expected.Source.Ordinal != mapping.SourceOrdinal || expected.DavisMapping == nil || expected.Source.DavisProblems == nil {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d lost its typed compiler expectation", mapping.SourceOrdinal))
	}
	if expected.Source.Class != SourceDavisProblemsView || expected.Source.Name != davisProblemsView ||
		actual.Class != SourceRecord || actual.Name != davisProblemsSnapshotTable || actual.RecordTimeField != "timestamp" ||
		actual.BoundaryPolicy != BoundaryExact || actual.dataObject == nil || actual.dataObject.Canonical != davisProblemsSnapshotTable {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d did not change exactly to the Davis problems snapshot source", mapping.SourceOrdinal))
	}
	if actual.node.Path != mapping.Candidate.SourcePath || actual.dataObject.Path != mapping.Candidate.DataObjectPath {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d changed its expected AST path", mapping.SourceOrdinal))
	}
	if !sameDavisCandidate(mapping.Candidate, expected.DavisMapping.Candidate) ||
		!sameDavisCandidate(mapping.Candidate, *expected.Source.DavisProblems) ||
		mapping.Logical != expected.DavisMapping.Logical || mapping.Physical != expected.DavisMapping.Physical ||
		mapping.Coverage != expected.DavisMapping.Coverage {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d compiler facts changed before audit", mapping.SourceOrdinal))
	}
	logical := mapping.Logical.interval()
	physical := mapping.Physical.interval()
	if !logical.Valid() || !physical.Valid() || !mapping.Logical.T.Equal(mapping.Physical.T) ||
		expected.Effective == nil || *expected.Effective != logical || expected.PhysicalRange == nil || *expected.PhysicalRange != physical {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d conflated its logical and physical ranges", mapping.SourceOrdinal))
	}
	requested, err := resolveRequestedRange(actual, context)
	if err != nil {
		return nil, err
	}
	if requested.From.Dependency != EndpointAbsolute || requested.To.Dependency != EndpointAbsolute || requested.Range != physical {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d snapshot fetch does not use exact [W,T) bounds", mapping.SourceOrdinal))
	}
	intersection, overlaps := physical.Intersect(context.VisibleInterval)
	if !overlaps || intersection != physical {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d snapshot range is outside the visible replay interval", mapping.SourceOrdinal))
	}
	if mapping.Coverage.Verified {
		if mapping.Coverage.OldestSnapshot.IsZero() || mapping.Coverage.ObservedAt.IsZero() || mapping.Coverage.OldestSnapshot.After(mapping.Physical.W) {
			return nil, auditError(actual.node, fmt.Sprintf("mapped source %d lacks successful snapshot coverage through W", mapping.SourceOrdinal))
		}
	} else if !mapping.Coverage.OldestSnapshot.IsZero() || !mapping.Coverage.ObservedAt.IsZero() || !compilationIsInspection(expected) {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d has an invalid coverage-unverified inspection marker", mapping.SourceOrdinal))
	}
	commands, index, ok := directCommandSequence(ast.Root, actual.node)
	if !ok || index+3 >= len(commands) {
		return nil, auditError(actual.node, fmt.Sprintf("mapped source %d is missing its immediate reconstruction stages", mapping.SourceOrdinal))
	}
	generated := commands[index+1 : index+4]
	timestampT := strconv.Quote(mapping.Logical.T.UTC().Format(generatedTimestampLayout))
	timestampF := strconv.Quote(mapping.Logical.F.UTC().Format(generatedTimestampLayout))
	if err := auditGeneratedCommand(generated[0], "sort", "expression", []auditTerminal{
		{"COMMAND_NAME", "sort"}, {"SIMPLE_IDENTIFIER", "timestamp"}, {"PARAMETER_MODIFIER", "desc"},
	}); err != nil {
		return nil, err
	}
	if err := auditGeneratedCommand(generated[1], "dedup", "expression", []auditTerminal{
		{"COMMAND_NAME", "dedup"}, {"SIMPLE_IDENTIFIER", "event.id"},
	}); err != nil {
		return nil, err
	}
	if err := auditGeneratedCommand(generated[2], "filter", "condition", []auditTerminal{
		{"COMMAND_NAME", "filter"}, {"SIMPLE_IDENTIFIER", "event.start"}, {"OPERATOR", "<"},
		{"FUNCTION_NAME", "toTimestamp"}, {"STRING", timestampT}, {"OPERATOR", "AND"},
		{"FUNCTION_NAME", "coalesce"}, {"SIMPLE_IDENTIFIER", "event.end"},
		{"FUNCTION_NAME", "toTimestamp"}, {"STRING", timestampT}, {"OPERATOR", ">="},
		{"FUNCTION_NAME", "toTimestamp"}, {"STRING", timestampF},
	}); err != nil {
		return nil, err
	}
	return generated, nil
}

func compilationIsInspection(source SourceCompilation) bool {
	return source.DavisMapping != nil && !source.DavisMapping.Coverage.Verified
}

func sameDavisCandidate(left, right DavisProblemsMappingCandidate) bool {
	return left.OriginalToken == right.OriginalToken && left.SnapshotToken == right.SnapshotToken &&
		left.DataObjectPath == right.DataObjectPath && left.SourcePath == right.SourcePath
}

type auditTerminal struct {
	Role      string
	Canonical string
}

func auditGeneratedCommand(command *Node, name, parameter string, expected []auditTerminal) error {
	if command == nil || !strings.EqualFold(ownCommandName(command), name) {
		return auditError(command, fmt.Sprintf("the generated %s stage is missing or reordered", name))
	}
	parameters, err := collectDirectParameters(command)
	if err != nil {
		return err
	}
	if len(parameters) != 1 || parameters[0].key != parameter || parameters[0].keyOrigin != "info" {
		return auditError(command, fmt.Sprintf("the generated %s stage has an unexpected parameter shape", name))
	}
	var actual []auditTerminal
	if err := walkOwned(command, func(node *Node) error {
		if node.Kind != NodeTerminal || insignificantTerminal(node) || node.Role == "PARAMETER_KEY" {
			return nil
		}
		actual = append(actual, auditTerminal{Role: node.Role, Canonical: node.Canonical})
		return nil
	}); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return auditError(command, fmt.Sprintf("the generated %s stage has unexpected executable tokens", name))
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return auditError(command, fmt.Sprintf("the generated %s stage changed token %d", name, index))
		}
	}
	return nil
}

func directCommandSequence(root, target *Node) ([]*Node, int, bool) {
	if root == nil {
		return nil, 0, false
	}
	var commands []*Node
	targetIndex := -1
	for _, child := range root.Children {
		if child.Kind == NodeContainer && child.Role == "COMMAND" {
			if child == target {
				targetIndex = len(commands)
			}
			commands = append(commands, child)
		}
	}
	if targetIndex >= 0 {
		return commands, targetIndex, true
	}
	for _, child := range root.Children {
		if commands, index, ok := directCommandSequence(child, target); ok {
			return commands, index, true
		}
	}
	for _, kind := range sortedAlternativeKinds(root.Alternatives) {
		if kind == AlternativeInfo {
			continue
		}
		if commands, index, ok := directCommandSequence(root.Alternatives[kind], target); ok {
			return commands, index, true
		}
	}
	return nil, 0, false
}

func sameSourceContract(left, right SourceDescriptor) bool {
	if left.Ordinal != right.Ordinal || left.Class != right.Class || left.Name != right.Name ||
		left.RecordTimeField != right.RecordTimeField || left.BoundaryPolicy != right.BoundaryPolicy {
		return false
	}
	if (left.DavisProblems == nil) != (right.DavisProblems == nil) {
		return false
	}
	if left.DavisProblems != nil && !sameDavisCandidate(*left.DavisProblems, *right.DavisProblems) {
		return false
	}
	if left.Metric == nil || right.Metric == nil {
		return left.Metric == nil && right.Metric == nil
	}
	return left.Metric.Shape == right.Metric.Shape && left.Metric.AutomaticInterval == right.Metric.AutomaticInterval &&
		equalDuration(left.Metric.DeclaredInterval, right.Metric.DeclaredInterval) &&
		equalStrings(left.Metric.Aggregations, right.Metric.Aggregations) && equalStrings(left.Metric.MetricKeys, right.Metric.MetricKeys)
}

func equalDuration(left, right *time.Duration) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func semanticFingerprintWithMappings(ast *AST, sources []*sourceAnalysis, virtualNow time.Time, mappings []DavisProblemsMappingExpectation, generatedCommands map[*Node]struct{}) (string, error) {
	skip := make(map[*Node]struct{})
	generated := generatedFingerprintNodes(ast.Root, generatedCommands)
	normalizedMappings := make(map[*Node]int, len(mappings))
	for _, source := range sources {
		for _, parameter := range source.parameters {
			switch parameter.key {
			case "from", "to", "timeframe":
				skip[parameter.node] = struct{}{}
			}
		}
	}
	for _, mapping := range mappings {
		if mapping.SourceOrdinal < 0 || mapping.SourceOrdinal >= len(sources) || sources[mapping.SourceOrdinal].dataObject == nil {
			return "", auditError(nil, fmt.Sprintf("mapped source %d has no fingerprint data-object token", mapping.SourceOrdinal))
		}
		normalizedMappings[sources[mapping.SourceOrdinal].dataObject] = mapping.SourceOrdinal
	}
	var tokens []string
	var walk func(*Node, AlternativeKind) error
	walk = func(node *Node, alternative AlternativeKind) error {
		if _, ok := generated[node]; ok {
			return nil
		}
		if _, ok := skip[node]; ok || alternative == AlternativeInfo {
			return nil
		}
		if ordinal, ok := normalizedMappings[node]; ok {
			tokens = append(tokens, "MAPPED_DAVIS_PROBLEMS_SOURCE:"+strconv.Itoa(ordinal))
			return nil
		}
		if isNormalizedVirtualNow(node, virtualNow) {
			tokens = append(tokens, "VIRTUAL_NOW")
			return nil
		}
		if node.Role == "PARAMETER_SEPARATOR" || insignificantTerminal(node) {
			return nil
		}
		tokens = append(tokens, fingerprintToken(node))
		for _, child := range node.Children {
			if err := walk(child, ""); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind == AlternativeInfo {
				continue
			}
			tokens = append(tokens, "ALT:"+string(kind))
			if err := walk(node.Alternatives[kind], kind); err != nil {
				return err
			}
		}
		tokens = append(tokens, "END:"+string(node.Kind)+":"+node.Role)
		return nil
	}
	if err := walk(ast.Root, ""); err != nil {
		return "", err
	}
	return strings.Join(tokens, "\x1f"), nil
}

func generatedFingerprintNodes(root *Node, commands map[*Node]struct{}) map[*Node]struct{} {
	result := make(map[*Node]struct{}, len(commands)*2)
	for command := range commands {
		result[command] = struct{}{}
	}
	var visit func(*Node)
	visit = func(node *Node) {
		if node == nil {
			return
		}
		for index, child := range node.Children {
			if _, generated := commands[child]; generated {
				previous := index - 1
				for previous >= 0 && insignificantTerminal(node.Children[previous]) {
					previous--
				}
				if previous >= 0 && node.Children[previous].Role == "COMMAND_SEPARATOR" {
					result[node.Children[previous]] = struct{}{}
				}
			}
			visit(child)
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			visit(node.Alternatives[kind])
		}
	}
	visit(root)
	return result
}

func isNormalizedVirtualNow(node *Node, virtualNow time.Time) bool {
	if node.Kind != NodeContainer || node.Role != "FUNCTION" {
		return false
	}
	switch strings.ToLower(ownFunctionName(node)) {
	case "now":
		return true
	case "totimestamp":
		parameters, err := collectDirectParameters(node)
		if err != nil || len(parameters) != 1 || parameters[0].key != "value" {
			return false
		}
		value, err := parameterValue(parameters[0].node)
		if err != nil || value.Kind != NodeTerminal || value.Role != "STRING" {
			return false
		}
		literal, err := strconv.Unquote(value.Canonical)
		if err != nil {
			return false
		}
		parsed, err := time.Parse(time.RFC3339Nano, literal)
		return err == nil && parsed.Equal(virtualNow)
	default:
		return false
	}
}

func insignificantTerminal(node *Node) bool {
	if node.Kind != NodeTerminal {
		return false
	}
	switch node.Role {
	case "SPACE", "INDENT", "LINEBREAK", "COLON", "COMMA", "PARENTHESIS_OPEN", "PARENTHESIS_CLOSE",
		"BRACE_OPEN", "BRACE_CLOSE", "BRACKET_OPEN", "BRACKET_CLOSE":
		return true
	default:
		return false
	}
}

func fingerprintToken(node *Node) string {
	value := string(node.Kind) + ":" + node.Role + ":" + node.Canonical
	return strconv.Itoa(len(value)) + ":" + value
}

func auditError(node *Node, reason string) *ReplayError {
	return replayError(ErrorAudit, node, "validation AST", "The effective-query audit failed: "+reason+".", "Do not execute the query; reparse the actual effective DQL and report the compatibility failure.")
}
