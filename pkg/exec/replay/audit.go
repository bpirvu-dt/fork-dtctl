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
	OK                bool
	SourceCount       int
	NoSemanticNow     bool
	AllSourcesBounded bool
	StructureMatches  bool
	Rules             []string
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
	sources, err := analyzeSources(validation, policy)
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
	if err := auditSources(sources, input.Compilation, context); err != nil {
		return result, err
	}
	result.AllSourcesBounded = true
	fingerprint, err := semanticFingerprint(validation, sources, clock.VirtualNow)
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

func auditSources(actual []*sourceAnalysis, compilation CompileResult, context timeframeContext) error {
	if len(actual) != len(compilation.Sources) {
		return auditError(nil, fmt.Sprintf("the source count changed from %d to %d", len(compilation.Sources), len(actual)))
	}
	contracts := make(map[int]ReplayResultContract, len(compilation.ResultContracts))
	for _, contract := range compilation.ResultContracts {
		if _, duplicate := contracts[contract.Source.Ordinal]; duplicate {
			return auditError(nil, fmt.Sprintf("metric source %d has duplicate result contracts", contract.Source.Ordinal))
		}
		contracts[contract.Source.Ordinal] = contract
	}
	metricSources := 0
	for index, source := range actual {
		expected := compilation.Sources[index]
		if source.Ordinal != expected.Source.Ordinal || !sameSourceContract(source.SourceDescriptor, expected.Source) {
			return auditError(source.node, fmt.Sprintf("source %d changed identity or source contract", index))
		}
		if source.Class == SourceSynthetic {
			if expected.Requested != nil || expected.Effective != nil {
				return auditError(source.node, fmt.Sprintf("synthetic source %d has a telemetry range", index))
			}
			continue
		}
		requested, err := resolveRequestedRange(source, context)
		if err != nil {
			return err
		}
		if requested.From.Dependency != EndpointAbsolute || requested.To.Dependency != EndpointAbsolute {
			return auditError(source.node, fmt.Sprintf("source %d retains an implicit or relative real-time boundary", index))
		}
		if expected.Effective == nil || requested.Range != *expected.Effective {
			return auditError(source.node, fmt.Sprintf("source %d boundaries do not match the compiled session-derived range", index))
		}
		intersection, overlaps := requested.Range.Intersect(context.VisibleInterval)
		if !overlaps || intersection != requested.Range {
			return auditError(source.node, fmt.Sprintf("source %d is empty or outside the visible replay interval", index))
		}
		switch source.Class {
		case SourceRecord:
			if source.BoundaryPolicy != BoundaryExact || expected.PhysicalRange == nil || *expected.PhysicalRange != requested.Range {
				return auditError(source.node, fmt.Sprintf("record source %d does not have exact physical boundaries", index))
			}
		case SourceMetric:
			metricSources++
			contract, ok := contracts[index]
			if !ok || contract.Source.Ordinal != index || contract.Source.Name != source.Name || contract.Source.Shape != source.Metric.Shape ||
				contract.BoundaryPolicy != BoundaryMetricBucket || contract.LogicalWindow != requested.Range || !contract.NaturalIntervalRequired {
				return auditError(source.node, fmt.Sprintf("metric source %d lacks a complete post-execution contract", index))
			}
		default:
			return auditError(source.node, fmt.Sprintf("source %d has an unknown class", index))
		}
	}
	if metricSources != len(contracts) {
		return auditError(nil, "the number of metric result contracts does not match the validation sources")
	}
	return nil
}

func sameSourceContract(left, right SourceDescriptor) bool {
	if left.Ordinal != right.Ordinal || left.Class != right.Class || left.Name != right.Name ||
		left.RecordTimeField != right.RecordTimeField || left.BoundaryPolicy != right.BoundaryPolicy {
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

func semanticFingerprint(ast *AST, sources []*sourceAnalysis, virtualNow time.Time) (string, error) {
	skip := make(map[*Node]struct{})
	for _, source := range sources {
		for _, parameter := range source.parameters {
			switch parameter.key {
			case "from", "to", "timeframe":
				skip[parameter.node] = struct{}{}
			}
		}
	}
	var tokens []string
	var walk func(*Node, AlternativeKind) error
	walk = func(node *Node, alternative AlternativeKind) error {
		if _, ok := skip[node]; ok || alternative == AlternativeInfo {
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
