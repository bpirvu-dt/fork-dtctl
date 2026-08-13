package replay

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func collectCommands(ast *AST) ([]commandView, error) {
	var commands []commandView
	err := ast.WalkExecutable(func(node *Node) error {
		if node.Kind != NodeContainer || node.Role != "COMMAND" {
			return nil
		}
		name := strings.ToLower(ownCommandName(node))
		if name == "" {
			return replayError(ErrorASTContract, node, "COMMAND", "A command has no unambiguous command name.", "Update dtctl if the server AST contract changed.")
		}
		commands = append(commands, commandView{node: node, name: name})
		return nil
	})
	return commands, err
}

func ownCommandName(command *Node) string {
	var names []string
	_ = walkOwned(command, func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == "COMMAND_NAME" {
			names = append(names, node.Canonical)
		}
		return nil
	})
	if len(names) != 1 {
		return ""
	}
	return names[0]
}

func ownFunctionName(function *Node) string {
	var names []string
	_ = walkOwned(function, func(node *Node) error {
		if node != function && node.Role == "FUNCTION" {
			return errStopBranch
		}
		if node.Kind == NodeTerminal && (node.Role == "FUNCTION_NAME" || node.Role == "TIMESERIES_AGGREGATION") {
			names = append(names, node.Canonical)
		}
		return nil
	})
	if len(names) != 1 {
		return ""
	}
	return names[0]
}

var errStopBranch = fmt.Errorf("stop branch")

func walkOwned(root *Node, visit func(*Node) error) error {
	var walk func(*Node, bool) error
	walk = func(node *Node, isRoot bool) error {
		if !isRoot && (node.Role == "COMMAND" || node.Role == "EXECUTION_BLOCK") {
			return nil
		}
		if err := visit(node); err != nil {
			if err == errStopBranch {
				return nil
			}
			return err
		}
		for _, child := range node.Children {
			if err := walk(child, false); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind == AlternativeInfo {
				continue
			}
			if err := walk(node.Alternatives[kind], false); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, true)
}

func collectDirectParameters(root *Node) ([]parameterView, error) {
	var out []parameterView
	var walk func(*Node, bool) error
	walk = func(node *Node, isRoot bool) error {
		if !isRoot && (node.Role == "COMMAND" || node.Role == "EXECUTION_BLOCK") {
			return nil
		}
		if node.Role == "PARAMETER_WITH_KEY" {
			parameter, err := classifyParameter(node)
			if err != nil {
				return err
			}
			out = append(out, parameter)
			return nil
		}
		for _, child := range node.Children {
			if err := walk(child, false); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind == AlternativeInfo {
				continue
			}
			if err := walk(node.Alternatives[kind], false); err != nil {
				return err
			}
		}
		return nil
	}
	return out, walk(root, true)
}

func classifyParameter(node *Node) (parameterView, error) {
	var userKeys, infoKeys []string
	var walk func(*Node, bool, bool)
	walk = func(candidate *Node, isRoot, inInfo bool) {
		if !isRoot && candidate.Role == "PARAMETER_WITH_KEY" {
			return
		}
		if candidate.Kind != NodeTerminal || candidate.Role != "PARAMETER_KEY" {
			for _, child := range candidate.Children {
				walk(child, false, inInfo)
			}
			for _, kind := range sortedAlternativeKinds(candidate.Alternatives) {
				walk(candidate.Alternatives[kind], false, inInfo || kind == AlternativeInfo)
			}
			return
		}
		if !inInfo && candidate.Span != nil {
			userKeys = append(userKeys, strings.ToLower(candidate.Canonical))
		} else if inInfo {
			infoKeys = append(infoKeys, strings.ToLower(candidate.Canonical))
		}
	}
	walk(node, true, false)
	if len(userKeys) == 1 {
		return parameterView{node: node, key: userKeys[0], keyOrigin: "user"}, nil
	}
	if len(userKeys) > 1 {
		return parameterView{}, replayError(ErrorASTContract, node, "parameter", "A parameter has multiple positioned keys.", "Update dtctl if the server AST contract changed.")
	}
	infoKeys = uniqueStrings(infoKeys)
	if len(infoKeys) == 1 {
		return parameterView{node: node, key: infoKeys[0], keyOrigin: "info"}, nil
	}
	return parameterView{}, replayError(ErrorASTContract, node, "parameter", "A positional parameter has no unambiguous grammar key.", "Update dtctl if the server AST contract changed.")
}

func ownedExecutionBlocks(command *Node) int {
	count := 0
	var walk func(*Node, bool)
	walk = func(node *Node, isRoot bool) {
		if !isRoot && node.Role == "COMMAND" {
			return
		}
		if node.Role == "EXECUTION_BLOCK" {
			count++
			return
		}
		for _, child := range node.Children {
			walk(child, false)
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind != AlternativeInfo {
				walk(node.Alternatives[kind], false)
			}
		}
	}
	walk(command, true)
	return count
}

func validateExecutionBlockOwners(node *Node, owner string) error {
	if node.Role == "COMMAND" {
		owner = strings.ToLower(ownCommandName(node))
	}
	if node.Role == "EXECUTION_BLOCK" && owner != "append" && owner != "join" && owner != "lookup" {
		return replayError(ErrorUnsupportedForm, node, "execution block", "A nested execution block is not owned by an approved source-bearing command.", "Use the tested append, join, or lookup form.")
	}
	for _, child := range node.Children {
		if err := validateExecutionBlockOwners(child, owner); err != nil {
			return err
		}
	}
	for _, kind := range sortedAlternativeKinds(node.Alternatives) {
		if kind == AlternativeInfo {
			continue
		}
		if err := validateExecutionBlockOwners(node.Alternatives[kind], owner); err != nil {
			return err
		}
	}
	return nil
}

func validateSemanticPlacements(ast *AST) error {
	allowedParents := map[string]map[string]struct{}{
		"COMMAND":                {"QUERY": {}, "EXECUTION_BLOCK": {}},
		"COMMAND_NAME":           {"COMMAND": {}},
		"DATA_OBJECT":            {"PARAMETER_WITH_KEY": {}},
		"FUNCTION":               {"EXPRESSION": {}, "PARAMETER_ASSIGNMENT": {}, "PARAMETER_WITH_KEY": {}},
		"FUNCTION_NAME":          {"FUNCTION": {}},
		"TIMESERIES_AGGREGATION": {"FUNCTION": {}},
		"METRIC_KEY":             {"PARAMETER_WITH_KEY": {}},
		"PARSE_PATTERN":          {"PARAMETER_WITH_KEY": {}},
		"PARAMETER_WITH_KEY":     {"FUNCTION": {}, "PARAMETERS": {}},
		"PARAMETER_KEY":          {"PARAMETER_NAMING": {}},
		"EXECUTION_BLOCK":        {"PARAMETER_WITH_KEY": {}},
		"DURATION":               {"EXPRESSION": {}, "PARAMETER_WITH_KEY": {}},
		"CALENDAR_DURATION":      {"EXPRESSION": {}},
	}
	var walk func(*Node, *Node, string, bool) error
	walk = func(node, parent *Node, command string, executable bool) error {
		if parent != nil {
			if parents, classified := allowedParents[node.Role]; classified {
				if _, ok := parents[parent.Role]; !ok {
					return replayError(ErrorASTContract, node, node.Role,
						fmt.Sprintf("The known AST role %s appears under unexpected parent %s at %s.", node.Role, parent.Role, node.Path),
						"Update dtctl if the server AST placement contract changed.")
				}
			}
		}
		if executable && node.Role == "COMMAND" {
			command = strings.ToLower(ownCommandName(node))
		}
		if executable {
			switch node.Role {
			case "DATA_OBJECT":
				if command != "fetch" && command != "describe" && command != "fieldssnapshot" && command != "load" {
					return replayError(ErrorASTContract, node, node.Role, "A data object appears outside an observed source-owning command.", "Remove the unclassified source-selection form.")
				}
			case "METRIC_KEY", "TIMESERIES_AGGREGATION":
				if command != "timeseries" {
					return replayError(ErrorASTContract, node, node.Role, "A metric source token appears outside a timeseries command.", "Remove the unclassified metric-source form.")
				}
			case "PARSE_PATTERN":
				if command != "parse" {
					return replayError(ErrorASTContract, node, node.Role, "A parse pattern appears outside the parse command.", "Remove the unclassified parse-pattern form.")
				}
			}
		}
		for _, child := range node.Children {
			if err := walk(child, node, command, executable); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if err := walk(node.Alternatives[kind], node, command, executable && kind != AlternativeInfo); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(ast.Root, nil, "", true)
}

func compileSource(source *sourceAnalysis, context timeframeContext, input CompileInput) (SourceCompilation, error) {
	compiled := SourceCompilation{Source: cloneSourceDescriptor(source.SourceDescriptor)}
	if source.Class == SourceSynthetic {
		compiled.Overlap = OverlapProof{
			Classification: OverlapPresent,
			Reason:         "synthetic data reads no tenant telemetry",
			VisibleNow:     input.VisibleInterval,
			ReplayInterval: input.ReplayInterval,
		}
		return compiled, nil
	}
	requested, err := resolveRequestedRange(source, context)
	if err != nil {
		return compiled, err
	}
	compiled.Requested = &requested
	effective, proof := ClassifyOverlap(requested, input.VisibleInterval, input.ReplayInterval, input.VirtualNow)
	compiled.Overlap = proof
	if proof.Classification != OverlapPresent {
		return compiled, nil
	}
	if effective.End.Sub(effective.Start) <= time.Nanosecond {
		return compiled, replayError(ErrorTimeframe, source.node, "effective source timeframe", "The intersected source timeframe is only one nanosecond wide.", "Use a wider requested range or advance virtual time before retrying.")
	}
	compiled.Effective = cloneInterval(&effective)
	if source.Class == SourceRecord {
		compiled.PhysicalRange = cloneInterval(&effective)
		return compiled, nil
	}
	compiled.PhysicalPending = true
	contract := newMetricResultContract(source, effective)
	compiled.ResultContract = &contract
	return compiled, nil
}

func cloneSourceDescriptor(source SourceDescriptor) SourceDescriptor {
	clone := source
	if source.Metric != nil {
		metric := *source.Metric
		metric.Aggregations = append([]string(nil), source.Metric.Aggregations...)
		metric.MetricKeys = append([]string(nil), source.Metric.MetricKeys...)
		if source.Metric.DeclaredInterval != nil {
			value := *source.Metric.DeclaredInterval
			metric.DeclaredInterval = &value
		}
		clone.Metric = &metric
	}
	return clone
}

func explainSource(source SourceCompilation) SourceExplain {
	return SourceExplain{
		Ordinal: source.Source.Ordinal, Path: source.Source.Path, Class: source.Source.Class,
		Name: source.Source.Name, BoundaryPolicy: source.Source.BoundaryPolicy,
		Requested: cloneRequestedRange(source.Requested), Effective: cloneInterval(source.Effective),
		Classification: source.Overlap.Classification, Proof: cloneOverlapProof(source.Overlap),
		RecordTimeField: source.Source.RecordTimeField,
	}
}

func cloneRequestedRange(value *RequestedRange) *RequestedRange {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneOverlapProof(value OverlapProof) OverlapProof {
	clone := value
	clone.TerminalRequested = cloneInterval(value.TerminalRequested)
	return clone
}

func sourceNotices(source *sourceAnalysis, input CompileInput) ([]Notice, error) {
	var notices []Notice
	fixedDay, err := sourceUsesFixedDayLiteral(source, input.OriginalDQL)
	if err != nil {
		return nil, err
	}
	if fixedDay {
		notices = append(notices, Notice{
			Kind: NoticeNotification, Code: NoticeFixedDayInterval, SourceOrdinal: source.Ordinal,
			Message: "timeseries interval:1d is a fixed 24-hour interval, not a calendar day.",
		})
	}
	if (source.Name == "dt.davis.events.snapshots" || source.Name == "dt.davis.problems.snapshots") &&
		input.VirtualStart.Sub(input.ReplayInterval.Start) < 6*time.Hour {
		notices = append(notices, Notice{
			Kind: NoticeWarning, Code: NoticeDavisWarmup, SourceOrdinal: source.Ordinal,
			Message: "The Davis snapshot query has less than six hours of warm-up between data_start and virtual_start. Problem-state reconstruction at virtual_start may be incomplete; Phase 0B did not observe this short-gap hazard live. Compilation continues, and this warning does not change the replay boundaries.",
		})
	}
	return notices, nil
}

func sourceUsesFixedDayLiteral(source *sourceAnalysis, original string) (bool, error) {
	if source.Metric == nil || source.Metric.DeclaredInterval == nil || *source.Metric.DeclaredInterval != 24*time.Hour {
		return false, nil
	}
	intervals := source.parametersByKey()["interval"]
	if len(intervals) != 1 {
		return false, replayError(ErrorASTContract, source.node, "interval", "A fixed 24-hour metric has no unambiguous interval parameter.", "Update dtctl if the metric AST contract changed.")
	}
	value, err := parameterValue(intervals[0].node)
	if err != nil || value.Span == nil || value.Span.End.Index == int(^uint(0)>>1) {
		return false, replayError(ErrorASTContract, intervals[0].node, "interval", "A fixed 24-hour metric interval has no usable source span.", "Update dtctl if the position contract changed.")
	}
	start, err := utf16OffsetToByte(original, value.Span.Start.Index)
	if err != nil {
		return false, positionError("invalid_start", "metric interval notification", err.Error())
	}
	end, err := utf16OffsetToByte(original, value.Span.End.Index+1)
	if err != nil {
		return false, positionError("invalid_end", "metric interval notification", err.Error())
	}
	return strings.TrimSpace(original[start:end]) == "1d", nil
}

func classifyWholeQueryNonOverlap(sources []SourceExplain) *NonOverlapError {
	classification := OverlapPresent
	var rejected []SourceExplain
	for _, source := range sources {
		if source.Classification == OverlapPresent {
			continue
		}
		rejected = append(rejected, source)
		switch source.Classification {
		case OverlapUnknown:
			classification = OverlapUnknown
		case OverlapPermanent:
			if classification != OverlapUnknown {
				classification = OverlapPermanent
			}
		case OverlapTemporary:
			if classification == OverlapPresent {
				classification = OverlapTemporary
			}
		default:
			classification = OverlapUnknown
		}
	}
	if len(rejected) == 0 {
		return nil
	}
	return &NonOverlapError{Classification: classification, Sources: rejected}
}

func uniqueStrings(values []string) []string {
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
