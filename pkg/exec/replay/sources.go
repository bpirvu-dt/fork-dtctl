package replay

import (
	"fmt"
	"strings"
	"time"
)

// SourceClass identifies the replay boundary behavior of one DQL source.
type SourceClass string

const (
	SourceRecord    SourceClass = "record"
	SourceMetric    SourceClass = "metric"
	SourceSynthetic SourceClass = "synthetic"
)

// BoundaryPolicy identifies how source-native results relate to the logical
// effective range.
type BoundaryPolicy string

const (
	BoundaryExact        BoundaryPolicy = "exact_half_open"
	BoundaryMetricBucket BoundaryPolicy = "one_natural_bucket_per_side"
	BoundaryNone         BoundaryPolicy = "none"
)

// RecordSourcePolicy is one explicitly approved record table contract.
type RecordSourcePolicy struct {
	Table           string
	RecordTimeField string
}

// SourcePolicy is the complete compiler source allowlist.
type SourcePolicy struct {
	RecordTables    map[string]RecordSourcePolicy
	DefaultLookback time.Duration
}

// Milestone1SourcePolicy returns the exact approved Wave 1 source policy.
func Milestone1SourcePolicy() SourcePolicy {
	tables := []RecordSourcePolicy{
		{Table: "logs", RecordTimeField: "timestamp"},
		{Table: "spans", RecordTimeField: "start_time"},
		{Table: "events", RecordTimeField: "timestamp"},
		{Table: "bizevents", RecordTimeField: "timestamp"},
		{Table: "dt.system.events", RecordTimeField: "timestamp"},
		{Table: "dt.davis.events.snapshots", RecordTimeField: "timestamp"},
		{Table: "dt.davis.problems.snapshots", RecordTimeField: "timestamp"},
	}
	policy := SourcePolicy{RecordTables: make(map[string]RecordSourcePolicy, len(tables)), DefaultLookback: verifiedDefaultLookback}
	for _, table := range tables {
		policy.RecordTables[table.Table] = table
	}
	return policy
}

// MetricShape is one exact allowlisted timeseries form.
type MetricShape string

const (
	MetricBaseline         MetricShape = "single_avg"
	MetricRate             MetricShape = "sum_rate_1s"
	MetricRollup           MetricShape = "avg_rollup_avg"
	MetricSingleDimension  MetricShape = "single_dt_entity_host_dimension"
	MetricMultiAggregation MetricShape = "avg_and_max"
)

// MetricForm describes the independently validated logical metric source.
type MetricForm struct {
	Shape             MetricShape
	Aggregations      []string
	MetricKeys        []string
	DeclaredInterval  *time.Duration
	DeclaredLiteral   string
	AutomaticInterval bool
}

// SourceDescriptor is a neutral classification returned to compiler callers.
type SourceDescriptor struct {
	Ordinal         int
	Path            string
	Class           SourceClass
	Name            string
	RecordTimeField string
	BoundaryPolicy  BoundaryPolicy
	Metric          *MetricForm
}

type sourceAnalysis struct {
	SourceDescriptor
	node       *Node
	command    commandView
	parameters []parameterView
}

type commandView struct {
	node *Node
	name string
}

type parameterView struct {
	node      *Node
	key       string
	keyOrigin string
}

// ClassifySources applies the milestone policy without reading state, the
// clock, configuration, or the network.
func ClassifySources(ast *AST, policy SourcePolicy) ([]SourceDescriptor, error) {
	analyses, err := analyzeSources(ast, policy)
	if err != nil {
		return nil, err
	}
	out := make([]SourceDescriptor, len(analyses))
	for i := range analyses {
		out[i] = analyses[i].SourceDescriptor
		if analyses[i].Metric != nil {
			metric := *analyses[i].Metric
			metric.Aggregations = append([]string(nil), analyses[i].Metric.Aggregations...)
			metric.MetricKeys = append([]string(nil), analyses[i].Metric.MetricKeys...)
			out[i].Metric = &metric
		}
	}
	return out, nil
}

func analyzeSources(ast *AST, policy SourcePolicy) ([]*sourceAnalysis, error) {
	if err := ValidateASTContract(ast); err != nil {
		return nil, err
	}
	if err := validateSemanticPlacements(ast); err != nil {
		return nil, err
	}
	commands, err := collectCommands(ast)
	if err != nil {
		return nil, err
	}
	if err := validateFunctions(ast); err != nil {
		return nil, err
	}
	if err := validateCommandSurface(ast, commands); err != nil {
		return nil, err
	}

	var sources []*sourceAnalysis
	for _, command := range commands {
		params, err := collectDirectParameters(command.node)
		if err != nil {
			return nil, err
		}
		source := &sourceAnalysis{node: command.node, command: command, parameters: params}
		source.Ordinal = len(sources)
		source.Path = command.node.Path
		switch command.name {
		case "fetch":
			table, err := fetchTable(command.node)
			if err != nil {
				return nil, err
			}
			if current, ok := currentDavisView(table, command.node); ok {
				return nil, current
			}
			if strings.HasPrefix(table, "dt.entity.") {
				return nil, replayError(ErrorCurrentState, command.node, table, fmt.Sprintf("%s reads current entity state. dtctl cannot reproduce its value at virtual now.", table), "Use stored historical fields without current entity enrichment.")
			}
			record, ok := policy.RecordTables[table]
			if !ok {
				return nil, replayError(ErrorUnsupportedSource, command.node, table, fmt.Sprintf("The DQL source %q is not in the replay record allowlist.", table), "Use one of the seven approved historical record tables.")
			}
			if err := validateParameterKeys(params, "dataobject", "from", "to", "timeframe"); err != nil {
				return nil, err
			}
			source.Class = SourceRecord
			source.Name = table
			source.RecordTimeField = record.RecordTimeField
			source.BoundaryPolicy = BoundaryExact
			sources = append(sources, source)
		case "timeseries":
			metric, err := classifyMetric(command.node, params)
			if err != nil {
				return nil, err
			}
			source.Class = SourceMetric
			source.Name = "timeseries"
			source.BoundaryPolicy = BoundaryMetricBucket
			source.Metric = metric
			sources = append(sources, source)
		case "data":
			if err := validateParameterKeys(params, "record"); err != nil {
				return nil, err
			}
			source.Class = SourceSynthetic
			source.Name = "data"
			source.BoundaryPolicy = BoundaryNone
			sources = append(sources, source)
		}
	}
	if len(sources) == 0 {
		return nil, replayError(ErrorUnsupportedSource, ast.Root, "query", "The DQL contains no approved historical or synthetic source.", "Use an allowlisted fetch, timeseries, or data source.")
	}
	return sources, nil
}

func (source *sourceAnalysis) parametersByKey() map[string][]parameterView {
	return parametersByKey(source.parameters)
}

func parametersByKey(parameters []parameterView) map[string][]parameterView {
	out := make(map[string][]parameterView)
	for _, parameter := range parameters {
		out[parameter.key] = append(out[parameter.key], parameter)
	}
	return out
}

func validateParameterKeys(parameters []parameterView, allowed ...string) error {
	allow := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allow[key] = struct{}{}
	}
	for _, parameter := range parameters {
		if _, ok := allow[parameter.key]; !ok {
			return replayError(ErrorUnsupportedForm, parameter.node, parameter.key, fmt.Sprintf("The source parameter %q has no approved replay contract.", parameter.key), "Remove the unsupported source parameter.")
		}
	}
	return nil
}

func fetchTable(command *Node) (string, error) {
	var tables []string
	_ = walkOwned(command, func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == "DATA_OBJECT" {
			tables = append(tables, strings.ToLower(node.Canonical))
		}
		return nil
	})
	if len(tables) != 1 {
		return "", replayError(ErrorASTContract, command, "fetch", "A fetch command has no unambiguous data object.", "Update dtctl if the server AST contract changed.")
	}
	return tables[0], nil
}

func currentDavisView(table string, node *Node) (*DavisCurrentViewError, bool) {
	var snapshot, identity string
	switch table {
	case "dt.davis.problems":
		snapshot, identity = "dt.davis.problems.snapshots", "problem"
	case "dt.davis.events":
		snapshot, identity = "dt.davis.events.snapshots", "event"
	default:
		return nil, false
	}
	err := &DavisCurrentViewError{
		View:               table,
		SnapshotTable:      snapshot,
		IdentityKind:       identity,
		LatestPerIDPattern: fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end> | sort timestamp desc | dedup <%s-id-field>", snapshot, identity),
		Path:               node.Path,
	}
	if node.Span != nil {
		span := *node.Span
		err.Span = &span
	}
	return err, true
}

func validateCommandSurface(ast *AST, commands []commandView) error {
	forbidden := map[string]string{
		"smartscapenodes": "current Smartscape nodes",
		"smartscapeedges": "current Smartscape edges",
		"traverse":        "current topology traversal",
		"fieldssnapshot":  "current field snapshot state",
		"load":            "mutable lookup content",
		"describe":        "current schema state",
	}
	allowed := map[string]struct{}{
		"fetch": {}, "timeseries": {}, "data": {}, "append": {}, "join": {}, "lookup": {},
		"filter": {}, "fields": {}, "fieldsadd": {}, "limit": {}, "sort": {}, "summarize": {}, "dedup": {},
	}
	for _, command := range commands {
		if detail, ok := forbidden[command.name]; ok {
			return replayError(ErrorCurrentState, command.node, command.name, fmt.Sprintf("%s is current mutable tenant state. dtctl cannot reproduce its value at virtual now.", detail), "Remove the current-state construct.")
		}
		if _, ok := allowed[command.name]; !ok {
			return replayError(ErrorUnsupportedForm, command.node, command.name, fmt.Sprintf("The DQL command %q has no approved replay contract.", command.name), "Remove the unsupported command.")
		}
		if command.name == "append" || command.name == "join" || command.name == "lookup" {
			blocks := ownedExecutionBlocks(command.node)
			if blocks != 1 {
				return replayError(ErrorUnsupportedForm, command.node, command.name, fmt.Sprintf("%s must contain exactly one AST-proven nested execution block.", command.name), "Use the tested source-bearing nested form.")
			}
			params, err := collectDirectParameters(command.node)
			if err != nil {
				return err
			}
			switch command.name {
			case "append":
				err = validateParameterKeys(params, "source")
			case "join":
				err = validateParameterKeys(params, "jointable", "on")
			case "lookup":
				err = validateParameterKeys(params, "lookuptable", "sourcefield", "lookupfield")
			}
			if err != nil {
				return err
			}
		}
	}
	return validateExecutionBlockOwners(ast.Root, "")
}

func validateFunctions(ast *AST) error {
	forbidden := map[string]struct{}{
		"entityname": {}, "entityattr": {}, "classicentityselector": {}, "getnodename": {}, "getnodefield": {}, "smartscapeattr": {},
	}
	allowed := map[string]struct{}{
		"now": {}, "totimestamp": {}, "timeframe": {}, "record": {}, "count": {}, "countif": {},
		"countdistinctexact": {}, "min": {}, "max": {}, "array": {}, "isnotnull": {}, "in": {},
	}
	return ast.WalkExecutable(func(node *Node) error {
		if node.Kind != NodeTerminal || node.Role != "FUNCTION_NAME" {
			return nil
		}
		name := strings.ToLower(node.Canonical)
		if _, ok := forbidden[name]; ok {
			return replayError(ErrorCurrentState, node, name, fmt.Sprintf("%s reads current mutable tenant state. dtctl cannot reproduce its value at virtual now.", node.Canonical), "Remove the current-state enrichment.")
		}
		if _, ok := allowed[name]; !ok {
			return replayError(ErrorUnsupportedForm, node, name, fmt.Sprintf("The DQL function %q has no approved replay contract.", node.Canonical), "Remove the unsupported function.")
		}
		return nil
	})
}

func classifyMetric(command *Node, params []parameterView) (*MetricForm, error) {
	byKey := parametersByKey(params)
	if len(byKey["shift"]) > 0 {
		return nil, replayError(ErrorShift, byKey["shift"][0].node, "shift", "Every timeseries shift form is rejected in milestone 1.", "Remove shift or wait for a separately evidenced milestone.")
	}
	if err := validateParameterKeys(params, "series", "from", "to", "timeframe", "interval", "by", "shift"); err != nil {
		return nil, err
	}
	for _, key := range []string{"series", "from", "to", "timeframe", "interval", "by"} {
		if len(byKey[key]) > 1 {
			return nil, replayError(ErrorUnsupportedForm, command, key, fmt.Sprintf("timeseries repeats the %q parameter.", key), "Use the exact tested metric form.")
		}
	}
	if len(byKey["series"]) != 1 {
		return nil, replayError(ErrorUnsupportedForm, command, "timeseries series", "timeseries must contain one tested series parameter group.", "Use a single avg series, the tested avg/max pair, or another exact allowlisted form.")
	}
	aggregations, err := metricAggregations(byKey["series"][0].node)
	if err != nil {
		return nil, err
	}
	form := &MetricForm{}
	for _, aggregation := range aggregations {
		form.Aggregations = append(form.Aggregations, aggregation.name)
		form.MetricKeys = append(form.MetricKeys, aggregation.metric)
	}
	if len(byKey["interval"]) == 0 {
		form.AutomaticInterval = true
	} else {
		value, err := parameterValue(byKey["interval"][0].node)
		if err != nil {
			return nil, replayError(ErrorUnsupportedForm, byKey["interval"][0].node, "interval", "The metric interval AST shape is unsupported.", "Use a parser-accepted fixed duration.")
		}
		duration, err := parseDurationNode(value)
		if err != nil || duration.fixed == nil || *duration.fixed <= 0 {
			if err != nil {
				return nil, err
			}
			return nil, replayError(ErrorUnsupportedForm, value, "interval", "The metric interval is not a positive fixed duration.", "Use a parser-accepted fixed duration; genuine calendar intervals are rejected.")
		}
		declared := *duration.fixed
		form.DeclaredInterval = &declared
		form.DeclaredLiteral = durationLiteral(value)
	}

	if len(byKey["by"]) == 1 {
		if !baselineAggregations(aggregations) {
			return nil, unsupportedMetric(command)
		}
		identifiers := terminalsWithRole(byKey["by"][0].node, "SIMPLE_IDENTIFIER")
		if len(identifiers) != 1 || !strings.EqualFold(identifiers[0].Canonical, "dt.entity.host") {
			return nil, unsupportedMetric(byKey["by"][0].node)
		}
		form.Shape = MetricSingleDimension
		return form, nil
	}
	if baselineAggregations(aggregations) {
		form.Shape = MetricBaseline
		return form, nil
	}
	if len(aggregations) == 1 && aggregations[0].name == "sum" && aggregations[0].rate == time.Second && aggregations[0].rollup == "" {
		form.Shape = MetricRate
		return form, nil
	}
	if len(aggregations) == 1 && aggregations[0].name == "avg" && aggregations[0].rollup == "avg" && aggregations[0].rate == 0 {
		form.Shape = MetricRollup
		return form, nil
	}
	if len(aggregations) == 2 && aggregations[0].name == "avg" && aggregations[1].name == "max" &&
		aggregations[0].metric == aggregations[1].metric && aggregations[0].plain() && aggregations[1].plain() {
		form.Shape = MetricMultiAggregation
		return form, nil
	}
	return nil, unsupportedMetric(command)
}

type metricAggregation struct {
	name   string
	metric string
	rate   time.Duration
	rollup string
}

func (aggregation metricAggregation) plain() bool {
	return aggregation.rate == 0 && aggregation.rollup == ""
}

func baselineAggregations(aggregations []metricAggregation) bool {
	return len(aggregations) == 1 && aggregations[0].name == "avg" && aggregations[0].plain()
}

func metricAggregations(series *Node) ([]metricAggregation, error) {
	var functions []*Node
	_ = walkOwned(series, func(node *Node) error {
		if node.Role == "FUNCTION" && ownAggregationName(node) != "" {
			functions = append(functions, node)
		}
		return nil
	})
	if len(functions) == 0 {
		return nil, unsupportedMetric(series)
	}
	var out []metricAggregation
	for _, function := range functions {
		aggregation := metricAggregation{name: strings.ToLower(ownAggregationName(function))}
		metrics := terminalsWithRole(function, "METRIC_KEY")
		if len(metrics) != 1 {
			return nil, unsupportedMetric(function)
		}
		aggregation.metric = metrics[0].Canonical
		params, err := collectDirectParameters(function)
		if err != nil {
			return nil, err
		}
		for _, parameter := range params {
			switch parameter.key {
			case "metric", "metrickey":
			case "rate":
				value, valueErr := parameterValue(parameter.node)
				if valueErr != nil {
					return nil, unsupportedMetric(parameter.node)
				}
				duration, durationErr := parseDurationNode(value)
				if durationErr != nil || duration.fixed == nil || *duration.fixed <= 0 {
					return nil, unsupportedMetric(parameter.node)
				}
				aggregation.rate = *duration.fixed
			case "rollup":
				identifiers := terminalsWithRole(parameter.node, "SIMPLE_IDENTIFIER")
				if len(identifiers) != 1 {
					return nil, unsupportedMetric(parameter.node)
				}
				aggregation.rollup = strings.ToLower(identifiers[0].Canonical)
			default:
				return nil, unsupportedMetric(parameter.node)
			}
		}
		out = append(out, aggregation)
	}
	return out, nil
}

func ownAggregationName(function *Node) string {
	var values []string
	for _, child := range function.Children {
		if child.Kind == NodeTerminal && child.Role == "TIMESERIES_AGGREGATION" {
			values = append(values, child.Canonical)
		}
	}
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

func unsupportedMetric(node *Node) error {
	return replayError(ErrorUnsupportedForm, node, "timeseries", "The timeseries form is outside the exact milestone 1 metric allowlist.", "Use single avg, sum with rate:1s, avg with rollup:avg, the tested single split, or the tested avg/max pair.")
}
