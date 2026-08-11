package replay

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const verifiedDefaultLookback = 2 * time.Hour

// Interval is a half-open time interval [Start, End).
type Interval struct {
	Start time.Time
	End   time.Time
}

// Valid reports whether the interval is non-empty.
func (i Interval) Valid() bool {
	return !i.Start.IsZero() && !i.End.IsZero() && i.Start.Before(i.End)
}

// Intersect returns the exact half-open intersection.
func (i Interval) Intersect(other Interval) (Interval, bool) {
	start := i.Start
	if other.Start.After(start) {
		start = other.Start
	}
	end := i.End
	if other.End.Before(end) {
		end = other.End
	}
	intersection := Interval{Start: start, End: end}
	return intersection, intersection.Valid()
}

// EndpointDependency records what is proven about an endpoint as virtual time
// advances.
type EndpointDependency string

const (
	EndpointAbsolute EndpointDependency = "absolute"
	EndpointRelative EndpointDependency = "relative_to_virtual_now"
	EndpointUnknown  EndpointDependency = "unknown"
)

// TimeEndpoint is one evaluated requested-range endpoint plus its future-time
// proof model.
type TimeEndpoint struct {
	Value      time.Time
	Dependency EndpointDependency
	Offset     time.Duration
}

// RequestedRange is the source range before intersection with the visible
// replay interval.
type RequestedRange struct {
	Range Interval
	From  TimeEndpoint
	To    TimeEndpoint
	Basis string
}

// OverlapClassification is the proof-backed state of a source range.
type OverlapClassification string

const (
	OverlapPresent   OverlapClassification = "overlap"
	OverlapTemporary OverlapClassification = "temporary"
	OverlapPermanent OverlapClassification = "permanent"
	OverlapUnknown   OverlapClassification = "unknown"
)

// OverlapProof explains a non-overlap decision without relying on error text.
type OverlapProof struct {
	Classification     OverlapClassification
	Reason             string
	RequestedNow       Interval
	VisibleNow         Interval
	TerminalRequested  *Interval
	ReplayInterval     Interval
	TerminalVirtualNow time.Time
}

// ClassifyOverlap intersects the current range and, when empty, uses the full
// replay interval to prove whether a later overlap is possible.
func ClassifyOverlap(requested RequestedRange, visible, replay Interval, virtualNow time.Time) (Interval, OverlapProof) {
	if effective, ok := requested.Range.Intersect(visible); ok {
		return effective, OverlapProof{
			Classification: OverlapPresent,
			Reason:         "the requested source range intersects the visible replay interval",
			RequestedNow:   requested.Range,
			VisibleNow:     visible,
			ReplayInterval: replay,
		}
	}
	proof := OverlapProof{
		RequestedNow:       requested.Range,
		VisibleNow:         visible,
		ReplayInterval:     replay,
		TerminalVirtualNow: replay.End,
	}
	if !virtualNow.Before(replay.End) {
		proof.Classification = OverlapPermanent
		proof.Reason = "virtual now is at data_end, so the visible replay interval cannot grow"
		return Interval{}, proof
	}
	terminal, known := requestedRangeAt(requested, replay.End)
	if !known {
		proof.Classification = OverlapUnknown
		proof.Reason = "the source range uses time semantics whose future behavior is not proven"
		return Interval{}, proof
	}
	proof.TerminalRequested = &terminal
	if terminal.Valid() {
		if _, ok := terminal.Intersect(replay); ok {
			proof.Classification = OverlapTemporary
			proof.Reason = "the requested source range is proven to intersect by data_end"
			return Interval{}, proof
		}
	}
	proof.Classification = OverlapPermanent
	proof.Reason = "the requested source range is proven not to intersect even at data_end"
	return Interval{}, proof
}

func requestedRangeAt(requested RequestedRange, virtualNow time.Time) (Interval, bool) {
	from, ok := endpointAt(requested.From, virtualNow)
	if !ok {
		return Interval{}, false
	}
	to, ok := endpointAt(requested.To, virtualNow)
	if !ok {
		return Interval{}, false
	}
	return Interval{Start: from, End: to}, true
}

func endpointAt(endpoint TimeEndpoint, virtualNow time.Time) (time.Time, bool) {
	switch endpoint.Dependency {
	case EndpointAbsolute:
		return endpoint.Value, true
	case EndpointRelative:
		return virtualNow.Add(endpoint.Offset), true
	default:
		return time.Time{}, false
	}
}

type timeframeContext struct {
	VirtualNow      time.Time
	ReplayInterval  Interval
	VisibleInterval Interval
	Timezone        *time.Location
	GlobalDefault   *Interval
	DefaultLookback time.Duration
}

func resolveRequestedRange(source *sourceAnalysis, context timeframeContext) (RequestedRange, error) {
	params := source.parametersByKey()
	if len(params["timeframe"]) > 0 && (len(params["from"]) > 0 || len(params["to"]) > 0) {
		return RequestedRange{}, replayError(ErrorTimeframe, source.node, "timeframe", "The source combines mutually exclusive timeframe forms.", "Use either timeframe or from and to.")
	}
	for _, key := range []string{"from", "to", "timeframe"} {
		if len(params[key]) > 1 {
			return RequestedRange{}, replayError(ErrorTimeframe, source.node, key, "The source repeats a timeframe parameter.", "Provide each timeframe parameter at most once.")
		}
	}
	if len(params["timeframe"]) == 1 {
		return parseTimeframeParameter(params["timeframe"][0], context)
	}
	if len(params["to"]) == 1 && len(params["from"]) == 0 {
		return RequestedRange{}, replayError(ErrorTimeframe, params["to"][0].node, "to", "A source with to but no from has no proven requested start.", "Add an explicit from value.")
	}
	if len(params["from"]) == 0 {
		if context.GlobalDefault != nil {
			if !context.GlobalDefault.Valid() {
				return RequestedRange{}, replayError(ErrorTimeframe, source.node, "global default timeframe", "The global default timeframe is empty or invalid.", "Provide both default-timeframe endpoints with start before end.")
			}
			return absoluteRequested(*context.GlobalDefault, "global default timeframe"), nil
		}
		lookback := context.DefaultLookback
		if lookback == 0 {
			lookback = verifiedDefaultLookback
		}
		if lookback <= 0 {
			return RequestedRange{}, replayError(ErrorTimeframe, source.node, "default timeframe", "The default lookback is not positive.", "Use the verified two-hour default or a positive policy value.")
		}
		from := relativeEndpoint(context.VirtualNow, -lookback)
		to := relativeEndpoint(context.VirtualNow, 0)
		return requestedFromEndpoints(from, to, "verified two-hour service default")
	}
	from, err := evaluateTimeParameter(params["from"][0], context)
	if err != nil {
		return RequestedRange{}, err
	}
	to := relativeEndpoint(context.VirtualNow, 0)
	basis := "explicit from with implicit virtual-now end"
	if len(params["to"]) == 1 {
		to, err = evaluateTimeParameter(params["to"][0], context)
		if err != nil {
			return RequestedRange{}, err
		}
		basis = "explicit from and to"
	}
	return requestedFromEndpoints(from, to, basis)
}

func absoluteRequested(value Interval, basis string) RequestedRange {
	return RequestedRange{
		Range: value,
		From:  TimeEndpoint{Value: value.Start, Dependency: EndpointAbsolute},
		To:    TimeEndpoint{Value: value.End, Dependency: EndpointAbsolute},
		Basis: basis,
	}
}

func requestedFromEndpoints(from, to TimeEndpoint, basis string) (RequestedRange, error) {
	value := Interval{Start: from.Value.UTC(), End: to.Value.UTC()}
	if !value.Valid() {
		return RequestedRange{}, replayError(ErrorTimeframe, nil, basis, "The requested source timeframe is empty or reversed.", "Use a start that is earlier than the end.")
	}
	if value.End.Sub(value.Start) <= time.Nanosecond {
		return RequestedRange{}, replayError(ErrorTimeframe, nil, basis, "A one-nanosecond source timeframe is not a valid replay window.", "Use a wider non-empty timeframe.")
	}
	from.Value, to.Value = value.Start, value.End
	return RequestedRange{Range: value, From: from, To: to, Basis: basis}, nil
}

func relativeEndpoint(value time.Time, offset time.Duration) TimeEndpoint {
	return TimeEndpoint{Value: value.Add(offset).UTC(), Dependency: EndpointRelative, Offset: offset}
}

func parseTimeframeParameter(parameter parameterView, context timeframeContext) (RequestedRange, error) {
	value, err := parameterValue(parameter.node)
	if err != nil {
		return RequestedRange{}, replayError(ErrorTimeframe, parameter.node, "timeframe", "The timeframe parameter has an unsupported AST shape.", "Use a quoted absolute start/end timeframe or timeframe(from:, to:).")
	}
	if value.Kind == NodeTerminal && value.Role == "STRING" {
		literal, err := strconv.Unquote(value.Canonical)
		if err != nil || strings.Count(literal, "/") != 1 {
			return RequestedRange{}, replayError(ErrorTimeframe, value, "timeframe", "The quoted timeframe is malformed.", "Use two absolute RFC 3339 timestamps separated by one slash.")
		}
		startText, endText, _ := strings.Cut(literal, "/")
		start, startErr := time.Parse(time.RFC3339Nano, startText)
		end, endErr := time.Parse(time.RFC3339Nano, endText)
		if startErr != nil || endErr != nil {
			return RequestedRange{}, replayError(ErrorTimeframe, value, "timeframe", "The quoted timeframe contains an invalid timestamp.", "Use absolute RFC 3339 timestamps with a timezone.")
		}
		return requestedFromEndpoints(
			TimeEndpoint{Value: start.UTC(), Dependency: EndpointAbsolute},
			TimeEndpoint{Value: end.UTC(), Dependency: EndpointAbsolute},
			"quoted absolute timeframe",
		)
	}
	if value.Role != "FUNCTION" || !strings.EqualFold(ownFunctionName(value), "timeframe") {
		return RequestedRange{}, replayError(ErrorTimeframe, value, "timeframe", "The timeframe value is dynamic or unsupported.", "Use a quoted absolute start/end timeframe or timeframe(from:, to:).")
	}
	params, err := collectDirectParameters(value)
	if err != nil {
		return RequestedRange{}, err
	}
	byKey := parametersByKey(params)
	if len(byKey) != 2 || len(byKey["from"]) != 1 || len(byKey["to"]) != 1 {
		return RequestedRange{}, replayError(ErrorTimeframe, value, "timeframe", "The structured timeframe must contain exactly from and to.", "Use timeframe(from:<timestamp>, to:<timestamp>).")
	}
	from, err := evaluateTimeParameter(byKey["from"][0], context)
	if err != nil {
		return RequestedRange{}, err
	}
	to, err := evaluateTimeParameter(byKey["to"][0], context)
	if err != nil {
		return RequestedRange{}, err
	}
	return requestedFromEndpoints(from, to, "structured timeframe")
}

func evaluateTimeParameter(parameter parameterView, context timeframeContext) (TimeEndpoint, error) {
	value, err := parameterValue(parameter.node)
	if err != nil {
		return TimeEndpoint{}, replayError(ErrorTimeframe, parameter.node, parameter.key, "The time expression has an unsupported AST shape.", "Use an absolute timestamp, now() with fixed arithmetic, or a supported implicit-now form.")
	}
	endpoint, err := evaluateTimeNode(value, context)
	if err != nil {
		return TimeEndpoint{}, err
	}
	endpoint.Value = endpoint.Value.UTC()
	return endpoint, nil
}

func evaluateTimeNode(node *Node, context timeframeContext) (TimeEndpoint, error) {
	switch node.Role {
	case "EXPRESSION":
		parts := semanticChildren(node)
		if len(parts) == 1 {
			return evaluateTimeNode(parts[0], context)
		}
		if len(parts) == 2 && parts[1].Kind == NodeTerminal && strings.HasPrefix(parts[1].Canonical, "@") {
			base, err := evaluateTimeNode(parts[0], context)
			if err != nil {
				return TimeEndpoint{}, err
			}
			return alignEndpoint(base, parts[1].Canonical, context)
		}
		if len(parts) == 3 && parts[1].Kind == NodeTerminal && (parts[1].Canonical == "+" || parts[1].Canonical == "-") {
			base, err := evaluateTimeNode(parts[0], context)
			if err != nil {
				return TimeEndpoint{}, err
			}
			return applyDuration(base, parts[1].Canonical, parts[2], context.location())
		}
	case "FUNCTION":
		switch strings.ToLower(ownFunctionName(node)) {
		case "now":
			return relativeEndpoint(context.VirtualNow, 0), nil
		case "totimestamp":
			stringsFound := terminalsWithRole(node, "STRING")
			if len(stringsFound) != 1 {
				break
			}
			literal, err := strconv.Unquote(stringsFound[0].Canonical)
			if err != nil {
				break
			}
			value, err := time.Parse(time.RFC3339Nano, literal)
			if err != nil {
				return TimeEndpoint{}, replayError(ErrorTimeframe, stringsFound[0], "toTimestamp", "The timestamp literal is not an absolute RFC 3339 value.", "Use an RFC 3339 timestamp with a timezone.")
			}
			return TimeEndpoint{Value: value.UTC(), Dependency: EndpointAbsolute}, nil
		}
	case "DURATION", "CALENDAR_DURATION":
		shift, err := parseDurationNode(node)
		if err != nil {
			return TimeEndpoint{}, err
		}
		if shift.fixed != nil {
			if *shift.fixed >= 0 {
				break
			}
			return relativeEndpoint(context.VirtualNow, *shift.fixed), nil
		}
		if shift.calendar != nil && shift.calendar.amount < 0 {
			value := addCalendar(context.VirtualNow, *shift.calendar, context.location())
			return TimeEndpoint{Value: value.UTC(), Dependency: EndpointUnknown}, nil
		}
	}
	return TimeEndpoint{}, replayError(ErrorTimeframe, node, node.Role, "The time expression is not supported by replay.", "Rewrite it using an absolute timestamp or a supported virtual-now expression.")
}

type durationValue struct {
	fixed    *time.Duration
	calendar *calendarDuration
}

type calendarDuration struct {
	amount int
	unit   string
}

func parseDurationNode(node *Node) (durationValue, error) {
	numbers := terminalsWithRole(node, "NUMBER")
	units := terminalsWithRole(node, "TIME_UNIT")
	if len(numbers) != 1 || len(units) != 1 {
		return durationValue{}, replayError(ErrorTimeframe, node, node.Role, "The duration AST shape is unsupported.", "Use one numeric fixed duration.")
	}
	text := numbers[0].Canonical + units[0].Canonical
	if node.Role == "DURATION" {
		value, err := time.ParseDuration(text)
		if err != nil {
			return durationValue{}, replayError(ErrorTimeframe, node, text, "The fixed duration is unsupported.", "Use a parser-accepted fixed duration.")
		}
		return durationValue{fixed: &value}, nil
	}
	amount, err := strconv.Atoi(numbers[0].Canonical)
	if err != nil {
		return durationValue{}, replayError(ErrorTimeframe, node, text, "The calendar duration is unsupported.", "Use a supported day-based implicit-now expression.")
	}
	unit := units[0].Canonical
	if unit != "d" {
		return durationValue{}, replayError(ErrorTimeframe, node, text, "Genuine calendar intervals are not supported in milestone 1.", "Use a fixed duration; calendar months, weeks, and years remain rejected.")
	}
	calendar := calendarDuration{amount: amount, unit: unit}
	return durationValue{calendar: &calendar}, nil
}

func durationLiteral(node *Node) string {
	numbers := terminalsWithRole(node, "NUMBER")
	units := terminalsWithRole(node, "TIME_UNIT")
	if len(numbers) != 1 || len(units) != 1 {
		return ""
	}
	return numbers[0].Canonical + units[0].Canonical
}

func applyDuration(base TimeEndpoint, operator string, node *Node, location *time.Location) (TimeEndpoint, error) {
	shift, err := parseDurationNode(node)
	if err != nil {
		return TimeEndpoint{}, err
	}
	sign := 1
	if operator == "-" {
		sign = -1
	}
	if shift.fixed != nil {
		delta := time.Duration(sign) * *shift.fixed
		base.Value = base.Value.Add(delta)
		if base.Dependency == EndpointRelative {
			base.Offset += delta
		}
		return base, nil
	}
	calendar := *shift.calendar
	calendar.amount *= sign
	base.Value = addCalendar(base.Value, calendar, location)
	base.Dependency = EndpointUnknown
	base.Offset = 0
	return base, nil
}

func alignEndpoint(base TimeEndpoint, operator string, context timeframeContext) (TimeEndpoint, error) {
	location := context.location()
	if location != time.UTC {
		return TimeEndpoint{}, replayError(ErrorTimeframe, nil, operator, "Calendar or DST-sensitive alignment outside UTC is not supported in milestone 1.", "Use an absolute timestamp or a UTC replay timezone for the tested @h and @d forms.")
	}
	local := base.Value.In(location)
	var aligned time.Time
	switch strings.ToLower(operator) {
	case "@h":
		aligned = time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), 0, 0, 0, location)
	case "@d":
		aligned = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	default:
		return TimeEndpoint{}, replayError(ErrorTimeframe, nil, operator, "This time alignment is not supported in milestone 1.", "Use @h, @d, or an absolute timestamp.")
	}
	base.Value = aligned.UTC()
	base.Dependency = EndpointUnknown
	base.Offset = 0
	return base, nil
}

func addCalendar(value time.Time, shift calendarDuration, location *time.Location) time.Time {
	local := value.In(location)
	return local.AddDate(0, 0, shift.amount)
}

func (context timeframeContext) location() *time.Location {
	if context.Timezone != nil {
		return context.Timezone
	}
	return time.UTC
}

func semanticChildren(node *Node) []*Node {
	var out []*Node
	for _, child := range node.Children {
		if child.Kind == NodeTerminal {
			switch child.Role {
			case "SPACE", "INDENT", "LINEBREAK", "PARENTHESIS_OPEN", "PARENTHESIS_CLOSE", "COLON", "COMMA":
				continue
			}
		}
		if child.Kind == NodeAlternative && len(child.Alternatives) == 1 && child.Alternatives[AlternativeInfo] != nil {
			continue
		}
		out = append(out, child)
	}
	return out
}

func parameterValue(parameter *Node) (*Node, error) {
	parts := semanticChildren(parameter)
	var values []*Node
	for _, part := range parts {
		if part.Role == "PARAMETER_NAMING" {
			continue
		}
		values = append(values, part)
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("parameter has %d semantic values", len(values))
	}
	return values[0], nil
}

func terminalsWithRole(root *Node, role string) []*Node {
	var out []*Node
	_ = walkOwned(root, func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == role {
			out = append(out, node)
		}
		return nil
	})
	return out
}
