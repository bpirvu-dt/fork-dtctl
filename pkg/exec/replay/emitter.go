package replay

import (
	"fmt"
	"strings"
	"time"
)

const generatedTimestampLayout = "2006-01-02T15:04:05.000000000Z"

func emitTimestamp(value time.Time) string {
	return `toTimestamp("` + value.UTC().Format(generatedTimestampLayout) + `")`
}

func emitBounds(value Interval) (string, string) {
	return "from:" + emitTimestamp(value.Start), "to:" + emitTimestamp(value.End)
}

func buildReplayEdits(ast *AST, original string, analyses map[int]*sourceAnalysis, compiled []SourceCompilation, virtualNow time.Time) ([]PositionEdit, error) {
	if original == "" {
		return nil, positionError("missing_source", "effective DQL", "the original DQL text is empty")
	}
	var edits []PositionEdit
	var covered []Span
	for _, sourceResult := range compiled {
		if sourceResult.Source.Class == SourceSynthetic {
			continue
		}
		analysis := analyses[sourceResult.Source.Ordinal]
		if analysis == nil || sourceResult.Effective == nil {
			return nil, replayError(ErrorAudit, nil, "source", "An accepted source has no compiler analysis or effective range.", "Do not execute this query; report the replay compiler mismatch.")
		}
		sourceEdits, spans, err := sourceBoundaryEdits(analysis, *sourceResult.Effective)
		if err != nil {
			return nil, err
		}
		edits = append(edits, sourceEdits...)
		covered = append(covered, spans...)
	}
	nowEdits, err := semanticNowEdits(ast, covered, virtualNow)
	if err != nil {
		return nil, err
	}
	edits = append(edits, nowEdits...)
	return edits, nil
}

func sourceBoundaryEdits(source *sourceAnalysis, effective Interval) ([]PositionEdit, []Span, error) {
	fromText, toText := emitBounds(effective)
	byKey := source.parametersByKey()
	if len(byKey["timeframe"]) == 1 {
		edit, err := ReplaceNode(byKey["timeframe"][0].node, fromText+", "+toText, fmt.Sprintf("source %d timeframe", source.Ordinal))
		if err != nil {
			return nil, nil, err
		}
		return []PositionEdit{edit}, []Span{*byKey["timeframe"][0].node.Span}, nil
	}
	var edits []PositionEdit
	var spans []Span
	if len(byKey["from"]) == 1 {
		edit, err := ReplaceNode(byKey["from"][0].node, fromText, fmt.Sprintf("source %d from", source.Ordinal))
		if err != nil {
			return nil, nil, err
		}
		edits = append(edits, edit)
		spans = append(spans, *byKey["from"][0].node.Span)
	}
	if len(byKey["to"]) == 1 {
		edit, err := ReplaceNode(byKey["to"][0].node, toText, fmt.Sprintf("source %d to", source.Ordinal))
		if err != nil {
			return nil, nil, err
		}
		edits = append(edits, edit)
		spans = append(spans, *byKey["to"][0].node.Span)
	}
	var missing string
	switch {
	case len(byKey["from"]) == 0 && len(byKey["to"]) == 0:
		missing = ", " + fromText + ", " + toText
	case len(byKey["from"]) == 1 && len(byKey["to"]) == 0:
		missing = ", " + toText
	}
	if missing != "" {
		edit, err := InsertAtCommandEnd(source.command.node, missing, fmt.Sprintf("source %d missing bounds", source.Ordinal))
		if err != nil {
			return nil, nil, err
		}
		edits = append(edits, edit)
	}
	return edits, spans, nil
}

func semanticNowEdits(ast *AST, covered []Span, virtualNow time.Time) ([]PositionEdit, error) {
	var edits []PositionEdit
	err := ast.WalkExecutable(func(node *Node) error {
		if node.Kind != NodeContainer || node.Role != "FUNCTION" || !strings.EqualFold(ownFunctionName(node), "now") {
			return nil
		}
		parts := semanticChildren(node)
		if len(parts) != 1 || parts[0].Kind != NodeTerminal || parts[0].Role != "FUNCTION_NAME" {
			return replayError(ErrorTimeframe, node, "now", "The semantic now function has an unsupported argument shape.", "Use now() without arguments.")
		}
		if node.Span == nil {
			return replayError(ErrorASTContract, node, "now", "A semantic now function has no source position.", "Update dtctl if the server AST position contract changed.")
		}
		if spanCovered(*node.Span, covered) {
			return nil
		}
		edit, err := ReplaceNode(node, emitTimestamp(virtualNow), "semantic now outside source parameter")
		if err != nil {
			return err
		}
		edits = append(edits, edit)
		return nil
	})
	return edits, err
}

func spanCovered(candidate Span, covered []Span) bool {
	for _, span := range covered {
		if candidate.Start.Index >= span.Start.Index && candidate.End.Index <= span.End.Index {
			return true
		}
	}
	return false
}
