package replay

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

// PositionEditKind distinguishes replacements from the only supported
// insertion: immediately after an AST-proven command span.
type PositionEditKind string

const (
	EditReplaceInclusive PositionEditKind = "replace_inclusive"
	EditInsertCommandEnd PositionEditKind = "insert_command_end"
)

// PositionEdit is a Method B source edit expressed in the query:parse
// coordinate system. Fields are private so callers cannot manufacture an
// insertion without proving its command boundary through the constructor.
type PositionEdit struct {
	kind        PositionEditKind
	startUTF16  int
	endUTF16    int
	replacement string
	purpose     string
	commandPath string
}

// PositionEditError is a typed failure of the observed UTF-16 position
// contract. It is safe for later phases to route by Kind rather than text.
type PositionEditError struct {
	Kind    string
	Purpose string
	Detail  string
}

func (e *PositionEditError) Error() string {
	if e.Purpose == "" {
		return fmt.Sprintf("DQL position edit %s: %s", e.Kind, e.Detail)
	}
	return fmt.Sprintf("DQL position edit %s for %q: %s", e.Kind, e.Purpose, e.Detail)
}

// ReplaceNode creates an edit for one AST node's inclusive source span.
func ReplaceNode(node *Node, replacement, purpose string) (PositionEdit, error) {
	if node == nil || node.Span == nil {
		return PositionEdit{}, positionError("missing_span", purpose, "the AST node has no source span")
	}
	return ReplaceSpan(*node.Span, replacement, purpose)
}

// ReplaceSpan creates an edit using the server's inclusive-end convention.
func ReplaceSpan(span Span, replacement, purpose string) (PositionEdit, error) {
	if span.Start.Index < 0 || span.End.Index < span.Start.Index {
		return PositionEdit{}, positionError("invalid_span", purpose, fmt.Sprintf("invalid inclusive UTF-16 range [%d,%d]", span.Start.Index, span.End.Index))
	}
	return PositionEdit{
		kind:        EditReplaceInclusive,
		startUTF16:  span.Start.Index,
		endUTF16:    span.End.Index,
		replacement: replacement,
		purpose:     purpose,
	}, nil
}

// InsertAtCommandEnd creates an insertion immediately after the inclusive
// span of a COMMAND container. No other insertion boundary is supported.
func InsertAtCommandEnd(command *Node, replacement, purpose string) (PositionEdit, error) {
	if command == nil || command.Kind != NodeContainer || command.Role != "COMMAND" {
		return PositionEdit{}, positionError("unproven_insertion", purpose, "the insertion owner is not a COMMAND container")
	}
	if command.Span == nil || command.Span.Start.Index < 0 || command.Span.End.Index < command.Span.Start.Index {
		return PositionEdit{}, positionError("unproven_insertion", purpose, "the command has no valid inclusive source span")
	}
	if command.Span.End.Index == int(^uint(0)>>1) {
		return PositionEdit{}, positionError("unproven_insertion", purpose, "the command end cannot be converted safely")
	}
	return PositionEdit{
		kind:        EditInsertCommandEnd,
		startUTF16:  command.Span.End.Index + 1,
		endUTF16:    command.Span.End.Index + 1,
		replacement: replacement,
		purpose:     purpose,
		commandPath: command.Path,
	}, nil
}

type byteEdit struct {
	kind        PositionEditKind
	start       int
	end         int
	replacement string
	purpose     string
	commandPath string
}

// ApplyPositionEdits applies Method B edits from right to left. Replacements
// use inclusive AST ends; insertions are zero-width and must have been built
// by InsertAtCommandEnd. Overlap and same-boundary insertion are rejected.
func ApplyPositionEdits(original string, edits []PositionEdit) (string, error) {
	if !utf8.ValidString(original) {
		return "", positionError("invalid_utf8", "", "the original DQL is not valid UTF-8")
	}
	converted := make([]byteEdit, 0, len(edits))
	for _, edit := range edits {
		if edit.startUTF16 < 0 || edit.endUTF16 < edit.startUTF16 {
			return "", positionError("invalid_span", edit.purpose, fmt.Sprintf("invalid UTF-16 coordinates [%d,%d]", edit.startUTF16, edit.endUTF16))
		}
		endExclusive := edit.endUTF16
		switch edit.kind {
		case EditReplaceInclusive:
			if edit.endUTF16 == int(^uint(0)>>1) {
				return "", positionError("invalid_span", edit.purpose, "the inclusive end cannot be converted safely")
			}
			endExclusive++
		case EditInsertCommandEnd:
			if edit.commandPath == "" {
				return "", positionError("unproven_insertion", edit.purpose, "the command boundary proof is missing")
			}
		default:
			return "", positionError("unknown_kind", edit.purpose, fmt.Sprintf("unknown edit kind %q", edit.kind))
		}
		start, err := utf16OffsetToByte(original, edit.startUTF16)
		if err != nil {
			return "", positionError("invalid_start", edit.purpose, err.Error())
		}
		end, err := utf16OffsetToByte(original, endExclusive)
		if err != nil {
			return "", positionError("invalid_end", edit.purpose, err.Error())
		}
		converted = append(converted, byteEdit{
			kind: edit.kind, start: start, end: end, replacement: edit.replacement,
			purpose: edit.purpose, commandPath: edit.commandPath,
		})
	}
	sort.SliceStable(converted, func(i, j int) bool {
		if converted[i].start == converted[j].start {
			if converted[i].end == converted[j].end {
				return converted[i].purpose < converted[j].purpose
			}
			return converted[i].end > converted[j].end
		}
		return converted[i].start > converted[j].start
	})
	for i := 1; i < len(converted); i++ {
		right := converted[i-1]
		left := converted[i]
		if byteEditsOverlap(left, right) {
			return "", positionError("overlap", left.purpose, fmt.Sprintf("conflicts with %q", right.purpose))
		}
	}
	out := original
	for _, edit := range converted {
		out = out[:edit.start] + edit.replacement + out[edit.end:]
	}
	return out, nil
}

func byteEditsOverlap(left, right byteEdit) bool {
	leftEmpty := left.start == left.end
	rightEmpty := right.start == right.end
	switch {
	case leftEmpty && rightEmpty:
		return left.start == right.start
	case leftEmpty:
		return left.start >= right.start && left.start < right.end
	case rightEmpty:
		return right.start >= left.start && right.start < left.end
	default:
		return left.start < right.end && right.start < left.end
	}
}

func utf16OffsetToByte(value string, wanted int) (int, error) {
	if wanted < 0 {
		return 0, fmt.Errorf("offset %d is negative", wanted)
	}
	units := 0
	for byteOffset, r := range value {
		if units == wanted {
			return byteOffset, nil
		}
		width := 1
		if r > 0xffff {
			width = 2
		}
		if wanted > units && wanted < units+width {
			return 0, fmt.Errorf("offset %d splits a UTF-16 surrogate pair", wanted)
		}
		units += width
	}
	if units == wanted {
		return len(value), nil
	}
	return 0, fmt.Errorf("offset %d exceeds UTF-16 length %d", wanted, units)
}

func positionError(kind, purpose, detail string) *PositionEditError {
	return &PositionEditError{Kind: kind, Purpose: purpose, Detail: detail}
}
