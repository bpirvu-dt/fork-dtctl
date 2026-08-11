package replay

import (
	"errors"
	"strings"
	"testing"
)

func TestPositionEditorMatrix(t *testing.T) {
	tests := []struct {
		name     string
		original string
		edits    func(t *testing.T) []PositionEdit
		want     string
	}{
		{
			name:     "ASCII replacement and adjacent insertion",
			original: `fetch logs, from:now()-1h`,
			edits: func(t *testing.T) []PositionEdit {
				return []PositionEdit{
					mustReplace(t, 12, 24, `from:toTimestamp("2026-06-14T09:00:00Z")`),
					mustInsert(t, 25, `, to:toTimestamp("2026-06-14T10:00:00Z")`),
				}
			},
			want: `fetch logs, from:toTimestamp("2026-06-14T09:00:00Z"), to:toTimestamp("2026-06-14T10:00:00Z")`,
		},
		{
			name:     "multibyte before edit",
			original: `fieldsAdd label="é🙂" | fetch logs`,
			edits: func(t *testing.T) []PositionEdit {
				return []PositionEdit{mustReplace(t, 24, 33, `fetch events`)}
			},
			want: `fieldsAdd label="é🙂" | fetch events`,
		},
		{
			name:     "multibyte inside complete edit",
			original: `fieldsAdd label="é🙂"`,
			edits: func(t *testing.T) []PositionEdit {
				return []PositionEdit{mustReplace(t, 16, 20, `"kept"`)}
			},
			want: `fieldsAdd label="kept"`,
		},
		{
			name:     "escaped string preserved",
			original: `data record(content="escaped \\"now()\\"") | fieldsAdd replay=now()`,
			edits: func(t *testing.T) []PositionEdit {
				return []PositionEdit{mustReplaceLast(t, `data record(content="escaped \\"now()\\"") | fieldsAdd replay=now()`, "now()", `toTimestamp("2026-06-14T10:00:00Z")`)}
			},
			want: `data record(content="escaped \\"now()\\"") | fieldsAdd replay=toTimestamp("2026-06-14T10:00:00Z")`,
		},
		{
			name:     "comment preserved",
			original: "fetch logs // now() is documentation\n| fieldsAdd replay=now()",
			edits: func(t *testing.T) []PositionEdit {
				return []PositionEdit{mustReplaceLast(t, "fetch logs // now() is documentation\n| fieldsAdd replay=now()", "now()", `toTimestamp("2026-06-14T10:00:00Z")`)}
			},
			want: "fetch logs // now() is documentation\n| fieldsAdd replay=toTimestamp(\"2026-06-14T10:00:00Z\")",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ApplyPositionEdits(test.original, test.edits(t))
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q\nwant %q", got, test.want)
			}
		})
	}
}

func TestPositionEditorRejectsOverlapAndAmbiguousInsertion(t *testing.T) {
	outer := mustReplace(t, 1, 4, "x")
	inner := mustReplace(t, 2, 3, "y")
	_, err := ApplyPositionEdits("abcdef", []PositionEdit{outer, inner})
	assertPositionError(t, err, "overlap")

	first := mustInsert(t, 6, "x")
	second := mustInsert(t, 6, "y")
	_, err = ApplyPositionEdits("abcdef", []PositionEdit{first, second})
	assertPositionError(t, err, "overlap")
}

func TestPositionEditorAllowsAdjacentReplacements(t *testing.T) {
	got, err := ApplyPositionEdits("abcdef", []PositionEdit{
		mustReplace(t, 1, 2, "X"),
		mustReplace(t, 3, 4, "Y"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "aXYf" {
		t.Fatalf("got %q", got)
	}
}

func TestPositionEditorRejectsSurrogateSplit(t *testing.T) {
	_, err := ApplyPositionEdits("🙂", []PositionEdit{{
		kind: EditReplaceInclusive, startUTF16: 1, endUTF16: 1, replacement: "x", purpose: "split",
	}})
	assertPositionError(t, err, "invalid_start")
}

func TestPositionEditorRequiresASTProvenCommandBoundary(t *testing.T) {
	_, err := InsertAtCommandEnd(&Node{Kind: NodeContainer, Role: "EXPRESSION"}, ", from:x", "bad")
	assertPositionError(t, err, "unproven_insertion")

	forged := PositionEdit{kind: EditInsertCommandEnd, startUTF16: 3, endUTF16: 3, replacement: "x", purpose: "forged"}
	_, err = ApplyPositionEdits("abc", []PositionEdit{forged})
	assertPositionError(t, err, "unproven_insertion")
}

func TestPositionEditorRejectsInvalidUTF8(t *testing.T) {
	_, err := ApplyPositionEdits(string([]byte{0xff}), nil)
	assertPositionError(t, err, "invalid_utf8")
}

func mustReplace(t *testing.T, start, end int, replacement string) PositionEdit {
	t.Helper()
	edit, err := ReplaceSpan(Span{Start: Position{Index: start}, End: Position{Index: end}}, replacement, "replace")
	if err != nil {
		t.Fatal(err)
	}
	return edit
}

func mustInsert(t *testing.T, end int, replacement string) PositionEdit {
	t.Helper()
	command := &Node{
		Kind: NodeContainer, Role: "COMMAND", Path: "test.command",
		Span: &Span{Start: Position{Index: 0}, End: Position{Index: end - 1}},
	}
	edit, err := InsertAtCommandEnd(command, replacement, "insert")
	if err != nil {
		t.Fatal(err)
	}
	return edit
}

func mustReplaceLast(t *testing.T, original, needle, replacement string) PositionEdit {
	t.Helper()
	byteStart := strings.LastIndex(original, needle)
	if byteStart < 0 {
		t.Fatalf("%q not found", needle)
	}
	start := utf16TestLength(original[:byteStart])
	end := start + utf16TestLength(needle) - 1
	return mustReplace(t, start, end, replacement)
}

func utf16TestLength(value string) int {
	length := 0
	for _, r := range value {
		length++
		if r > 0xffff {
			length++
		}
	}
	return length
}

func assertPositionError(t *testing.T, err error, kind string) {
	t.Helper()
	var positionErr *PositionEditError
	if !errors.As(err, &positionErr) {
		t.Fatalf("got %T %v, want PositionEditError", err, err)
	}
	if positionErr.Kind != kind {
		t.Fatalf("got kind %q, want %q", positionErr.Kind, kind)
	}
}
