package replay

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestAdapterAcceptsCompleteRealFixtureCorpus(t *testing.T) {
	fixtureRoot := filepath.Join("..", "..", "..", "sdk", "api", "query", "testdata")
	var paths []string
	err := filepath.WalkDir(fixtureRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && (entry.Name() == "parse.json" || entry.Name() == "validation-parse.json") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk fixture corpus: %v", err)
	}
	sort.Strings(paths)
	if len(paths) != 225 {
		t.Fatalf("fixture count = %d, want 225", len(paths))
	}

	adapted, captures := 0, 0
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		body, ok, err := fixtureAST(raw)
		if err != nil {
			t.Fatalf("classify %s: %v", path, err)
		}
		if !ok {
			captures++
			continue
		}
		ast, err := AdaptJSON(body)
		if err != nil {
			t.Fatalf("adapt %s: %v", path, err)
		}
		if err := ValidateASTContract(ast); err != nil {
			t.Fatalf("validate %s: %v", path, err)
		}
		adapted++
	}
	if adapted != 207 || captures != 18 {
		t.Fatalf("adapted = %d, non-AST captures = %d; want 207 and 18", adapted, captures)
	}
}

func TestAdapterReturnsFreshInternalTrees(t *testing.T) {
	raw := []byte(`{
      "nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":[
        {"nodeType":"TERMINAL","type":"COMMAND_NAME","canonicalString":"data","isOptional":false,"isMandatoryOnUserOrder":false,
         "tokenPosition":{"start":{"index":0,"line":1,"column":1},"end":{"index":3,"line":1,"column":4}}}
      ]}`)
	first, err := AdaptJSON(raw)
	if err != nil {
		t.Fatalf("first AdaptJSON: %v", err)
	}
	second, err := AdaptJSON(raw)
	if err != nil {
		t.Fatalf("second AdaptJSON: %v", err)
	}
	first.Root.Children[0].Canonical = "changed"
	first.Root.Children[0].Span.Start.Index = 99
	if second.Root.Children[0].Canonical != "data" || second.Root.Children[0].Span.Start.Index != 0 {
		t.Fatal("one adapted working tree contaminated another")
	}
	clone := second.Clone()
	clone.Root.Children[0].Canonical = "clone-change"
	if second.Root.Children[0].Canonical != "data" {
		t.Fatal("Clone did not return an independent tree")
	}
}

func TestAdapterPreservesUnknownFormsForTypedRejection(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "node type",
			raw:  queryWithChild(`{"nodeType":"FUTURE_SOURCE","isOptional":false}`),
			want: "unknown node type",
		},
		{
			name: "container role",
			raw:  `{"nodeType":"CONTAINER","type":"FUTURE_QUERY","isOptional":false,"children":[]}`,
			want: "root must be a QUERY container",
		},
		{
			name: "terminal role",
			raw:  queryWithChild(`{"nodeType":"TERMINAL","type":"FUTURE_TOKEN","canonicalString":"x","isOptional":false,"isMandatoryOnUserOrder":false}`),
			want: "unknown terminal role",
		},
		{
			name: "alternative kind",
			raw:  queryWithChild(`{"nodeType":"ALTERNATIVE","isOptional":false,"alternatives":{"FUTURE":{"nodeType":"TERMINAL","type":"STRING","canonicalString":"x","isOptional":false,"isMandatoryOnUserOrder":false}}}`),
			want: "unknown alternative kind",
		},
		{
			name: "unclassified semantic field",
			raw:  queryWithChild(`{"nodeType":"TERMINAL","type":"COMMAND_NAME","canonicalString":"data","isOptional":false,"isMandatoryOnUserOrder":false,"sourceSelector":{"table":"future.records"}}`),
			want: "unclassified node fields",
		},
		{
			name: "unclassified position field",
			raw:  queryWithChild(`{"nodeType":"TERMINAL","type":"COMMAND_NAME","canonicalString":"data","isOptional":false,"isMandatoryOnUserOrder":false,"tokenPosition":{"start":{"index":0,"line":1,"column":1,"encoding":"future"},"end":{"index":3,"line":1,"column":4}}}`),
			want: "unclassified node fields",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := AdaptJSON([]byte(tt.raw))
			if err != nil {
				t.Fatalf("AdaptJSON: %v", err)
			}
			err = ValidateASTContract(ast)
			if err == nil || !contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want text %q", err, tt.want)
			}
			var contractErr *ASTContractError
			if !errors.As(err, &contractErr) {
				t.Fatalf("error type = %T, want *ASTContractError", err)
			}
		})
	}
}

func queryWithChild(child string) string {
	return `{"nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":[` + child + `]}`
}

func fixtureAST(raw []byte) (json.RawMessage, bool, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, false, err
	}
	if _, ok := root["nodeType"]; ok {
		return raw, true, nil
	}
	if body, ok := root["response_body"]; ok {
		var response map[string]json.RawMessage
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, false, err
		}
		if _, ok := response["nodeType"]; ok {
			return body, true, nil
		}
		return nil, false, nil
	}
	return nil, false, nil
}

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
