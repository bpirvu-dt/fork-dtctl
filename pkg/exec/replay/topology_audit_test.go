package replay

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func topologyTokens(ast *AST, role string) []*Node {
	var tokens []*Node
	_ = ast.Walk(func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == role {
			tokens = append(tokens, node)
		}
		return nil
	})
	return tokens
}

func TestTopologyAuditRejectsTamperedValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *AST, *CompileResult)
	}{
		{"bound", func(t *testing.T, ast *AST, _ *CompileResult) {
			for _, token := range topologyTokens(ast, "TIMESTAMP_VALUE") {
				if strings.Contains(token.Canonical, "11:00:00") {
					token.Canonical = `"2026-08-01T09:00:00.000000000Z"`
					return
				}
			}
			t.Fatal("fixture missing bound")
		}},
		{"node selector", func(_ *testing.T, ast *AST, _ *CompileResult) {
			topologyTokens(ast, "SMARTSCAPE_NODE_PATTERN")[0].Canonical = `"HOST"`
		}},
		{"edge selector", func(_ *testing.T, ast *AST, _ *CompileResult) {
			topologyTokens(ast, "SMARTSCAPE_EDGE_PATTERN")[0].Canonical = `"runs_on"`
		}},
		{"unapproved edge selector", func(_ *testing.T, ast *AST, _ *CompileResult) {
			topologyTokens(ast, "SMARTSCAPE_EDGE_PATTERN")[0].Canonical = `"contains"`
		}},
		{"missing binding", func(_ *testing.T, _ *AST, result *CompileResult) { result.Traversals = nil }},
		{"binding window", func(_ *testing.T, _ *AST, result *CompileResult) {
			result.Traversals[0].Effective.Start = result.Traversals[0].Effective.Start.Add(time.Second)
		}},
		{"binding feeder", func(_ *testing.T, _ *AST, result *CompileResult) { result.Traversals[0].FeederOrdinal++ }},
		{"physical range", func(_ *testing.T, _ *AST, result *CompileResult) {
			result.Sources[0].PhysicalRange.End = result.Sources[0].PhysicalRange.End.Add(time.Second)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := topologyFixtureInput(t, "traverse-window")
			result, err := Compile(input)
			if err != nil {
				t.Fatal(err)
			}
			ast := loadSDKFixture(t, "topology/fixtures/traverse-window/validation-parse.json")
			test.mutate(t, ast, &result)
			if _, err := Audit(AuditInput{ValidationAST: ast, Compilation: result, SourcePolicy: input.SourcePolicy}); err == nil {
				t.Fatal("tampered AST passed audit")
			}
		})
	}
}

func TestTopologyFailsClosedOnPlacementAndKeys(t *testing.T) {
	for _, role := range []string{"SMARTSCAPE_NODE_TYPE", "SMARTSCAPE_NODE_PATTERN", "SMARTSCAPE_EDGE_PATTERN", "SMARTSCAPE_EDGE_TYPE"} {
		t.Run(role, func(t *testing.T) {
			input := topologyFixtureInput(t, "traverse-fields-add")
			commands, err := collectCommands(input.AST)
			if err != nil {
				t.Fatal(err)
			}
			for _, command := range commands {
				if command.name == "fieldsadd" {
					tokens := terminalsWithRole(command.node, "NUMBER")
					if len(tokens) == 0 {
						t.Fatal("fixture has no fieldsAdd number")
					}
					tokens[0].Role, tokens[0].Canonical = role, `"calls"`
				}
			}
			_, err = Compile(input)
			var rejection *ReplayError
			if !errors.As(err, &rejection) || rejection.Code != ErrorASTContract {
				t.Fatalf("placement=%v", err)
			}
		})
	}
	for _, key := range []string{"unknownOption", "from", "to", "timeframe"} {
		t.Run("traverse "+key, func(t *testing.T) {
			input := topologyFixtureInput(t, "traverse-params")
			keys := topologyTokens(input.AST, "PARAMETER_KEY")
			found := false
			for _, token := range keys {
				if token.Canonical == "direction" {
					token.Canonical = key
					found = true
				}
			}
			if !found {
				t.Fatal("fixture missing direction key")
			}
			_, err := Compile(input)
			var rejection *ReplayError
			if !errors.As(err, &rejection) || rejection.Code != ErrorUnsupportedForm {
				t.Fatalf("key=%v", err)
			}
			if key != "unknownOption" && rejection.Construct != "traverse" {
				t.Fatalf("timeframe lacks command: %v", err)
			}
		})
	}
	for _, key := range []string{"from", "unknownOption"} {
		t.Run("source "+key, func(t *testing.T) {
			input := topologyFixtureInput(t, "nodes-window")
			for _, token := range topologyTokens(input.AST, "PARAMETER_KEY") {
				if token.Canonical == "to" {
					token.Canonical = key
				}
			}
			if _, err := Compile(input); err == nil {
				t.Fatal("unsupported or repeated key accepted")
			}
		})
	}
}
