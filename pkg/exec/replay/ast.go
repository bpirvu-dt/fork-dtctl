// Package replay contains the pure compiler used to prepare DQL for a
// historical replay. It has no dependency on the parent exec package.
package replay

import (
	"fmt"
	"sort"
	"strings"
)

// NodeKind is a dtctl-owned structural DQL node kind.
type NodeKind string

const (
	NodeUnknown     NodeKind = "unknown"
	NodeTerminal    NodeKind = "terminal"
	NodeContainer   NodeKind = "container"
	NodeAlternative NodeKind = "alternative"
)

// AlternativeKind identifies one server-provided alternative form.
type AlternativeKind string

const (
	AlternativeCanonical AlternativeKind = "CANONICAL"
	AlternativeUser      AlternativeKind = "USER"
	AlternativeInfo      AlternativeKind = "INFO"
)

// Position is one UTF-16 code-unit position in the original DQL.
type Position struct {
	Index  int
	Line   int
	Column int
}

// Span is an inclusive UTF-16 source span.
type Span struct {
	Start Position
	End   Position
}

// AST is a fresh dtctl-owned view of one server-provided DQL tree.
type AST struct {
	Root *Node
}

// Node is one node in dtctl's internal DQL representation. UnknownFields and
// unknown discriminator values are retained so policy code can fail closed.
type Node struct {
	Kind                 NodeKind
	ExternalKind         string
	Role                 string
	Canonical            string
	Optional             bool
	MandatoryOnUserOrder bool
	Span                 *Span
	Path                 string
	UnknownFields        []string
	Children             []*Node
	Alternatives         map[AlternativeKind]*Node
}

// ASTContractError reports an external AST shape that replay has not
// classified. It is deliberately typed so later phases can route it without
// inspecting error text.
type ASTContractError struct {
	Path        string
	NodeKind    string
	Role        string
	Problem     string
	Fields      []string
	Alternative string
}

func (e *ASTContractError) Error() string {
	location := e.Path
	if location == "" {
		location = "root"
	}
	detail := e.Problem
	if len(e.Fields) > 0 {
		detail += ": " + strings.Join(e.Fields, ", ")
	}
	if e.Alternative != "" {
		detail += ": " + e.Alternative
	}
	if e.Role != "" {
		return fmt.Sprintf("DQL AST contract error at %s (%s %s): %s", location, e.NodeKind, e.Role, detail)
	}
	return fmt.Sprintf("DQL AST contract error at %s (%s): %s", location, e.NodeKind, detail)
}

var knownContainerRoles = map[string]struct{}{
	"CALENDAR_DURATION":    {},
	"COMMAND":              {},
	"COMMAND_SEPARATOR":    {},
	"DURATION":             {},
	"EXECUTION_BLOCK":      {},
	"EXPRESSION":           {},
	"FUNCTION":             {},
	"GROUP":                {},
	"IDENTIFIER":           {},
	"PARAMETERS":           {},
	"PARAMETER_ASSIGNMENT": {},
	"PARAMETER_NAMING":     {},
	"PARAMETER_SEPARATOR":  {},
	"PARAMETER_WITH_KEY":   {},
	"QUERY":                {},
}

var knownTerminalRoles = map[string]struct{}{
	"ASSIGNMENT":              {},
	"BOOLEAN_FALSE":           {},
	"BRACE_CLOSE":             {},
	"BRACE_OPEN":              {},
	"BRACKET_CLOSE":           {},
	"BRACKET_OPEN":            {},
	"COLON":                   {},
	"COMMA":                   {},
	"COMMAND_NAME":            {},
	"DATA_OBJECT":             {},
	"FUNCTION_NAME":           {},
	"INDENT":                  {},
	"LINEBREAK":               {},
	"METRIC_KEY":              {},
	"NUMBER":                  {},
	"OPERATOR":                {},
	"PARAMETER_KEY":           {},
	"PARAMETER_MODIFIER":      {},
	"PARENTHESIS_CLOSE":       {},
	"PARENTHESIS_OPEN":        {},
	"PIPE":                    {},
	"SIMPLE_IDENTIFIER":       {},
	"SMARTSCAPE_EDGE_PATTERN": {},
	"SMARTSCAPE_NODE_PATTERN": {},
	"SMARTSCAPE_NODE_TYPE":    {},
	"SPACE":                   {},
	"STRING":                  {},
	"TIMESERIES_AGGREGATION":  {},
	"TIME_UNIT":               {},
}

// ValidateASTContract rejects every unclassified structural form. Replay
// policy performs additional context-sensitive checks after this structural
// gate.
func ValidateASTContract(ast *AST) error {
	if ast == nil || ast.Root == nil {
		return &ASTContractError{Path: "root", Problem: "missing root node"}
	}
	if ast.Root.Kind != NodeContainer || ast.Root.Role != "QUERY" {
		return contractError(ast.Root, "root must be a QUERY container")
	}
	return validateNodeContract(ast.Root)
}

func validateNodeContract(node *Node) error {
	if node == nil {
		return &ASTContractError{Problem: "nil node"}
	}
	if len(node.UnknownFields) > 0 {
		fields := append([]string(nil), node.UnknownFields...)
		sort.Strings(fields)
		err := contractError(node, "unclassified node fields")
		err.Fields = fields
		return err
	}
	switch node.Kind {
	case NodeTerminal:
		if _, ok := knownTerminalRoles[node.Role]; !ok {
			return contractError(node, "unknown terminal role")
		}
		if len(node.Children) != 0 || len(node.Alternatives) != 0 {
			return contractError(node, "terminal contains child structure")
		}
	case NodeContainer:
		if _, ok := knownContainerRoles[node.Role]; !ok {
			return contractError(node, "unknown container role")
		}
		if len(node.Alternatives) != 0 {
			return contractError(node, "container contains alternatives")
		}
	case NodeAlternative:
		if node.Role != "" || len(node.Children) != 0 {
			return contractError(node, "alternative contains an unexpected role or child list")
		}
		semanticAlternatives := 0
		for kind := range node.Alternatives {
			switch kind {
			case AlternativeInfo:
			case AlternativeCanonical, AlternativeUser:
				semanticAlternatives++
			default:
				err := contractError(node, "unknown alternative kind")
				err.Alternative = string(kind)
				return err
			}
		}
		if semanticAlternatives > 1 {
			return contractError(node, "multiple executable alternative kinds are ambiguous")
		}
	case NodeUnknown:
		return contractError(node, "unknown node type")
	default:
		return contractError(node, "invalid internal node kind")
	}
	for _, child := range node.Children {
		if err := validateNodeContract(child); err != nil {
			return err
		}
	}
	for _, kind := range sortedAlternativeKinds(node.Alternatives) {
		if err := validateNodeContract(node.Alternatives[kind]); err != nil {
			return err
		}
	}
	return nil
}

func contractError(node *Node, problem string) *ASTContractError {
	if node == nil {
		return &ASTContractError{Problem: problem}
	}
	return &ASTContractError{
		Path:     node.Path,
		NodeKind: node.ExternalKind,
		Role:     node.Role,
		Problem:  problem,
	}
}

// Clone returns an independent deep copy of the internal tree.
func (ast *AST) Clone() *AST {
	if ast == nil {
		return nil
	}
	return &AST{Root: cloneNode(ast.Root)}
}

func cloneNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	clone := *node
	clone.UnknownFields = append([]string(nil), node.UnknownFields...)
	if node.Span != nil {
		span := *node.Span
		clone.Span = &span
	}
	clone.Children = make([]*Node, len(node.Children))
	for i, child := range node.Children {
		clone.Children[i] = cloneNode(child)
	}
	if node.Alternatives != nil {
		clone.Alternatives = make(map[AlternativeKind]*Node, len(node.Alternatives))
		for kind, alternative := range node.Alternatives {
			clone.Alternatives[kind] = cloneNode(alternative)
		}
	}
	return &clone
}

// Walk visits every node, including informational alternatives, in stable
// source-tree order.
func (ast *AST) Walk(visit func(*Node) error) error {
	if ast == nil || ast.Root == nil {
		return nil
	}
	return walkNode(ast.Root, true, visit)
}

// WalkExecutable visits executable structure. INFO alternatives describe the
// grammar and are deliberately excluded.
func (ast *AST) WalkExecutable(visit func(*Node) error) error {
	if ast == nil || ast.Root == nil {
		return nil
	}
	return walkNode(ast.Root, false, visit)
}

func walkNode(node *Node, includeInfo bool, visit func(*Node) error) error {
	if err := visit(node); err != nil {
		return err
	}
	for _, child := range node.Children {
		if err := walkNode(child, includeInfo, visit); err != nil {
			return err
		}
	}
	for _, kind := range sortedAlternativeKinds(node.Alternatives) {
		if !includeInfo && kind == AlternativeInfo {
			continue
		}
		if err := walkNode(node.Alternatives[kind], includeInfo, visit); err != nil {
			return err
		}
	}
	return nil
}

func sortedAlternativeKinds(alternatives map[AlternativeKind]*Node) []AlternativeKind {
	kinds := make([]AlternativeKind, 0, len(alternatives))
	for kind := range alternatives {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}
