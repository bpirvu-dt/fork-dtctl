package replay

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dynatrace-oss/dtctl/sdk/api/query"
)

// Adapt converts an immutable server DQL tree into a fresh dtctl-owned tree.
// The source object and its raw JSON are never modified.
func Adapt(root *query.DQLNode) (*AST, error) {
	if root == nil {
		return nil, &ASTContractError{Path: "root", Problem: "missing server AST"}
	}
	adapted, err := adaptNode(root, "root")
	if err != nil {
		return nil, err
	}
	return &AST{Root: adapted}, nil
}

// AdaptJSON is a fixture-oriented entry point that decodes the SDK type before
// adapting it. Production callers normally use Adapt with Handler.Parse output.
func AdaptJSON(data []byte) (*AST, error) {
	var root query.DQLNode
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("decode server DQL AST: %w", err)
	}
	return Adapt(&root)
}

func adaptNode(external *query.DQLNode, path string) (*Node, error) {
	if external == nil {
		return nil, &ASTContractError{Path: path, Problem: "nil server node"}
	}
	raw := external.RawJSON()
	if len(raw) == 0 {
		return nil, &ASTContractError{Path: path, NodeKind: string(external.NodeType), Problem: "node has no preserved raw JSON"}
	}
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode raw DQL node at %s: %w", path, err)
	}

	node := &Node{
		ExternalKind: string(external.NodeType),
		Optional:     external.IsOptional,
		Path:         path,
	}
	if external.TokenPosition != nil {
		span, unknown, err := adaptSpan(external.TokenPosition, fields["tokenPosition"], path)
		if err != nil {
			return nil, err
		}
		node.Span = span
		node.UnknownFields = append(node.UnknownFields, unknown...)
	}

	allowed := map[string]struct{}{
		"nodeType": {}, "isOptional": {}, "tokenPosition": {},
	}
	switch external.NodeType {
	case query.DQLNodeTypeTerminal:
		node.Kind = NodeTerminal
		if external.Terminal == nil {
			return nil, &ASTContractError{Path: path, NodeKind: node.ExternalKind, Problem: "terminal payload is missing"}
		}
		node.Role = external.Terminal.Type
		node.Canonical = external.Terminal.CanonicalString
		node.MandatoryOnUserOrder = external.Terminal.IsMandatoryOnUserOrder
		allowed["type"] = struct{}{}
		allowed["canonicalString"] = struct{}{}
		allowed["isMandatoryOnUserOrder"] = struct{}{}
	case query.DQLNodeTypeContainer:
		node.Kind = NodeContainer
		if external.Container == nil {
			return nil, &ASTContractError{Path: path, NodeKind: node.ExternalKind, Problem: "container payload is missing"}
		}
		node.Role = external.Container.Type
		allowed["type"] = struct{}{}
		allowed["children"] = struct{}{}
		node.Children = make([]*Node, len(external.Container.Children))
		for i, child := range external.Container.Children {
			adapted, err := adaptNode(child, fmt.Sprintf("%s.children[%d]", path, i))
			if err != nil {
				return nil, err
			}
			node.Children[i] = adapted
		}
	case query.DQLNodeTypeAlternative:
		node.Kind = NodeAlternative
		if external.Alternative == nil {
			return nil, &ASTContractError{Path: path, NodeKind: node.ExternalKind, Problem: "alternative payload is missing"}
		}
		allowed["alternatives"] = struct{}{}
		node.Alternatives = make(map[AlternativeKind]*Node, len(external.Alternative.Alternatives))
		keys := make([]string, 0, len(external.Alternative.Alternatives))
		for key := range external.Alternative.Alternatives {
			keys = append(keys, string(key))
		}
		sort.Strings(keys)
		for _, key := range keys {
			child := external.Alternative.Alternatives[query.AlternativeType(key)]
			adapted, err := adaptNode(child, fmt.Sprintf("%s.alternatives[%s]", path, key))
			if err != nil {
				return nil, err
			}
			node.Alternatives[AlternativeKind(key)] = adapted
		}
	default:
		node.Kind = NodeUnknown
		if role, ok := rawString(fields["type"]); ok {
			node.Role = role
		}
		if canonical, ok := rawString(fields["canonicalString"]); ok {
			node.Canonical = canonical
		}
	}
	for key := range fields {
		if _, ok := allowed[key]; !ok {
			node.UnknownFields = append(node.UnknownFields, key)
		}
	}
	sort.Strings(node.UnknownFields)
	return node, nil
}

func adaptSpan(position *query.TokenPosition, raw json.RawMessage, path string) (*Span, []string, error) {
	if len(raw) == 0 {
		return nil, nil, &ASTContractError{Path: path, Problem: "typed token position is absent from raw node"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("decode token position at %s: %w", path, err)
	}
	unknown := unknownKeys(fields, map[string]struct{}{"start": {}, "end": {}}, "tokenPosition")
	unknown = append(unknown, unknownPositionFields(fields["start"], "tokenPosition.start")...)
	unknown = append(unknown, unknownPositionFields(fields["end"], "tokenPosition.end")...)
	span := &Span{
		Start: Position{Index: position.Start.Index, Line: position.Start.Line, Column: position.Start.Column},
		End:   Position{Index: position.End.Index, Line: position.End.Line, Column: position.End.Column},
	}
	if span.Start.Index < 0 || span.End.Index < span.Start.Index ||
		span.Start.Line < 1 || span.End.Line < 1 || span.Start.Column < 1 || span.End.Column < 1 {
		return nil, nil, &ASTContractError{Path: path, Problem: "invalid inclusive token position"}
	}
	sort.Strings(unknown)
	return span, unknown, nil
}

func unknownPositionFields(raw json.RawMessage, prefix string) []string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return []string{prefix + ".<invalid>"}
	}
	return unknownKeys(fields, map[string]struct{}{"index": {}, "line": {}, "column": {}}, prefix)
}

func unknownKeys(fields map[string]json.RawMessage, allowed map[string]struct{}, prefix string) []string {
	var out []string
	for key := range fields {
		if _, ok := allowed[key]; !ok {
			out = append(out, prefix+"."+key)
		}
	}
	return out
}

func rawString(raw json.RawMessage) (string, bool) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}
