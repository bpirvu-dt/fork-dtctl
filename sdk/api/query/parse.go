package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// QueryOptions contains parser options understood by the Query API. These
// options are intended for Dynatrace internal services; ordinary callers
// should normally leave the map empty.
type QueryOptions map[string]string

// ParseRequest represents a DQL query parsing request body.
type ParseRequest struct {
	Query        string       `json:"query"`
	Locale       string       `json:"locale,omitempty"`
	Timezone     string       `json:"timezone,omitempty"`
	QueryOptions QueryOptions `json:"queryOptions,omitempty"`
}

// DQLNodeType identifies the structural variant of a DQL node. The service may
// add values over time, so callers must not assume that the constants below are
// exhaustive.
type DQLNodeType string

const (
	// DQLNodeTypeTerminal identifies a node containing one canonical token.
	DQLNodeTypeTerminal DQLNodeType = "TERMINAL"
	// DQLNodeTypeContainer identifies a node containing an ordered child list.
	DQLNodeTypeContainer DQLNodeType = "CONTAINER"
	// DQLNodeTypeAlternative identifies a node containing equivalent forms.
	DQLNodeTypeAlternative DQLNodeType = "ALTERNATIVE"
)

// AlternativeType identifies one form offered by an alternative DQL node. The
// service may add values over time, so this string type deliberately accepts
// values beyond the constants below.
type AlternativeType string

const (
	// AlternativeTypeCanonical is the form used for the canonical query.
	AlternativeTypeCanonical AlternativeType = "CANONICAL"
	// AlternativeTypeUser is a valid non-canonical form supplied by the user.
	AlternativeTypeUser AlternativeType = "USER"
	// AlternativeTypeInfo contains explanatory structure that is not emitted.
	AlternativeTypeInfo AlternativeType = "INFO"
)

// PositionInfo is one exact location in the submitted DQL. Index follows the
// Query API's source-coordinate contract; replay's UTF-16 interpretation is a
// concern of the adapter above this SDK package.
type PositionInfo struct {
	Index  int `json:"index"`
	Line   int `json:"line"`
	Column int `json:"column"`

	raw json.RawMessage
}

// TokenPosition is an inclusive source span in the submitted DQL.
type TokenPosition struct {
	Start PositionInfo `json:"start"`
	End   PositionInfo `json:"end"`

	raw json.RawMessage
}

// DQLTerminalNode contains the fields specific to a terminal DQL node. Type is
// intentionally a string because the service's token-role set may grow.
type DQLTerminalNode struct {
	Type                   string `json:"type"`
	CanonicalString        string `json:"canonicalString"`
	IsMandatoryOnUserOrder bool   `json:"isMandatoryOnUserOrder"`
}

// DQLContainerNode contains the fields specific to a container DQL node. Type
// is intentionally a string because the service's container-role set may grow.
type DQLContainerNode struct {
	Type     string     `json:"type"`
	Children []*DQLNode `json:"children"`
}

// DQLAlternativeNode contains the forms offered by an alternative DQL node.
// Unknown alternative keys remain present in Alternatives.
type DQLAlternativeNode struct {
	Alternatives map[AlternativeType]*DQLNode `json:"alternatives"`
}

// DQLNode is one node in the structured DQL tree returned by query:parse.
// Exactly one of Terminal, Container, and Alternative is populated for a known
// NodeType. An unrecognized NodeType is retained without populating a known
// variant, and its complete wire representation remains available through
// RawJSON so a higher-level adapter can fail closed.
type DQLNode struct {
	NodeType      DQLNodeType    `json:"nodeType"`
	IsOptional    bool           `json:"isOptional"`
	TokenPosition *TokenPosition `json:"tokenPosition,omitempty"`

	Terminal    *DQLTerminalNode    `json:"-"`
	Container   *DQLContainerNode   `json:"-"`
	Alternative *DQLAlternativeNode `json:"-"`

	raw json.RawMessage
}

// ParseResponse is the root DQL node returned by query:parse.
type ParseResponse = DQLNode

// RawJSON returns a defensive copy of the complete JSON object received for
// this node, including fields that the typed model does not classify.
func (n *DQLNode) RawJSON() json.RawMessage {
	if n == nil || len(n.raw) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), n.raw...)
}

// Parse requests the server's structured canonical DQL tree. The replay layer
// may use that tree as its AST input, but this SDK method applies no replay
// policy and performs a real HTTP request on every call.
func (h *Handler) Parse(ctx context.Context, req ParseRequest) (*ParseResponse, error) {
	var result ParseResponse

	httpReq := h.client.HTTP().R().SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		SetResult(&result)
	h.applyHeaders(httpReq)

	resp, err := httpReq.Post(basePath + ":parse")
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}
	if resp.IsError() {
		return nil, parseError(resp.StatusCode(), resp.Body())
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("query parse returned unexpected status code %d", resp.StatusCode())
	}
	if result.NodeType == "" || len(result.raw) == 0 {
		return nil, fmt.Errorf("query parse returned a structurally incomplete DQL tree")
	}

	return &result, nil
}

// UnmarshalJSON decodes a DQL node according to its discriminator while
// retaining the complete node object for compatibility checks above the SDK.
func (n *DQLNode) UnmarshalJSON(data []byte) error {
	fields, err := decodeJSONObject(data, "DQL node")
	if err != nil {
		return err
	}

	nodeTypeText, err := requiredStringField(fields, "nodeType", "DQL node")
	if err != nil {
		return err
	}
	if nodeTypeText == "" {
		return fmt.Errorf("DQL node field %q must not be empty", "nodeType")
	}
	isOptional, err := requiredBoolField(fields, "isOptional", "DQL node")
	if err != nil {
		return err
	}

	decoded := DQLNode{
		NodeType:   DQLNodeType(nodeTypeText),
		IsOptional: isOptional,
		raw:        append(json.RawMessage(nil), data...),
	}
	if rawPosition, ok := fields["tokenPosition"]; ok {
		if isJSONNull(rawPosition) {
			return fmt.Errorf("DQL node field %q must be an object", "tokenPosition")
		}
		var position TokenPosition
		if err := json.Unmarshal(rawPosition, &position); err != nil {
			return fmt.Errorf("DQL node field %q: %w", "tokenPosition", err)
		}
		decoded.TokenPosition = &position
	}

	switch decoded.NodeType {
	case DQLNodeTypeTerminal:
		role, err := requiredNonEmptyStringField(fields, "type", "terminal DQL node")
		if err != nil {
			return err
		}
		canonical, err := requiredStringField(fields, "canonicalString", "terminal DQL node")
		if err != nil {
			return err
		}
		mandatory, err := requiredBoolField(fields, "isMandatoryOnUserOrder", "terminal DQL node")
		if err != nil {
			return err
		}
		decoded.Terminal = &DQLTerminalNode{
			Type:                   role,
			CanonicalString:        canonical,
			IsMandatoryOnUserOrder: mandatory,
		}
	case DQLNodeTypeContainer:
		role, err := requiredNonEmptyStringField(fields, "type", "container DQL node")
		if err != nil {
			return err
		}
		childrenRaw, err := requiredField(fields, "children", "container DQL node")
		if err != nil {
			return err
		}
		if isJSONNull(childrenRaw) {
			return fmt.Errorf("container DQL node field %q must be an array", "children")
		}
		var children []*DQLNode
		if err := json.Unmarshal(childrenRaw, &children); err != nil {
			return fmt.Errorf("container DQL node field %q: %w", "children", err)
		}
		for i, child := range children {
			if child == nil {
				return fmt.Errorf("container DQL node child %d must be an object", i)
			}
		}
		decoded.Container = &DQLContainerNode{Type: role, Children: children}
	case DQLNodeTypeAlternative:
		alternativesRaw, err := requiredField(fields, "alternatives", "alternative DQL node")
		if err != nil {
			return err
		}
		if isJSONNull(alternativesRaw) {
			return fmt.Errorf("alternative DQL node field %q must be an object", "alternatives")
		}
		var alternatives map[AlternativeType]*DQLNode
		if err := json.Unmarshal(alternativesRaw, &alternatives); err != nil {
			return fmt.Errorf("alternative DQL node field %q: %w", "alternatives", err)
		}
		for name, alternative := range alternatives {
			if alternative == nil {
				return fmt.Errorf("alternative DQL node form %q must be an object", name)
			}
		}
		decoded.Alternative = &DQLAlternativeNode{Alternatives: alternatives}
	}

	*n = decoded
	return nil
}

// MarshalJSON re-encodes the typed fields while merging them into the original
// node object. This preserves unclassified fields during a decode/encode round
// trip and still reflects deliberate edits to the exported typed fields.
func (n DQLNode) MarshalJSON() ([]byte, error) {
	fields := make(map[string]json.RawMessage)
	if len(n.raw) > 0 {
		var err error
		fields, err = decodeJSONObject(n.raw, "raw DQL node")
		if err != nil {
			return nil, err
		}
	}
	if n.NodeType == "" {
		return nil, fmt.Errorf("DQL node field %q must not be empty", "nodeType")
	}
	if err := setJSONField(fields, "nodeType", n.NodeType); err != nil {
		return nil, err
	}
	if err := setJSONField(fields, "isOptional", n.IsOptional); err != nil {
		return nil, err
	}
	if n.TokenPosition == nil {
		delete(fields, "tokenPosition")
	} else if err := setJSONField(fields, "tokenPosition", n.TokenPosition); err != nil {
		return nil, err
	}

	switch n.NodeType {
	case DQLNodeTypeTerminal:
		if n.Terminal == nil {
			return nil, fmt.Errorf("terminal DQL node has no terminal fields")
		}
		if err := setJSONField(fields, "type", n.Terminal.Type); err != nil {
			return nil, err
		}
		if err := setJSONField(fields, "canonicalString", n.Terminal.CanonicalString); err != nil {
			return nil, err
		}
		if err := setJSONField(fields, "isMandatoryOnUserOrder", n.Terminal.IsMandatoryOnUserOrder); err != nil {
			return nil, err
		}
	case DQLNodeTypeContainer:
		if n.Container == nil {
			return nil, fmt.Errorf("container DQL node has no container fields")
		}
		if err := setJSONField(fields, "type", n.Container.Type); err != nil {
			return nil, err
		}
		if err := setJSONField(fields, "children", n.Container.Children); err != nil {
			return nil, err
		}
	case DQLNodeTypeAlternative:
		if n.Alternative == nil {
			return nil, fmt.Errorf("alternative DQL node has no alternative fields")
		}
		if err := setJSONField(fields, "alternatives", n.Alternative.Alternatives); err != nil {
			return nil, err
		}
	}

	return json.Marshal(fields)
}

func (p *PositionInfo) UnmarshalJSON(data []byte) error {
	fields, err := decodeJSONObject(data, "DQL position")
	if err != nil {
		return err
	}
	index, err := requiredIntField(fields, "index", "DQL position")
	if err != nil {
		return err
	}
	line, err := requiredIntField(fields, "line", "DQL position")
	if err != nil {
		return err
	}
	column, err := requiredIntField(fields, "column", "DQL position")
	if err != nil {
		return err
	}
	*p = PositionInfo{Index: index, Line: line, Column: column, raw: append(json.RawMessage(nil), data...)}
	return nil
}

func (p PositionInfo) MarshalJSON() ([]byte, error) {
	fields := make(map[string]json.RawMessage)
	if len(p.raw) > 0 {
		var err error
		fields, err = decodeJSONObject(p.raw, "raw DQL position")
		if err != nil {
			return nil, err
		}
	}
	if err := setJSONField(fields, "index", p.Index); err != nil {
		return nil, err
	}
	if err := setJSONField(fields, "line", p.Line); err != nil {
		return nil, err
	}
	if err := setJSONField(fields, "column", p.Column); err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func (p *TokenPosition) UnmarshalJSON(data []byte) error {
	fields, err := decodeJSONObject(data, "DQL token position")
	if err != nil {
		return err
	}
	startRaw, err := requiredField(fields, "start", "DQL token position")
	if err != nil {
		return err
	}
	endRaw, err := requiredField(fields, "end", "DQL token position")
	if err != nil {
		return err
	}
	if isJSONNull(startRaw) || isJSONNull(endRaw) {
		return fmt.Errorf("DQL token position start and end must be objects")
	}
	var start, end PositionInfo
	if err := json.Unmarshal(startRaw, &start); err != nil {
		return fmt.Errorf("DQL token position start: %w", err)
	}
	if err := json.Unmarshal(endRaw, &end); err != nil {
		return fmt.Errorf("DQL token position end: %w", err)
	}
	*p = TokenPosition{Start: start, End: end, raw: append(json.RawMessage(nil), data...)}
	return nil
}

func (p TokenPosition) MarshalJSON() ([]byte, error) {
	fields := make(map[string]json.RawMessage)
	if len(p.raw) > 0 {
		var err error
		fields, err = decodeJSONObject(p.raw, "raw DQL token position")
		if err != nil {
			return nil, err
		}
	}
	if err := setJSONField(fields, "start", p.Start); err != nil {
		return nil, err
	}
	if err := setJSONField(fields, "end", p.End); err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func decodeJSONObject(data []byte, description string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("decode %s: %w", description, err)
	}
	if fields == nil {
		return nil, fmt.Errorf("%s must be a JSON object", description)
	}
	return fields, nil
}

func requiredField(fields map[string]json.RawMessage, name, description string) (json.RawMessage, error) {
	value, ok := fields[name]
	if !ok {
		return nil, fmt.Errorf("%s is missing required field %q", description, name)
	}
	return value, nil
}

func requiredStringField(fields map[string]json.RawMessage, name, description string) (string, error) {
	raw, err := requiredField(fields, name, description)
	if err != nil {
		return "", err
	}
	if isJSONNull(raw) {
		return "", fmt.Errorf("%s field %q must be a string", description, name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s field %q must be a string: %w", description, name, err)
	}
	return value, nil
}

func requiredNonEmptyStringField(fields map[string]json.RawMessage, name, description string) (string, error) {
	value, err := requiredStringField(fields, name, description)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("%s field %q must not be empty", description, name)
	}
	return value, nil
}

func requiredBoolField(fields map[string]json.RawMessage, name, description string) (bool, error) {
	raw, err := requiredField(fields, name, description)
	if err != nil {
		return false, err
	}
	if isJSONNull(raw) {
		return false, fmt.Errorf("%s field %q must be a boolean", description, name)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s field %q must be a boolean: %w", description, name, err)
	}
	return value, nil
}

func requiredIntField(fields map[string]json.RawMessage, name, description string) (int, error) {
	raw, err := requiredField(fields, name, description)
	if err != nil {
		return 0, err
	}
	if isJSONNull(raw) {
		return 0, fmt.Errorf("%s field %q must be an integer", description, name)
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("%s field %q must be an integer: %w", description, name, err)
	}
	return value, nil
}

func setJSONField(fields map[string]json.RawMessage, name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode DQL field %q: %w", name, err)
	}
	fields[name] = raw
	return nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
