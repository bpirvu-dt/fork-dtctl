package replay

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const topologyMinimumWindow = time.Minute

// TraversalBinding ties a traversal to its source within one execution block.
// It describes a window, not a historical snapshot of fields or relationships.
type TraversalBinding struct {
	Path          string
	FeederPath    string
	FeederOrdinal int
	Effective     Interval
}

func isTopologyCommand(name string) bool {
	return name == "smartscapenodes" || name == "smartscapeedges" || name == "traverse"
}

func topologyFeeder(ast *AST, traversal *Node) (*Node, error) {
	commands, index, ok := directCommandSequence(ast.Root, traversal)
	if ok {
		for i := index - 1; i >= 0; i-- {
			name := strings.ToLower(ownCommandName(commands[i]))
			switch name {
			case "smartscapenodes", "smartscapeedges":
				return commands[i], nil
			case "traverse", "filter", "filterout", "fields", "fieldsadd", "fieldsremove", "fieldsrename", "sort", "dedup", "limit", "expand", "parse", "summarize":
				// This exact subset of the pipeline allowlist introduces no source.
				// A nested source, even under a known command, must break the chain.
				if ownedExecutionBlocks(commands[i]) == 0 {
					continue
				}
			}
			break
		}
	}
	return nil, replayError(ErrorUnsupportedForm, traversal, "traverse",
		"traverse has no topology feeder in the same execution block through source-free pipeline commands.",
		"Place traverse after smartscapeNodes or smartscapeEdges through source-free pipeline commands.")
}

func validateTopologyCommand(ast *AST, command commandView) error {
	params, err := collectDirectParameters(command.node)
	if err != nil {
		return err
	}
	if command.name == "traverse" {
		for _, parameter := range params {
			if parameter.key == "from" || parameter.key == "to" || parameter.key == "timeframe" {
				return replayError(ErrorUnsupportedForm, command.node, "traverse", "traverse cannot carry its own timeframe.",
					"Put from, to, or timeframe on the feeding smartscapeNodes or smartscapeEdges command.")
			}
		}
		if _, err := topologyFeeder(ast, command.node); err != nil {
			return err
		}
		if err := validateParameterKeys(params, "edgetypes", "targettypes", "direction", "fieldskeep", "nodeid"); err != nil {
			return err
		}
		byKey := parametersByKey(params)
		for key, values := range byKey {
			if len(values) > 1 {
				return replayError(ErrorUnsupportedForm, command.node, "traverse", fmt.Sprintf("traverse repeats the %q parameter.", key), "Provide each traverse parameter at most once.")
			}
		}
		if len(byKey["edgetypes"]) != 1 {
			return topologyEdgeError(command.node, command.name)
		}
		return validateTopologyEdges(byKey["edgetypes"][0].node, command.name)
	}
	if err := validateParameterKeys(params, "type", "from", "to", "timeframe"); err != nil {
		return err
	}
	byKey := parametersByKey(params)
	if len(byKey["type"]) == 0 {
		return replayError(ErrorUnsupportedForm, command.node, command.name, "A topology source must have a type selector.", "Provide a node or edge type selector.")
	}
	if command.name == "smartscapeedges" {
		for _, parameter := range byKey["type"] {
			if err := validateTopologyEdges(parameter.node, command.name); err != nil {
				return err
			}
		}
	}
	return nil
}

func topologyEdgeError(node *Node, command string) *ReplayError {
	return replayError(ErrorUnsupportedForm, node, command,
		fmt.Sprintf("%s supports only calls and runs_on edge types; wildcards and unresolvable selectors are not supported.", command),
		"Use only calls or runs_on edge types.")
}

// Validate the whole selector subtree. Merely finding one approved token is
// insufficient: an expression or a second, unknown type must not slip through.
func validateTopologyEdges(root *Node, command string) error {
	count := 0
	err := walkOwned(root, func(node *Node) error {
		if node.Kind != NodeTerminal || insignificantTerminal(node) {
			return nil
		}
		switch node.Role {
		case "SMARTSCAPE_EDGE_TYPE":
			if node.Canonical != "calls" && node.Canonical != "runs_on" {
				return topologyEdgeError(node, command)
			}
			count++
		case "SMARTSCAPE_EDGE_PATTERN":
			value, err := strconv.Unquote(node.Canonical)
			if err != nil || (value != "calls" && value != "runs_on") {
				return topologyEdgeError(node, command)
			}
			count++
		case "PARAMETER_KEY":
			switch strings.ToLower(node.Canonical) {
			case "type", "edgetypes", "edgetype":
			default:
				return topologyEdgeError(node, command)
			}
		default:
			return topologyEdgeError(node, command)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return topologyEdgeError(root, command)
	}
	return nil
}

func bindTraversals(ast *AST, sources []*sourceAnalysis, compiled []SourceCompilation) ([]TraversalBinding, error) {
	commands, err := collectCommands(ast)
	if err != nil {
		return nil, err
	}
	var bindings []TraversalBinding
	for _, command := range commands {
		if command.name != "traverse" {
			continue
		}
		feeder, err := topologyFeeder(ast, command.node)
		if err != nil {
			return nil, err
		}
		found := false
		for index, source := range sources {
			if source.node != feeder {
				continue
			}
			if index >= len(compiled) || compiled[index].Effective == nil || source.Class != SourceTopology {
				return nil, auditError(command.node, "traverse feeder has no compiled topology window")
			}
			bindings = append(bindings, TraversalBinding{
				Path: command.node.Path, FeederPath: feeder.Path, FeederOrdinal: source.Ordinal,
				Effective: *compiled[index].Effective,
			})
			found = true
			break
		}
		if !found {
			return nil, auditError(command.node, "traverse feeder was not classified as a source")
		}
	}
	return bindings, nil
}
