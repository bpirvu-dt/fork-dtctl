package replay

import (
	"fmt"
	"strings"
)

func collectCommands(ast *AST) ([]commandView, error) {
	var commands []commandView
	err := ast.WalkExecutable(func(node *Node) error {
		if node.Kind != NodeContainer || node.Role != "COMMAND" {
			return nil
		}
		name := strings.ToLower(ownCommandName(node))
		if name == "" {
			return replayError(ErrorASTContract, node, "COMMAND", "A command has no unambiguous command name.", "Update dtctl if the server AST contract changed.")
		}
		commands = append(commands, commandView{node: node, name: name})
		return nil
	})
	return commands, err
}

func ownCommandName(command *Node) string {
	var names []string
	_ = walkOwned(command, func(node *Node) error {
		if node.Kind == NodeTerminal && node.Role == "COMMAND_NAME" {
			names = append(names, node.Canonical)
		}
		return nil
	})
	if len(names) != 1 {
		return ""
	}
	return names[0]
}

func ownFunctionName(function *Node) string {
	var names []string
	_ = walkOwned(function, func(node *Node) error {
		if node != function && node.Role == "FUNCTION" {
			return errStopBranch
		}
		if node.Kind == NodeTerminal && (node.Role == "FUNCTION_NAME" || node.Role == "TIMESERIES_AGGREGATION") {
			names = append(names, node.Canonical)
		}
		return nil
	})
	if len(names) != 1 {
		return ""
	}
	return names[0]
}

var errStopBranch = fmt.Errorf("stop branch")

func walkOwned(root *Node, visit func(*Node) error) error {
	var walk func(*Node, bool) error
	walk = func(node *Node, isRoot bool) error {
		if !isRoot && (node.Role == "COMMAND" || node.Role == "EXECUTION_BLOCK") {
			return nil
		}
		if err := visit(node); err != nil {
			if err == errStopBranch {
				return nil
			}
			return err
		}
		for _, child := range node.Children {
			if err := walk(child, false); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind == AlternativeInfo {
				continue
			}
			if err := walk(node.Alternatives[kind], false); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, true)
}

func collectDirectParameters(root *Node) ([]parameterView, error) {
	var out []parameterView
	var walk func(*Node, bool) error
	walk = func(node *Node, isRoot bool) error {
		if !isRoot && (node.Role == "COMMAND" || node.Role == "EXECUTION_BLOCK") {
			return nil
		}
		if node.Role == "PARAMETER_WITH_KEY" {
			parameter, err := classifyParameter(node)
			if err != nil {
				return err
			}
			out = append(out, parameter)
			return nil
		}
		for _, child := range node.Children {
			if err := walk(child, false); err != nil {
				return err
			}
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind == AlternativeInfo {
				continue
			}
			if err := walk(node.Alternatives[kind], false); err != nil {
				return err
			}
		}
		return nil
	}
	return out, walk(root, true)
}

func classifyParameter(node *Node) (parameterView, error) {
	var userKeys, infoKeys []string
	var walk func(*Node, bool, bool)
	walk = func(candidate *Node, isRoot, inInfo bool) {
		if !isRoot && candidate.Role == "PARAMETER_WITH_KEY" {
			return
		}
		if candidate.Kind != NodeTerminal || candidate.Role != "PARAMETER_KEY" {
			for _, child := range candidate.Children {
				walk(child, false, inInfo)
			}
			for _, kind := range sortedAlternativeKinds(candidate.Alternatives) {
				walk(candidate.Alternatives[kind], false, inInfo || kind == AlternativeInfo)
			}
			return
		}
		if !inInfo && candidate.Span != nil {
			userKeys = append(userKeys, strings.ToLower(candidate.Canonical))
		} else if inInfo {
			infoKeys = append(infoKeys, strings.ToLower(candidate.Canonical))
		}
	}
	walk(node, true, false)
	if len(userKeys) == 1 {
		return parameterView{node: node, key: userKeys[0], keyOrigin: "user"}, nil
	}
	if len(userKeys) > 1 {
		return parameterView{}, replayError(ErrorASTContract, node, "parameter", "A parameter has multiple positioned keys.", "Update dtctl if the server AST contract changed.")
	}
	infoKeys = uniqueStrings(infoKeys)
	if len(infoKeys) == 1 {
		return parameterView{node: node, key: infoKeys[0], keyOrigin: "info"}, nil
	}
	return parameterView{}, replayError(ErrorASTContract, node, "parameter", "A positional parameter has no unambiguous grammar key.", "Update dtctl if the server AST contract changed.")
}

func ownedExecutionBlocks(command *Node) int {
	count := 0
	var walk func(*Node, bool)
	walk = func(node *Node, isRoot bool) {
		if !isRoot && node.Role == "COMMAND" {
			return
		}
		if node.Role == "EXECUTION_BLOCK" {
			count++
			return
		}
		for _, child := range node.Children {
			walk(child, false)
		}
		for _, kind := range sortedAlternativeKinds(node.Alternatives) {
			if kind != AlternativeInfo {
				walk(node.Alternatives[kind], false)
			}
		}
	}
	walk(command, true)
	return count
}

func validateExecutionBlockOwners(node *Node, owner string) error {
	if node.Role == "COMMAND" {
		owner = strings.ToLower(ownCommandName(node))
	}
	if node.Role == "EXECUTION_BLOCK" && owner != "append" && owner != "join" && owner != "lookup" {
		return replayError(ErrorUnsupportedForm, node, "execution block", "A nested execution block is not owned by an approved source-bearing command.", "Use the tested append, join, or lookup form.")
	}
	for _, child := range node.Children {
		if err := validateExecutionBlockOwners(child, owner); err != nil {
			return err
		}
	}
	for _, kind := range sortedAlternativeKinds(node.Alternatives) {
		if kind == AlternativeInfo {
			continue
		}
		if err := validateExecutionBlockOwners(node.Alternatives[kind], owner); err != nil {
			return err
		}
	}
	return nil
}
