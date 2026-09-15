package exec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
)

// Enumerate the production constructors, not a handwritten list of messages.
// The golden records an explicit public outcome for every site. A new site,
// remedy, reason, or disclosure choice changes the golden and requires review.
// Real compiler/executor tests separately verify the constructor paths. This
// inventory also covers defensive validator branches that valid inputs cannot
// normally reach. Typed errors and wrappers both pass through the real formatter.
func TestReplayMessageClassificationGolden(t *testing.T) {
	paths, err := filepath.Glob("replay/*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]*ast.File)
	constants := make(map[string]ast.Expr)
	constructors := make(map[string]*ast.FuncDecl)
	fset := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = file
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "queryTimeframeError" {
				constructors[fn.Name.Name] = fn
			}
			if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.CONST {
				for _, spec := range group.Specs {
					value := spec.(*ast.ValueSpec)
					if len(value.Names) == 1 && len(value.Values) == 1 {
						constants[value.Names[0].Name] = value.Values[0]
					}
				}
			}
		}
	}
	var rendered strings.Builder
	encoder := json.NewEncoder(&rendered)
	encoder.SetEscapeHTML(false)
	for _, path := range paths {
		file := files[path]
		if file == nil {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "replayError" || fn.Name.Name == "queryTimeframeError" || fn.Name.Name == "resultError" {
				continue
			}
			for _, sample := range []string{"synthetic", "replay internal", "2026-06-14T10:00:00Z"} {
				inventory := replayErrorInventory{t: t, constants: constants, constructors: constructors, sample: sample}
				number := 0
				inventory.visit = func(cause error, category replayErrorCategory) {
					number++
					info := restrictedInfo()
					info.Output = &output.ReplayMetadata{VirtualNow: "2026-06-14T10:00:00Z"}
					message := restrictedMessageForInfo(category, cause, info, false)
					wrapped := restrictedMessageForInfo(category, fmt.Errorf("private wrapper: %w", cause), info, false)
					if message != wrapped {
						t.Errorf("%s/%s/%d: wrapping changed public output", path, fn.Name.Name, number)
					}
					outcome := "guidance"
					if message == restrictedQueryInvalidMessage || message == restrictedValidationFallbackMessage {
						outcome = "generic"
					}
					if err := encoder.Encode(struct {
						Site, Sample, Outcome, Message, Detail string
					}{fmt.Sprintf("%s/%s/%d", filepath.Base(path), fn.Name.Name, number), sample, outcome, message, cause.Error()}); err != nil {
						t.Fatal(err)
					}
				}
				inventory.walk(fn.Body, map[string]any{
					"index": 0, "observed.NaturalInterval": time.Minute, "*contract.DeclaredNaturalInterval": 5 * time.Minute,
				})
			}
		}
	}
	if rendered.Len() == 0 {
		t.Fatal("no production error sites found")
	}
	testutil.AssertGolden(t, "replay/message-classification", rendered.String())
}

type replayErrorInventory struct {
	t            *testing.T
	constants    map[string]ast.Expr
	constructors map[string]*ast.FuncDecl
	sample       string
	visit        func(error, replayErrorCategory)
}

func (i *replayErrorInventory) walk(node ast.Node, values map[string]any) {
	if node == nil {
		return
	}
	if block, ok := node.(*ast.BlockStmt); ok {
		local := maps.Clone(values)
		for _, stmt := range block.List {
			i.walk(stmt, local)
		}
		return
	}
	if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
		if name, ok := assignment.Lhs[0].(*ast.Ident); ok {
			values[name.Name] = i.value(assignment.Rhs[0], values)
		}
	}
	if call, ok := node.(*ast.CallExpr); ok {
		if category, cause := i.errorValue(call, values); cause != nil {
			i.visit(cause, category)
			return
		}
	}
	if literal, ok := node.(*ast.CompositeLit); ok {
		if name, ok := literal.Type.(*ast.Ident); ok && (name.Name == "ReplayError" || name.Name == "ResultContractError") {
			i.t.Fatalf("new direct %s construction must be included in the message inventory", name.Name)
		}
	}
	ast.Inspect(node, func(child ast.Node) bool {
		if child == nil || child == node {
			return true
		}
		i.walk(child, values)
		return false
	})
}

func (i *replayErrorInventory) errorValue(call *ast.CallExpr, values map[string]any) (replayErrorCategory, error) {
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "withPublicMessage" {
		base, ok := selector.X.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			i.t.Fatal("unsupported public-message construction")
		}
		category, cause := i.errorValue(base, values)
		compilerErr, ok := cause.(*execreplay.ReplayError)
		if !ok {
			i.t.Fatal("public message has no compiler error")
		}
		compilerErr.PublicMessage = i.text(call.Args[0], values)
		return category, compilerErr
	}
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", nil
	}
	switch name.Name {
	case "queryTimeframeError":
		// Read the helper's actual return expression so changing its public
		// wording also requires an explicit golden review.
		helper := i.constructors[name.Name]
		if helper == nil || helper.Body == nil || len(helper.Body.List) != 1 {
			i.t.Fatal("timeframe constructor changed; update the message inventory")
		}
		returned, ok := helper.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(returned.Results) != 1 {
			i.t.Fatal("timeframe constructor has no single return value")
		}
		base, ok := returned.Results[0].(*ast.CallExpr)
		if !ok {
			i.t.Fatal("timeframe constructor has no error construction")
		}
		local := maps.Clone(values)
		argument := 0
		for _, parameter := range helper.Type.Params.List {
			for _, name := range parameter.Names {
				if argument >= len(call.Args) {
					i.t.Fatal("timeframe constructor is missing arguments")
				}
				local[name.Name] = i.value(call.Args[argument], values)
				argument++
			}
		}
		if argument != len(call.Args) {
			i.t.Fatal("timeframe constructor has extra arguments")
		}
		return i.errorValue(base, local)
	case "replayError":
		if len(call.Args) != 5 {
			i.t.Fatal("compiler constructor changed; update the message inventory")
		}
		cause := &execreplay.ReplayError{
			Code: execreplay.ErrorCode(i.text(call.Args[0], values)), Construct: i.text(call.Args[2], values),
			Message: i.text(call.Args[3], values), Remedy: i.text(call.Args[4], values),
		}
		return replayErrorPrepare, cause
	case "resultError":
		if len(call.Args) != 3 {
			i.t.Fatal("validator constructor changed; update the message inventory")
		}
		return replayErrorValidation, &execreplay.ResultContractError{Reason: i.text(call.Args[1], values), PublicReason: i.text(call.Args[2], values)}
	}
	return "", nil
}

func (i *replayErrorInventory) text(expr ast.Expr, values map[string]any) string {
	value, ok := i.value(expr, values).(string)
	if !ok {
		i.t.Fatal("error text is not a string")
	}
	return value
}

func (i *replayErrorInventory) value(expr ast.Expr, values map[string]any) any {
	var printed bytes.Buffer
	if err := format.Node(&printed, token.NewFileSet(), expr); err != nil {
		i.t.Fatal(err)
	}
	if value, ok := values[printed.String()]; ok {
		return value
	}
	switch expr := expr.(type) {
	case *ast.BasicLit:
		if expr.Kind == token.STRING {
			value, err := strconv.Unquote(expr.Value)
			if err != nil {
				i.t.Fatal(err)
			}
			return value
		}
	case *ast.Ident:
		if value, ok := i.constants[expr.Name]; ok {
			return i.value(value, values)
		}
	case *ast.BinaryExpr:
		if expr.Op == token.ADD {
			return i.text(expr.X, values) + i.text(expr.Y, values)
		}
	case *ast.CallExpr:
		if selector, ok := expr.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "fmt" && selector.Sel.Name == "Sprintf" {
				args := make([]any, 0, len(expr.Args)-1)
				for _, arg := range expr.Args[1:] {
					args = append(args, i.value(arg, values))
				}
				return fmt.Sprintf(i.text(expr.Args[0], values), args...)
			}
		}
	}
	// Values from the query, AST, or internal state use ordinary and protected
	// samples. Their surrounding format strings always come from production.
	return i.sample
}
