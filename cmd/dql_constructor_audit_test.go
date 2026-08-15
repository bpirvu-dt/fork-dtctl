package cmd

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	"github.com/dynatrace-oss/dtctl/pkg/exec"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestDQLExecutorConstructorAuditHasNoUnwiredProductionPath(t *testing.T) {
	t.Parallel()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate the repository")
	}
	repoRoot := filepath.Dir(filepath.Dir(filename))
	allowed := map[string]bool{
		"cmd/get_lookups.go":           false, // notification formatting only
		"cmd/replay_query_executor.go": false, // the central production factory
	}
	allowedDirectQueryHandlers := map[string]int{
		"pkg/exec/dql.go":          1, // handler owned by the central DQL executor
		"pkg/exec/replay_probe.go": 1, // shared fixed bounded reads; intentional replay-compiler bypass
	}

	var found []string
	foundDirectQueryHandlers := make(map[string]int)
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".scratchpad", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		directQueryHandlers, err := directSDKQueryHandlerConstructorReferences(parsed)
		if err != nil {
			return fmt.Errorf("audit %s: %w", rel, err)
		}
		if directQueryHandlers > 0 {
			foundDirectQueryHandlers[rel] = directQueryHandlers
		}

		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewDQLExecutor" {
				return true
			}
			found = append(found, rel)
			if _, ok := allowed[rel]; ok {
				allowed[rel] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk production Go files: %v", err)
	}

	sort.Strings(found)
	for _, path := range found {
		if _, ok := allowed[path]; !ok {
			t.Errorf("unwired production DQL executor constructor: %s", path)
		}
	}
	for path, seen := range allowed {
		if !seen {
			t.Errorf("expected audited constructor was removed or renamed: %s", path)
		}
	}

	var unexpectedDirectQueryHandlers []string
	for path := range foundDirectQueryHandlers {
		if _, ok := allowedDirectQueryHandlers[path]; !ok {
			unexpectedDirectQueryHandlers = append(unexpectedDirectQueryHandlers, path)
		}
	}
	sort.Strings(unexpectedDirectQueryHandlers)
	for _, path := range unexpectedDirectQueryHandlers {
		t.Errorf("unapproved production SDK query handler constructor: %s", path)
	}

	var approvedDirectQueryHandlerPaths []string
	for path := range allowedDirectQueryHandlers {
		approvedDirectQueryHandlerPaths = append(approvedDirectQueryHandlerPaths, path)
	}
	sort.Strings(approvedDirectQueryHandlerPaths)
	for _, path := range approvedDirectQueryHandlerPaths {
		got := foundDirectQueryHandlers[path]
		if want := allowedDirectQueryHandlers[path]; got != want {
			t.Errorf("audited SDK query handler constructors in %s = %d, want %d", path, got, want)
		}
	}
}

const sdkQueryImportPath = "github.com/dynatrace-oss/dtctl/sdk/api/query"

// directSDKQueryHandlerConstructorReferences counts direct references to the
// Query API handler constructor. Counting references rather than only calls
// also catches assigning the constructor to a variable before invoking it.
func directSDKQueryHandlerConstructorReferences(file *ast.File) (int, error) {
	aliases := make(map[string]struct{})
	for _, imported := range file.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return 0, fmt.Errorf("decode import path %q: %w", imported.Path.Value, err)
		}
		if importPath != sdkQueryImportPath {
			continue
		}

		alias := pathpkg.Base(importPath)
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		switch alias {
		case "_":
			continue
		case ".":
			return 0, errors.New("dot-importing the SDK query package bypasses the constructor audit")
		default:
			aliases[alias] = struct{}{}
		}
	}

	count := 0
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "NewHandler" {
			return true
		}
		identifier, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		if _, ok := aliases[identifier.Name]; ok {
			count++
		}
		return true
	})
	return count, nil
}

func TestDirectSDKQueryHandlerConstructorDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		want    int
		wantErr bool
	}{
		{
			name: "explicit alias call",
			source: `package synthetic
import sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
func use() { _ = sdkquery.NewHandler(nil) }`,
			want: 1,
		},
		{
			name: "default alias method value",
			source: `package synthetic
import "github.com/dynatrace-oss/dtctl/sdk/api/query"
var constructor = query.NewHandler`,
			want: 1,
		},
		{
			name: "query types only",
			source: `package synthetic
import sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
var handler *sdkquery.Handler`,
		},
		{
			name: "unrelated constructor",
			source: `package synthetic
import other "example.invalid/synthetic/query"
func use() { _ = other.NewHandler(nil) }`,
		},
		{
			name: "dot import rejected",
			source: `package synthetic
import . "github.com/dynatrace-oss/dtctl/sdk/api/query"
func use() { _ = NewHandler(nil) }`,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", test.source, 0)
			if err != nil {
				t.Fatalf("parse fixture: %v", err)
			}
			got, err := directSDKQueryHandlerConstructorReferences(parsed)
			if test.wantErr {
				if err == nil {
					t.Fatal("detector error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("detect constructors: %v", err)
			}
			if got != test.want {
				t.Fatalf("constructor references = %d, want %d", got, test.want)
			}
		})
	}
}

func TestPrintLookupNotificationsDoesNotExecuteDQL(t *testing.T) {
	t.Parallel()

	c, err := client.NewForTesting("https://example.invalid", "synthetic-token")
	if err != nil {
		t.Fatalf("client.NewForTesting: %v", err)
	}
	var requests atomic.Int32
	c.HTTP().SetTransport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, nil
	}))

	printLookupNotifications(c, nil, []exec.QueryNotification{{Severity: "INFO", Message: "synthetic notification"}})

	if got := requests.Load(); got != 0 {
		t.Fatalf("notification formatting made %d HTTP requests; want 0", got)
	}
}

func TestPrintLookupNotificationsRestrictedRouteIsSilent(t *testing.T) {
	c, err := client.NewForTesting("https://example.invalid", "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	executor := exec.NewDQLExecutor(c).WithQueryPreparer(disclosureNotificationPreparer(session.ReplayDisclosureRestricted))
	stdout, stderr := captureReplayQueryStreams(t, func() {
		printLookupNotifications(c, executor, []exec.QueryNotification{{
			Severity: "WARNING", Message: "replay virtual session clock interval effective",
		}})
	})
	if stdout != "" || stderr != "" {
		t.Fatalf("restricted lookup notification reached ordinary output: stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestExecDQLOutputCompatibilityAndFullReplayEnvelopeSelection(t *testing.T) {
	c, err := client.NewForTesting("https://example.invalid", "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	originalAgent, originalFormat := agentMode, outputFormat
	agentMode, outputFormat = true, "json"
	t.Cleanup(func() { agentMode, outputFormat = originalAgent, originalFormat })

	plain := execDQLExecutionOptions(exec.NewDQLExecutor(c))
	restricted := execDQLExecutionOptions(exec.NewDQLExecutor(c).WithQueryPreparer(disclosureNotificationPreparer(session.ReplayDisclosureRestricted)))
	full := execDQLExecutionOptions(exec.NewDQLExecutor(c).WithQueryPreparer(disclosureNotificationPreparer(session.ReplayDisclosureFull)))
	if plain.AgentMode || restricted.AgentMode || len(plain.MetadataFields) != 0 || len(restricted.MetadataFields) != 0 {
		t.Fatalf("plain=%+v restricted=%+v, want existing non-envelope serializer", plain, restricted)
	}
	if !full.AgentMode || len(full.MetadataFields) != 1 || full.MetadataFields[0] != "all" {
		t.Fatalf("full replay options = %+v, want replay agent envelope metadata", full)
	}
}

type disclosureNotificationPreparer string

func (disclosureNotificationPreparer) Prepare(context.Context, exec.PrepareInput) (exec.PreparedQuery, error) {
	return exec.PreparedQuery{}, errors.New("not used")
}

func (p disclosureNotificationPreparer) Disclosure(context.Context) (string, bool) {
	return string(p), true
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
