package cmd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	"github.com/dynatrace-oss/dtctl/pkg/exec"
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

	var found []string
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
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewDQLExecutor" {
				return true
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				t.Fatalf("filepath.Rel(%q): %v", path, err)
			}
			rel = filepath.ToSlash(rel)
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

	printLookupNotifications(c, []exec.QueryNotification{{Severity: "INFO", Message: "synthetic notification"}})

	if got := requests.Load(); got != 0 {
		t.Fatalf("notification formatting made %d HTTP requests; want 0", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
