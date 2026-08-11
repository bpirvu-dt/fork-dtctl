package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const minimalParseTree = `{"nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":[{"nodeType":"TERMINAL","type":"COMMAND_NAME","canonicalString":"fetch","isOptional":false,"isMandatoryOnUserOrder":false,"tokenPosition":{"start":{"index":0,"line":1,"column":1},"end":{"index":4,"line":1,"column":5}}}]}`

func TestParse_SuccessAndRequestContract(t *testing.T) {
	const clientContext = `{"app":"dtctl-test","version":"0","context":"parse-contract"}`
	var calls atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.RequestURI() != "/platform/storage/query/v1/query:parse" {
			t.Errorf("request URI = %q", r.URL.RequestURI())
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		if got := r.Header.Get("dt-client-context"); got != clientContext {
			t.Errorf("dt-client-context = %q, want %q", got, clientContext)
		}
		if got := r.Header.Get("Authorization"); got != "Api-Token dt0c01.test" {
			t.Errorf("Authorization = %q, want synthetic API token header", got)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		} else {
			assertJSONEqual(t, body, []byte(`{
              "query":"fetch logs | limit 1",
              "locale":"en_US",
              "timezone":"Europe/Vienna",
              "queryOptions":{"feature":"enabled"}
            }`))
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, minimalParseTree)
	})

	h := NewHandler(newTestClient(t, mux)).WithHeaders(map[string]string{
		"dt-client-context": clientContext,
	})
	req := ParseRequest{
		Query:        "fetch logs | limit 1",
		Locale:       "en_US",
		Timezone:     "Europe/Vienna",
		QueryOptions: QueryOptions{"feature": "enabled"},
	}
	for i := 0; i < 2; i++ {
		result, err := h.Parse(context.Background(), req)
		if err != nil {
			t.Fatalf("Parse() call %d: %v", i+1, err)
		}
		if result.NodeType != DQLNodeTypeContainer || result.Container == nil {
			t.Fatalf("root = %#v, want typed container", result)
		}
		if len(result.Container.Children) != 1 || result.Container.Children[0].Terminal == nil {
			t.Fatalf("children = %#v, want one typed terminal", result.Container.Children)
		}
		position := result.Container.Children[0].TokenPosition
		if position == nil || position.Start.Index != 0 || position.End.Index != 4 {
			t.Fatalf("token position = %#v, want inclusive indexes 0..4", position)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("parse calls = %d, want 2; Handler.Parse must not cache", got)
	}
}

func TestParse_HTTPFailures(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantType   string
	}{
		{
			name:       "bad DQL",
			statusCode: http.StatusBadRequest,
			body:       `{"error":{"message":"SYNTAX_ERROR","details":{"errorType":"SYNTAX_ERROR","errorMessage":"unexpected token","arguments":["bogus"]}}}`,
			wantType:   "SYNTAX_ERROR",
		},
		{
			name:       "unauthorized",
			statusCode: http.StatusUnauthorized,
			body:       `{"error":{"message":"authentication required"}}`,
		},
		{
			name:       "insufficient permission",
			statusCode: http.StatusForbidden,
			body:       `{"error":{"message":"INSUFFICIENT_PERMISSION","details":{"errorType":"INSUFFICIENT_PERMISSION","errorMessage":"table permission is missing"}}}`,
			wantType:   "INSUFFICIENT_PERMISSION",
		},
		{
			name:       "service unavailable",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"error":{"message":"service unavailable"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				_, _ = io.WriteString(w, tt.body)
			})
			client := newTestClient(t, mux)
			client.HTTP().SetRetryCount(0)

			_, err := NewHandler(client).Parse(context.Background(), ParseRequest{Query: "bogus"})
			if err == nil {
				t.Fatal("Parse() returned nil error")
			}
			var queryErr *QueryError
			if !errors.As(err, &queryErr) {
				t.Fatalf("error = %T %v, want *QueryError", err, err)
			}
			if queryErr.StatusCode != tt.statusCode {
				t.Errorf("status = %d, want %d", queryErr.StatusCode, tt.statusCode)
			}
			if tt.wantType != "" && queryErr.ErrorType != tt.wantType {
				t.Errorf("error type = %q, want %q", queryErr.ErrorType, tt.wantType)
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("requests = %d, want 1", got)
			}
		})
	}
}

func TestParse_Timeout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, minimalParseTree)
		}
	})
	client := newTestClient(t, mux)
	client.HTTP().SetRetryCount(0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := NewHandler(client).Parse(ctx, ParseRequest{Query: "fetch logs"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
}

func TestParse_RejectsMalformedAndIncompleteTrees(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "truncated JSON", body: `{"nodeType":"CONTAINER"`},
		{name: "empty response", body: ``},
		{name: "missing discriminator", body: `{"isOptional":false}`},
		{name: "empty discriminator", body: `{"nodeType":"","isOptional":false}`},
		{name: "missing optional marker", body: `{"nodeType":"FUTURE"}`},
		{name: "null optional marker", body: `{"nodeType":"FUTURE","isOptional":null}`},
		{name: "container missing children", body: `{"nodeType":"CONTAINER","type":"QUERY","isOptional":false}`},
		{name: "container children wrong type", body: `{"nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":{}}`},
		{name: "terminal missing canonical string", body: `{"nodeType":"TERMINAL","type":"STRING","isOptional":false,"isMandatoryOnUserOrder":false}`},
		{name: "terminal empty role", body: `{"nodeType":"TERMINAL","type":"","canonicalString":"x","isOptional":false,"isMandatoryOnUserOrder":false}`},
		{name: "alternative missing forms", body: `{"nodeType":"ALTERNATIVE","isOptional":false}`},
		{name: "alternative null form", body: `{"nodeType":"ALTERNATIVE","isOptional":false,"alternatives":{"INFO":null}}`},
		{name: "null child", body: `{"nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":[null]}`},
		{name: "null token position", body: `{"nodeType":"FUTURE","isOptional":false,"tokenPosition":null}`},
		{
			name: "incomplete token position",
			body: `{
              "nodeType":"TERMINAL","type":"STRING","canonicalString":"x",
              "isOptional":false,"isMandatoryOnUserOrder":false,
              "tokenPosition":{"start":{"index":0,"line":1},"end":{"index":0,"line":1,"column":1}}
            }`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.body)
			})
			client := newTestClient(t, mux)
			client.HTTP().SetRetryCount(0)
			_, err := NewHandler(client).Parse(
				context.Background(), ParseRequest{Query: "fetch logs"},
			)
			if err == nil {
				t.Fatal("Parse() accepted malformed or incomplete tree")
			}
		})
	}
}

func TestParse_PreservesUnknownMetadata(t *testing.T) {
	const body = `{
      "nodeType":"CONTAINER","type":"QUERY","isOptional":false,
      "futureMetadata":{"schema":2},
      "tokenPosition":{
        "start":{"index":0,"line":1,"column":1,"futureCoordinate":"a"},
        "end":{"index":0,"line":1,"column":1},
        "futureSpanMetadata":true
      },
      "children":[]
    }`
	result := parseTreeFromBody(t, body)
	raw := result.RawJSON()
	if !bytes.Contains(raw, []byte(`"futureMetadata"`)) || !bytes.Contains(raw, []byte(`"futureSpanMetadata"`)) {
		t.Fatalf("RawJSON() lost unclassified metadata: %s", raw)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	assertJSONEqual(t, encoded, []byte(body))

	raw[0] = '['
	if result.RawJSON()[0] != '{' {
		t.Fatal("RawJSON() did not return a defensive copy")
	}
}

func TestParse_PreservesUnknownSemanticNodeType(t *testing.T) {
	const body = `{
      "nodeType":"FUTURE_SOURCE","isOptional":false,"type":"TIME_AWARE_SOURCE",
      "sourceSelector":{"table":"future.records"}
    }`
	result := parseTreeFromBody(t, body)
	if result.NodeType != DQLNodeType("FUTURE_SOURCE") {
		t.Fatalf("NodeType = %q, want FUTURE_SOURCE", result.NodeType)
	}
	if result.Terminal != nil || result.Container != nil || result.Alternative != nil {
		t.Fatal("unknown semantic node was misclassified as a known variant")
	}
	if !bytes.Contains(result.RawJSON(), []byte(`"sourceSelector"`)) {
		t.Fatalf("unknown semantic fields unavailable in raw node: %s", result.RawJSON())
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	assertJSONEqual(t, encoded, []byte(body))

	alternative := parseTreeFromBody(t, `{"nodeType":"ALTERNATIVE","isOptional":false,"alternatives":{"FUTURE":{"nodeType":"TERMINAL","type":"STRING","canonicalString":"x","isOptional":false,"isMandatoryOnUserOrder":false}}}`)
	if alternative.Alternative == nil || alternative.Alternative.Alternatives[AlternativeType("FUTURE")] == nil {
		t.Fatal("unknown alternative form was not retained")
	}
}

func TestParse_TokenRefresh(t *testing.T) {
	disableAgentDetection(t)
	var attempts, refreshes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		switch r.Header.Get("Authorization") {
		case "Bearer expired-token":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"expired"}}`)
		case "Bearer refreshed-token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, minimalParseTree)
		default:
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(srv.Close)

	sessionClient, err := session.NewForTesting(srv.URL, "expired-token")
	if err != nil {
		t.Fatalf("session.NewForTesting: %v", err)
	}
	sessionClient.HTTP().SetRetryCount(1)
	sessionClient.EnableTokenRefresh(func(rejected string) (string, error) {
		refreshes.Add(1)
		if rejected != "expired-token" {
			t.Errorf("rejected token = %q", rejected)
		}
		return "refreshed-token", nil
	})

	h := NewHandler(httpclient.Wrap(sessionClient.HTTP()))
	if _, err := h.Parse(context.Background(), ParseRequest{Query: "fetch logs"}); err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	if attempts.Load() != 2 || refreshes.Load() != 1 {
		t.Fatalf("attempts = %d, refreshes = %d; want 2 and 1", attempts.Load(), refreshes.Load())
	}
}

func TestDQLNode_RoundTripsPhase0AndPhase0BFixtures(t *testing.T) {
	var paths []string
	err := filepath.WalkDir("testdata", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && (entry.Name() == "parse.json" || entry.Name() == "validation-parse.json") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk fixtures: %v", err)
	}
	sort.Strings(paths)
	if len(paths) != 225 {
		t.Fatalf("fixture count = %d, want 225", len(paths))
	}

	typeCounts := make(map[DQLNodeType]int)
	astFixtures, nonASTCaptures := 0, 0
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		body, isAST, err := fixtureASTBody(raw)
		if err != nil {
			t.Fatalf("classify %s: %v", path, err)
		}
		if !isAST {
			nonASTCaptures++
			continue
		}

		var node DQLNode
		if err := json.Unmarshal(body, &node); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		encoded, err := json.Marshal(&node)
		if err != nil {
			t.Fatalf("encode %s: %v", path, err)
		}
		assertJSONEqual(t, encoded, body)
		assertRawNodeTree(t, path, body, &node, typeCounts)
		astFixtures++
	}

	if astFixtures != 207 || nonASTCaptures != 18 {
		t.Fatalf("AST fixtures = %d, non-AST captures = %d; want 207 and 18", astFixtures, nonASTCaptures)
	}
	for _, nodeType := range []DQLNodeType{DQLNodeTypeTerminal, DQLNodeTypeContainer, DQLNodeTypeAlternative} {
		if typeCounts[nodeType] == 0 {
			t.Errorf("fixture corpus contains no %s nodes", nodeType)
		}
	}
}

func parseTreeFromBody(t *testing.T, body string) *ParseResponse {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
	result, err := NewHandler(newTestClient(t, mux)).Parse(context.Background(), ParseRequest{Query: "synthetic"})
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	return result
}

func fixtureASTBody(raw []byte) (json.RawMessage, bool, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, false, err
	}
	if _, ok := root["nodeType"]; ok {
		return append(json.RawMessage(nil), raw...), true, nil
	}
	if body, ok := root["response_body"]; ok {
		var response map[string]json.RawMessage
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, false, errors.New("response_body is not an object: " + err.Error())
		}
		if _, ok := response["nodeType"]; ok {
			return body, true, nil
		}
		var status int
		if statusRaw, ok := root["http_status"]; !ok || json.Unmarshal(statusRaw, &status) != nil || status < 400 {
			return nil, false, errors.New("capture has neither an AST nor a recorded HTTP error")
		}
		return nil, false, nil
	}
	var status string
	if statusRaw, ok := root["status"]; ok && json.Unmarshal(statusRaw, &status) == nil && status == "not tested" {
		return nil, false, nil
	}
	return nil, false, errors.New("unrecognized parse fixture shape")
}

func assertRawNodeTree(t *testing.T, path string, raw json.RawMessage, node *DQLNode, counts map[DQLNodeType]int) {
	t.Helper()
	if got := bytes.TrimSpace(node.RawJSON()); !bytes.Equal(got, bytes.TrimSpace(raw)) {
		t.Fatalf("raw node changed in %s\ngot:  %s\nwant: %s", path, got, raw)
	}
	counts[node.NodeType]++

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode raw node in %s: %v", path, err)
	}
	switch node.NodeType {
	case DQLNodeTypeTerminal:
		if node.Terminal == nil {
			t.Fatalf("terminal in %s has no typed terminal data", path)
		}
	case DQLNodeTypeContainer:
		var children []json.RawMessage
		if err := json.Unmarshal(fields["children"], &children); err != nil {
			t.Fatalf("decode children in %s: %v", path, err)
		}
		if node.Container == nil || len(node.Container.Children) != len(children) {
			t.Fatalf("typed children differ in %s", path)
		}
		for i := range children {
			assertRawNodeTree(t, path, children[i], node.Container.Children[i], counts)
		}
	case DQLNodeTypeAlternative:
		var alternatives map[AlternativeType]json.RawMessage
		if err := json.Unmarshal(fields["alternatives"], &alternatives); err != nil {
			t.Fatalf("decode alternatives in %s: %v", path, err)
		}
		if node.Alternative == nil || len(node.Alternative.Alternatives) != len(alternatives) {
			t.Fatalf("typed alternatives differ in %s", path)
		}
		for name, alternative := range alternatives {
			assertRawNodeTree(t, path, alternative, node.Alternative.Alternatives[name], counts)
		}
	default:
		t.Fatalf("fixture %s contains unexpected node type %q", path, node.NodeType)
	}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	decode := func(raw []byte) any {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode JSON for comparison: %v\n%s", err, raw)
		}
		return value
	}
	if gotValue, wantValue := decode(got), decode(want); !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON differs\ngot:  %s\nwant: %s", got, want)
	}
}

func disableAgentDetection(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"CLAUDECODE", "CODEX", "CURSOR_AGENT", "COPILOT_CLI", "GITHUB_COPILOT",
		"CODEIUM_AGENT", "TABNINE_AGENT", "AMAZON_Q", "JUNIE", "KIRO",
		"AGENT_CONTEXT_OUT", "KIRO_SESSION_ID", "OPENCODE", "OPENCLAW", "AI_AGENT",
	} {
		t.Setenv(name, "0")
	}
}
