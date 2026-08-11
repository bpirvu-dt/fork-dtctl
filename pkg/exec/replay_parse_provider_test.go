package exec

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
)

const providerTestAST = `{
  "nodeType":"CONTAINER","type":"QUERY","isOptional":false,"children":[
    {"nodeType":"TERMINAL","type":"COMMAND_NAME","canonicalString":"data","isOptional":false,"isMandatoryOnUserOrder":false,
     "tokenPosition":{"start":{"index":0,"line":1,"column":1},"end":{"index":3,"line":1,"column":4}}}
  ]}`

func TestMemoizedOriginalASTProviderCoalescesConcurrentMissesAndReturnsImmutableViews(t *testing.T) {
	provider := NewMemoizedOriginalASTProvider()
	request := sdkquery.ParseRequest{Query: "data", Locale: "en_US", Timezone: "UTC", QueryOptions: sdkquery.QueryOptions{"mode": "strict"}}
	key, err := NewOriginalParseKey(request.Query, "environment-a", "principal-a", request.Locale, request.Timezone, ReplayQueryAPIVersion, request.QueryOptions)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	parse := func(context.Context, sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
		calls.Add(1)
		time.Sleep(5 * time.Millisecond)
		var result sdkquery.ParseResponse
		if err := json.Unmarshal([]byte(providerTestAST), &result); err != nil {
			return nil, err
		}
		return &result, nil
	}

	const goroutines = 24
	views := make([]OriginalASTView, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	for index := range views {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			views[index], errs[index] = provider.OriginalAST(context.Background(), key, request, parse)
		}(index)
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("parse calls = %d, want 1", calls.Load())
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	before := string(views[0].Bytes())
	first, err := views[0].AST()
	if err != nil {
		t.Fatal(err)
	}
	first.Container.Children[0].Terminal.CanonicalString = "changed"
	second, err := views[1].AST()
	if err != nil {
		t.Fatal(err)
	}
	if second.Container.Children[0].Terminal.CanonicalString != "data" {
		t.Fatal("one AST view contaminated another")
	}
	if got := string(views[0].Bytes()); got != before {
		t.Fatal("memoized AST bytes changed after a consumer mutation")
	}
}

func TestMemoizedOriginalASTProviderCompleteKeyAndInvocationScope(t *testing.T) {
	request := sdkquery.ParseRequest{Query: "data", Locale: "en_US", Timezone: "UTC", QueryOptions: sdkquery.QueryOptions{"mode": "strict"}}
	base, err := NewOriginalParseKey(request.Query, "environment-a", "principal-a", request.Locale, request.Timezone, "api-a", request.QueryOptions)
	if err != nil {
		t.Fatal(err)
	}
	keys := []OriginalParseKey{
		base,
		func() OriginalParseKey { value := base; value.Query = "data "; return value }(),
		func() OriginalParseKey { value := base; value.EnvironmentID = "environment-b"; return value }(),
		func() OriginalParseKey { value := base; value.ClientIdentity = "principal-b"; return value }(),
		func() OriginalParseKey { value := base; value.Locale = "de_DE"; return value }(),
		func() OriginalParseKey { value := base; value.Timezone = "Europe/Vienna"; return value }(),
		func() OriginalParseKey { value := base; value.APIVersion = "api-b"; return value }(),
		func() OriginalParseKey { value := base; value.ParserOptions = `{"mode":"future"}`; return value }(),
	}
	var calls int
	parse := func(_ context.Context, req sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
		calls++
		var result sdkquery.ParseResponse
		if err := json.Unmarshal([]byte(providerTestAST), &result); err != nil {
			return nil, err
		}
		return &result, nil
	}
	provider := NewMemoizedOriginalASTProvider()
	for index, key := range keys {
		req := request
		req.Query, req.Locale, req.Timezone = key.Query, key.Locale, key.Timezone
		if key.ParserOptions == `{"mode":"future"}` {
			req.QueryOptions = sdkquery.QueryOptions{"mode": "future"}
		}
		if _, err := provider.OriginalAST(context.Background(), key, req, parse); err != nil {
			t.Fatalf("key %d: %v", index, err)
		}
	}
	if calls != len(keys) {
		t.Fatalf("parse calls = %d, want %d distinct complete keys", calls, len(keys))
	}
	if _, err := provider.OriginalAST(context.Background(), base, request, parse); err != nil {
		t.Fatal(err)
	}
	if calls != len(keys) {
		t.Fatal("an identical complete key missed the memo")
	}
	secondInvocation := NewMemoizedOriginalASTProvider()
	if _, err := secondInvocation.OriginalAST(context.Background(), base, request, parse); err != nil {
		t.Fatal(err)
	}
	if calls != len(keys)+1 {
		t.Fatal("a separate invocation reused the first invocation's memo")
	}
}

func TestMemoizedOriginalASTProviderDoesNotStoreFailures(t *testing.T) {
	provider := NewMemoizedOriginalASTProvider()
	request := sdkquery.ParseRequest{Query: "data", Timezone: "UTC"}
	key, err := NewOriginalParseKey(request.Query, "environment-a", "principal-a", "", request.Timezone, ReplayQueryAPIVersion, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	want := errors.New("parse unavailable")
	parse := func(context.Context, sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
		calls++
		return nil, want
	}
	for range 2 {
		if _, err := provider.OriginalAST(context.Background(), key, request, parse); !errors.Is(err, want) {
			t.Fatalf("error = %v, want parse failure", err)
		}
	}
	if calls != 2 {
		t.Fatalf("failed parse calls = %d, want 2", calls)
	}
}
