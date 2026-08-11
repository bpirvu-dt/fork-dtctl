package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
)

// ReplayQueryAPIVersion is the parse-contract identity used by the current
// typed Query API client.
const ReplayQueryAPIVersion = "platform/storage/query/v1"

// OriginalParseKey contains every non-secret input that can affect parsing of
// expanded original DQL. It is comparable so one command invocation can use it
// directly as its memo key.
type OriginalParseKey struct {
	Query          string
	EnvironmentID  string
	ClientIdentity string
	Locale         string
	Timezone       string
	APIVersion     string
	ParserOptions  string
}

// NewOriginalParseKey builds a complete, stable key. JSON object keys are
// sorted by encoding/json, so semantically identical parser-option maps share
// one entry without admitting request IDs or credentials into the key.
func NewOriginalParseKey(query, environmentID, clientIdentity, locale, timezone, apiVersion string, options sdkquery.QueryOptions) (OriginalParseKey, error) {
	encodedOptions, err := canonicalParserOptions(options)
	if err != nil {
		return OriginalParseKey{}, err
	}
	return OriginalParseKey{
		Query: query, EnvironmentID: environmentID, ClientIdentity: clientIdentity,
		Locale: locale, Timezone: timezone, APIVersion: apiVersion, ParserOptions: encodedOptions,
	}, nil
}

// OriginalASTView is immutable read-only access to one successful original
// parse. AST returns a fresh SDK tree on every call, so adapters and compilers
// cannot contaminate a later execution.
type OriginalASTView interface {
	AST() (*sdkquery.ParseResponse, error)
	Bytes() []byte
}

type immutableOriginalASTView struct {
	encoded []byte
}

func (v *immutableOriginalASTView) AST() (*sdkquery.ParseResponse, error) {
	var root sdkquery.ParseResponse
	if err := json.Unmarshal(v.encoded, &root); err != nil {
		return nil, fmt.Errorf("decode immutable original DQL AST: %w", err)
	}
	return &root, nil
}

func (v *immutableOriginalASTView) Bytes() []byte {
	return append([]byte(nil), v.encoded...)
}

// QueryParseFunc is the narrow query:parse HTTP boundary used by the memo.
type QueryParseFunc func(context.Context, sdkquery.ParseRequest) (*sdkquery.ParseResponse, error)

// OriginalASTProvider supplies successful immutable original parses. The
// implementation is invocation-local; callers must not place it in globals.
type OriginalASTProvider interface {
	OriginalAST(context.Context, OriginalParseKey, sdkquery.ParseRequest, QueryParseFunc) (OriginalASTView, error)
}

type originalParseEntry struct {
	ready chan struct{}
	view  OriginalASTView
	err   error
}

// MemoizedOriginalASTProvider coalesces concurrent misses for one key and
// stores only successful immutable answers. Its owner is one DQLExecutor.
type MemoizedOriginalASTProvider struct {
	mu      sync.Mutex
	entries map[OriginalParseKey]*originalParseEntry
}

// NewMemoizedOriginalASTProvider returns an empty invocation-local memo.
func NewMemoizedOriginalASTProvider() *MemoizedOriginalASTProvider {
	return &MemoizedOriginalASTProvider{entries: make(map[OriginalParseKey]*originalParseEntry)}
}

// OriginalAST returns a fresh read-only view of a memoized answer or performs
// and coalesces the one real parse needed for a miss.
func (p *MemoizedOriginalASTProvider) OriginalAST(ctx context.Context, key OriginalParseKey, request sdkquery.ParseRequest, parse QueryParseFunc) (OriginalASTView, error) {
	if parse == nil {
		return nil, fmt.Errorf("original DQL parse function is required")
	}
	if err := validateOriginalParseKey(key, request); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if entry, ok := p.entries[key]; ok {
		p.mu.Unlock()
		select {
		case <-entry.ready:
			return entry.view, entry.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	entry := &originalParseEntry{ready: make(chan struct{})}
	p.entries[key] = entry
	p.mu.Unlock()

	parsed, err := parse(ctx, request)
	if err == nil {
		var encoded []byte
		encoded, err = json.Marshal(parsed)
		if err == nil {
			entry.view = &immutableOriginalASTView{encoded: append([]byte(nil), encoded...)}
		}
	}
	if err != nil {
		entry.err = err
		p.mu.Lock()
		delete(p.entries, key)
		p.mu.Unlock()
	}
	close(entry.ready)
	return entry.view, entry.err
}

func validateOriginalParseKey(key OriginalParseKey, request sdkquery.ParseRequest) error {
	if key.Query == "" || key.EnvironmentID == "" || key.ClientIdentity == "" || key.APIVersion == "" {
		return fmt.Errorf("original-parse key is incomplete")
	}
	options, err := canonicalParserOptions(request.QueryOptions)
	if err != nil {
		return err
	}
	if key.Query != request.Query || key.Locale != request.Locale || key.Timezone != request.Timezone || key.ParserOptions != options {
		return fmt.Errorf("original-parse key does not match the parse request")
	}
	return nil
}

func canonicalParserOptions(options sdkquery.QueryOptions) (string, error) {
	if len(options) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		return "", fmt.Errorf("encode query parser options: %w", err)
	}
	return string(encoded), nil
}
