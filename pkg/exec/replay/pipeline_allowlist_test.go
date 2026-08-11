package replay

import (
	"errors"
	"testing"
)

func TestCompilerAllowsEvidenceBackedPipelineSurface(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		dql     string
	}{
		{"parse", "pipeline/parse/parse.json", `fetch logs, from:now()-5m, to:now() | parse content, "LD:parsed_content" | limit 1`},
		{"filterOut", "pipeline/filterOut/parse.json", `fetch logs, from:now()-5m, to:now() | filterOut loglevel == "DEBUG" | limit 1`},
		{"fieldsRemove", "pipeline/fieldsRemove/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsRemove content | limit 1`},
		{"fieldsRename", "pipeline/fieldsRename/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsRename message=content | limit 1`},
		{"expand", "pipeline/expand/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd sample_values=array("first", "second") | expand sample_value=sample_values | limit 1`},
		{"contains", "pipeline/contains/parse.json", `fetch logs, from:now()-5m, to:now() | filter contains(content, "error") | limit 1`},
		{"startsWith", "pipeline/startsWith/parse.json", `fetch logs, from:now()-5m, to:now() | filter startsWith(content, "ERROR") | limit 1`},
		{"endsWith", "pipeline/endsWith/parse.json", `fetch logs, from:now()-5m, to:now() | filter endsWith(content, "failed") | limit 1`},
		{"matchesPhrase", "pipeline/matchesPhrase/parse.json", `fetch logs, from:now()-5m, to:now() | filter matchesPhrase(content, "connection failed") | limit 1`},
		{"matchesValue", "pipeline/matchesValue/parse.json", `fetch logs, from:now()-5m, to:now() | filter matchesValue(content, "*error*") | limit 1`},
		{"lower", "pipeline/lower/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd normalized_content=lower(content) | limit 1`},
		{"upper", "pipeline/upper/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd normalized_level=upper(loglevel) | limit 1`},
		{"bin", "pipeline/bin/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd minute_bucket=bin(timestamp, 1m) | limit 1`},
		{"if", "pipeline/if/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd severity=if(loglevel == "ERROR", "high", else:"normal") | limit 1`},
		{"coalesce", "pipeline/coalesce/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd message=coalesce(content, "missing") | limit 1`},
		{"toString", "pipeline/toString/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd timestamp_text=toString(timestamp) | limit 1`},
		{"toLong", "pipeline/toLong/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd status_code=toLong(http.status_code) | limit 1`},
		{"toDuration", "pipeline/toDuration/parse.json", `fetch logs, from:now()-5m, to:now() | fieldsAdd one_millisecond=toDuration(1000000) | limit 1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Compile(fixedCompileInput(loadSDKFixture(t, test.fixture), test.dql))
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			if len(result.Sources) != 1 || result.Sources[0].Source.Name != "logs" || result.EffectiveDQL == "" || result.EffectiveDQL == test.dql {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestCompilerPipelineExpansionKeepsDefaultClosed(t *testing.T) {
	tests := []struct {
		name      string
		role      string
		from      string
		to        string
		original  string
		construct string
	}{
		{
			name: "command search", role: "COMMAND_NAME", from: "filter", to: "search", construct: "search",
			original: `fetch logs, from:now()-5m, to:now() | search content ~ "error" | limit 1`,
		},
		{
			name: "function concat", role: "FUNCTION_NAME", from: "contains", to: "concat", construct: "concat",
			original: `fetch logs, from:now()-5m, to:now() | filter concat(content, "error") | limit 1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast := loadSDKFixture(t, "pipeline/contains/parse.json").Clone()
			changed := 0
			for _, node := range terminalNodes(ast, test.role) {
				if node.Canonical == test.from {
					node.Canonical = test.to
					changed++
				}
			}
			if changed != 1 {
				t.Fatalf("changed %d %s nodes, want 1", changed, test.role)
			}
			_, err := Compile(fixedCompileInput(ast, test.original))
			var replayErr *ReplayError
			if !errors.As(err, &replayErr) || replayErr.Code != ErrorUnsupportedForm || replayErr.Construct != test.construct {
				t.Fatalf("error = %T %#v, want unsupported %s", err, err, test.construct)
			}
		})
	}
}
