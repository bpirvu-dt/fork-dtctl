package replay

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

type auditCorpusCase struct {
	Name          string `json:"name"`
	OriginalAST   string `json:"original_ast"`
	ValidationAST string `json:"validation_ast"`
	OriginalDQL   string `json:"original_dql"`
	EffectiveDQL  string `json:"effective_dql"`
	Start         string `json:"start"`
	End           string `json:"end"`
}

func TestAuditRealOriginalAndValidationASTCorpus(t *testing.T) {
	cases := loadAuditCorpus(t)
	if len(cases) != 8 {
		t.Fatalf("audit corpus cases = %d, want 8", len(cases))
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			compilation := compileAuditCase(t, test)
			if compilation.EffectiveDQL != test.EffectiveDQL {
				t.Fatalf("effective DQL:\n got: %s\nwant: %s", compilation.EffectiveDQL, test.EffectiveDQL)
			}
			result, err := Audit(AuditInput{
				ValidationAST: loadSDKFixture(t, test.ValidationAST), Compilation: compilation,
				SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
			})
			if err != nil {
				t.Fatalf("Audit: %v", err)
			}
			if !result.OK || !result.NoSemanticNow || !result.AllSourcesBounded || !result.StructureMatches {
				t.Fatalf("audit result = %#v", result)
			}
		})
	}
}

func TestAuditRejectsSemanticNowAndImplicitSourceTime(t *testing.T) {
	compilation, err := Compile(fixedCompileInput(loadSDKFixture(t, "phase0/fixtures/01-fetch-explicit-now/parse.json"), "fetch logs, from:now()-1h"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Audit(AuditInput{
		ValidationAST: loadSDKFixture(t, "phase0/fixtures/01-fetch-explicit-now/parse.json"),
		Compilation:   compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	assertAuditError(t, err)

	implicitCompilation, err := Compile(fixedCompileInput(loadSDKFixture(t, "phase0/fixtures/02-fetch-implicit-duration/parse.json"), "fetch logs, from:-1h"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Audit(AuditInput{
		ValidationAST: loadSDKFixture(t, "phase0/fixtures/02-fetch-implicit-duration/parse.json"),
		Compilation:   implicitCompilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	assertAuditError(t, err)
}

func TestAuditRejectsBoundaryAndUntouchedSemanticChanges(t *testing.T) {
	test := loadAuditCorpus(t)[0]
	compilation := compileAuditCase(t, test)
	validation := loadSDKFixture(t, test.ValidationAST).Clone()
	source := firstSourceAnalysis(t, validation)
	fromValue, err := parameterValue(source.parametersByKey()["from"][0].node)
	if err != nil {
		t.Fatal(err)
	}
	stringsFound := terminalsWithRole(fromValue, "STRING")
	if len(stringsFound) != 1 {
		t.Fatalf("from strings = %d", len(stringsFound))
	}
	stringsFound[0].Canonical = `"2026-08-09T10:55:04Z"`
	_, err = Audit(AuditInput{ValidationAST: validation, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC})
	assertAuditError(t, err)

	validation = loadSDKFixture(t, test.ValidationAST).Clone()
	numbers := terminalNodes(validation, "NUMBER")
	if len(numbers) != 1 {
		t.Fatalf("NUMBER nodes = %d, want limit value", len(numbers))
	}
	numbers[0].Canonical = "2"
	_, err = Audit(AuditInput{ValidationAST: validation, Compilation: compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC})
	assertAuditError(t, err)
}

func TestAuditRejectsForbiddenValidationConstruct(t *testing.T) {
	test := loadAuditCorpus(t)[0]
	compilation := compileAuditCase(t, test)
	_, err := Audit(AuditInput{
		ValidationAST: loadPhase0BFixture(t, "current-state-rejection/01-smartscape-nodes/historical-context/parse.json"),
		Compilation:   compilation, SourcePolicy: Milestone1SourcePolicy(), Timezone: time.UTC,
	})
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorCurrentState {
		t.Fatalf("error = %T %v", err, err)
	}
}

func loadAuditCorpus(t *testing.T) []auditCorpusCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/audit_corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []auditCorpusCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func compileAuditCase(t *testing.T, test auditCorpusCase) CompileResult {
	t.Helper()
	input := compileInputAt(t, loadSDKFixture(t, test.OriginalAST), test.OriginalDQL, test.Start, test.End, test.End)
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertAuditError(t *testing.T, err error) {
	t.Helper()
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.Code != ErrorAudit {
		t.Fatalf("error = %T %v, want audit ReplayError", err, err)
	}
}
