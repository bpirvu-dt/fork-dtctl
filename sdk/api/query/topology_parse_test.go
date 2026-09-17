package query

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestParse_CapturedTraversalRequiresTargetSelector(t *testing.T) {
	dir := filepath.Join("testdata", "topology", "fixtures", "traverse-single-selector")
	body, err := os.ReadFile(filepath.Join(dir, "parse.json"))
	if err != nil {
		t.Fatal(err)
	}
	var captured struct {
		Status int             `json:"http_status"`
		Body   json.RawMessage `json:"response_body"`
	}
	if err := json.Unmarshal(body, &captured); err != nil {
		t.Fatal(err)
	}
	if captured.Status != http.StatusBadRequest || len(captured.Body) == 0 {
		t.Fatalf("fixture is not a captured parser rejection: %s", body)
	}
	original, err := os.ReadFile(filepath.Join(dir, "original.dql"))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/platform/storage/query/v1/query:parse", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req ParseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query != string(original) {
			t.Errorf("parse request=%#v err=%v", req, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(captured.Status)
		_, _ = w.Write(captured.Body)
	})
	client := newTestClient(t, mux)
	client.HTTP().SetRetryCount(0)
	parsed, err := NewHandler(client).Parse(context.Background(), ParseRequest{Query: string(original)})
	var rejection *QueryError
	if parsed != nil || !errors.As(err, &rejection) || rejection.StatusCode != captured.Status ||
		rejection.ErrorType != "TOO_FEW_PARAMETERS_FOR_COMMAND" || !strings.Contains(rejection.Error(), "targetTypes") {
		t.Fatalf("parsed=%#v rejection=%#v err=%v", parsed, rejection, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("parse calls=%d, want one", got)
	}
}
