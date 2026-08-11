package replay

import (
	"fmt"
	"strings"
)

// ErrorCode is a stable replay preparation error category.
type ErrorCode string

const (
	ErrorASTContract       ErrorCode = "ast_contract"
	ErrorUnsupportedSource ErrorCode = "unsupported_source"
	ErrorUnsupportedForm   ErrorCode = "unsupported_form"
	ErrorCurrentState      ErrorCode = "current_state"
	ErrorTimeframe         ErrorCode = "timeframe"
	ErrorShift             ErrorCode = "shift"
	ErrorNonOverlap        ErrorCode = "non_overlap"
	ErrorAudit             ErrorCode = "audit"
	ErrorResultContract    ErrorCode = "result_contract"
)

// ReplayError is a typed, full-disclosure compiler rejection. Later phases can
// map Code to a restricted message without discarding the detailed reason.
type ReplayError struct {
	Code      ErrorCode
	Message   string
	Remedy    string
	Construct string
	Path      string
	Span      *Span
}

func (e *ReplayError) Error() string {
	parts := []string{e.Message, "The query was not executed."}
	if e.Remedy != "" {
		parts = append(parts, e.Remedy)
	}
	return strings.Join(parts, "\n")
}

func replayError(code ErrorCode, node *Node, construct, message, remedy string) *ReplayError {
	err := &ReplayError{Code: code, Construct: construct, Message: message, Remedy: remedy}
	if node != nil {
		err.Path = node.Path
		if node.Span != nil {
			span := *node.Span
			err.Span = &span
		}
	}
	return err
}

// DavisCurrentViewError rejects a mutable Davis view while carrying the safe,
// author-owned snapshot reconstruction pattern needed by full disclosure and
// provenance routing.
type DavisCurrentViewError struct {
	View               string
	SnapshotTable      string
	IdentityKind       string
	LatestPerIDPattern string
	Path               string
	Span               *Span
}

func (e *DavisCurrentViewError) Error() string {
	return fmt.Sprintf(
		"%s is a current-state Davis view. dtctl cannot reproduce its value at virtual now.\n"+
			"The query was not executed. Fetch %s over the visible replay interval, then reduce to the latest snapshot per %s ID.\n"+
			"Pattern: %s",
		e.View, e.SnapshotTable, e.IdentityKind, e.LatestPerIDPattern,
	)
}
