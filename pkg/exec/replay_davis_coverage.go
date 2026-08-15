package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
)

const (
	davisSnapshotCoverageTimeout   = 5 * time.Second
	davisSnapshotCoverageScanLimit = 1.0
	davisSnapshotCoverageLower     = "1970-01-01T00:00:00.000Z"
	davisSnapshotCoverageTime      = "2006-01-02T15:04:05.000000000Z"
)

// DavisSnapshotCoverage is the telemetry-free result of one bounded oldest-
// snapshot inspection.
type DavisSnapshotCoverage = execreplay.DavisSnapshotCoverage

// DavisSnapshotCoverageKey is the complete invocation-local reuse key. Every
// field is non-secret and any mismatch requires a fresh inspection.
type DavisSnapshotCoverageKey struct {
	SessionID           string
	EnvironmentIdentity string
	PrincipalIdentity   string
	Table               string
	ProbeUpperBound     time.Time
}

// DavisSnapshotCoverageInspector performs a real request whenever called.
type DavisSnapshotCoverageInspector interface {
	Inspect(ctx context.Context, sessionID string, dataEnd time.Time) (DavisSnapshotCoverage, error)
}

// DavisSnapshotCoverageReuse records whether this invocation reused a typed
// success or failure for an exactly matching key.
type DavisSnapshotCoverageReuse string

const (
	DavisSnapshotCoverageMiss DavisSnapshotCoverageReuse = "miss"
	DavisSnapshotCoverageHit  DavisSnapshotCoverageReuse = "hit"
)

// DavisSnapshotCoverageFailure is a sanitized, typed inspection failure.
type DavisSnapshotCoverageFailure string

const (
	DavisCoverageUnavailable            DavisSnapshotCoverageFailure = "unavailable"
	DavisCoverageTimeout                DavisSnapshotCoverageFailure = "timeout"
	DavisCoverageCanceled               DavisSnapshotCoverageFailure = "canceled"
	DavisCoverageRateLimited            DavisSnapshotCoverageFailure = "rate_limited"
	DavisCoverageUnauthorized           DavisSnapshotCoverageFailure = "unauthorized"
	DavisCoverageForbidden              DavisSnapshotCoverageFailure = "forbidden"
	DavisCoverageAsync                  DavisSnapshotCoverageFailure = "asynchronous"
	DavisCoverageScanLimited            DavisSnapshotCoverageFailure = "scan_limited"
	DavisCoverageResultLimited          DavisSnapshotCoverageFailure = "result_limited"
	DavisCoverageTruncated              DavisSnapshotCoverageFailure = "truncated"
	DavisCoverageUnexpectedNotification DavisSnapshotCoverageFailure = "unexpected_notification"
	DavisCoverageMalformed              DavisSnapshotCoverageFailure = "malformed"
	DavisCoverageEmpty                  DavisSnapshotCoverageFailure = "empty"
)

// DavisSnapshotCoverageError never includes a remote body, tenant value,
// problem record, or identifier.
type DavisSnapshotCoverageError struct {
	Failure    DavisSnapshotCoverageFailure
	ObservedAt time.Time
}

func (e *DavisSnapshotCoverageError) Error() string {
	return "bounded oldest-snapshot inspection failed: " + string(e.Failure)
}

// DavisSnapshotCoverageProvider owns the invocation-local keyed memo.
type DavisSnapshotCoverageProvider interface {
	Coverage(ctx context.Context, key DavisSnapshotCoverageKey) (DavisSnapshotCoverage, DavisSnapshotCoverageReuse, error)
}

type grailDavisSnapshotCoverageInspector struct {
	handler *sdkquery.Handler
	timeout time.Duration
	now     func() time.Time
}

// NewDavisSnapshotCoverageInspector creates the one approved direct Query API
// handler used by the fixed, bounded coverage probe.
func NewDavisSnapshotCoverageInspector(c *client.Client) DavisSnapshotCoverageInspector {
	if c == nil {
		return nil
	}
	handler := newReplayProbeHandler(c, "replay-davis-snapshot-coverage")
	return &grailDavisSnapshotCoverageInspector{handler: handler, timeout: davisSnapshotCoverageTimeout, now: time.Now}
}

func (i *grailDavisSnapshotCoverageInspector) Inspect(ctx context.Context, sessionID string, dataEnd time.Time) (DavisSnapshotCoverage, error) {
	observedAt := i.observedAt()
	if i == nil || i.handler == nil || strings.TrimSpace(sessionID) == "" || dataEnd.IsZero() {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageUnavailable, observedAt)
	}
	timeout := i.timeout
	if timeout <= 0 {
		timeout = davisSnapshotCoverageTimeout
	}
	response, err := executeReplayProbe(ctx, i.handler, timeout, sdkquery.ExecuteRequest{
		Query:                      davisSnapshotCoverageDQL(dataEnd),
		RequestTimeoutMilliseconds: timeout.Milliseconds(),
		MaxResultRecords:           1,
		DefaultScanLimitGbytes:     davisSnapshotCoverageScanLimit,
		DefaultSamplingRatio:       1,
		FetchTimeoutSeconds:        int32(timeout / time.Second),
		PollingPromiseSeconds:      int32(timeout / time.Second),
		Locale:                     "en_US",
		Timezone:                   "UTC",
	})
	observedAt = i.observedAt()
	if err != nil {
		if errors.Is(err, errReplayProbeIncomplete) {
			return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageAsync, observedAt)
		}
		failure := classifyCoverageRequestFailure(err)
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(failure, observedAt)
	}
	if response.RequestToken != "" {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageAsync, observedAt)
	}
	if failure, failed := classifyCoverageNotifications(response.GetNotifications()); failed {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(failure, observedAt)
	}
	records := replayProbeRecords(response)
	if len(records) == 0 {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageEmpty, observedAt)
	}
	if len(records) != 1 || len(records[0]) != 1 {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageMalformed, observedAt)
	}
	raw, ok := records[0]["oldest_snapshot"].(string)
	if !ok || raw == "" {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageMalformed, observedAt)
	}
	oldest, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return DavisSnapshotCoverage{ObservedAt: observedAt}, coverageInspectionError(DavisCoverageMalformed, observedAt)
	}
	return DavisSnapshotCoverage{OldestSnapshot: oldest.UTC(), ObservedAt: observedAt}, nil
}

func (i *grailDavisSnapshotCoverageInspector) observedAt() time.Time {
	if i != nil && i.now != nil {
		return i.now().UTC()
	}
	return time.Now().UTC()
}

func davisSnapshotCoverageDQL(dataEnd time.Time) string {
	return fmt.Sprintf(`fetch %s,
  from:toTimestamp(%q),
  to:toTimestamp(%q)
| summarize oldest_snapshot=min(timestamp)`,
		execreplay.DavisProblemsSnapshotTable, davisSnapshotCoverageLower,
		dataEnd.UTC().Format(davisSnapshotCoverageTime),
	)
}

func classifyCoverageRequestFailure(err error) DavisSnapshotCoverageFailure {
	if errors.Is(err, context.Canceled) {
		return DavisCoverageCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return DavisCoverageTimeout
	}
	if _, rateLimited := sdkquery.RetryAfter(err); rateLimited {
		return DavisCoverageRateLimited
	}
	status := 0
	var queryErr *sdkquery.QueryError
	if errors.As(err, &queryErr) {
		status = queryErr.StatusCode
	}
	var apiErr *httpclient.APIError
	if status == 0 && errors.As(err, &apiErr) {
		status = apiErr.StatusCode
	}
	switch status {
	case http.StatusUnauthorized:
		return DavisCoverageUnauthorized
	case http.StatusForbidden:
		return DavisCoverageForbidden
	case http.StatusTooManyRequests:
		return DavisCoverageRateLimited
	default:
		return DavisCoverageUnavailable
	}
}

func classifyCoverageNotifications(notifications []sdkquery.Notification) (DavisSnapshotCoverageFailure, bool) {
	for _, notification := range notifications {
		switch classifyNotification(notification.NotificationType, notification.Message) {
		case notifScanLimit:
			return DavisCoverageScanLimited, true
		case notifResultLimit:
			return DavisCoverageResultLimited, true
		case notifTimeout:
			return DavisCoverageTimeout, true
		case notifConsumption:
			return DavisCoverageTruncated, true
		case notifSampling:
			continue
		default:
			return DavisCoverageUnexpectedNotification, true
		}
	}
	return "", false
}

func coverageInspectionError(failure DavisSnapshotCoverageFailure, observedAt time.Time) error {
	return &DavisSnapshotCoverageError{Failure: failure, ObservedAt: observedAt.UTC()}
}

type memoizedDavisSnapshotCoverageProvider struct {
	mu        sync.Mutex
	inspector DavisSnapshotCoverageInspector
	entries   map[DavisSnapshotCoverageKey]davisSnapshotCoverageMemoEntry
}

type davisSnapshotCoverageMemoEntry struct {
	coverage DavisSnapshotCoverage
	err      error
}

// NewMemoizedDavisSnapshotCoverageProvider creates one empty memo for one
// top-level command invocation.
func NewMemoizedDavisSnapshotCoverageProvider(inspector DavisSnapshotCoverageInspector) DavisSnapshotCoverageProvider {
	return &memoizedDavisSnapshotCoverageProvider{
		inspector: inspector,
		entries:   make(map[DavisSnapshotCoverageKey]davisSnapshotCoverageMemoEntry),
	}
}

func (p *memoizedDavisSnapshotCoverageProvider) Coverage(ctx context.Context, key DavisSnapshotCoverageKey) (DavisSnapshotCoverage, DavisSnapshotCoverageReuse, error) {
	normalized, err := normalizeDavisSnapshotCoverageKey(key)
	if err != nil {
		return DavisSnapshotCoverage{}, DavisSnapshotCoverageMiss, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[normalized]; ok {
		return entry.coverage, DavisSnapshotCoverageHit, entry.err
	}
	var coverage DavisSnapshotCoverage
	if p.inspector == nil {
		observedAt := time.Now().UTC()
		coverage = DavisSnapshotCoverage{ObservedAt: observedAt}
		err = coverageInspectionError(DavisCoverageUnavailable, observedAt)
	} else {
		coverage, err = p.inspector.Inspect(ctx, normalized.SessionID, normalized.ProbeUpperBound)
	}
	p.entries[normalized] = davisSnapshotCoverageMemoEntry{coverage: coverage, err: err}
	return coverage, DavisSnapshotCoverageMiss, err
}

func normalizeDavisSnapshotCoverageKey(key DavisSnapshotCoverageKey) (DavisSnapshotCoverageKey, error) {
	key.SessionID = strings.TrimSpace(key.SessionID)
	key.EnvironmentIdentity = strings.TrimSpace(key.EnvironmentIdentity)
	key.PrincipalIdentity = strings.TrimSpace(key.PrincipalIdentity)
	key.Table = strings.TrimSpace(key.Table)
	key.ProbeUpperBound = key.ProbeUpperBound.UTC().Round(0)
	if key.SessionID == "" || key.EnvironmentIdentity == "" || key.PrincipalIdentity == "" || key.Table == "" || key.ProbeUpperBound.IsZero() {
		return DavisSnapshotCoverageKey{}, errors.New("davis snapshot coverage memo requires a complete non-secret key")
	}
	return key, nil
}
