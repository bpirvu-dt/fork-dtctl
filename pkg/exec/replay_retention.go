package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/client"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

const (
	replayRetentionInspectionTimeout = 5 * time.Second
	replayMaximumRetentionDays       = int64(3657)
	replayRetentionInspectionDQL     = `fetch dt.system.buckets
| filter in(dt.system.table, array("logs", "spans", "events", "bizevents", "metrics", "dt.system.events"))
| summarize bucket_count=count(), minimum_retention_days=min(retention_days), maximum_retention_days=max(retention_days), by:{dt.system.table}
| sort dt.system.table asc`
)

// RetentionTableBounds is current aggregate bucket metadata for one public
// table family. It deliberately contains no bucket name or tenant identifier.
type RetentionTableBounds struct {
	Table                string
	BucketCount          int64
	MinimumRetentionDays int64
	MaximumRetentionDays int64
}

// RetentionInspection is the best information available to one replay
// invocation. Current Grail metadata never sets HistoricalResolutionVerified;
// the field keeps the warning decision explicit and independently testable.
type RetentionInspection struct {
	Tables                       map[string]RetentionTableBounds
	HistoricalResolutionVerified bool
}

// RetentionInspector reads current aggregate retention metadata. Failure is a
// soft replay warning; implementations must not change tenant configuration.
type RetentionInspector interface {
	Inspect(context.Context) (RetentionInspection, error)
}

type grailRetentionInspector struct {
	handler *sdkquery.Handler
	timeout time.Duration
}

// NewGrailRetentionInspector creates the single intentional direct DQL read
// used by replay preparation. The fixed query reads aggregate current bucket
// metadata. It must not pass through the replay compiler because
// dt.system.buckets is current metadata, not replayed telemetry.
func NewGrailRetentionInspector(c *client.Client) RetentionInspector {
	if c == nil {
		return nil
	}
	handler := sdkquery.NewHandler(httpclient.Wrap(c.HTTP())).
		WithHeaders(map[string]string{"dt-client-context": dtClientContextHeader("replay-retention-inspection")}).
		WithFirstRateLimitResponse()
	return &grailRetentionInspector{handler: handler, timeout: replayRetentionInspectionTimeout}
}

func (i *grailRetentionInspector) Inspect(ctx context.Context) (RetentionInspection, error) {
	if i == nil || i.handler == nil {
		return RetentionInspection{}, errors.New("retention inspector is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := i.timeout
	if timeout <= 0 {
		timeout = replayRetentionInspectionTimeout
	}
	inspectionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// The evidence query normally completes synchronously. An asynchronous
	// response is treated as unavailable instead of adding a long poll loop to
	// the user's command latency.
	response, err := i.handler.Execute(inspectionCtx, sdkquery.ExecuteRequest{
		Query:                      replayRetentionInspectionDQL,
		RequestTimeoutMilliseconds: timeout.Milliseconds(),
		PollingPromiseSeconds:      5,
		MaxResultRecords:           100,
		DefaultScanLimitGbytes:     1,
		DefaultSamplingRatio:       1,
		Locale:                     "en_US",
		Timezone:                   "UTC",
	})
	if err != nil {
		return RetentionInspection{}, fmt.Errorf("inspect current retention metadata: %w", err)
	}
	if response == nil || !strings.EqualFold(response.State, "SUCCEEDED") {
		return RetentionInspection{}, errors.New("current retention metadata did not complete synchronously")
	}

	records := response.Records
	if response.Result != nil {
		records = response.Result.Records
	}
	inspection := RetentionInspection{Tables: make(map[string]RetentionTableBounds, len(records))}
	for _, record := range records {
		bounds, err := parseRetentionBounds(record)
		if err != nil {
			return RetentionInspection{}, err
		}
		if _, duplicate := inspection.Tables[bounds.Table]; duplicate {
			return RetentionInspection{}, fmt.Errorf("current retention metadata repeated table family %q", bounds.Table)
		}
		inspection.Tables[bounds.Table] = bounds
	}
	if err := validateRetentionInspection(inspection); err != nil {
		return RetentionInspection{}, err
	}
	return inspection, nil
}

func parseRetentionBounds(record map[string]interface{}) (RetentionTableBounds, error) {
	table, ok := record["dt.system.table"].(string)
	if !ok || !allowedRetentionFamily(table) {
		return RetentionTableBounds{}, errors.New("current retention metadata contained an unexpected table family")
	}
	bucketCount, err := retentionInteger(record["bucket_count"])
	if err != nil {
		return RetentionTableBounds{}, fmt.Errorf("%s bucket count: %w", table, err)
	}
	minimum, err := retentionInteger(record["minimum_retention_days"])
	if err != nil {
		return RetentionTableBounds{}, fmt.Errorf("%s minimum retention: %w", table, err)
	}
	maximum, err := retentionInteger(record["maximum_retention_days"])
	if err != nil {
		return RetentionTableBounds{}, fmt.Errorf("%s maximum retention: %w", table, err)
	}
	return RetentionTableBounds{
		Table: table, BucketCount: bucketCount,
		MinimumRetentionDays: minimum, MaximumRetentionDays: maximum,
	}, nil
}

func retentionInteger(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return 0, errors.New("value is not an integer")
		}
		return parsed, nil
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, errors.New("value is not an integer")
		}
		return parsed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed != math.Trunc(typed) || typed < 0 || typed >= 9223372036854775808.0 {
			return 0, errors.New("value is not a non-negative integer")
		}
		return int64(typed), nil
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	default:
		return 0, errors.New("value is missing")
	}
}

func validateRetentionInspection(value RetentionInspection) error {
	if len(value.Tables) == 0 {
		return errors.New("current retention metadata returned no table families")
	}
	for key, bounds := range value.Tables {
		if key != bounds.Table || !allowedRetentionFamily(bounds.Table) {
			return errors.New("current retention metadata contained an unexpected table family")
		}
		if bounds.BucketCount <= 0 || bounds.MinimumRetentionDays <= 0 ||
			bounds.MaximumRetentionDays < bounds.MinimumRetentionDays ||
			bounds.MaximumRetentionDays > replayMaximumRetentionDays {
			return fmt.Errorf("current retention metadata for %s is invalid", bounds.Table)
		}
	}
	return nil
}

func allowedRetentionFamily(table string) bool {
	switch table {
	case "logs", "spans", "events", "bizevents", "metrics", "dt.system.events":
		return true
	default:
		return false
	}
}

func (p *ReplayQueryPreparer) addRetentionNotices(ctx context.Context, mode ReplayExecutionMode, compilation *execreplay.CompileResult, state session.ReplaySession, hostNow time.Time) {
	if compilation == nil || mode == ReplayExecutionExplain || mode == ReplayExecutionVerify || !hasStoredTelemetrySource(compilation.Sources) {
		return
	}
	p.retentionOnce.Do(func() {
		if p.config.RetentionInspector == nil {
			p.retentionErr = errors.New("retention inspection is unavailable")
			return
		}
		inspection, err := p.config.RetentionInspector.Inspect(ctx)
		if err == nil {
			err = validateRetentionInspection(inspection)
		}
		if err != nil {
			p.retentionErr = err
			return
		}
		p.retentionInspection = cloneRetentionInspection(inspection)
	})

	notices := retentionNotices(compilation.Sources, state.DataStart.UTC(), hostNow.UTC(), p.retentionInspection, p.retentionErr)
	compilation.Notices = append(compilation.Notices, notices...)
	compilation.Explain.Notices = append([]execreplay.Notice(nil), compilation.Notices...)
}

func hasStoredTelemetrySource(sources []execreplay.SourceCompilation) bool {
	for _, source := range sources {
		if source.Source.Class == execreplay.SourceRecord || source.Source.Class == execreplay.SourceMetric {
			return true
		}
	}
	return false
}

func cloneRetentionInspection(value RetentionInspection) RetentionInspection {
	clone := RetentionInspection{
		Tables:                       make(map[string]RetentionTableBounds, len(value.Tables)),
		HistoricalResolutionVerified: value.HistoricalResolutionVerified,
	}
	for key, bounds := range value.Tables {
		clone.Tables[key] = bounds
	}
	return clone
}

func retentionNotices(sources []execreplay.SourceCompilation, replayStart, hostNow time.Time, inspection RetentionInspection, inspectionErr error) []execreplay.Notice {
	if inspectionErr != nil {
		return []execreplay.Notice{{
			Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionNotVerified, SourceOrdinal: -1,
			Message: "Retention and historical metric resolution were not verified because current retention metadata was unavailable. The query will continue without changing tenant retention.",
		}}
	}

	var notices []execreplay.Notice
	for _, source := range sources {
		family := retentionFamily(source.Source)
		if family == "" {
			if source.Source.Class != execreplay.SourceSynthetic {
				notices = append(notices, execreplay.Notice{
					Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionNotVerified, SourceOrdinal: source.Source.Ordinal,
					Message: fmt.Sprintf("Retention was not verified for %s because current bucket metadata exposes only public table families. The query will continue without changing tenant retention.", source.Source.Name),
				})
			}
			continue
		}

		bounds, ok := inspection.Tables[family]
		if !ok {
			notices = append(notices, execreplay.Notice{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionNotVerified, SourceOrdinal: source.Source.Ordinal,
				Message: fmt.Sprintf("Retention was not verified for %s because current retention metadata did not include that table family. The query will continue without changing tenant retention.", family),
			})
		} else if replayStart.Before(hostNow.Add(-time.Duration(bounds.MinimumRetentionDays) * 24 * time.Hour)) {
			message := fmt.Sprintf(
				"The replay interval starts before the shortest current %s retention setting of %d days. Data from some matching buckets may be unavailable.",
				family, bounds.MinimumRetentionDays,
			)
			if replayStart.Before(hostNow.Add(-time.Duration(bounds.MaximumRetentionDays) * 24 * time.Hour)) {
				message = fmt.Sprintf(
					"The replay interval starts before the longest current %s retention setting of %d days. Matching historical data may be unavailable.",
					family, bounds.MaximumRetentionDays,
				)
			}
			notices = append(notices, execreplay.Notice{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeRetentionBoundary,
				Message: message, SourceOrdinal: source.Source.Ordinal,
			})
		}

		if source.Source.Class == execreplay.SourceMetric && !inspection.HistoricalResolutionVerified {
			notices = append(notices, execreplay.Notice{
				Kind: execreplay.NoticeWarning, Code: execreplay.NoticeHistoricalResolutionUnverified, SourceOrdinal: source.Source.Ordinal,
				Message: "Historical metric resolution was not verified because current bucket metadata does not expose past resolution transitions. Fine-grained measurements may already be rolled up.",
			})
		}
	}
	return notices
}

func retentionFamily(source execreplay.SourceDescriptor) string {
	if source.Class == execreplay.SourceMetric {
		return "metrics"
	}
	if source.Class != execreplay.SourceRecord {
		return ""
	}
	switch source.Name {
	case "logs", "spans", "events", "bizevents", "dt.system.events":
		return source.Name
	default:
		return ""
	}
}
