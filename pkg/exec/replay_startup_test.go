package exec

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestDQLExecutorLegacyReplayStartupHistoryFailsReadiness(t *testing.T) {
	for _, mode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			for _, query := range []string{"fetch logs", "timeseries value=avg(dt.host.cpu.usage)"} {
				t.Run(mode+"/"+disclosure+"/"+query, func(t *testing.T) {
					api := newReplayMockAPI(t)
					sink := &replayTestSink{}
					fixture := newReplayExecutorFixture(t, api, mode, disclosure,
						replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd,
						func(string) session.ProvenanceSink { return sink })
					legacy := fixture.started
					legacy.VirtualStart = legacy.DataStart.Add(time.Minute - time.Nanosecond)
					legacy.ReplayConfigHash = session.ReplayConfigHash(session.ResolvedReplayConfig{
						DataStart: legacy.DataStart, DataEnd: legacy.DataEnd, VirtualStart: legacy.VirtualStart,
						ClockMode: legacy.ClockMode, Disclosure: legacy.Disclosure, ProvenancePath: legacy.ProvenancePath,
					})
					// Persist an older writer's snapshot. The already advanced anchor
					// and the current clock must not repair its invalid initial history.
					data, err := json.Marshal(legacy)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(fixture.store.StatePath(fixture.locator.ContextKey), data, 0600); err != nil {
						t.Fatal(err)
					}
					fixture.clock.Add(2 * time.Minute)
					// Simulate edited context settings and output defaults. The
					// stored disclosure still owns ordinary readiness diagnostics.
					preparer := fixture.executor.preparer.(*ReplayQueryPreparer)
					preparer.config.FallbackDisclosure = session.ReplayDisclosureFull
					preparer.config.ExpectedContextInputHash = strings.Repeat("e", 64)
					_, err = fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), query, DQLExecuteOptions{AgentMode: true})
					if disclosure == session.ReplayDisclosureRestricted {
						if err == nil || err.Error() != restrictedReadinessMessage {
							t.Fatalf("restricted readiness error = %v", err)
						}
						_, appends, records := sink.snapshot()
						if appends != 1 || len(records) != 1 {
							t.Fatalf("private readiness records = %+v", records)
						}
						private, err := json.Marshal(records[0])
						if err != nil || !strings.Contains(string(private), "at least 60 seconds") {
							t.Fatalf("private startup detail missing: %s, %v", private, err)
						}
					} else if err == nil || !strings.Contains(err.Error(), "at least 60 seconds") || !strings.Contains(err.Error(), "restart with valid settings") {
						t.Fatalf("full readiness error = %v", err)
					}
					parseCalls, executeCalls, _ := api.counts()
					if parseCalls != 0 || executeCalls != 0 {
						t.Fatalf("invalid legacy startup made parse=%d execute=%d requests", parseCalls, executeCalls)
					}
					if _, err := fixture.store.Status(fixture.locator); err != nil {
						t.Fatalf("legacy status failed: %v", err)
					}
					if _, err := fixture.store.Stop(fixture.locator); err != nil {
						t.Fatalf("legacy stop failed: %v", err)
					}
				})
			}
		}
	}
}
