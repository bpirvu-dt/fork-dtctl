package exec

import (
	"testing"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestReplayDavisMappingPolicyUsesModeInBothDisclosures(t *testing.T) {
	tests := []struct {
		name       string
		disclosure string
		mode       ReplayExecutionMode
		want       execreplay.DavisProblemsMappingMode
	}{
		{"full execution", session.ReplayDisclosureFull, ReplayExecutionOneShot, execreplay.DavisProblemsMappingExecution},
		{"restricted execution", session.ReplayDisclosureRestricted, ReplayExecutionOneShot, execreplay.DavisProblemsMappingExecution},
		{"full wait", session.ReplayDisclosureFull, ReplayExecutionWait, execreplay.DavisProblemsMappingExecution},
		{"restricted live", session.ReplayDisclosureRestricted, ReplayExecutionLive, execreplay.DavisProblemsMappingExecution},
		{"full explain", session.ReplayDisclosureFull, ReplayExecutionExplain, execreplay.DavisProblemsMappingInspection},
		{"restricted explain", session.ReplayDisclosureRestricted, ReplayExecutionExplain, execreplay.DavisProblemsMappingInspection},
		{"full verify", session.ReplayDisclosureFull, ReplayExecutionVerify, execreplay.DavisProblemsMappingInspection},
		{"restricted verify", session.ReplayDisclosureRestricted, ReplayExecutionVerify, execreplay.DavisProblemsMappingInspection},
		{"unknown mode executes", session.ReplayDisclosureRestricted, ReplayExecutionMode("synthetic-mode"), execreplay.DavisProblemsMappingExecution},
		{"unknown disclosure fails closed", "synthetic-disclosure", ReplayExecutionOneShot, execreplay.DavisProblemsMappingDisabled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := replayDavisMappingPolicy(test.disclosure, test.mode).Mode; got != test.want {
				t.Fatalf("mapping mode = %q, want %q", got, test.want)
			}
		})
	}
}
