package cmd

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/dynatrace-oss/dtctl/pkg/diagnostic"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

type replayGuardTestSink struct {
	preflightErr, appendErr     error
	preflightCalls, appendCalls int
	record                      session.ReplayProvenanceRecord
}

func (s *replayGuardTestSink) Preflight(context.Context) error {
	s.preflightCalls++
	return s.preflightErr
}

func (s *replayGuardTestSink) Append(_ context.Context, record session.ReplayProvenanceRecord) error {
	s.appendCalls++
	s.record = record
	return s.appendErr
}

func TestRestrictedReplayGuardRecordingOutcomesShareAgentEnvelope(t *testing.T) {
	configureReplayGuardTest(t)
	// Even a recording cause with its own rich error fields must stay private.
	privateCause := &diagnostic.Error{Message: "private recording failure", StatusCode: 503}
	want := errorToDetail(&ReplayGuardError{Restricted: true})
	if want.Code != "command_unavailable" || want.Message != restrictedReplayGuardMessage || len(want.Suggestions) != 3 {
		t.Fatalf("unexpected normal rejection: %+v", want)
	}
	for _, plugin := range []bool{false, true} {
		for _, test := range []struct {
			name                      string
			missingPath               bool
			preflightErr, appendErr   error
			wantPreflight, wantAppend int
		}{
			{name: "recorded", wantPreflight: 1, wantAppend: 1},
			{name: "missing path", missingPath: true},
			{name: "preflight failure", preflightErr: privateCause, wantPreflight: 1},
			{name: "append failure", appendErr: privateCause, wantPreflight: 1, wantAppend: 1},
		} {
			t.Run(fmt.Sprintf("plugin=%t/%s", plugin, test.name), func(t *testing.T) {
				guard := &ReplayGuardError{Command: "delete workflows", ContextName: "historical-window", Plugin: plugin}
				if plugin {
					guard.Command = "synthetic-plugin"
				}
				privateDetail := guard.Error()
				activation := replayActivation{Disclosure: session.ReplayDisclosureRestricted, ProvenancePath: "unused-by-test-sink"}
				if test.missingPath {
					activation.ProvenancePath = ""
				}
				sink := &replayGuardTestSink{preflightErr: test.preflightErr, appendErr: test.appendErr}
				err := routeReplayGuardFailureWithSink(activation, guard, sink)
				if err.Error() != restrictedReplayGuardMessage {
					t.Fatalf("public message = %q", err.Error())
				}
				var retainedGuard *ReplayGuardError
				if !errors.As(err, &retainedGuard) || retainedGuard != guard || !retainedGuard.Restricted {
					t.Fatalf("restricted rejection was lost: %v", err)
				}
				for _, candidate := range []error{err, fmt.Errorf("private wrapper: %w", err)} {
					if got := errorToDetail(candidate); !reflect.DeepEqual(got, want) {
						t.Fatalf("recording changed agent fields: got %+v, want %+v", got, want)
					}
				}
				var recordingErr *replayGuardRecordingError
				wantRecordingFailure := test.missingPath || test.preflightErr != nil || test.appendErr != nil
				if errors.As(err, &recordingErr) != wantRecordingFailure {
					t.Fatalf("recording failure type = %T, want failure=%t", err, wantRecordingFailure)
				}
				if wantRecordingFailure && (recordingErr.detail == nil || !errors.Is(err, recordingErr.detail)) {
					t.Fatal("private recording cause was lost")
				}
				if (test.preflightErr != nil || test.appendErr != nil) && !errors.Is(err, privateCause) {
					t.Fatal("the recording cause changed")
				}
				if sink.preflightCalls != test.wantPreflight || sink.appendCalls != test.wantAppend {
					t.Fatalf("recording calls: preflight=%d append=%d", sink.preflightCalls, sink.appendCalls)
				}
				if test.wantAppend > 0 && (sink.record.Fields["detail"] != privateDetail || sink.record.Fields["command"] != guard.Command) {
					t.Fatalf("private rejection detail changed: %#v", sink.record.Fields)
				}
			})
		}
	}
}

func TestReplayGuardFullDisclosureDoesNotRecordRejection(t *testing.T) {
	guard := &ReplayGuardError{Command: "ctx token", ContextName: "historical-window"}
	privateDetail := guard.Error()
	sink := &replayGuardTestSink{}
	err := routeReplayGuardFailureWithSink(replayActivation{Disclosure: session.ReplayDisclosureFull}, guard, sink)
	if err != guard || guard.Restricted || err.Error() != privateDetail || sink.preflightCalls != 0 || sink.appendCalls != 0 {
		t.Fatalf("full disclosure changed: error=%v sink=%+v", err, sink)
	}
}
