package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

const ReplayStateSchemaVersion = 1

const (
	ReplayStatusInactive      = "inactive"
	ReplayStatusActive        = "active"
	ReplayStatusTerminalReady = "terminal-ready"
	ReplayStatusCompleted     = "completed"
	ReplayStatusStopped       = "stopped"
	ReplayStatusDrifted       = "drifted"
)

const (
	CompletionRecorded         CompletionDisposition = "recorded"
	CompletionAlreadyCompleted CompletionDisposition = "already-completed"
	CompletionSessionReplaced  CompletionDisposition = "session-replaced"
)

var ErrReplaySessionNotFound = errors.New("replay session not found")

// Clock supplies host time to lifecycle operations. Production uses
// SystemClock; tests can share a deterministic fake across store instances.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the host wall clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// CompletionDisposition describes the result of one guarded completion write.
type CompletionDisposition string

// ReplaySession is the complete, versioned local replay state snapshot. It
// intentionally contains no credential and no returned telemetry.
type ReplaySession struct {
	SchemaVersion       int                `json:"schema_version" yaml:"schema_version"`
	SessionID           string             `json:"session_id" yaml:"session_id"`
	Status              string             `json:"status" yaml:"status"`
	ContextName         string             `json:"context_name" yaml:"context_name"`
	ContextKey          ContextKey         `json:"context_key" yaml:"context_key"`
	ContextIdentityHash string             `json:"context_identity_hash" yaml:"context_identity_hash"`
	EnvironmentHash     string             `json:"environment_hash" yaml:"environment_hash"`
	ReplayConfigHash    string             `json:"replay_config_hash" yaml:"replay_config_hash"`
	ContextInputHash    string             `json:"context_input_hash" yaml:"context_input_hash"`
	ValueSources        ReplayValueSources `json:"value_sources" yaml:"value_sources"`
	DataStart           time.Time          `json:"data_start" yaml:"data_start"`
	DataEnd             time.Time          `json:"data_end" yaml:"data_end"`
	VirtualStart        time.Time          `json:"virtual_start" yaml:"virtual_start"`
	ClockMode           string             `json:"clock_mode" yaml:"clock_mode"`
	Disclosure          string             `json:"disclosure" yaml:"disclosure"`
	ProvenancePath      string             `json:"provenance_path,omitempty" yaml:"provenance_path,omitempty"`
	SessionStartedAt    time.Time          `json:"session_started_at" yaml:"session_started_at"`
	AnchorHost          time.Time          `json:"anchor_host" yaml:"anchor_host"`
	AnchorVirtual       time.Time          `json:"anchor_virtual" yaml:"anchor_virtual"`
	CompletedAt         *time.Time         `json:"completed_at,omitempty" yaml:"completed_at,omitempty"`
	StoppedAt           *time.Time         `json:"stopped_at,omitempty" yaml:"stopped_at,omitempty"`
	FinalVirtualNow     *time.Time         `json:"final_virtual_now,omitempty" yaml:"final_virtual_now,omitempty"`
	Revision            uint64             `json:"revision" yaml:"revision"`
}

// ReplayLocator identifies a named context even when its environment has
// drifted since start.
type ReplayLocator struct {
	ContextKey          ContextKey
	ContextIdentityHash string
}

// ReplayStartRequest contains the already-validated context and replay inputs
// needed for one atomic start or restart.
type ReplayStartRequest struct {
	Locator          ReplayLocator
	ContextName      string
	EnvironmentHash  string
	ContextInputHash string
	Config           ResolvedReplayConfig
	Restart          bool
}

// ReplayStateDiagnostics contains opaque local-state warnings intended only
// for explicit replay management output. Filenames never include directories
// or decoded state-file contents.
type ReplayStateDiagnostics struct {
	UnreadableStateFiles []string
}

// ReplayStore is the lifecycle boundary shared by CLI management and later
// replay-aware query integration. Status and ReadActive are lock-free; every
// other method performs a mandatory cross-process locked update.
type ReplayStore interface {
	Start(ReplayStartRequest) (ReplaySession, error)
	Advance(ReplayLocator, time.Duration, string) (ReplaySession, error)
	Stop(ReplayLocator) (ReplaySession, error)
	Status(ReplayLocator) (ReplaySession, error)
	ReadActive(ReplayLocator) (ReplaySession, error)
	MarkCompleted(ReplayLocator, string, time.Time) (ReplaySession, CompletionDisposition, error)
}

var _ ReplayStore = (*ReplayStateStore)(nil)

// ReplayStateStore owns local replay state I/O and mandatory writer locking.
type ReplayStateStore struct {
	dir           string
	clock         Clock
	lockTimeout   time.Duration
	retryInterval time.Duration
}

// NewReplayStateStore returns a store rooted at dir. The caller can inject a
// fake Clock; nil selects the system clock.
func NewReplayStateStore(dir string, clock Clock) *ReplayStateStore {
	if clock == nil {
		clock = SystemClock{}
	}
	return &ReplayStateStore{
		dir:           filepath.Clean(dir),
		clock:         clock,
		lockTimeout:   30 * time.Second,
		retryInterval: 50 * time.Millisecond,
	}
}

// NewReplayStore uses the default private replay state directory.
func NewReplayStore(clock Clock) *ReplayStateStore {
	return NewReplayStateStore(ReplayDir(), clock)
}

// StatePath returns the opaque path for a context state file.
func (s *ReplayStateStore) StatePath(key ContextKey) string {
	return filepath.Join(s.dir, string(key)+".state.json")
}

// StateKey returns the opaque filename identity without exposing config or URL.
func (s *ReplayStateStore) StateKey(key ContextKey) string { return string(key) }

func (s *ReplayStateStore) writerLockPath(identityHash string) string {
	return filepath.Join(s.dir, identityHash+".lock")
}

// Start creates a new session under the mandatory context-identity writer lock.
// A normal start may replace a stopped state but not a live/completed session.
func (s *ReplayStateStore) Start(req ReplayStartRequest) (ReplaySession, error) {
	if err := validateReplayStartRequest(req); err != nil {
		return ReplaySession{}, err
	}
	if err := ensurePrivateReplayDirectory(s.dir); err != nil {
		return ReplaySession{}, err
	}
	unlock, err := acquireReplayFileLock(s.writerLockPath(req.Locator.ContextIdentityHash), s.lockTimeout, s.retryInterval)
	if err != nil {
		return ReplaySession{}, err
	}
	defer unlock()

	currentKey, current, locateErr := s.locate(req.Locator)
	if locateErr != nil && !errors.Is(locateErr, ErrReplaySessionNotFound) {
		if !req.Restart {
			return ReplaySession{}, locateErr
		}
		// --restart is the explicit recovery path for a corrupt state at the
		// current key. It never guesses which file to replace when the failing
		// state cannot be tied unambiguously to that key.
		if currentKey != req.Locator.ContextKey {
			return ReplaySession{}, locateErr
		}
	}
	if locateErr == nil && current.Status != ReplayStatusStopped && !req.Restart {
		if current.Status == ReplayStatusCompleted {
			return ReplaySession{}, fmt.Errorf("replay session for context %q is completed; inspect it with 'dtctl replay status' or restart it with 'dtctl replay start --restart'", req.ContextName)
		}
		return ReplaySession{}, fmt.Errorf("replay session is already active for context %q; run 'dtctl replay status' or restart it with 'dtctl replay start --restart'", req.ContextName)
	}

	sessionID, err := newReplaySessionID()
	if err != nil {
		return ReplaySession{}, fmt.Errorf("generate replay session ID: %w", err)
	}
	revision := uint64(1)
	if locateErr == nil {
		revision = current.Revision + 1
	}
	// Capture the activation anchor only after validation, existing-state
	// checks, and session-ID generation, immediately before constructing and
	// atomically replacing the state snapshot.
	hostNow := s.clock.Now().UTC()
	next := ReplaySession{
		SchemaVersion:       ReplayStateSchemaVersion,
		SessionID:           sessionID,
		Status:              ReplayStatusActive,
		ContextName:         req.ContextName,
		ContextKey:          req.Locator.ContextKey,
		ContextIdentityHash: req.Locator.ContextIdentityHash,
		EnvironmentHash:     req.EnvironmentHash,
		ReplayConfigHash:    ReplayConfigHash(req.Config),
		ContextInputHash:    req.ContextInputHash,
		ValueSources:        req.Config.ValueSources,
		DataStart:           req.Config.DataStart.UTC(),
		DataEnd:             req.Config.DataEnd.UTC(),
		VirtualStart:        req.Config.VirtualStart.UTC(),
		ClockMode:           req.Config.ClockMode,
		Disclosure:          req.Config.Disclosure,
		ProvenancePath:      req.Config.ProvenancePath,
		SessionStartedAt:    hostNow,
		AnchorHost:          hostNow,
		AnchorVirtual:       req.Config.VirtualStart.UTC(),
		Revision:            revision,
	}
	if next.AnchorVirtual.Equal(next.DataEnd) {
		next.Status = ReplayStatusTerminalReady
	}
	if err := s.write(next); err != nil {
		return ReplaySession{}, err
	}
	if locateErr == nil && currentKey != "" && currentKey != next.ContextKey {
		if err := s.removeState(currentKey); err != nil {
			if rollbackErr := s.removeState(next.ContextKey); rollbackErr != nil {
				return ReplaySession{}, fmt.Errorf("remove replaced replay state: %w (rollback of new state also failed: %v)", err, rollbackErr)
			}
			return ReplaySession{}, fmt.Errorf("remove replaced replay state: %w", err)
		}
	}
	return next, nil
}

// Status returns a lock-free validated snapshot with terminal readiness
// derived from the caller's current host clock. It performs no write.
func (s *ReplayStateStore) Status(locator ReplayLocator) (ReplaySession, error) {
	_, state, err := s.locateSnapshot(locator)
	if err != nil {
		return ReplaySession{}, err
	}
	return observedReplaySession(state, s.clock.Now()), nil
}

// StatusWithDiagnostics returns the same lock-free snapshot as Status plus
// opaque warnings about unreadable state files at other context keys. Callers
// must only surface these warnings in explicit replay management output.
func (s *ReplayStateStore) StatusWithDiagnostics(locator ReplayLocator) (ReplaySession, ReplayStateDiagnostics, error) {
	_, state, diagnostics, err := s.locateSnapshotWithDiagnostics(locator)
	if err != nil {
		return ReplaySession{}, diagnostics, err
	}
	return observedReplaySession(state, s.clock.Now()), diagnostics, nil
}

// ReadActive returns an active or terminal-ready lock-free snapshot.
func (s *ReplayStateStore) ReadActive(locator ReplayLocator) (ReplaySession, error) {
	state, err := s.Status(locator)
	if err != nil {
		return ReplaySession{}, err
	}
	if state.Status != ReplayStatusActive && state.Status != ReplayStatusTerminalReady {
		return ReplaySession{}, fmt.Errorf("replay session for context %q is %s", state.ContextName, state.Status)
	}
	return state, nil
}

// Advance atomically moves an active session by a positive fixed duration.
// expectedContextInputHash makes drift validation part of the locked update.
func (s *ReplayStateStore) Advance(locator ReplayLocator, by time.Duration, expectedContextInputHash string) (ReplaySession, error) {
	if by <= 0 {
		return ReplaySession{}, fmt.Errorf("replay advance duration must be positive")
	}
	return s.update(locator, func(state ReplaySession, hostNow time.Time) (ReplaySession, error) {
		state = observedReplaySession(state, hostNow)
		if state.ContextInputHash != expectedContextInputHash {
			return ReplaySession{}, replayContextDriftError()
		}
		if state.Status != ReplayStatusActive && state.Status != ReplayStatusTerminalReady {
			return ReplaySession{}, fmt.Errorf("cannot advance replay session in %s state", state.Status)
		}
		current := VirtualNow(state, hostNow)
		remaining := state.DataEnd.Sub(current)
		if by > remaining {
			return ReplaySession{}, fmt.Errorf("replay advance would pass data_end; remaining duration is %s", remaining)
		}
		target := current.Add(by).UTC()
		state.AnchorVirtual = target
		if state.ClockMode == ReplayClockRealtime {
			state.AnchorHost = hostNow.UTC()
		}
		state.Status = ReplayStatusActive
		if target.Equal(state.DataEnd) {
			state.Status = ReplayStatusTerminalReady
		}
		state.Revision++
		return state, nil
	})
}

// Stop atomically preserves a final clamped virtual timestamp and stopped_at.
func (s *ReplayStateStore) Stop(locator ReplayLocator) (ReplaySession, error) {
	return s.update(locator, func(state ReplaySession, hostNow time.Time) (ReplaySession, error) {
		if state.Status == ReplayStatusStopped {
			return ReplaySession{}, fmt.Errorf("replay session for context %q is already stopped", state.ContextName)
		}
		finalVirtual := VirtualNow(state, hostNow).UTC()
		state.Status = ReplayStatusStopped
		state.StoppedAt = replayTimePtr(hostNow.UTC())
		state.FinalVirtualNow = replayTimePtr(finalVirtual)
		state.AnchorHost = hostNow.UTC()
		state.AnchorVirtual = finalVirtual
		state.Revision++
		return state, nil
	})
}

// MarkCompleted performs the guarded, idempotent terminal completion write.
func (s *ReplayStateStore) MarkCompleted(locator ReplayLocator, expectedSessionID string, now time.Time) (ReplaySession, CompletionDisposition, error) {
	if err := ensurePrivateReplayDirectory(s.dir); err != nil {
		return ReplaySession{}, "", err
	}
	unlock, err := acquireReplayFileLock(s.writerLockPath(locator.ContextIdentityHash), s.lockTimeout, s.retryInterval)
	if err != nil {
		return ReplaySession{}, "", err
	}
	defer unlock()

	_, state, err := s.locate(locator)
	if err != nil {
		return ReplaySession{}, "", err
	}
	if state.SessionID != expectedSessionID || state.Status == ReplayStatusStopped {
		return state, CompletionSessionReplaced, nil
	}
	if state.Status == ReplayStatusCompleted {
		return state, CompletionAlreadyCompleted, nil
	}
	hostNow := now.UTC()
	state = observedReplaySession(state, hostNow)
	if state.Status != ReplayStatusTerminalReady {
		return ReplaySession{}, "", fmt.Errorf("replay session for context %q is not terminal-ready", state.ContextName)
	}
	state.Status = ReplayStatusCompleted
	state.CompletedAt = replayTimePtr(hostNow)
	state.AnchorHost = hostNow
	state.AnchorVirtual = state.DataEnd
	state.Revision++
	if err := s.write(state); err != nil {
		return ReplaySession{}, "", err
	}
	return state, CompletionRecorded, nil
}

func (s *ReplayStateStore) update(locator ReplayLocator, mutate func(ReplaySession, time.Time) (ReplaySession, error)) (ReplaySession, error) {
	if err := ensurePrivateReplayDirectory(s.dir); err != nil {
		return ReplaySession{}, err
	}
	unlock, err := acquireReplayFileLock(s.writerLockPath(locator.ContextIdentityHash), s.lockTimeout, s.retryInterval)
	if err != nil {
		return ReplaySession{}, err
	}
	defer unlock()

	_, state, err := s.locate(locator)
	if err != nil {
		return ReplaySession{}, err
	}
	next, err := mutate(state, s.clock.Now().UTC())
	if err != nil {
		return ReplaySession{}, err
	}
	if err := s.write(next); err != nil {
		return ReplaySession{}, err
	}
	return next, nil
}

// VirtualNow is the pure replay clock calculation. Realtime follows elapsed
// host time; manual remains anchored. Both clamp at data_end.
func VirtualNow(session ReplaySession, hostNow time.Time) time.Time {
	if session.Status == ReplayStatusStopped && session.FinalVirtualNow != nil {
		return session.FinalVirtualNow.UTC()
	}
	if session.Status == ReplayStatusCompleted {
		return session.DataEnd.UTC()
	}
	virtual := session.AnchorVirtual.UTC()
	if session.ClockMode == ReplayClockRealtime {
		virtual = virtual.Add(hostNow.Sub(session.AnchorHost)).UTC()
	}
	if virtual.After(session.DataEnd) {
		return session.DataEnd.UTC()
	}
	return virtual
}

// VisibleEnd returns the causal upper bound, clamped to an empty interval if a
// backward host-clock adjustment moves virtual time before data_start.
func VisibleEnd(session ReplaySession, hostNow time.Time) time.Time {
	virtual := VirtualNow(session, hostNow)
	if virtual.Before(session.DataStart) {
		return session.DataStart.UTC()
	}
	return virtual.UTC()
}

// ReplayGuardActive reports whether this stored state remains an activation
// signal for the hard command guard. A stopped flags-only state deliberately is not.
func ReplayGuardActive(session ReplaySession) bool {
	return session.Status != ReplayStatusStopped
}

func observedReplaySession(state ReplaySession, hostNow time.Time) ReplaySession {
	if state.Status == ReplayStatusActive && VirtualNow(state, hostNow).Equal(state.DataEnd) {
		state.Status = ReplayStatusTerminalReady
	}
	return state
}

func validateReplayStartRequest(req ReplayStartRequest) error {
	if req.Locator.ContextKey == "" || req.Locator.ContextIdentityHash == "" || req.ContextName == "" {
		return fmt.Errorf("replay start requires a complete context identity")
	}
	if req.EnvironmentHash == "" || req.ContextInputHash == "" {
		return fmt.Errorf("replay start requires environment and context-input hashes")
	}
	return validateResolvedReplayConfig(req.Config)
}

func validateResolvedReplayConfig(cfg ResolvedReplayConfig) error {
	if cfg.DataStart.IsZero() || cfg.DataEnd.IsZero() || cfg.VirtualStart.IsZero() {
		return fmt.Errorf("resolved replay configuration has a zero timestamp")
	}
	if !cfg.DataStart.Before(cfg.DataEnd) || cfg.VirtualStart.Before(cfg.DataStart) || cfg.VirtualStart.After(cfg.DataEnd) {
		return fmt.Errorf("resolved replay configuration has invalid time bounds")
	}
	if cfg.ClockMode != ReplayClockRealtime && cfg.ClockMode != ReplayClockManual {
		return fmt.Errorf("resolved replay configuration has invalid clock mode %q", cfg.ClockMode)
	}
	if cfg.Disclosure != ReplayDisclosureFull && cfg.Disclosure != ReplayDisclosureRestricted {
		return fmt.Errorf("resolved replay configuration has invalid disclosure %q", cfg.Disclosure)
	}
	if cfg.Disclosure == ReplayDisclosureRestricted && cfg.ProvenancePath == "" {
		return fmt.Errorf("restricted replay disclosure requires a provenance path")
	}
	return nil
}

func replayContextDriftError() error {
	return fmt.Errorf("the replay context changed after this session started; restart the replay session to use the new configuration")
}

func newReplaySessionID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func replayTimePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
