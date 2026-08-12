package session

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const maxReplayStateBytes = 1 << 20

var errReplaySnapshotRace = errors.New("replay state snapshot changed during lock-free read")

func ensurePrivateReplayDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("create replay state directory %s: %w", dir, err)
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return fmt.Errorf("set replay state directory mode %s: %w", dir, err)
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return fmt.Errorf("inspect replay state directory %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing symlinked replay path %s", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("replay path %s is not a directory", dir)
	}
	if !replayFileOwnedByCurrentUser(dir, info) {
		return fmt.Errorf("replay directory %s is not owned by the current user", dir)
	}
	// Tightening an owner-controlled existing directory to the required mode
	// is safe and makes an explicitly supplied empty state directory usable.
	if err := setReplayPrivatePermissions(dir, 0700); err != nil {
		return fmt.Errorf("set replay state directory mode %s: %w", dir, err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect replay state directory %s: %w", dir, err)
	}
	return validatePrivateDirectoryInfo(dir, info)
}

func validatePrivateReplayDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return ErrReplaySessionNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect replay state directory %s: %w", dir, err)
	}
	return validatePrivateDirectoryInfo(dir, info)
}

func validatePrivateDirectoryInfo(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing symlinked replay path %s", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("replay path %s is not a directory", path)
	}
	return validateReplayPrivateDirectoryPermissions(path, info)
}

func validatePrivateRegularFile(path string, file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect replay file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("replay path %s is not a regular file", path)
	}
	return validateReplayPrivateHandlePermissions(path, file, info)
}

func (s *ReplayStateStore) locate(locator ReplayLocator) (ContextKey, ReplaySession, error) {
	key, state, _, err := s.locateWithDiagnostics(locator)
	return key, state, err
}

func (s *ReplayStateStore) locateWithDiagnostics(locator ReplayLocator) (ContextKey, ReplaySession, ReplayStateDiagnostics, error) {
	if locator.ContextKey == "" || locator.ContextIdentityHash == "" {
		return "", ReplaySession{}, ReplayStateDiagnostics{}, fmt.Errorf("replay state lookup requires a complete context identity")
	}
	if err := validatePrivateReplayDirectory(s.dir); err != nil {
		return "", ReplaySession{}, ReplayStateDiagnostics{}, err
	}

	entries, err := readReplayDirectory(s.dir)
	if err != nil {
		return "", ReplaySession{}, ReplayStateDiagnostics{}, fmt.Errorf("read replay state directory: %w", err)
	}
	type candidate struct {
		key   ContextKey
		state ReplaySession
	}
	type failedCandidate struct {
		key ContextKey
		err error
	}
	var matches []candidate
	var currentFailure *failedCandidate
	diagnostics := ReplayStateDiagnostics{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".state.json") {
			continue
		}
		keyText := strings.TrimSuffix(name, ".state.json")
		if !validReplayHash(keyText) {
			return ContextKey(keyText), ReplaySession{}, diagnostics, fmt.Errorf("unsafe replay state filename %q", name)
		}
		key := ContextKey(keyText)
		state, readErr := s.read(key)
		if readErr != nil {
			if key == locator.ContextKey {
				if errors.Is(readErr, ErrReplaySessionNotFound) {
					return "", ReplaySession{}, diagnostics, errReplaySnapshotRace
				}
				failure := failedCandidate{key: key, err: readErr}
				currentFailure = &failure
				continue
			}

			// A different key is not provably a different context identity:
			// ContextKey hashes (source, name, environment), while the identity
			// hashes only (source, name). An unreadable file at another key could
			// therefore be this context's session under an old environment URL.
			// We deliberately accept the narrow loss of that session when the file
			// is corrupt and the environment changed, rather than let one context's
			// unreadable state deny service to every other context.
			diagnostics.UnreadableStateFiles = append(diagnostics.UnreadableStateFiles, name)
			continue
		}
		if state.ContextIdentityHash == locator.ContextIdentityHash {
			matches = append(matches, candidate{key: key, state: state})
		}
	}
	sort.Strings(diagnostics.UnreadableStateFiles)
	if currentFailure != nil {
		// Explicit restart may recover exactly one corrupt state at the current
		// key. Any additional readable match makes recovery ambiguous, so return
		// no key and force manual inspection.
		if len(matches) == 0 {
			return currentFailure.key, ReplaySession{}, diagnostics, currentFailure.err
		}
		return "", ReplaySession{}, diagnostics, fmt.Errorf("replay state lookup is ambiguous (1 unreadable state file(s), %d readable match(es)); preserve the files before recovery: %w", len(matches), currentFailure.err)
	}
	if len(matches) == 0 {
		return "", ReplaySession{}, diagnostics, ErrReplaySessionNotFound
	}

	var live []candidate
	for _, match := range matches {
		if match.state.Status != ReplayStatusStopped {
			live = append(live, match)
		}
	}
	if len(live) > 1 {
		return "", ReplaySession{}, diagnostics, fmt.Errorf("%w: multiple live replay states exist for context identity %s; preserve the files and inspect them before recovery", errReplaySnapshotRace, locator.ContextIdentityHash)
	}
	if len(live) == 1 {
		return live[0].key, live[0].state, diagnostics, nil
	}
	for _, match := range matches {
		if match.key == locator.ContextKey {
			return match.key, match.state, diagnostics, nil
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].state.SessionStartedAt.After(matches[j].state.SessionStartedAt)
	})
	return matches[0].key, matches[0].state, diagnostics, nil
}

// locateSnapshot retries only races created by a cross-key atomic replacement:
// a reader can briefly observe both the old and new complete files, or an entry
// that the writer removes between ReadDir and open. It never retries corrupt,
// unsafe, or unsupported state and never takes the writer lock.
func (s *ReplayStateStore) locateSnapshot(locator ReplayLocator) (ContextKey, ReplaySession, error) {
	key, state, _, err := s.locateSnapshotWithDiagnostics(locator)
	return key, state, err
}

func (s *ReplayStateStore) locateSnapshotWithDiagnostics(locator ReplayLocator) (ContextKey, ReplaySession, ReplayStateDiagnostics, error) {
	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		key, state, diagnostics, err := s.locateWithDiagnostics(locator)
		if !errors.Is(err, errReplaySnapshotRace) || attempt == attempts-1 {
			return key, state, diagnostics, err
		}
		time.Sleep(s.retryInterval)
	}
	return "", ReplaySession{}, ReplayStateDiagnostics{}, errReplaySnapshotRace
}

func (s *ReplayStateStore) read(key ContextKey) (ReplaySession, error) {
	path := s.StatePath(key)
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return ReplaySession{}, fmt.Errorf("refusing symlinked replay state path %s", path)
	}
	f, err := openReplayFileNoFollow(path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return ReplaySession{}, ErrReplaySessionNotFound
	}
	if err != nil {
		return ReplaySession{}, fmt.Errorf("open replay state %s: %w", key, err)
	}
	defer f.Close()
	if err := validatePrivateRegularFile(path, f); err != nil {
		return ReplaySession{}, err
	}

	limited := &io.LimitedReader{R: f, N: maxReplayStateBytes + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return ReplaySession{}, fmt.Errorf("read replay state %s: %w", key, err)
	}
	if len(data) > maxReplayStateBytes {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: file exceeds %d bytes", key, maxReplayStateBytes)
	}

	var version struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: %w", key, err)
	}
	if version.SchemaVersion != ReplayStateSchemaVersion {
		return ReplaySession{}, fmt.Errorf("replay state %s has unsupported schema version %d (supported: %d)", key, version.SchemaVersion, ReplayStateSchemaVersion)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state ReplaySession
	if err := decoder.Decode(&state); err != nil {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: %w", key, err)
	}
	if err := requireReplayJSONEOF(decoder); err != nil {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: %w", key, err)
	}
	if state.ContextKey != key {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: context_key does not match its filename", key)
	}
	if err := validateReplaySession(&state); err != nil {
		return ReplaySession{}, fmt.Errorf("replay state %s is corrupt: %w", key, err)
	}
	return state, nil
}

func requireReplayJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return err
}

func (s *ReplayStateStore) write(state ReplaySession) error {
	if err := validateReplaySession(&state); err != nil {
		return fmt.Errorf("refuse invalid replay state write: %w", err)
	}
	if err := ensurePrivateReplayDirectory(s.dir); err != nil {
		return err
	}
	target := s.StatePath(state.ContextKey)
	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlinked replay state path %s", target)
		}
		f, openErr := openReplayFileNoFollow(target, os.O_RDONLY, 0)
		if openErr != nil {
			return fmt.Errorf("inspect existing replay state: %w", openErr)
		}
		validationErr := validatePrivateRegularFile(target, f)
		_ = f.Close()
		if validationErr != nil {
			return validationErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect replay state target: %w", err)
	}

	temp, err := os.CreateTemp(s.dir, ".replay-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create replay state temporary file: %w", err)
	}
	tempPath := temp.Name()
	keepTemp := true
	defer func() {
		_ = temp.Close()
		if keepTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := setReplayPrivatePermissions(tempPath, 0600); err != nil {
		return fmt.Errorf("set replay state temporary file mode: %w", err)
	}
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("encode replay state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("flush replay state temporary file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close replay state temporary file: %w", err)
	}
	if err := atomicReplaceReplayFile(tempPath, target); err != nil {
		return fmt.Errorf("replace replay state atomically: %w", err)
	}
	keepTemp = false
	return nil
}

func (s *ReplayStateStore) removeState(key ContextKey) error {
	path := s.StatePath(key)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing symlinked replay state path %s", path)
	}
	f, err := openReplayFileNoFollow(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	validationErr := validatePrivateRegularFile(path, f)
	_ = f.Close()
	if validationErr != nil {
		return validationErr
	}
	return os.Remove(path)
}

func validateReplaySession(state *ReplaySession) error {
	if state.SchemaVersion != ReplayStateSchemaVersion {
		return fmt.Errorf("unsupported schema version %d", state.SchemaVersion)
	}
	if len(state.SessionID) != 32 || !validReplayHex(state.SessionID) {
		return fmt.Errorf("invalid session_id")
	}
	if state.ContextName == "" || !validReplayHash(string(state.ContextKey)) ||
		!validReplayHash(state.ContextIdentityHash) || !validReplayHash(state.EnvironmentHash) ||
		!validReplayHash(state.ReplayConfigHash) || !validReplayHash(state.ContextInputHash) {
		return fmt.Errorf("missing or invalid context identity/hash field")
	}
	switch state.Status {
	case ReplayStatusActive, ReplayStatusTerminalReady, ReplayStatusCompleted, ReplayStatusStopped:
	default:
		return fmt.Errorf("invalid status %q", state.Status)
	}
	if state.Revision == 0 {
		return fmt.Errorf("revision must be positive")
	}
	if err := validateReplayTimes(state); err != nil {
		return err
	}
	if state.ClockMode != ReplayClockRealtime && state.ClockMode != ReplayClockManual {
		return fmt.Errorf("invalid clock_mode %q", state.ClockMode)
	}
	if state.Disclosure != ReplayDisclosureFull && state.Disclosure != ReplayDisclosureRestricted {
		return fmt.Errorf("invalid disclosure %q", state.Disclosure)
	}
	if state.Disclosure == ReplayDisclosureRestricted {
		if !filepath.IsAbs(state.ProvenancePath) || filepath.Clean(state.ProvenancePath) != state.ProvenancePath {
			return fmt.Errorf("invalid restricted provenance_path")
		}
	} else if state.ProvenancePath != "" {
		return fmt.Errorf("full disclosure state must not store provenance_path")
	}
	resolved := ResolvedReplayConfig{
		DataStart:      state.DataStart,
		DataEnd:        state.DataEnd,
		VirtualStart:   state.VirtualStart,
		ClockMode:      state.ClockMode,
		Disclosure:     state.Disclosure,
		ProvenancePath: state.ProvenancePath,
	}
	if state.ReplayConfigHash != ReplayConfigHash(resolved) {
		return fmt.Errorf("replay_config_hash does not match the stored session values")
	}
	if err := validateReplayValueSources(state.ValueSources); err != nil {
		return err
	}
	switch state.Status {
	case ReplayStatusActive, ReplayStatusTerminalReady:
		if state.CompletedAt != nil || state.StoppedAt != nil || state.FinalVirtualNow != nil {
			return fmt.Errorf("live state contains terminal lifecycle timestamps")
		}
		if state.Status == ReplayStatusTerminalReady && !state.AnchorVirtual.Equal(state.DataEnd) {
			return fmt.Errorf("terminal-ready state anchor_virtual does not equal data_end")
		}
	case ReplayStatusCompleted:
		if state.CompletedAt == nil {
			return fmt.Errorf("completed state is missing completed_at")
		}
		if state.StoppedAt != nil || state.FinalVirtualNow != nil {
			return fmt.Errorf("completed state contains stopped lifecycle timestamps")
		}
		if !state.AnchorVirtual.Equal(state.DataEnd) {
			return fmt.Errorf("completed state anchor_virtual does not equal data_end")
		}
	case ReplayStatusStopped:
		if state.StoppedAt == nil || state.FinalVirtualNow == nil {
			return fmt.Errorf("stopped state is missing stopped_at or final_virtual_now")
		}
		if !state.AnchorHost.Equal(*state.StoppedAt) || !state.AnchorVirtual.Equal(*state.FinalVirtualNow) {
			return fmt.Errorf("stopped state anchors do not match its final timestamps")
		}
	}
	return nil
}

func validateReplayTimes(state *ReplaySession) error {
	required := []*time.Time{
		&state.DataStart, &state.DataEnd, &state.VirtualStart,
		&state.SessionStartedAt, &state.AnchorHost, &state.AnchorVirtual,
	}
	for _, value := range required {
		if value.IsZero() {
			return fmt.Errorf("required timestamp is zero")
		}
		*value = value.UTC()
	}
	if !state.DataStart.Before(state.DataEnd) || state.VirtualStart.Before(state.DataStart) || state.VirtualStart.After(state.DataEnd) {
		return fmt.Errorf("invalid replay time bounds")
	}
	if state.AnchorVirtual.After(state.DataEnd) {
		return fmt.Errorf("anchor_virtual exceeds data_end")
	}
	for _, optional := range []*time.Time{state.CompletedAt, state.StoppedAt, state.FinalVirtualNow} {
		if optional != nil {
			*optional = optional.UTC()
		}
	}
	if state.FinalVirtualNow != nil && state.FinalVirtualNow.After(state.DataEnd) {
		return fmt.Errorf("final_virtual_now exceeds data_end")
	}
	return nil
}

func validateReplayValueSources(sources ReplayValueSources) error {
	values := []ReplayValueSource{
		sources.DataStart, sources.DataEnd, sources.VirtualStart,
		sources.ClockMode, sources.Disclosure, sources.ProvenancePath,
	}
	for _, source := range values {
		switch source {
		case ReplayValueFromFlag, ReplayValueFromContext, ReplayValueFromDefault:
		default:
			return fmt.Errorf("invalid replay value source %q", source)
		}
	}
	return nil
}

func validReplayHash(value string) bool {
	return len(value) == 64 && validReplayHex(value)
}

func validReplayHex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded)*2 == len(value)
}
