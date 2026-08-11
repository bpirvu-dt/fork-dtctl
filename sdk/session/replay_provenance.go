package session

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const ReplayProvenanceSchemaVersion = 1

// ReplayProvenanceRecord is one local JSON Lines management/query event. Phase
// 1 builds the durable sink; query integration populates richer fields later.
type ReplayProvenanceRecord struct {
	SchemaVersion int            `json:"schema_version"`
	RecordedAt    time.Time      `json:"recorded_at"`
	Event         string         `json:"event"`
	SessionID     string         `json:"session_id,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
}

// ProvenanceSink is the private serialized append boundary used by restricted
// disclosure. Query paths do not call it until replay executor integration.
type ProvenanceSink interface {
	Preflight(ctx context.Context) error
	Append(ctx context.Context, record ReplayProvenanceRecord) error
}

// FileProvenanceSink writes complete, durably flushed JSON Lines records under
// a mandatory lock keyed by the normalized sink path.
type FileProvenanceSink struct {
	path          string
	lockDir       string
	lockTimeout   time.Duration
	retryInterval time.Duration
}

// NewFileProvenanceSink creates a sink with an injectable private lock
// directory. The provenance parent must already exist; the default replay
// directory is created by Preflight when path is below lockDir.
func NewFileProvenanceSink(path, lockDir string) *FileProvenanceSink {
	return &FileProvenanceSink{
		path:          filepath.Clean(path),
		lockDir:       filepath.Clean(lockDir),
		lockTimeout:   30 * time.Second,
		retryInterval: 50 * time.Millisecond,
	}
}

// NewReplayProvenanceSink uses the default replay state directory for locks.
func NewReplayProvenanceSink(path string) *FileProvenanceSink {
	return NewFileProvenanceSink(path, ReplayDir())
}

// Path returns the normalized sink path stored in replay state.
func (s *FileProvenanceSink) Path() string { return s.path }

// LockPath returns an opaque append-lock path. Its filename reveals no context,
// environment, or configured provenance path.
func (s *FileProvenanceSink) LockPath() string {
	digest := hashReplayValue(s.path)
	return filepath.Join(s.lockDir, "provenance-"+digest+".lock")
}

// ValidateReplayProvenancePath validates syntax, privacy, ownership, file type,
// and no-symlink requirements. allowMissingParent is reserved for the default
// path below the replay directory, which Preflight creates privately.
func ValidateReplayProvenancePath(path string, allowMissingParent bool) error {
	if path == "" {
		return fmt.Errorf("restricted replay disclosure requires provenance_path")
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("replay provenance_path must be absolute: %q", path)
	}
	if filepath.Clean(path) != path {
		return fmt.Errorf("replay provenance_path must be normalized: %q", path)
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if errors.Is(err, os.ErrNotExist) && allowMissingParent {
		// The default replay directory is created with mode 0700 by the sink.
	} else if err != nil {
		return fmt.Errorf("inspect replay provenance parent %s: %w", parent, err)
	} else if allowMissingParent {
		if parentInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlinked replay path %s", parent)
		}
		if !parentInfo.IsDir() || !replayFileOwnedByCurrentUser(parent, parentInfo) {
			return fmt.Errorf("default replay provenance parent %s is unsafe", parent)
		}
	} else if err := validatePrivateDirectoryInfo(parent, parentInfo); err != nil {
		return err
	}

	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect replay provenance path %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing symlinked replay provenance path %s", path)
	}
	f, err := openReplayFileNoFollow(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open replay provenance path %s: %w", path, err)
	}
	defer f.Close()
	return validatePrivateRegularFile(path, f)
}

// Preflight proves that the private sink and its mandatory append lock are
// usable without appending a record.
func (s *FileProvenanceSink) Preflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.prepareDirectories(); err != nil {
		return err
	}
	unlock, err := acquireReplayFileLock(s.LockPath(), s.lockTimeout, s.retryInterval)
	if err != nil {
		return fmt.Errorf("replay provenance lock: %w", err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := s.openAppendFile()
	if err != nil {
		return err
	}
	if err := validateProvenanceJSONLines(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close replay provenance file: %w", err)
	}
	return nil
}

// Append serializes exactly one complete JSON Lines record and flushes it
// durably before releasing the cross-process lock.
func (s *FileProvenanceSink) Append(ctx context.Context, record ReplayProvenanceRecord) error {
	if err := validateReplayProvenanceRecord(record); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.prepareDirectories(); err != nil {
		return err
	}
	unlock, err := acquireReplayFileLock(s.LockPath(), s.lockTimeout, s.retryInterval)
	if err != nil {
		return fmt.Errorf("replay provenance lock: %w", err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	f, err := s.openAppendFile()
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if err := validateProvenanceJSONLines(f); err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode replay provenance record: %w", err)
	}
	data = append(data, '\n')
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek replay provenance append position: %w", err)
	}
	if err := writeReplayBytes(f, data); err != nil {
		return fmt.Errorf("append replay provenance record: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("flush replay provenance record: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close replay provenance file: %w", err)
	}
	closed = true
	return nil
}

func (s *FileProvenanceSink) prepareDirectories() error {
	if !filepath.IsAbs(s.path) || filepath.Clean(s.path) != s.path {
		return ValidateReplayProvenancePath(s.path, false)
	}
	if err := ensurePrivateReplayDirectory(s.lockDir); err != nil {
		return err
	}
	parent := filepath.Dir(s.path)
	if parent == s.lockDir {
		if err := ensurePrivateReplayDirectory(parent); err != nil {
			return err
		}
	}
	return ValidateReplayProvenancePath(s.path, false)
}

func (s *FileProvenanceSink) openAppendFile() (*os.File, error) {
	f, err := openReplayFileNoFollow(s.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		if chmodErr := setReplayPrivatePermissions(s.path, 0600); chmodErr != nil {
			_ = f.Close()
			return nil, fmt.Errorf("set replay provenance file mode: %w", chmodErr)
		}
		return f, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create replay provenance file: %w", err)
	}
	f, err = openReplayFileNoFollow(s.path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open replay provenance file: %w", err)
	}
	if err := validatePrivateRegularFile(s.path, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func validateProvenanceJSONLines(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek replay provenance file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect replay provenance file: %w", err)
	}
	if info.Size() == 0 {
		return nil
	}
	if _, err := file.Seek(-1, io.SeekEnd); err != nil {
		return fmt.Errorf("inspect replay provenance tail: %w", err)
	}
	last := []byte{0}
	if _, err := io.ReadFull(file, last); err != nil {
		return fmt.Errorf("read replay provenance tail: %w", err)
	}
	if last[0] != '\n' {
		return fmt.Errorf("replay provenance file is corrupt: partial trailing record")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek replay provenance file: %w", err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxReplayStateBytes)
	line := 0
	for scanner.Scan() {
		line++
		data := scanner.Bytes()
		if len(strings.TrimSpace(string(data))) == 0 || !json.Valid(data) {
			return fmt.Errorf("replay provenance file is corrupt at line %d", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read replay provenance file: %w", err)
	}
	return nil
}

func validateReplayProvenanceRecord(record ReplayProvenanceRecord) error {
	if record.SchemaVersion != ReplayProvenanceSchemaVersion {
		return fmt.Errorf("replay provenance record has unsupported schema version %d", record.SchemaVersion)
	}
	if record.RecordedAt.IsZero() {
		return fmt.Errorf("replay provenance record requires recorded_at")
	}
	if record.Event == "" {
		return fmt.Errorf("replay provenance record requires event")
	}
	return nil
}

func writeReplayBytes(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
