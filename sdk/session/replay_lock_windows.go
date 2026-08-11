//go:build windows

package session

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func acquireReplayFileLock(path string, timeout, retryInterval time.Duration) (func(), error) {
	f, err := openOrCreateReplayLockFile(path)
	if err != nil {
		return func() {}, err
	}

	handle := windows.Handle(f.Fd())
	deadline := time.Now().Add(timeout)
	for {
		overlapped := new(windows.Overlapped)
		lockErr := windows.LockFileEx(handle,
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0, 1, 0, overlapped)
		if lockErr == nil {
			break
		}
		if !errors.Is(lockErr, windows.ERROR_LOCK_VIOLATION) {
			_ = f.Close()
			return func() {}, fmt.Errorf("replay lock: LockFileEx %s: %w", path, lockErr)
		}
		if !time.Now().Before(deadline) {
			_ = f.Close()
			return func() {}, fmt.Errorf("replay lock: timed out waiting for %s after %s", path, timeout)
		}
		time.Sleep(retryInterval)
	}

	return func() {
		overlapped := new(windows.Overlapped)
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = f.Close()
	}, nil
}

func openOrCreateReplayLockFile(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("replay lock: refusing symlink %s", path)
	}
	f, err := openReplayFileNoFollow(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		if chmodErr := setReplayPrivatePermissions(path, 0600); chmodErr != nil {
			_ = f.Close()
			return nil, fmt.Errorf("replay lock: set mode on %s: %w", path, chmodErr)
		}
		return f, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("replay lock: create %s: %w", path, err)
	}

	f, err = openReplayFileNoFollow(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("replay lock: open %s: %w", path, err)
	}
	if err := validatePrivateRegularFile(path, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func atomicReplaceReplayFile(tempPath, targetPath string) error {
	from, err := windows.UTF16PtrFromString(tempPath)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
