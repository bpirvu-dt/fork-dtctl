//go:build !windows

package session

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func acquireReplayFileLock(path string, timeout, retryInterval time.Duration) (func(), error) {
	f, err := openOrCreateReplayLockFile(path)
	if err != nil {
		return func() {}, err
	}

	deadline := time.Now().Add(timeout)
	for {
		lockErr := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if lockErr == nil {
			break
		}
		if !errors.Is(lockErr, unix.EWOULDBLOCK) {
			_ = f.Close()
			return func() {}, fmt.Errorf("replay lock: flock %s: %w", path, lockErr)
		}
		if !time.Now().Before(deadline) {
			_ = f.Close()
			return func() {}, fmt.Errorf("replay lock: timed out waiting for %s after %s", path, timeout)
		}
		time.Sleep(retryInterval)
	}

	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

func openOrCreateReplayLockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|unix.O_NOFOLLOW, 0600)
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

	f, err = os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("replay lock: open %s: %w", path, err)
	}
	if err := validatePrivateRegularFile(path, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func openReplayFileNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|unix.O_NOFOLLOW, perm)
}

func atomicReplaceReplayFile(tempPath, targetPath string) error {
	return os.Rename(tempPath, targetPath)
}

func replayFileOwnedByCurrentUser(_ string, info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func setReplayPrivatePermissions(path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

func validateReplayPrivatePermissions(path string, info os.FileInfo) error {
	kind := "file"
	if info.IsDir() {
		kind = "directory"
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("replay %s %s is not private: mode is %04o, require no group or other access", kind, path, info.Mode().Perm())
	}
	if !replayFileOwnedByCurrentUser(path, info) {
		return fmt.Errorf("replay %s %s is not owned by the current user", kind, path)
	}
	return nil
}

func validateReplayPrivateFilePermissions(path string, _ *os.File, info os.FileInfo) error {
	return validateReplayPrivatePermissions(path, info)
}
