//go:build windows

package session

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type replayFileRenameInfo struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

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
	source, err := openReplayFileWithAccessNoFollow(
		tempPath,
		windows.DELETE|windows.SYNCHRONIZE,
		windows.OPEN_EXISTING,
	)
	if err != nil {
		return err
	}
	targetDir, err := openReplayFileNoFollow(filepath.Dir(targetPath), os.O_RDONLY, 0)
	if err != nil {
		_ = source.Close()
		return err
	}

	targetName, err := windows.UTF16FromString(filepath.Base(targetPath))
	if err != nil {
		_ = targetDir.Close()
		_ = source.Close()
		return err
	}
	targetName = targetName[:len(targetName)-1]
	nameBytes := len(targetName) * 2
	buffer := make([]byte, int(unsafe.Offsetof(replayFileRenameInfo{}.FileName))+nameBytes)
	renameInfo := (*replayFileRenameInfo)(unsafe.Pointer(&buffer[0]))
	renameInfo.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	renameInfo.RootDirectory = windows.Handle(targetDir.Fd())
	renameInfo.FileNameLength = uint32(nameBytes)
	copy(unsafe.Slice(&renameInfo.FileName[0], len(targetName)), targetName)

	// Share-delete readers remove the application-level conflict. Keep a short
	// bound for transient denials from filesystem filters around the rename.
	const attempts = 5
	for attempt := 0; ; attempt++ {
		err = windows.SetFileInformationByHandle(
			windows.Handle(source.Fd()),
			windows.FileRenameInfoEx,
			&buffer[0],
			uint32(len(buffer)),
		)
		if err == nil || attempt == attempts-1 ||
			(!errors.Is(err, windows.ERROR_ACCESS_DENIED) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = targetDir.Close()
	_ = source.Close()
	if err == nil {
		return nil
	}
	if !errors.Is(err, windows.ERROR_INVALID_FUNCTION) &&
		!errors.Is(err, windows.ERROR_INVALID_PARAMETER) &&
		!errors.Is(err, windows.ERROR_NOT_SUPPORTED) {
		return err
	}

	// Older filesystems may not implement POSIX rename semantics. Preserve the
	// legacy behavior there; it still succeeds when no destination handle is open.
	from, fromErr := windows.UTF16PtrFromString(tempPath)
	if fromErr != nil {
		return fromErr
	}
	to, toErr := windows.UTF16PtrFromString(targetPath)
	if toErr != nil {
		return toErr
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
