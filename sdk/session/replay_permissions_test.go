package session

import (
	"os"
	"runtime"
	"testing"
)

func assertReplayPrivatePath(t *testing.T, path string, wantMode os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if got := info.Mode().Perm(); got != wantMode {
			t.Fatalf("%s mode = %04o, want %04o", path, got, wantMode)
		}
		return
	}
	if info.IsDir() {
		if err := validatePrivateDirectoryInfo(path, info); err != nil {
			t.Fatalf("%s Windows DACL is not private: %v", path, err)
		}
		return
	}
	f, err := openReplayFileNoFollow(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := validatePrivateRegularFile(path, f); err != nil {
		t.Fatalf("%s Windows DACL is not private: %v", path, err)
	}
}

func skipPOSIXModeAssertionsOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode assertions")
	}
}

func replayReadOnlyModeForTest() os.FileMode {
	if runtime.GOOS == "windows" {
		return 0444
	}
	return 0400
}
