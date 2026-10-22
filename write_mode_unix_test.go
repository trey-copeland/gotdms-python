//go:build !windows

package bridge

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// create follows the umask like os.Create, and a replaced file keeps its mode.
func TestCreateFileMode(t *testing.T) {
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	dir := t.TempDir()
	path := filepath.Join(dir, "new.tdms")
	writeFloatFile(t, path, []float64{1})
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o644 {
		t.Fatalf("new file mode %o, want 644", got)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	writeFloatFile(t, path, []float64{2})
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o640 {
		t.Fatalf("replaced file mode %o, want 640", got)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("leftover files: %v", entries)
	}
}
