package testing_util

import (
	"os"
	"testing"
)

func MkdirTemp(t *testing.T, prefix string) (path string, cleanup func()) {
	out, err := os.MkdirTemp(os.TempDir(), prefix)
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	if err := os.Chmod(out, 0o777); err != nil {
		t.Fatalf("failed to make temporary directory accessible: %s", err)
	}

	return out, func() {
		os.RemoveAll(out)
	}
}
