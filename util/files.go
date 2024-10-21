package util

import (
	"errors"
	"os"
)

func FileExists(path string) (exists bool, _ error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}
