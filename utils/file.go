package utils

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/mattn/go-isatty"
)

func WriteStringToFileIndexed() func(filePath, data string, writer io.Writer) error {
	index := 0
	return func(filePath, data string, writer io.Writer) error {
		if err := ioutil.WriteFile(filePath,
			[]byte(data), 0o600); err != nil {
			return fmt.Errorf("failed to write file at %s: %w", filePath, err)
		}
		index++
		_, err := fmt.Fprintf(writer, "%d. writing file at %s\n", index, filePath)
		return err
	}
}

// IsPathOccupied checks whether the targeted path is already occupied
func IsPathOccupied(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsTerminal checks if file descriptor is terminal or not
func IsTerminal(f *os.File) bool {
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}
