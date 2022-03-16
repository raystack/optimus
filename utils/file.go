package utils

import (
	"fmt"
	"io"
	"os"
)

func WriteStringToFileIndexed() func(filePath, data string, writer io.Writer) error {
	index := 0
	return func(filePath, data string, writer io.Writer) error {
		if err := os.WriteFile(filePath,
			[]byte(data), 0o600); err != nil {
			return fmt.Errorf("failed to write file at %s: %w", filePath, err)
		}
		index++
		_, err := fmt.Fprintf(writer, "%d. writing file at %s\n", index, filePath)
		return err
	}
}
