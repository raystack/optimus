package utils

import (
	"fmt"
	"io"
	"io/ioutil"
)

func WriteStringToFileIndexed() func(filePath, data string, writer io.Writer) error {
	index := 0
	return func(filePath, data string, writer io.Writer) error {
		if err := ioutil.WriteFile(filePath,
			[]byte(data), 0644); err != nil {
			return fmt.Errorf("failed to write file at %s: %w", filePath, err)
		}
		index++
		_, err := writer.Write([]byte(fmt.Sprintf("%d. writing file at %s\n", index, filePath)))
		return err
	}
}
