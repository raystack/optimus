package survey

import (
	"fmt"
	"path/filepath"

	"github.com/AlecAivazis/survey/v2"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/spf13/afero"

	"github.com/odpf/optimus/internal/utils"
)

const (
	answerYes = "Yes"
	answerNo  = "No"
)

// AskWorkingDirectory asks and returns the directory where the new spec folder should be created
func AskWorkingDirectory(specFS afero.Fs, rootDirPath string) (string, error) {
	directories, err := afero.ReadDir(specFS, rootDirPath)
	if err != nil {
		return "", err
	}
	if len(directories) == 0 {
		return rootDirPath, nil
	}

	currentFolder := ". (current directory)"

	subDirectories := []string{currentFolder}
	for _, dir := range directories {
		if !dir.IsDir() {
			continue
		}

		dirItems, err := afero.ReadDir(specFS, filepath.Join(rootDirPath, dir.Name()))
		if err != nil {
			return "", err
		}
		var alreadyOccupied bool
		for _, dirItem := range dirItems {
			specFileNames := []string{"resource.yaml", "job.yaml"}
			if utils.ContainsString(specFileNames, dirItem.Name()) {
				alreadyOccupied = true
				break
			}
		}
		if alreadyOccupied {
			continue
		}
		subDirectories = append(subDirectories, dir.Name())
	}

	messageStr := "Select directory to save specification?"
	if rootDirPath != "" {
		messageStr = fmt.Sprintf("%s [%s]", messageStr, rootDirPath)
	}
	var selectedDir string
	if err := survey.AskOne(&survey.Select{
		Message: messageStr,
		Default: currentFolder,
		Help:    "Optimus helps organize specifications in sub-directories.\nPlease select where you want this new specification to be stored",
		Options: subDirectories,
	}, &selectedDir); err != nil {
		return "", err
	}

	if selectedDir != currentFolder {
		return AskWorkingDirectory(specFS, filepath.Join(rootDirPath, selectedDir))
	}
	return rootDirPath, nil
}

// AskDirectoryName asks and returns the directory name of the new spec folder
func AskDirectoryName(root string) (string, error) {
	standardizedRoot := root
	if standardizedRoot == "" {
		standardizedRoot = "."
	}

	numberOfWordsToGenerate := 2
	sampleDirectoryName := petname.Generate(numberOfWordsToGenerate, "_")

	var selectedDir string
	if err := survey.AskOne(&survey.Input{
		Message: fmt.Sprintf("Provide new directory name to create for this spec? [%s]", standardizedRoot),
		Default: sampleDirectoryName,
		Help:    fmt.Sprintf("A new directory will be created under [%s]", filepath.Join(root, sampleDirectoryName)),
	}, &selectedDir); err != nil {
		return "", err
	}

	return selectedDir, nil
}
