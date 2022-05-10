package survey

import (
	"fmt"
	"path/filepath"

	"github.com/AlecAivazis/survey/v2"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/spf13/afero"

	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

const (
	answerYes = "Yes"
	answerNo  = "No"
)

var specFileNames = []string{local.ResourceSpecFileName, local.JobSpecFileName}

// AskWorkingDirectory asks and returns the directory where the new spec folder should be created
func AskWorkingDirectory(jobSpecFs afero.Fs, root string) (string, error) {
	directories, err := afero.ReadDir(jobSpecFs, root)
	if err != nil {
		return "", err
	}
	if len(directories) == 0 {
		return root, nil
	}

	currentFolder := ". (current directory)"

	availableDirs := []string{currentFolder}
	for _, dir := range directories {
		if !dir.IsDir() {
			continue
		}

		// if it contains job or resource, skip it from valid options
		dirItems, err := afero.ReadDir(jobSpecFs, filepath.Join(root, dir.Name()))
		if err != nil {
			return "", err
		}
		var alreadyOccupied bool
		for _, dirItem := range dirItems {
			if utils.ContainsString(specFileNames, dirItem.Name()) {
				alreadyOccupied = true
				break
			}
		}
		if alreadyOccupied {
			continue
		}
		availableDirs = append(availableDirs, dir.Name())
	}

	messageStr := "Select directory to save specification?"
	if root != "" {
		messageStr = fmt.Sprintf("%s [%s]", messageStr, root)
	}
	var selectedDir string
	if err := survey.AskOne(&survey.Select{
		Message: messageStr,
		Default: currentFolder,
		Help:    "Optimus helps organize specifications in sub-directories.\nPlease select where you want this new specification to be stored",
		Options: availableDirs,
	}, &selectedDir); err != nil {
		return "", err
	}

	// check for sub-directories
	if selectedDir != currentFolder {
		return AskWorkingDirectory(jobSpecFs, filepath.Join(root, selectedDir))
	}
	return root, nil
}

// AskDirectoryName asks and returns the directory name of the new spec folder
func AskDirectoryName(root string) (string, error) {
	numberOfWordsToGenerate := 2
	sampleDirectoryName := petname.Generate(numberOfWordsToGenerate, "_")

	var selectedDir string
	if err := survey.AskOne(&survey.Input{
		Message: fmt.Sprintf("Provide new directory name to create for this spec?[%s/.]", root),
		Default: sampleDirectoryName,
		Help:    fmt.Sprintf("A new directory will be created under '%s/%s'", root, sampleDirectoryName),
	}, &selectedDir); err != nil {
		return "", err
	}

	return selectedDir, nil
}

// AskToSelectDatastorer asks the user to select a available datastorer
func AskToSelectDatastorer(datastorers []string) (string, error) {
	var storerName string
	return storerName, survey.AskOne(
		&survey.Select{
			Message: "Select supported datastores?",
			Options: datastorers,
		},
		&storerName,
	)
}
