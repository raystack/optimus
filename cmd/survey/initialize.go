package survey

import (
	"fmt"
	"path"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
)

// InititalizeSurvey defines surveys related to init client config
type InititalizeSurvey struct {
	logger log.Logger
}

// NewInitializeSurvey initializes init survey
func NewInitializeSurvey(logger log.Logger) *InititalizeSurvey {
	return &InititalizeSurvey{
		logger: logger,
	}
}

// AskToConfirm askes the user to confirm on a message
func (i *InititalizeSurvey) AskToConfirm(message, help string, defaultValue bool) (bool, error) {
	prompt := &survey.Confirm{
		Message: message,
		Help:    help,
		Default: defaultValue,
	}
	var response bool
	if err := survey.AskOne(prompt, &response); err != nil {
		return defaultValue, err
	}
	return response, nil
}

// AskInitClientConfig askes the user to init client config
func (i *InititalizeSurvey) AskInitClientConfig(dirPath string) (*config.ClientConfig, error) {
	output := &config.ClientConfig{}
	host, err := i.askHost()
	if err != nil {
		return nil, err
	}
	project, err := i.askInitProject()
	if err != nil {
		return nil, err
	}
	namespaces, err := i.askInitNamespaces(dirPath)
	if err != nil {
		return nil, err
	}
	output.Version = config.DefaultVersion
	output.Host = host
	output.Project = project
	output.Namespaces = namespaces
	output.Log = config.LogConfig{
		Level: config.LogLevelInfo,
	}
	return output, nil
}

func (i *InititalizeSurvey) askHost() (host string, err error) {
	for {
		prompt := &survey.Input{
			Message: "What is the Optimus service host?",
			Help:    "Example - localhost:9100",
		}
		if err = survey.AskOne(prompt, &host); err != nil {
			return
		}
		if host != "" {
			return
		}
		i.logger.Warn("Host name is empty, let's try again")
	}
}

func (i *InititalizeSurvey) askInitProject() (project config.Project, err error) {
	for {
		prompt := &survey.Input{
			Message: "What is the Optimus project name?",
		}
		var projectName string
		if err = survey.AskOne(prompt, &projectName); err != nil {
			return
		}
		if projectName == "" {
			i.logger.Warn("Project name is empty, let's try again")
			continue
		}
		project.Name = projectName
		project.Config = make(map[string]string)
		return
	}
}

func (i *InititalizeSurvey) askInitNamespaces(dirPath string) ([]*config.Namespace, error) {
	var output []*config.Namespace
	for {
		name, err := i.askInitNamespaceName(dirPath)
		if err != nil {
			return nil, err
		}
		datastoreType, err := i.askInitNamespaceDatastoreType()
		if err != nil {
			return nil, err
		}
		namespace := &config.Namespace{
			Name: name,
			Datastore: []config.Datastore{
				{
					Type:   datastoreType,
					Path:   path.Join(name, "resources"),
					Backup: make(map[string]string),
				},
			},
			Job: config.Job{
				Path: path.Join(name, "jobs"),
			},
		}
		output = append(output, namespace)

		confirmMessage := "Do you want to add another namespace?"
		confirmHelp := "If yes, then you will be prompted to create another namespace"
		confirmedToAddMore, err := i.AskToConfirm(confirmMessage, confirmHelp, false)
		if err != nil {
			return nil, err
		}
		if !confirmedToAddMore {
			break
		}
		i.logger.Info("Adding more namespaces")
	}
	return output, nil
}

func (i *InititalizeSurvey) askInitNamespaceDatastoreType() (string, error) {
	prompt := &survey.Select{
		Message: "What is the type of data store for this namespace?",
		Options: []string{
			"bigquery",
		},
		Default: "bigquery",
	}
	var dataStoreType string
	if err := survey.AskOne(prompt, &dataStoreType); err != nil {
		return dataStoreType, err
	}
	return dataStoreType, nil
}

func (i *InititalizeSurvey) askInitNamespaceName(dirPath string) (string, error) {
	for {
		prompt := &survey.Input{
			Message: "What is the namespace name?",
		}
		var name string
		if err := survey.AskOne(prompt, &name); err != nil {
			return name, err
		}
		if name == "" {
			i.logger.Warn("Namespace name is empty, let's try again")
			continue
		}
		namespaceDirPath := path.Join(dirPath, name)
		pathOccupied, err := utils.IsPathOccupied(namespaceDirPath)
		if err != nil {
			return name, err
		}
		if !pathOccupied {
			return name, nil
		}
		confirmMessage := fmt.Sprintf("Directory [%s] for namespace [%s] is occupied, replace?",
			namespaceDirPath, name,
		)
		confirmHelp := fmt.Sprintf("If yes, then [%s] will be replaced for namespace [%s]",
			namespaceDirPath, name,
		)
		confirmedToReplace, err := i.AskToConfirm(confirmMessage, confirmHelp, false)
		if err != nil {
			return name, err
		}
		if confirmedToReplace {
			i.logger.Info(fmt.Sprintf("Confirmed to replace [%s] for namespace [%s]", namespaceDirPath, name))
			return name, nil
		}
		i.logger.Info(fmt.Sprintf("Confirmed NOT to replace [%s], let's initiate another namespace", namespaceDirPath))
	}
}
