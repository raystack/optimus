package plugin

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/core/logger"
	"gopkg.in/yaml.v2"
)

type PluginConfig struct {
	Path    string
	Version string
	Binary  struct {
		Env  []string
		OS   []string
		Arch []string
	}
	Docker struct {
		Header string
		Footer string
		Tag    []string
	}
}

type BuildConfig struct {
	Plugins struct {
		Task []PluginConfig
		Hook []PluginConfig
	}
}

const (
	DockerTemplate = `{{.Header}}

RUN mkdir -p /opt
ADD "{{.OpctlDownloadUrl}}" /opt/opctl
RUN chmod +x /opt/opctl

RUN {{.EntrypointTemplate}}
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
{{.Footer}}
`
	// don't use single quote in this file
	EntrypointTemplate = `#!/bin/sh
# wait for few seconds to prepare scheduler for the run
sleep 5

# get resources
echo "-- initializing opctl assets"
OPTIMUS_ADMIN=1 /opt/opctl admin build instance $JOB_NAME --project $PROJECT --output-dir $JOB_DIR --type $TASK_TYPE --name $TASK_NAME --scheduled-at $SCHEDULED_AT --host $OPTIMUS_HOSTNAME

# TODO: this doesnt support using back quote sign in env vars, fix it
echo "-- exporting env"
set -o allexport
source $JOB_DIR/in/.env
set +o allexport

echo "-- current envs"
printenv

echo "-- running unit"
exec "$@"
`

	BinaryNameFormat = "optimus-%s-%s_%s_%s_%s"
)

func BuildHelper(templateEngine models.TemplateEngine, configBytes []byte, binaryBuildPath, opctlDownloadUrl string, skipDockerBuild, skipBinaryBuild bool) error {
	inputConfig := BuildConfig{}
	if err := yaml.Unmarshal(configBytes, &inputConfig); err != nil {
		return err
	}
	binAbsBuildPath, err := filepath.Abs(binaryBuildPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(binAbsBuildPath, os.ModeDir|os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create output dir")
	}

	//prepare entrypoint string blob
	entrypointLines := []string{}
	for _, line := range strings.Split(EntrypointTemplate, "\n") {
		if len(line) == 0 {
			continue
		}
		entrypointLines = append(entrypointLines, fmt.Sprintf("echo '%s' >> /opt/entrypoint.sh", line))
	}
	preparedEntrypoint := strings.Join(entrypointLines, " && ")

	// parse tasks
	for _, taskPlugin := range inputConfig.Plugins.Task {
		var destPath string
		logger.If("generating docker files at %s", taskPlugin.Path)

		dockerFile, err := templateEngine.CompileString(DockerTemplate, map[string]interface{}{
			"Header":             taskPlugin.Docker.Header,
			"OpctlDownloadUrl":   opctlDownloadUrl,
			"EntrypointTemplate": preparedEntrypoint,
			"Footer":             taskPlugin.Docker.Footer,
		})
		if err != nil {
			return err
		}
		destPath = filepath.Join(taskPlugin.Path, "Dockerfile")
		if err := ioutil.WriteFile(destPath, []byte(dockerFile), 0655); err != nil {
			return err
		}

		pluginName := filepath.Base(taskPlugin.Path)
		// build binary
		if !skipBinaryBuild {
			logger.If("building binary for %s", taskPlugin.Path)
			if len(taskPlugin.Binary.OS) > 0 {
				for _, binOS := range taskPlugin.Binary.OS {
					for _, binArch := range taskPlugin.Binary.Arch {
						binName := strings.ToLower(fmt.Sprintf(BinaryNameFormat, TaskPluginName, pluginName, taskPlugin.Version, binOS, binArch))
						args := []string{
							"build",
							"-ldflags", fmt.Sprintf("-X '%s=%s'", "main.Version", taskPlugin.Version),
							"-o", filepath.Join(binAbsBuildPath, binName),
						}

						envs := append(taskPlugin.Binary.Env, os.Environ()...)
						envs = append(envs, []string{fmt.Sprintf("GOOS=%s", binOS), fmt.Sprintf("GOARCH=%s", binArch)}...)

						out, err := ExecuteCmd(taskPlugin.Path, "go", args, envs)
						if len(out) > 0 {
							logger.I(string(out))
						}
						if err != nil {
							return errors.Wrap(err, "failed to build binary")
						}
					}
				}
			}
		}

		if !skipDockerBuild {
			// build docker
			logger.If("building docker image for %s", taskPlugin.Path)
			if len(taskPlugin.Docker.Tag) > 0 {
				dockerBuildArgs := []string{"build"}
				for _, tag := range taskPlugin.Docker.Tag {
					compiledTag, err := templateEngine.CompileString(tag, map[string]interface{}{
						"Name":    pluginName,
						"Version": taskPlugin.Version,
					})
					if err != nil {
						return err
					}
					dockerBuildArgs = append(dockerBuildArgs, []string{"-t", compiledTag}...)
				}
				dockerBuildArgs = append(dockerBuildArgs, ".")
				out, err := ExecuteCmd(taskPlugin.Path, "docker", dockerBuildArgs, nil)
				if len(out) > 0 {
					logger.I(string(out))
				}
				if err != nil {
					return errors.Wrap(err, "failed to build docker image")
				}
			}
		}
		logger.If("build complete for %s", taskPlugin.Path)
	}

	// parse hooks
	for _, hookPlugin := range inputConfig.Plugins.Hook {
		dockerFile, err := templateEngine.CompileString(DockerTemplate, map[string]interface{}{
			"Header":             hookPlugin.Docker.Header,
			"OpctlDownloadUrl":   opctlDownloadUrl,
			"EntrypointTemplate": preparedEntrypoint,
			"Footer":             hookPlugin.Docker.Footer,
		})
		if err != nil {
			return err
		}
		destPath := filepath.Join(hookPlugin.Path, "Dockerfile")
		if err := ioutil.WriteFile(destPath, []byte(dockerFile), 0655); err != nil {
			return err
		}

		pluginName := filepath.Base(hookPlugin.Path)
		if !skipBinaryBuild {
			// build binary
			logger.If("building binary for %s", hookPlugin.Path)
			if len(hookPlugin.Binary.OS) > 0 {
				for _, binOS := range hookPlugin.Binary.OS {
					for _, binArch := range hookPlugin.Binary.Arch {
						binName := strings.ToLower(fmt.Sprintf(BinaryNameFormat, HookPluginName, pluginName, hookPlugin.Version, binOS, binArch))
						args := []string{
							"build",
							"-ldflags", fmt.Sprintf("-X '%s=%s'", "main.Version", hookPlugin.Version),
							"-o", filepath.Join(binAbsBuildPath, binName),
						}

						envs := append(hookPlugin.Binary.Env, os.Environ()...)
						envs = append(envs, []string{fmt.Sprintf("GOOS=%s", binOS), fmt.Sprintf("GOARCH=%s", binArch)}...)

						out, err := ExecuteCmd(hookPlugin.Path, "go", args, envs)
						if len(out) > 0 {
							logger.I(string(out))
						}
						if err != nil {
							return err
						}
					}
				}
			}
		}

		if !skipDockerBuild {
			// build docker
			logger.If("building docker image for %s", hookPlugin.Path)
			if len(hookPlugin.Docker.Tag) > 0 {
				dockerBuildArgs := []string{"build"}
				for _, tag := range hookPlugin.Docker.Tag {
					compiledTag, err := templateEngine.CompileString(tag, map[string]interface{}{
						"Name":    pluginName,
						"Version": hookPlugin.Version,
					})
					if err != nil {
						return err
					}
					dockerBuildArgs = append(dockerBuildArgs, []string{"-t", compiledTag}...)
				}
				dockerBuildArgs = append(dockerBuildArgs, ".")
				out, err := ExecuteCmd(hookPlugin.Path, "docker", dockerBuildArgs, nil)
				if len(out) > 0 {
					logger.I(string(out))
				}
				if err != nil {
					return errors.Wrap(err, "failed to build docker image")
				}
			}
		}

		logger.If("build complete for %s", hookPlugin.Path)
	}

	return nil
}

func ExecuteCmd(dir, binPath string, args, env []string) ([]byte, error) {
	if filepath.Base(binPath) == binPath {
		if lp, err := exec.LookPath(binPath); err != nil {
			return nil, errors.Wrap(err, "failed to find binary")
		} else {
			binPath = lp
		}
	}
	absPath, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find path %s", dir)
	}

	cmd := &exec.Cmd{
		Path: binPath,
		Args: append([]string{binPath}, args...),
		Dir:  absPath,
		Env:  env,
	}
	return cmd.CombinedOutput()
}
