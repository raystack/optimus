package plugin

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	getter "github.com/hashicorp/go-getter"
	"github.com/hashicorp/go-hclog"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin/yaml"
)

var (
	// for server these values are configurable
	// for client these are static
	DefaultClientPluginDir         = ".plugins"
	DefaultClientPluginArchiveName = "yaml-plugins.zip"
)

type IPluginManager interface {
	Install(dst string, sources ...string) error
	Archive(name string) error
	UnArchive(src, dest string) error
}

func NewPluginManager() IPluginManager {
	// currently only implemenetation of https://github.com/hashicorp/go-getter

	pluginLoggerOpt := &hclog.LoggerOptions{
		Name:   "plugin-manager",
		Output: os.Stdout,
		Level:  hclog.Info,
	}
	logger := hclog.New(pluginLoggerOpt)

	pwd, err := os.Getwd()
	if err != nil {
		logger.Error(fmt.Sprintf("Error getting pwd: %s", err))
	}
	client := &getter.Client{
		Src:  "",
		Dst:  "",
		Pwd:  pwd,
		Mode: getter.ClientModeAny,
	}
	return &PluginManager{
		logger: logger,
		client: client,
	}
}

type PluginManager struct {
	logger hclog.Logger
	client *getter.Client
}

func (p *PluginManager) installOne(dst, src string) error {
	p.client.Src = src
	p.client.Dst = dst

	if err := p.client.Get(); err != nil {
		p.logger.Error(fmt.Sprintf("Error installing plugin from [%s]", src))
		return err
	}
	p.logger.Info(fmt.Sprintf("Success installing plugin from [%s]", src))
	return nil
}

func (p *PluginManager) Install(dst string, sources ...string) error {
	// p.preCleanUp(dst) -- TODO: after making plugin discovery static
	for _, src := range sources {
		if err := p.installOne(dst, src); err != nil {
			p.logger.Error("*** Plugin Installation Aborted !!. Please check if plugin.artifacts are correct")
			return err
		}
	}
	p.logger.Info(fmt.Sprintf("Success installing plugin in dir=%s", dst))
	return nil
}

func copyToDest(dest string, file *zip.File) error {
	if file.Mode().IsDir() {
		return nil
	}
	open, err := file.Open()
	if err != nil {
		return err
	}
	p, _ := filepath.Abs(file.Name)
	if strings.Contains(p, "..") || strings.Contains(p, ".") {
		return nil
	}
	name := path.Join(dest, p)
	os.MkdirAll(path.Dir(name), os.ModePerm)
	create, err := os.Create(name)
	if err != nil {
		return err
	}
	defer create.Close()
	create.ReadFrom(open)
	return nil
}

func (p *PluginManager) UnArchive(src, dest string) error {
	dest = filepath.Clean(dest) + string(os.PathSeparator)
	p.logger.Info("deleting " + dest + " folder")
	err := os.RemoveAll(dest)
	if err != nil {
		return err
	}
	p.logger.Info("unzipping " + src + " to " + dest)
	read, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer read.Close()
	for _, file := range read.File {
		err := copyToDest(dest, file)
		if err != nil {
			return err
		}
	}
	p.logger.Info("cleaning " + dest)
	err = os.RemoveAll(src)
	if err != nil {
		return err
	}
	return nil
}

func (p *PluginManager) Archive(archiveName string) error {
	discoveredYamlPlugins := DiscoverPluginsGivenFilePattern(p.logger, yaml.Prefix, yaml.Suffix)
	p.logger.Info(fmt.Sprintf("Archiving yaml plugins [%d]... --> %s", len(discoveredYamlPlugins), archiveName))
	archive, err := os.Create(archiveName)
	if err != nil {
		return err
	}
	defer archive.Close()
	zipWriter := zip.NewWriter(archive)
	for _, pluginPath := range discoveredYamlPlugins {
		err := func() error { // to avoid defer in loop
			f1, err := os.Open(pluginPath)
			if err != nil {
				return err
			}
			defer f1.Close()
			fileName := filepath.Base(pluginPath)
			w1, err := zipWriter.Create(fileName)
			if err != nil {
				return err
			}
			if _, err := io.Copy(w1, f1); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	zipWriter.Close()
	return nil
}

// used during server start
// also exposed as cmd
func InstallPlugins(conf *config.ServerConfig) error {
	dst := conf.Plugin.Dir
	sources := conf.Plugin.Artifacts
	pluginManger := NewPluginManager()

	installErr := pluginManger.Install(dst, sources...)
	if installErr != nil {
		return installErr
	}
	archiveErr := pluginManger.Archive(conf.Plugin.Archive)
	if archiveErr != nil {
		return archiveErr
	}
	return nil
}
