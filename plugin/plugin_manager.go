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
	// for plugin installation, discovery & sync on both client and server
	PluginsDir         = ".plugins"
	PluginsArchiveName = "yaml-plugins.zip"
)

type IPluginManager interface {
	Install(dst string, sources ...string) error
	Archive(name string) error
	UnArchive(src, dest string) error
}

func NewPluginManager() *PluginManager {
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
	for _, src := range sources {
		if err := p.installOne(dst, src); err != nil {
			p.logger.Error("*** Plugin Installation Aborted !!. Please check if plugin.artifacts are correct")
			return err
		}
	}
	p.logger.Info(fmt.Sprintf("Success installing plugin in dir=%s", dst))
	return nil
}

func sanitizeArchivePath(d, t string) (v string, err error) {
	v = filepath.Join(d, t)
	if strings.HasPrefix(v, filepath.Clean(d)) {
		return v, nil
	}
	return "", fmt.Errorf("%s: %s", "content filepath is tainted", t)
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
		err := func() error {
			if file.Mode().IsDir() {
				return nil
			}
			open, err := file.Open()
			if err != nil {
				return err
			}
			destFileName, err := sanitizeArchivePath(dest, file.Name)
			if err != nil {
				return err
			}
			os.MkdirAll(path.Dir(destFileName), os.ModePerm)
			create, err := os.Create(destFileName)
			if err != nil {
				return err
			}
			defer create.Close()
			create.ReadFrom(open)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	p.logger.Info("cleaning " + src)
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
		err := func() error {
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
	dst := PluginsDir
	sources := conf.Plugin.Artifacts
	pluginManger := NewPluginManager()

	installErr := pluginManger.Install(dst, sources...)
	if installErr != nil {
		return installErr
	}
	archiveErr := pluginManger.Archive(PluginsArchiveName)
	if archiveErr != nil {
		return archiveErr
	}
	return nil
}
