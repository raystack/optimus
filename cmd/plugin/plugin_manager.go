package plugin

import (
	"fmt"
	"os"

	"github.com/odpf/salt/log"

	getter "github.com/hashicorp/go-getter"
)

type IPluginManager interface {
	Install(src, dst string) error
	InstallMany(srcList []string, dst string) error
}

func NewPluginManager(logger log.Logger) IPluginManager {
	// currently only implemenetation of https://github.com/hashicorp/go-getter
	pwd, err := os.Getwd()
	if err != nil {
		logger.Fatal(fmt.Sprintf("Error getting pwd: %s", err))
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
	logger log.Logger
	client *getter.Client
}

func (p *PluginManager) Install(src, dst string) error {
	p.client.Src = src
	p.client.Dst = dst

	if err := p.client.Get(); err != nil {
		p.logger.Error(fmt.Sprintf("Error installing plugin from [%s]", src))
		return err
	}
	p.logger.Debug(fmt.Sprintf("Success installing plugin from [%s]", src))
	return nil
}

func (p *PluginManager) InstallMany(srcList []string, dst string) error {
	// p.preCleanUp(dst) -- TODO: after making plugin discovery static
	for _, src := range srcList {
		if err := p.Install(src, dst); err != nil {
			p.logger.Error("*** Plugin Installation Aborted !!. Please check if plugin.artifacts are correct")
			return err
		}
	}
	p.logger.Info(fmt.Sprintf("Success installing plugin in dir=%s", dst))
	return nil
}
