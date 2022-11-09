package dag

import (
	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

type Task struct {
	Name  string
	Image string
}

type Hook struct {
	Name       string
	Image      string
	IsFailHook bool
}

type Hooks struct {
	Pre          []Hook
	Post         []Hook
	Fail         []Hook
	Dependencies map[string]string
}

func (h Hooks) List() []Hook {
	list := h.Pre
	list = append(list, h.Post...)
	list = append(list, h.Fail...)
	return list
}

func ArrangeHooksForJob(job *job_run.Job, pluginRepo models.PluginRepository) (Hooks, error) {
	var hooks Hooks
	hooks.Dependencies = map[string]string{}

	for _, h := range job.GetHooks() {
		hook, err := pluginRepo.GetByName(h.Name())
		if err != nil {
			return Hooks{}, errors.NotFound("schedulerAirflow", "hook not found for name "+h.Name())
		}

		info := hook.Info()
		hk := Hook{
			Name:  h.Name(),
			Image: info.Image,
		}
		switch info.HookType {
		case models.HookTypePre:
			hooks.Pre = append(hooks.Pre, hk)
		case models.HookTypePost:
			hooks.Post = append(hooks.Post, hk)
		case models.HookTypeFail:
			hk.IsFailHook = true
			hooks.Fail = append(hooks.Fail, hk)
		}

		for _, before := range info.DependsOn {
			hooks.Dependencies[before] = h.Name()
		}
	}

	return hooks, nil
}
