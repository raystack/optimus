package local_test

import (
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/mock"

	"github.com/odpf/optimus/store/local"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestSpecAdapter(t *testing.T) {
	t.Run("should convert job with task from yaml to optimus model & back successfully", func(t *testing.T) {
		yamlSpec := `
version: 1
name: test_job
owner: test@example.com
schedule:
  start_date: "2021-02-03"
  interval: 0 2 * * *
behavior:
  depends_on_past: true
  catch_up: false
  notify:
    - on: test
      channel:
        - test://hello
task:
  name: bq2bq
  config:
    PROJECT: project
    DATASET: dataset
    TABLE: table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 168h
    offset: 0
    truncate_to: w
labels:
  orchestrator: optimus
dependencies: []
hooks: []
external-dependencies :
 http :
    -
     Name : http-sensor-1
     request-params:
       key-1 : value-1
       key-2 : value-2
     url : https://optimus-host:80/serve/1/
     headers:
        Content-type : application/json
        Authentication : Token-1
    -
     Name : http-sensor-2
     request-params:
       key-1 : value-3
       key-2 : value-4
     url : https://optimus-host:80/serve/2/
     headers:
        Content-type : application/json
        Authentication : Token-2 
`
		var localJobParsed local.Job
		err := yaml.Unmarshal([]byte(yamlSpec), &localJobParsed)
		assert.Nil(t, err)

		execUnit := new(mock.BasePlugin)
		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: "bq2bq",
		}, nil)

		pluginRepo := new(mock.SupportedPluginRepo)
		pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
			Base: execUnit,
		}, nil)
		adapter := local.NewJobSpecAdapter(pluginRepo)

		modelJob, err := adapter.ToSpec(localJobParsed)
		assert.Nil(t, err)

		localJobBack, err := adapter.FromSpec(modelJob)
		assert.Nil(t, err)

		assert.Equal(t, localJobParsed, localJobBack)
	})
	t.Run("should not convert job with task from yaml when URL present at http dependencies is invalid", func(t *testing.T) {
		yamlSpec := `
version: 1
name: test_job
owner: test@example.com
schedule:
  start_date: "2021-02-03"
  interval: 0 2 * * *
behavior:
  depends_on_past: true
  catch_up: false
  notify:
    - on: test
      channel:
        - test://hello
task:
  name: bq2bq
  config:
    PROJECT: project
    DATASET: dataset
    TABLE: table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 168h
    offset: 0
    truncate_to: w
labels:
  orchestrator: optimus
dependencies: []
hooks: []
external-dependencies :
 http :
    -
     Name : http-sensor-1
     request-params:
       key-1 : value-1
       key-2 : value-2
     url : ""
     headers:
        Content-type : application/json
        Authentication : Token-1
    -
     Name : http-sensor-2
     request-params:
       key-1 : value-3
       key-2 : value-4
     url : https://optimus-host:80/serve/2/
     headers:
        Content-type : application/json
        Authentication : Token-2 
`
		var localJobParsed local.Job
		err := yaml.Unmarshal([]byte(yamlSpec), &localJobParsed)
		assert.Nil(t, err)

		execUnit := new(mock.BasePlugin)
		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: "bq2bq",
		}, nil)

		pluginRepo := new(mock.SupportedPluginRepo)
		pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
			Base: execUnit,
		}, nil)
		adapter := local.NewJobSpecAdapter(pluginRepo)

		modelJob, actualErr := adapter.ToSpec(localJobParsed)
		_, urlErr := url.ParseRequestURI("")
		errOnIndex := 0
		expErr := fmt.Errorf("invalid url present on HTTPDependencies index %d of jobs.yaml, invalid reason : %v", errOnIndex, urlErr)
		assert.Equal(t, expErr, actualErr)
		assert.Equal(t, models.JobSpec{}, modelJob)
	})
	t.Run("should not convert job with task from yaml when Name present at http dependencies is invalid", func(t *testing.T) {
		yamlSpec := `
version: 1
name: test_job
owner: test@example.com
schedule:
  start_date: "2021-02-03"
  interval: 0 2 * * *
behavior:
  depends_on_past: true
  catch_up: false
  notify:
    - on: test
      channel:
        - test://hello
task:
  name: bq2bq
  config:
    PROJECT: project
    DATASET: dataset
    TABLE: table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 168h
    offset: 0
    truncate_to: w
labels:
  orchestrator: optimus
dependencies: []
hooks: []
external-dependencies :
 http :
    -
     Name : ""
     request-params:
       key-1 : value-1
       key-2 : value-2
     url : "https://optimus-host:80/serve/1/"
     headers:
        Content-type : application/json
        Authentication : Token-1
    -
     Name : http-sensor-2
     request-params:
       key-1 : value-3
       key-2 : value-4
     url : https://optimus-host:80/serve/2/
     headers:
        Content-type : application/json
        Authentication : Token-2 
`
		var localJobParsed local.Job
		err := yaml.Unmarshal([]byte(yamlSpec), &localJobParsed)
		assert.Nil(t, err)

		execUnit := new(mock.BasePlugin)
		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: "bq2bq",
		}, nil)

		pluginRepo := new(mock.SupportedPluginRepo)
		pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
			Base: execUnit,
		}, nil)
		adapter := local.NewJobSpecAdapter(pluginRepo)

		modelJob, actualErr := adapter.ToSpec(localJobParsed)

		errOnIndex := 0
		expErr := fmt.Errorf("empty name present on HTTPDependencies index %d of jobs.yaml", errOnIndex)
		assert.Equal(t, expErr, actualErr)
		assert.Equal(t, models.JobSpec{}, modelJob)
	})
}

func TestJob_MergeFrom(t *testing.T) {
	type fields struct {
		child    local.Job
		expected local.Job
	}
	type args struct {
		parent local.Job
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "should successfully copy version if child has zero value",
			fields: fields{
				child: local.Job{
					Version: 0,
				},
				expected: local.Job{
					Version: 1,
				},
			},
			args: args{
				parent: local.Job{
					Version: 1,
				},
			},
		},
		{
			name: "should successfully copy root level values if child has zero value",
			fields: fields{
				child: local.Job{},
				expected: local.Job{
					Description: "hey",
					Labels: map[string]string{
						"optimus": "prime",
						"gogo":    "gadget",
					},
					Behavior: local.JobBehavior{
						DependsOnPast: false,
						Catchup:       true,
						Retry: local.JobBehaviorRetry{
							Count:              3,
							Delay:              "2m",
							ExponentialBackoff: false,
						},
					},
					Schedule: local.JobSchedule{
						StartDate: "2020",
						EndDate:   "2021",
						Interval:  "@daily",
					},
				},
			},
			args: args{
				parent: local.Job{
					Description: "hey",
					Labels: map[string]string{
						"optimus": "prime",
						"gogo":    "gadget",
					},
					Behavior: local.JobBehavior{
						DependsOnPast: false,
						Catchup:       true,
						Retry: local.JobBehaviorRetry{
							Count:              3,
							Delay:              "2m",
							ExponentialBackoff: false,
						},
					},
					Schedule: local.JobSchedule{
						StartDate: "2020",
						EndDate:   "2021",
						Interval:  "@daily",
					},
				},
			},
		},
		{
			name: "should not merge if child already contains non zero values",
			fields: fields{
				child: local.Job{
					Version: 2,
				},
				expected: local.Job{
					Version: 2,
				},
			},
			args: args{
				parent: local.Job{
					Version: 1,
				},
			},
		},
		{
			name: "should merge task configs properly",
			fields: fields{
				child: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "dance",
								Value: "happy",
							},
						},
					},
				},
				expected: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "dance",
								Value: "happy",
							},
							{
								Key:   "eat",
								Value: "ramen",
							},
						},
					},
				},
			},
			args: args{
				parent: local.Job{
					Task: local.JobTask{
						Name: "panda",
						Config: []yaml.MapItem{
							{
								Key:   "eat",
								Value: "ramen",
							},
						},
					},
				},
			},
		},
		{
			name: "should merge hooks configs properly",
			fields: fields{
				child: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "2",
								},
							},
						},
					},
				},
				expected: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
							Config: []yaml.MapItem{
								{
									Key:   "ninza",
									Value: "run",
								},
							},
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "2",
								},
								{
									Key:   "kick",
									Value: "high",
								},
							},
						},
						{
							Name: "saitama",
							Config: []yaml.MapItem{
								{
									Key:   "punch",
									Value: 1,
								},
							},
						},
					},
				},
			},
			args: args{
				parent: local.Job{
					Hooks: []local.JobHook{
						{
							Name: "kungfu",
							Config: []yaml.MapItem{
								{
									Key:   "ninza",
									Value: "run",
								},
							},
						},
						{
							Name: "martial",
							Config: []yaml.MapItem{
								{
									Key:   "arts",
									Value: "3",
								},
								{
									Key:   "kick",
									Value: "high",
								},
							},
						},
						{
							Name: "saitama",
							Config: []yaml.MapItem{
								{
									Key:   "punch",
									Value: 1,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "should inherit notify configs from parent if doesn't exists",
			fields: fields{
				child: local.Job{
					Behavior: local.JobBehavior{
						Notify: []local.JobNotifier{
							{
								On:       "test",
								Channels: []string{"t://hello"},
							},
						},
					},
				},
				expected: local.Job{
					Behavior: local.JobBehavior{
						Notify: []local.JobNotifier{
							{
								On:       "test",
								Channels: []string{"t://hello", "t://hello-parent"},
								Config: map[string]string{
									"duration": "2h",
								},
							},
							{
								On:       "test-2",
								Channels: []string{"t://hello-2"},
							},
						},
					},
				},
			},
			args: args{
				parent: local.Job{
					Behavior: local.JobBehavior{
						Notify: []local.JobNotifier{
							{
								On:       "test",
								Channels: []string{"t://hello-parent"},
								Config: map[string]string{
									"duration": "2h",
								},
							},
							{
								On:       "test-2",
								Channels: []string{"t://hello-2"},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.child.MergeFrom(tt.args.parent)
			assert.Equal(t, tt.fields.expected.Version, tt.fields.child.Version)
			assert.Equal(t, tt.fields.expected.Name, tt.fields.child.Name)
			assert.Equal(t, tt.fields.expected.Description, tt.fields.child.Description)
			assert.Equal(t, tt.fields.expected.Schedule.Interval, tt.fields.child.Schedule.Interval)
			assert.Equal(t, tt.fields.expected.Schedule.StartDate, tt.fields.child.Schedule.StartDate)
			assert.Equal(t, tt.fields.expected.Schedule.EndDate, tt.fields.child.Schedule.EndDate)
			assert.Equal(t, tt.fields.expected.Behavior.DependsOnPast, tt.fields.child.Behavior.DependsOnPast)
			assert.Equal(t, tt.fields.expected.Behavior.Catchup, tt.fields.child.Behavior.Catchup)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.Count, tt.fields.child.Behavior.Retry.Count)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.Delay, tt.fields.child.Behavior.Retry.Delay)
			assert.Equal(t, tt.fields.expected.Behavior.Retry.ExponentialBackoff, tt.fields.child.Behavior.Retry.ExponentialBackoff)
			assert.ElementsMatch(t, tt.fields.expected.Dependencies, tt.fields.child.Dependencies)
			assert.Equal(t, len(tt.fields.expected.Labels), len(tt.fields.child.Labels))
			assert.Equal(t, tt.fields.expected.Task.Name, tt.fields.child.Task.Name)
			assert.Equal(t, tt.fields.expected.Task.Window.Offset, tt.fields.child.Task.Window.Offset)
			assert.Equal(t, tt.fields.expected.Task.Window.Size, tt.fields.child.Task.Window.Size)
			assert.Equal(t, tt.fields.expected.Task.Window.TruncateTo, tt.fields.child.Task.Window.TruncateTo)
			assert.ElementsMatch(t, tt.fields.expected.Task.Config, tt.fields.child.Task.Config)
			for idx, eh := range tt.fields.expected.Hooks {
				assert.Equal(t, eh.Name, tt.fields.child.Hooks[idx].Name)
				assert.ElementsMatch(t, eh.Config, tt.fields.child.Hooks[idx].Config)
			}
			for idx, en := range tt.fields.expected.Behavior.Notify {
				assert.Equal(t, en.On, tt.fields.child.Behavior.Notify[idx].On)
				assert.ElementsMatch(t, en.Channels, tt.fields.child.Behavior.Notify[idx].Channels)
				if !reflect.DeepEqual(en.Config, tt.fields.child.Behavior.Notify[idx].Config) {
					t.Errorf("want: %v, got: %v", en.Config, tt.fields.child.Behavior.Notify[idx].Config)
				}
			}
		})
	}
}
