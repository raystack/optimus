package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/odpf/optimus/models"
	api "github.com/slack-go/slack"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func getTestUserProfile() api.UserProfile {
	return api.UserProfile{
		StatusText:  "testStatus",
		StatusEmoji: ":construction:",
		RealName:    "Test Real Name",
		Email:       "optimus@test.com",
	}
}

func getTestUserWithID(id string) api.User {
	return api.User{
		ID:                id,
		Name:              "Test User",
		Deleted:           false,
		Color:             "9f69e7",
		RealName:          "testuser",
		TZ:                "America/Los_Angeles",
		TZLabel:           "Pacific Daylight Time",
		TZOffset:          -25200,
		Profile:           getTestUserProfile(),
		IsBot:             false,
		IsAdmin:           false,
		IsOwner:           false,
		IsPrimaryOwner:    false,
		IsRestricted:      false,
		IsUltraRestricted: false,
		Updated:           1555425715,
		Has2FA:            false,
	}
}

func TestSlack(t *testing.T) {
	t.Run("should send message to user using email address successfully", func(t *testing.T) {
		muxRouter := mux.NewRouter()
		server := httptest.NewServer(muxRouter)
		muxRouter.HandleFunc("/users.lookupByEmail", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				Ok   bool     `json:"ok"`
				User api.User `json:"user"`
			}{
				Ok:   true,
				User: getTestUserWithID("ABCD"),
			})
			rw.Write(response)
		})
		muxRouter.HandleFunc("/chat.postMessage", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				SlackResponse api.SlackResponse
			}{
				SlackResponse: api.SlackResponse{
					Ok: true,
				},
			})
			rw.Write(response)
		})

		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error
		client := NewNotifier(
			ctx,
			"http://"+server.Listener.Addr().String()+"/",
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
		)
		defer client.Close()
		err := client.Notify(context.Background(), models.NotifyAttrs{
			Namespace: models.NamespaceSpec{
				Name: "test",
				ProjectSpec: models.ProjectSpec{
					Name: "foo",
					Secret: []models.ProjectSecretItem{
						{
							Name:  OAuthTokenSecretName,
							Value: "test-token",
						},
					},
				},
			},
			JobSpec: models.JobSpec{
				Name: "foo-job-spec",
			},
			JobEvent: models.JobEvent{
				Type: models.JobEventTypeSLAMiss,
			},
			Route: "optimus@test.com",
		})
		assert.Nil(t, err)
		cancel()
		assert.Nil(t, client.Close())
		assert.Nil(t, sendErrors)
	})
	t.Run("should send message to user groups successfully", func(t *testing.T) {
		muxRouter := mux.NewRouter()
		server := httptest.NewServer(muxRouter)
		muxRouter.HandleFunc("/usergroups.list", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				Ok         bool            `json:"ok"`
				UserGroups []api.UserGroup `json:"usergroups"`
			}{
				Ok: true,
				UserGroups: []api.UserGroup{
					{ID: "test-id", Handle: "optimus-devs"},
				},
			})
			rw.Write(response)
		})
		muxRouter.HandleFunc("/usergroups.users.list", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				Ok    bool     `json:"ok"`
				Users []string `json:"users"`
			}{
				Ok:    true,
				Users: []string{"ABCD"},
			})
			rw.Write(response)
		})
		muxRouter.HandleFunc("/chat.postMessage", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				SlackResponse api.SlackResponse
			}{
				SlackResponse: api.SlackResponse{
					Ok: true,
				},
			})
			rw.Write(response)
		})

		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error
		client := NewNotifier(
			ctx,
			"http://"+server.Listener.Addr().String()+"/",
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
		)

		eventValues, _ := structpb.NewStruct(map[string]interface{}{
			"task_id":   "some_task_name",
			"duration":  "2s",
			"log_url":   "http://localhost:8081/tree?dag_id=hello_1",
			"message":   "some failure",
			"exception": "this much data failed",
		})
		err := client.Notify(context.Background(), models.NotifyAttrs{
			Namespace: models.NamespaceSpec{
				Name: "test",
				ProjectSpec: models.ProjectSpec{
					Name: "foo",
					Secret: []models.ProjectSecretItem{
						{
							Name:  OAuthTokenSecretName,
							Value: "test-token",
						},
					},
				},
			},
			JobSpec: models.JobSpec{
				Name: "foo-job-spec",
			},
			JobEvent: models.JobEvent{
				Type:  models.JobEventTypeFailure,
				Value: eventValues.GetFields(),
			},
			Route: "@optimus-devs",
		})
		assert.Nil(t, err)
		cancel()
		assert.Nil(t, client.Close())
		assert.Nil(t, sendErrors)
	})
}

func TestBuildMessages(t *testing.T) {
	eventValues := &structpb.Struct{}
	_ = eventValues.UnmarshalJSON([]byte(`{
            "slas": [
                {
                    "task_id": "bq2bq",
                    "dag_id": "hello_1",
                    "scheduled_at": "2021-07-12T07:40:00Z",
                    "timestamp": "2021-07-12T07:54:37Z"
                },
                {
                    "task_id": "bq2bq",
                    "dag_id": "hello_1",
                    "scheduled_at": "2021-07-12T07:41:00Z",
                    "timestamp": "2021-07-12T07:54:37Z"
                }
            ]
        }`))
	type args struct {
		events []event
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should parse sla values of sla_miss correctly",
			args: args{events: []event{
				{
					authToken:     "xx",
					projectName:   "ss",
					namespaceName: "bb",
					jobName:       "cc",
					owner:         "rr",
					meta: models.JobEvent{
						Type:  models.JobEventTypeSLAMiss,
						Value: eventValues.GetFields(),
					},
				},
			}},
			want: `[
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "[Job] SLA Breached | ss/bb",
            "emoji": true
        }
    },
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*Job:*\ncc"
            },
            {
                "type": "mrkdwn",
                "text": "*Owner:*\nrr"
            },
            {
                "type": "mrkdwn",
                "text": "*Breached item:*\nTask: bq2bq\nScheduled at: 2021-07-12T07:40:00Z"
            },
            {
                "type": "mrkdwn",
                "text": "*Breached item:*\nTask: bq2bq\nScheduled at: 2021-07-12T07:41:00Z"
            }
        ]
    }
]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMessageBlocks(tt.args.events)
			b, err := json.MarshalIndent(got, "", "    ")
			assert.Nil(t, err)
			assert.Equal(t, tt.want, string(b))
		})
	}
}
