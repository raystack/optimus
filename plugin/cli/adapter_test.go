package cli

import (
	"reflect"
	"testing"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
	"github.com/odpf/optimus/models"
)

func Test_AdaptQuestionToProto(t *testing.T) {
	type args struct {
		conf models.PluginQuestion
	}
	tests := []struct {
		name string
		args args
		want *pb.PluginQuestion
	}{
		{
			name: "should work for non empty inputs (with empty SubQuestion parameter)",
			args: args{
				conf: models.PluginQuestion{
					Name:         "key",
					Prompt:       "sec",
					Help:         "do",
					Default:      "this",
					Multiselect:  nil,
					SubQuestions: nil,
				},
			},
			want: &pb.PluginQuestion{
				Name:         "key",
				Prompt:       "sec",
				Help:         "do",
				Default:      "this",
				Multiselect:  nil,
				SubQuestions: nil,
			},
		},
		{
			name: "should work for non empty inputs",
			args: args{
				conf: models.PluginQuestion{
					Name:        "key",
					Prompt:      "sec",
					Help:        "do",
					Default:     "this",
					Multiselect: nil,
					SubQuestions: []models.PluginSubQuestion{
						{
							IfValue:   "",
							Questions: []models.PluginQuestion{},
						},
					},
				},
			},
			want: &pb.PluginQuestion{
				Name:        "key",
				Prompt:      "sec",
				Help:        "do",
				Default:     "this",
				Multiselect: nil,
				SubQuestions: []*pb.PluginQuestion_SubQuestion{
					{
						IfValue:   "",
						Questions: []*pb.PluginQuestion{},
					},
				},
			},
		},
		{
			name: "should work for non empty inputs",
			args: args{
				conf: models.PluginQuestion{
					Name:        "key",
					Prompt:      "sec",
					Help:        "this",
					Default:     "do",
					Multiselect: []string{"a", "ba", "b"},
					SubQuestions: []models.PluginSubQuestion{
						{
							IfValue: "",
							Questions: []models.PluginQuestion{
								{
									Name:         "key",
									Prompt:       "sec",
									Help:         "this",
									Default:      "do",
									Multiselect:  []string{"a", "ba", "b"},
									SubQuestions: []models.PluginSubQuestion{},
								},
							},
						},
					},
				},
			},
			want: &pb.PluginQuestion{
				Name:        "key",
				Prompt:      "sec",
				Help:        "this",
				Default:     "do",
				Multiselect: []string{"a", "ba", "b"},
				SubQuestions: []*pb.PluginQuestion_SubQuestion{
					{
						IfValue: "",
						Questions: []*pb.PluginQuestion{
							{
								Name:         "key",
								Prompt:       "sec",
								Help:         "this",
								Default:      "do",
								Multiselect:  []string{"a", "ba", "b"},
								SubQuestions: nil,
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptQuestionToProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("adaptQuestionToProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_AdaptQuestionFromProto(t *testing.T) {
	type args struct {
		conf *pb.PluginQuestion
	}
	tests := []struct {
		name string
		args args
		want models.PluginQuestion
	}{
		{
			name: "should work for empty SubQuestions part",
			args: args{
				conf: &pb.PluginQuestion{
					Name:         "key",
					Prompt:       "sec",
					Help:         "this",
					Default:      "do",
					Multiselect:  nil,
					SubQuestions: []*pb.PluginQuestion_SubQuestion{},
				},
			},
			want: models.PluginQuestion{
				Name:         "key",
				Prompt:       "sec",
				Help:         "this",
				Default:      "do",
				Multiselect:  nil,
				SubQuestions: []models.PluginSubQuestion{},
			},
		},
		{
			name: "should work for empty Multiselect input",
			args: args{
				conf: &pb.PluginQuestion{
					Name:        "key",
					Prompt:      "sec",
					Help:        "this",
					Default:     "do",
					Multiselect: []string{"a", "ba", "b"},
					SubQuestions: []*pb.PluginQuestion_SubQuestion{
						{
							IfValue: "",
							Questions: []*pb.PluginQuestion{
								{
									Name:         "key",
									Prompt:       "sec",
									Help:         "this",
									Default:      "do",
									Multiselect:  nil,
									SubQuestions: []*pb.PluginQuestion_SubQuestion{},
								},
							},
						},
					},
				},
			},
			want: models.PluginQuestion{
				Name:        "key",
				Prompt:      "sec",
				Help:        "this",
				Default:     "do",
				Multiselect: []string{"a", "ba", "b"},
				SubQuestions: []models.PluginSubQuestion{
					{
						IfValue: "",
						Questions: []models.PluginQuestion{
							{
								Name:         "key",
								Prompt:       "sec",
								Help:         "this",
								Default:      "do",
								Multiselect:  nil,
								SubQuestions: []models.PluginSubQuestion{},
							},
						},
					},
				},
			},
		},
		{
			name: "should work for non empty inputs",
			args: args{
				conf: &pb.PluginQuestion{
					Name:        "key",
					Prompt:      "sec",
					Help:        "this",
					Default:     "do",
					Multiselect: []string{"a", "ba", "b"},
					SubQuestions: []*pb.PluginQuestion_SubQuestion{
						{
							IfValue: "",
							Questions: []*pb.PluginQuestion{
								{
									Name:         "key",
									Prompt:       "sec",
									Help:         "this",
									Default:      "do",
									Multiselect:  []string{"a", "ba", "b"},
									SubQuestions: []*pb.PluginQuestion_SubQuestion{},
								},
							},
						},
					},
				},
			},
			want: models.PluginQuestion{
				Name:        "key",
				Prompt:      "sec",
				Help:        "this",
				Default:     "do",
				Multiselect: []string{"a", "ba", "b"},
				SubQuestions: []models.PluginSubQuestion{
					{
						IfValue: "",
						Questions: []models.PluginQuestion{
							{
								Name:         "key",
								Prompt:       "sec",
								Help:         "this",
								Default:      "do",
								Multiselect:  []string{"a", "ba", "b"},
								SubQuestions: []models.PluginSubQuestion{},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptQuestionFromProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdaptQuestionFromProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptAssetsFromProto(t *testing.T) {
	type args struct {
		conf *pb.Assets
	}
	tests := []struct {
		name string
		args args
		want models.PluginAssets
	}{
		{
			name: "nil asset should be handled correctly",
			args: args{
				conf: &pb.Assets{Assets: nil},
			},
			want: models.PluginAssets{},
		},
		{
			name: "partial asset (Name) should be adapted from proto correctly",
			args: args{
				conf: &pb.Assets{
					Assets: []*pb.Assets_Asset{
						{
							Name: "key",
						},
					},
				},
			},
			want: models.PluginAssets{
				models.PluginAsset{
					Name: "key",
				},
			},
		},
		{
			name: "asset should be adapted from proto correctly",
			args: args{
				conf: &pb.Assets{
					Assets: []*pb.Assets_Asset{
						{
							Name:  "key",
							Value: "sec",
						},
					},
				},
			},
			want: models.PluginAssets{
				models.PluginAsset{
					Name:  "key",
					Value: "sec",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptAssetsFromProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdaptAssetsFromProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptAssetsToProto(t *testing.T) {
	type args struct {
		conf models.PluginAssets
	}
	tests := []struct {
		name string
		args args
		want *pb.Assets
	}{
		{
			name: "should work for empty input",
			args: args{
				conf: nil,
			},
			want: &pb.Assets{
				Assets: []*pb.Assets_Asset{},
			},
		},
		{
			name: "partial asset (Value) should be adapted from proto correctly",
			args: args{
				conf: models.PluginAssets{
					models.PluginAsset{
						Value: "sec",
					},
				},
			},
			want: &pb.Assets{
				Assets: []*pb.Assets_Asset{
					{
						Value: "sec",
					},
				},
			},
		},
		{
			name: "asset should be adapted from proto correctly",
			args: args{
				conf: models.PluginAssets{
					models.PluginAsset{
						Name:  "key",
						Value: "sec",
					},
				},
			},
			want: &pb.Assets{
				Assets: []*pb.Assets_Asset{
					{
						Name:  "key",
						Value: "sec",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptAssetsToProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdaptAssetsToProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptConfigsFromProto(t *testing.T) {
	type args struct {
		conf *pb.Configs
	}
	tests := []struct {
		name string
		args args
		want models.PluginConfigs
	}{
		{
			name: "should work for empty input",
			args: args{
				conf: &pb.Configs{Configs: nil},
			},
			want: models.PluginConfigs{},
		},
		{
			name: "should work for partial config (Value) input",
			args: args{
				conf: &pb.Configs{
					Configs: []*pb.Configs_Config{
						{
							Value: "sec",
						},
					},
				},
			},
			want: models.PluginConfigs{
				models.PluginConfig{
					Value: "sec",
				},
			},
		},
		{
			name: "should work for non empty input",
			args: args{
				conf: &pb.Configs{
					Configs: []*pb.Configs_Config{
						{
							Name:  "key",
							Value: "sec",
						},
					},
				},
			},
			want: models.PluginConfigs{
				models.PluginConfig{
					Name:  "key",
					Value: "sec",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptConfigsFromProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdaptConfigsFromProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptConfigsToProto(t *testing.T) {
	type args struct {
		conf models.PluginConfigs
	}
	tests := []struct {
		name string
		args args
		want *pb.Configs
	}{
		{
			name: "should work for partial non empty inputs",
			args: args{
				conf: models.PluginConfigs{
					models.PluginConfig{
						Value: "sec",
					},
				},
			},
			want: &pb.Configs{
				Configs: []*pb.Configs_Config{
					{
						Value: "sec",
					},
				},
			},
		},
		{
			name: "should work for non empty inputs",
			args: args{
				conf: models.PluginConfigs{
					models.PluginConfig{
						Name:  "key",
						Value: "sec",
					},
				},
			},
			want: &pb.Configs{
				Configs: []*pb.Configs_Config{
					{
						Name:  "key",
						Value: "sec",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AdaptConfigsToProto(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AdaptConfigsToProto() = %v, want %v", got, tt.want)
			}
		})
	}
}
