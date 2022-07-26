package setup

import (
	"fmt"

	"github.com/odpf/optimus/models"
)

func Project(i int) models.ProjectSpec {
	return models.ProjectSpec{
		Name: fmt.Sprintf("t-optimus-%d", i),
		Config: map[string]string{
			"environment":                         "production",
			"bucket":                              "gs://some_folder-2",
			"storage_path":                        "gs://storage_bucket",
			"transporterKafka":                    "10.12.12.12:6668,10.12.12.13:6668",
			"predator_host":                       "https://predator.example.com",
			"scheduler_host":                      "https://optimus.example.com/",
			"transporter_kafka_brokers":           "10.5.5.5:6666",
			"transporter_stencil_host":            "https://artifactory.example.com/artifactory/proto-descriptors/ocean-proton/latest",
			"transporter_stencil_broker_host":     "https://artifactory.example.com/artifactory/proto-descriptors/latest",
			"transporter_stencil_server_url":      "https://stencil.example.com",
			"transporter_stencil_namespace":       "optimus",
			"transporter_stencil_descriptor_name": "transporter-log-entities",
			"bq2email_smtp_address":               "smtp.example.com",
			"bridge_host":                         "1.1.1.1",
			"bridge_port":                         "80",
			"ocean_gcs_tmp_bucket":                "bq2-plugins",
		},
		Secret: models.ProjectSecrets{
			Secret(1),
			Secret(2),
			Secret(3),
		},
	}
}

func Secret(i int) models.ProjectSecretItem {
	return models.ProjectSecretItem{
		Name:  fmt.Sprintf("Secret%d", i),
		Value: "secret",
		Type:  models.SecretTypeUserDefined,
	}
}

func Namespace(i int, project models.ProjectSpec) models.NamespaceSpec {
	return models.NamespaceSpec{
		Name: fmt.Sprintf("ns-optimus-%d", i),
		Config: map[string]string{
			"environment":                   "production",
			"bucket":                        "gs://some_folder-2",
			"storage_path":                  "gs://storage_bucket",
			"predator_host":                 "https://predator.example.com",
			"scheduler_host":                "https://optimus.example.com/",
			"transporter_kafka_brokers":     "10.5.5.5:6666",
			"transporter_stencil_namespace": "optimus",
			"bq2email_smtp_address":         "smtp.example.com",
			"bridge_host":                   "1.1.1.1",
			"bridge_port":                   "80",
			"ocean_gcs_tmp_bucket":          "bq2-plugins",
		},
		ProjectSpec: project,
	}
}
