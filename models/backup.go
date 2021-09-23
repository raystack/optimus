package models

type BackupRequest struct {
	ResourceName     string
	Project          ProjectSpec
	Datastore        string
	Description      string
	IgnoreDownstream bool
}
