module github.com/odpf/optimus

require (
	cloud.google.com/go v0.65.0
	cloud.google.com/go/storage v1.10.0
	github.com/AlecAivazis/survey/v2 v2.2.7
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible
	github.com/Masterminds/sprig/v3 v3.2.2
	github.com/fatih/color v1.7.0
	github.com/gernest/wow v0.1.0
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/golang-migrate/migrate/v4 v4.14.1
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.2
	github.com/googleapis/google-cloud-go-testing v0.0.0-20200911160855-bcd43fbb19e8
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.2.0
	github.com/hashicorp/go-multierror v1.1.0
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/iancoleman/strcase v0.1.3
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/jinzhu/gorm v1.9.16
	github.com/lib/pq v1.9.0
	github.com/mattn/go-sqlite3 v2.0.1+incompatible // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cobra v1.1.2
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.22.6 // indirect
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/api v0.30.0
	google.golang.org/genproto v0.0.0-20210207032614-bba0dbe2a9ea
	google.golang.org/grpc v1.35.0
	google.golang.org/protobuf v1.25.1-0.20201208041424-160c7477e0e8
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	gorm.io/datatypes v1.0.0
)

go 1.14

replace github.com/gernest/wow v0.1.0 => github.com/kushsharma/wow v0.1.2
