.ONESHELL:
.DELETE_ON_ERROR:
MAKEFLAGS += --no-builtin-rules
NAME = "github.com/odpf/optimus"
#CTL_VERSION := `git describe --tags $(shell git rev-list --tags --max-count=1)`
CTL_VERSION := "$(shell git rev-parse --short HEAD)"
OPMS_VERSION := "$(shell git rev-parse --short HEAD)"

all: build

.PHONY: build build-optimus smoke-test unit-test test clean generate dist init

build-ctl: generate
	@echo " > building opctl version ${CTL_VERSION}"
	@go build -ldflags "-X main.Version=${CTL_VERSION}" ${NAME}/cmd/opctl

build-optimus: generate
	@echo " > building optimus version ${OPMS_VERSION}"
	@go build -ldflags "-X 'main.Version=${OPMS_VERSION}'" ${NAME}/cmd/optimus

build: build-optimus build-ctl
	@echo " - build complete"
	
test: smoke-test unit-test

generate: pack-files generate-proto
	
pack-files: ./resources/pack ./resources/resource_fs_gen.go
	@echo " > packing resources"
	@go generate ./resources

generate-proto:
	@echo " > cloning protos from odpf/proton"
	@rm -rf proton/
	@git -c advice.detachedHead=false clone https://github.com/odpf/proton --depth 1 --quiet --branch optimus-runtime
	@echo " > generating protos"
	@buf generate

unit-test:
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 1 -cover -race -timeout 1m -tags=unit_test

smoke-test: build-ctl
	@bash ./scripts/smoke-test.sh

integration-test: build
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 1 -cover -race -timeout 1m

coverage:
	go test -coverprofile test_coverage.html ./... -tags=unit_test && go tool cover -html=test_coverage.html

dist: generate
	@bash ./scripts/build-distributables.sh

clean:
	rm -rf ./optimus ./opctl ./dist ./proton

install:
	@echo "> installing dependencies"
	go get -u github.com/golang/protobuf/proto@v1.4.3
	go get -u github.com/golang/protobuf/protoc-gen-go@v1.4.3
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0
	go get -u google.golang.org/grpc@v1.35.0
	go get -u github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.2.0
	go get -u github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.2.0
	go get -u github.com/bufbuild/buf/cmd/buf@v0.37.0
