.ONESHELL:
.DELETE_ON_ERROR:
MAKEFLAGS += --no-builtin-rules
NAME = "github.com/odpf/optimus"
CTL_VERSION := "$(shell git rev-parse --short HEAD)"
OPMS_VERSION := "$(shell git rev-parse --short HEAD)"

all: build

.PHONY: build build-optimus smoke-test unit-test test clean generate dist init vet

build-ctl: generate ## generate opctl
	@echo " > building opctl version ${CTL_VERSION}"
	@go build -ldflags "-X config.Version=${CTL_VERSION}" ${NAME}/cmd/opctl

build-optimus: generate ## generate optimus server
	@echo " > building optimus version ${OPMS_VERSION}"
	@go build -ldflags "-X 'config.Version=${OPMS_VERSION}'" ${NAME}/cmd/optimus

build: build-optimus build-ctl
	@echo " - build complete"
	
test: smoke-test unit-test vet ## run tests

generate: pack-files
	@echo " > notice: skipped proto generation, use 'generate-proto' make command"
	
pack-files: ./resources/pack ./resources/resource_fs_gen.go
	@echo " > packing resources"
	@go generate ./resources

generate-proto: ## regenerate protos
	@echo " > cloning protos from odpf/proton"
	@rm -rf proton/
	@git -c advice.detachedHead=false clone https://github.com/odpf/proton --depth 1 --quiet --branch main
	@echo " > generating protos"
	@buf generate

unit-test:
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 1 -cover -race -timeout 1m -tags=unit_test

smoke-test: build-ctl
	@bash ./scripts/smoke-test.sh

integration-test: build
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 1 -cover -race -timeout 1m

vet: ## run go vet
	go vet ./...

coverage: ## print code coverage
	go test -race -coverprofile coverage.txt -covermode=atomic ./... -tags=unit_test && go tool cover -html=coverage.txt

dist: generate
	@bash ./scripts/build-distributables.sh

clean:
	rm -rf ./optimus ./opctl ./dist ./proton ./api/proto/*

install: ## install required dependencies
	@echo "> installing dependencies"
	go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.25.0
	go get github.com/golang/protobuf/proto@v1.4.3
	go get github.com/golang/protobuf/protoc-gen-go@v1.4.3
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0
	go get google.golang.org/grpc@v1.35.0
	go get github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.2.0
	go get github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.2.0
	go get github.com/bufbuild/buf/cmd/buf@v0.37.0
