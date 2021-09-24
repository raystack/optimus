.ONESHELL:
.DELETE_ON_ERROR:
MAKEFLAGS += --no-builtin-rules
NAME = "github.com/odpf/optimus"
LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_TAG := "$(shell git rev-list --tags --max-count=1)"
OPMS_VERSION := "$(shell git describe --tags ${LAST_TAG})-next"
PROTON_COMMIT := "a3dee4ac5b3139a1be21789a48081b8674119207"

all: build

.PHONY: build smoke-test unit-test test clean generate dist init vet

build: generate # build optimus binary
	@echo " > building optimus version ${OPMS_VERSION}"
	@go build -ldflags "-X ${NAME}/config.Version=${OPMS_VERSION} -X ${NAME}/config.BuildCommit=${LAST_COMMIT}" -o optimus .
	@echo " - build complete"
	
test: smoke-test unit-test vet ## run tests

generate: pack-files
	@echo " > notice: skipped proto generation, use 'generate-proto' make command"
	
pack-files:
	@echo " > packing resources"
	@go generate ./..

generate-proto: ## regenerate protos
	@echo " > generating protobuf from odpf/proton"
	@echo " > [info] make sure correct version of dependencies are installed using 'make install'"
	@buf generate https://github.com/odpf/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --template buf.gen.yaml --path odpf/optimus --path odpf/metadata
	@echo " > protobuf compilation finished"

unit-test:
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 5 -cover -race -timeout 1m -tags=unit_test

smoke-test: build
	@bash ./scripts/smoke-test.sh

integration-test: 
	go list ./... | grep -v -e third_party -e api/proto | xargs go test -count 1 -cover -race -timeout 1m

vet: ## run go vet
	go vet ./...

coverage: ## print code coverage
	go test -race -coverprofile coverage.txt -covermode=atomic ./... -tags=unit_test && go tool cover -html=coverage.txt

dist: generate
	@bash ./scripts/build-distributables.sh

clean:
	rm -rf ./optimus ./dist ./api/proto/* ./api/third_party/odpf/*

install: ## install required dependencies
	@echo "> installing dependencies"
	go mod tidy
	go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1
	go get github.com/golang/protobuf/proto@v1.5.2
	go get github.com/golang/protobuf/protoc-gen-go@v1.5.2
	go get google.golang.org/grpc@v1.40.0
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0
	go get github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.5.0
	go get github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.5.0
	go get github.com/bufbuild/buf/cmd/buf@v0.54.1
