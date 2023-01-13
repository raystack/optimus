.ONESHELL:
.DELETE_ON_ERROR:
MAKEFLAGS += --no-builtin-rules
NAME = "github.com/odpf/optimus"
LAST_COMMIT := $(shell git rev-parse --short HEAD)
LAST_TAG := "$(shell git rev-list --tags --max-count=1)"
OPMS_VERSION := "$(shell git describe --tags ${LAST_TAG})-next"
PROTON_COMMIT := "bf29fa847fc87f22f6596df24b62f430073ab077"


.PHONY: build test test-ci generate-proto unit-test-ci integration-test vet coverage clean install lint

.DEFAULT_GOAL := build

build: # build optimus binary
	@echo " > notice: skipped proto generation, use 'generate-proto' make command"
	@echo " > building optimus version ${OPMS_VERSION}"
	@go build -ldflags "-X ${NAME}/config.BuildVersion=${OPMS_VERSION} -X ${NAME}/config.BuildCommit=${LAST_COMMIT}" -o optimus .
	@echo " - build complete"

build-linux: # build optimus binary for linux
	@echo " > notice: skipped proto generation, use 'generate-proto' make command"
	@echo " > building optimus version ${OPMS_VERSION}"
	@GOOS=linux GOARCH=arm64 go build -ldflags "-X ${NAME}/config.BuildVersion=${OPMS_VERSION} -X ${NAME}/config.BuildCommit=${LAST_COMMIT}" -o optimus .
	@echo " - build complete"

test-ci: unit-test-ci vet ## run tests

scheduler-resource-test:
	cd ./ext/scheduler/airflow2/tests && pip3 install -r requirements.txt && python3 -m unittest discover .

generate-proto: ## regenerate protos
	@echo " > generating protobuf from odpf/proton"
	@echo " > [info] make sure correct version of dependencies are installed using 'make install'"
	@buf generate https://github.com/odpf/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --template buf.gen.yaml --path odpf/optimus
	@echo " > protobuf compilation finished"

unit-test-ci:
	go test -count 5 -race -coverprofile coverage.txt -covermode=atomic -timeout 1m -tags=unit_test ./...

integration-test:
	go test -p 1 -count 1 -cover -race -timeout 1m ./internal/store/postgres/... -run TestPostgres

vet: ## run go vet
	go vet ./...

test:
	go test -race -cover -timeout 1m -tags=unit_test ./...

bench:
	@go test -bench=. ./tests/bench/... -benchmem -timeout 20m

coverage: ## print code coverage
	go test -race -coverprofile coverage.txt -covermode=atomic ./... -tags=unit_test && go tool cover -html=coverage.txt

clean:
	rm -rf ./optimus ./dist ./api/proto/* ./api/third_party/odpf/*

lint:
	golangci-lint run --fix

install: ## install required dependencies
	@echo "> installing dependencies"
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.47.3
	go install github.com/bufbuild/buf/cmd/buf@v1.11.0
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.15.0
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.15.0
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0