#!/bin/bash
NAME="github.com/odpf/optimus"
#CTL_VERSION=`git describe --tags $(git rev-list --tags --max-count=1)`
CTL_VERSION="$(git rev-parse --short HEAD)"
OPMS_VERSION="$(git rev-parse --short HEAD)"

SYS=("linux" "darwin")
ARCH=("amd64" "arm64")
BUILD_DIR="dist"

build_executable() {
    EXECUTABLE_NAME=$1
    LD_FLAGS=$2
    for os in ${SYS[*]}; do
        for arch in ${ARCH[*]}; do

            # create a folder named via the combination of os and arch
            TARGET="./$BUILD_DIR/$EXECUTABLE_NAME/${os}-${arch}"
            mkdir -p $TARGET

            # place the executable within that folder
            executable="${TARGET}/$EXECUTABLE_NAME"
            echo $executable
            CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -ldflags "$LD_FLAGS" -o $executable $NAME/cmd/$EXECUTABLE_NAME
        done
    done
}

build_executable optimus "-X main.Version=${OPMS_VERSION}"
build_executable opctl "-X main.Version=${CTL_VERSION}"