#!/bin/bash

RELEASE_VERSION=
if [ -z "${RELEASE_VERSION}" ] ; then
    RELEASE_VERSION=$(cat RELEASE_VERSION)
fi
target=go-data-checksum
ldflags="-X main.AppVersion=${RELEASE_VERSION}"

go build -ldflags "$ldflags" -o $target go-data-checksum.go