#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/.env

echo "Pushing docker manifest for subzerocloud/subzero:$VERSION"

docker manifest rm subzerocloud/subzero:$VERSION
docker manifest rm subzerocloud/subzero:latest

docker manifest create subzerocloud/subzero:$VERSION \
    subzerocloud/subzero:$VERSION-x86_64 \
    subzerocloud/subzero:$VERSION-arm64

docker manifest create subzerocloud/subzero:latest \
    subzerocloud/subzero:latest-x86_64 \
    subzerocloud/subzero:latest-arm64

docker manifest push subzerocloud/subzero:$VERSION
docker manifest push subzerocloud/subzero:latest
