#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/.env

echo "Pushing docker manifest for subzerocloud/subzero:$VERSION"

docker manifest rm subzerocloud/subzero:$VERSION
docker manifest rm subzerocloud/subzero:latest

docker manifest create subzerocloud/subzero:$VERSION \
    --amend subzerocloud/subzero:$VERSION-amd64 \
    --amend subzerocloud/subzero:$VERSION-arm64

docker manifest create subzerocloud/subzero:latest \
    --amend subzerocloud/subzero:latest-amd64 \
    --amend subzerocloud/subzero:latest-arm64

docker manifest push subzerocloud/subzero:$VERSION
docker manifest push subzerocloud/subzero:latest
