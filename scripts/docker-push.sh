#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/.env

echo "Pushing docker image for subzerocloud/subzero:$VERSION-$ARCH"
docker push subzerocloud/subzero:$VERSION-$ARCH
docker push subzerocloud/subzero:latest-$ARCH