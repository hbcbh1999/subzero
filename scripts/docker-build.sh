#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source $SCRIPT_DIR/.env

echo "Building docker image for subzerocloud/subzero:$VERSION-$ARCH"
cd "$SCRIPT_DIR/.."
docker build -t subzerocloud/subzero:$VERSION-$ARCH .
docker tag subzerocloud/subzero:$VERSION-$ARCH subzerocloud/subzero:latest-$ARCH
