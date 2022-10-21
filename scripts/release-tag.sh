#!/usr/bin/env bash

# exit when any command fails
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../")

if [ -z "$1" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

VERSION=$1

cd "$PROJECT_DIR"
cargo set-version $VERSION

cd subzero-node
npm version $VERSION

cd "$PROJECT_DIR"

git add .
git commit -m "Release $VERSION"
git tag -a $VERSION -m "Release $VERSION"
git push
git push --tags
