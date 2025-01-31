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

# set the version for rust subprojects
cd "$PROJECT_DIR"
cargo set-version $VERSION

# set the version for the node subproject
cd js-bindings
npm version $VERSION

# set the version for java subproject
cd "$PROJECT_DIR"
cd java-bindings
mvn versions:set -DnewVersion=$VERSION

cd "$PROJECT_DIR"
cargo generate-lockfile

git add .
git commit -m "Release $VERSION [ci skip]"
git tag -a $VERSION -m "Release $VERSION"
git push
git push --tags
