#!/usr/bin/env bash

# exit when any command fails
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../")

for i in $(find . -name '*.rs' -o -name '*.ts')
do
    # exclude files if the path contains one of the following strings
    if [[ $i == *"target"* ]] || [[ $i == *"node_modules"* ]] || [[ $i == *"dist"* ]] || [[ $i == *"pkg-"* ]]
    then
        continue
    fi
    if ! grep -q Copyright $i
    then
        cat $PROJECT_DIR/COPYRIGHT.txt $i >$i.new && mv $i.new $i
    fi
done