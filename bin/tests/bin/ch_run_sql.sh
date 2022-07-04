#!/bin/sh
FILE=$1
URL=$2

grep -v ^-- $FILE | while read -d ";" q; do
    echo "-----\n $q"
    curl -X POST -d "$q" $URL
done
