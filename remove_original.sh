#!/usr/bin/env bash

CURR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

skill=$CURR_DIR/

for skill in $HIGH_DIR*/; do
	for dataset in $skill*/; do
		rm -rf $dataset/original
	done
done