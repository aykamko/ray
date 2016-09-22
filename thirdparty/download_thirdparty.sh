#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

git submodule update --init --recursive -- "$TP_DIR/common"
git submodule update --init --recursive -- "$TP_DIR/halo"
git submodule update --init --recursive -- "$TP_DIR/plasma"
git submodule update --init --recursive -- "$TP_DIR/arrow"
git submodule update --init --recursive -- "$TP_DIR/numbuf"
