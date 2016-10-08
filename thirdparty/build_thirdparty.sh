#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
PREFIX=$TP_DIR/installed

# Determine how many parallel jobs to use for make based on the number of cores
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
  echo "Platform is macosx."
else
  echo "Unrecognized platform."
  exit 1
fi

echo "building photon"
cd $TP_DIR/photon
# Build the common submodule first.
cd common
make
make test
cd lib/python
sudo python setup.py install
cd ../../..
# Now build photon.
make
cd lib/python
sudo python setup.py install

echo "building plasma"
cd $TP_DIR/plasma
make

echo "building arrow"
cd $TP_DIR/arrow/cpp
source setup_build_env.sh
mkdir -p $TP_DIR/arrow/cpp/build
cd $TP_DIR/arrow/cpp/build
cmake -DLIBARROW_LINKAGE=STATIC -DCMAKE_BUILD_TYPE=Release ..
make VERBOSE=1 -j$PARALLEL

echo "building numbuf"
cd $TP_DIR/numbuf
mkdir -p build
cd $TP_DIR/numbuf/build
cmake ..
make VERBOSE=1 -j$PARALLEL
