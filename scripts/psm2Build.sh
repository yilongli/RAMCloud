#!/bin/bash

# Utility script that automates the process of fetching a stable opa-psm2
# release, configuring its compilation options, and building the libraries.

# Create the deps directory.
PSM2_DIR="opa-psm2"
mkdir -p deps
if [ ! -d ./deps/${PSM2_DIR} ]; then
  cd deps
  git clone --depth 1 https://github.com/intel/opa-psm2
  cd ..
fi
ln -sfn deps/${PSM2_DIR} opa-psm2
cd opa-psm2

# Build and install libpsm2 under directory build_release/usr/.
make clean && make -j && make DESTDIR="$(pwd)/build_release" install