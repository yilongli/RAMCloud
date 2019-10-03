#!/bin/bash

# Utility script that automates the process of fetching the latest psm2 source,
# configuring its compilation options, and building the libraries.

# Download the latest stable release.
LIBPSM2_DIR="opa-psm2"

# Create the deps directory.
mkdir -p deps
if [ ! -d ./deps/${LIBPSM2_DIR} ]; then
  cd deps
  git clone --depth 1 git@github.com:intel/opa-psm2.git
  cd ..
fi
ln -sfn deps/${LIBPSM2_DIR} ${LIBPSM2_DIR}
cd ${LIBPSM2_DIR}

make clean && make -j && make install DESTDIR=`pwd`/build_release/