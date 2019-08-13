#!/bin/bash

# Utility script that automates the process of fetching a stable libfabric
# release, configuring its compilation options, and building the libraries.

# Uncomment the following line to build libfabric with debug symbols.
# EXTRA_CFLAGS="-g -O0"

# Download the latest stable release.
LIBFABRIC_VER="1.8.0"
LIBFABRIC_SRC="https://github.com/ofiwg/libfabric/releases/download/v${LIBFABRIC_VER}/libfabric-${LIBFABRIC_VER}.tar.bz2"
LIBFABRIC_DIR="libfabric-${LIBFABRIC_VER}"

# Create the deps directory.
mkdir -p deps
if [ ! -d ./deps/${LIBFABRIC_DIR} ]; then
  cd deps
  wget --no-clobber ${LIBFABRIC_SRC}
  tar xvf libfabric-${LIBFABRIC_VER}.tar.bz2
  cd ..
fi
ln -sfn deps/${LIBFABRIC_DIR} libfabric
cd libfabric

# Manually disable useless providers (e.g., udp, tcp, etc.), build libfabric,
# and install all the artifacts (i.e., header files, library objects, and
# utility programs) under directory ${LIBFABRIC_DIR}/build.
#./autogen.sh
mkdir -p build && cd build
CFLAGS=${EXTRA_CFLAGS} ../configure --prefix="$(pwd)" --disable-sockets \
    --disable-tcp --disable-udp --disable-psm
make clean && make -j && make install