#!/bin/bash

if [ ! -d ./deps/mtcp ];
then
    cd deps;
    git clone --depth 1 https://github.com/yilongli/mtcp.git
    cd ..
fi

cd deps/mtcp
cp dpdk-16.11/mk/rte.app.mk ../../dpdk/mk
cp dpdk-16.11/mk/rte.cpuflags.mk ../../dpdk/mk
./configure --with-dpdk-lib=`echo $PWD`/../../dpdk/x86_64-native-linuxapp-gcc/ CFLAGS="-DMAX_CPUS=8"
cd -

#MLNX_DPDK=y scripts/dpdkBuild.sh
cd deps/mtcp; make clean; make -j8; cd -
ln -sfn deps/mtcp/mtcp mtcp
