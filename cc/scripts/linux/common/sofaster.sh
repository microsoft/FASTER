#!/bin/bash
# This script sets up Makefiles for SoFASTER.
#
# This script optionally takes in 3 arguments:
#    - A flag (ON/OFF) indicating whether to compile with Infiniband.
#      ON compiles Infiniband, OFF compiles TCP.
#    - An integer specifying the value sizes to use. Passing in 8 uses
#      64 bit atomic values. Others use fine grained locks for synchronization.
#    - A flag (ON/OFF) indication whether to compile with Azure blob store.
#      ON compiles Azure blobs, OFF compiles a local filesystem.

# By default, setup makefile for TCP with 8 byte values without blob store.
INFRC=OFF
VALUE=8
AZURE=OFF

# Parse command line arguments if any. Assume that the first argument is the
# networking layer to use, the second argument is the value size and the
# third is the persistent layer (local disk/SSD or Azure blob storage).
if [ $# -ge 1 ]
then
    INFRC=$1
fi

if [ $# -ge 2 ]
then
    VALUE=$2
fi

if [ $# -ge 3 ]
then
    AZURE=$3
fi

# Sets up a Debug or Release version of SoFASTER.
setup_sofaster() {
    echo "= Running "$1" setup (INFRC="$INFRC", VALUE="$VALUE", BLOBS="$AZURE")"

    # The directory that builds will be performed under.
    dir="cc/build/$1"

    # Setup directory for builds if required.
    mkdir -p "$dir"

    # Create makefiles and compile if needed.
    pushd "$dir" >> /dev/null
    cmake -DCMAKE_BUILD_TYPE="$1" -DENABLE_INFINIBAND="$INFRC" \
          -DVALUE_SIZE="$VALUE" -DUSE_AZURE="$AZURE" ../..

    popd >> /dev/null
}

setup_sofaster "Debug"
setup_sofaster "Release"
