#!/bin/bash
#
# Installs all Azure related dependencies to run Sofaster with blob store.

CASABLANCA_VERSION="v2.10.14"

# Clones, compiles and installs Casablanca (cpprestsdk). The version
# used is that specified in CASABLANCA_VERSION above.
setup_casablanca() {
    git clone https://github.com/Microsoft/cpprestsdk.git casablanca
    cd casablanca

    git checkout tags/"$CASABLANCA_VERSION"
    mkdir build.release
    cd build.release
    cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
    ninja
    ninja install

    cd ../
    cd ../
}

AZURESTORE_VERSION="v7.0.0"

# Clones, compiles and installs the azure storage cpp library. The version
# used is that specified in AZURESTORE_VERSION above.
setup_azurestore() {
    git clone https://github.com/Azure/azure-storage-cpp.git
    cd azure-storage-cpp/Microsoft.WindowsAzure.Storage

    git checkout tags/"$AZURESTORE_VERSION"
    mkdir build.release
    cd build.release
    CXX=g++-5 cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j8
    make install

    cd ../
    cd ../
}

setup_casablanca
setup_azurestore
