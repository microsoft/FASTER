#!/bin/bash
#
# Installs linux packages required to compile and run SoFASTER.

apt update && \
    apt install --assume-yes cmake libaio-dev libtbb-dev uuid-dev \
    htop libboost-all-dev libjemalloc-dev wget lsb-release git g++ \
    libboost-atomic-dev libboost-thread-dev libboost-system-dev \
    libboost-date-time-dev libboost-regex-dev libboost-filesystem-dev \
    libboost-random-dev libboost-chrono-dev libboost-serialization-dev \
    libwebsocketpp-dev openssl libssl-dev ninja-build libxml2-dev g++-5 \
    net-tools vim python-pip gdb

pip install psutil numpy
