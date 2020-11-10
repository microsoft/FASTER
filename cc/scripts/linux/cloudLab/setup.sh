#!/bin/bash
#
# This script sets up a CloudLab box for sofaster. Tested on Ubuntu 16.04.

# For printing output in bold
bold=$(tput bold)
normal=$(tput sgr0)

# Prints to terminal in bold font
echo_term_bold() {
    echo -e "$bold$1$normal"
}

# Performs generic Linux and CloudLab setup
setup_cloudlab() {
    echo_term_bold "================ Running basic setup ================"

    # Install Linux dependencies
    sudo apt update
    sudo apt install --assume-yes cmake libaio-dev libtbb-dev openjdk-8-jdk \
        xorg openbox htop libboost-all-dev libjemalloc-dev

    # Setup the vim editor
    cp /proj/sandstorm-PG0/sofaster/vimrc-sample ~/.vimrc
    cp -r /proj/sandstorm-PG0/sofaster/vim ~/.vim
    vim +PlugClean +PlugInstall +qall

    # Set permissions on /scratch so that sofaster can access it
    sudo chmod 777 /scratch

    # Create a directory for the hybrid log.
    mkdir -p /scratch/storage
}

# Performs SoFASTER specific setup
setup_sofaster() {
    echo_term_bold "================ Running sofaster setup ============="

    # Clone SoFASTER if needed
    if [[ ! -d ~/SoFASTER ]]
    then
        git clone https://github.com/badrishc/SoFASTER.git
    fi

    # Setup directory for builds if required
    mkdir -p SoFASTER/cc/build/Debug
    mkdir -p SoFASTER/cc/build/Release

    # Setup Mellanox OFED for Infrc
    ./SoFASTER/scripts/cloudLab/mlnx.sh

    # Setup Debug build
    pushd SoFASTER/cc/build/Debug >> /dev/null
    cmake -DCMAKE_BUILD_TYPE=Debug ../..
    popd >> /dev/null

    # Setup Release build
    pushd SoFASTER/cc/build/Release >> /dev/null
    cmake -DCMAKE_BUILD_TYPE=Release ../..
    popd >> /dev/null
}

# Run setup
setup_cloudlab
setup_sofaster
