#!/bin/bash
#
# This script installs MLNX OFED for sofaster.

# For printing output in bold
bold=$(tput bold)
normal=$(tput sgr0)

# Prints to terminal in bold font
bold() {
    echo -e "$bold$1$normal"
}

# Downloads and installs Mellanox OFED.
download_install() {
    bold "============= Downloading and Installing OFED ============="

    # Version, distribution, and architecture to install for.
    LINK="http://www.mellanox.com/downloads/ofed"
    VERS="4.7-1.0.0.1"
    DIST="ubuntu18.04"
    ARCH="x86_64"

    wget $LINK/MLNX_OFED-$VERS/MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz
    tar -zxvf MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz

    # Install the OFED.
    cd MLNX_OFED_LINUX-$VERS-$DIST-$ARCH
    sudo ./mlnxofedinstall --force
}

# Enables IPoIB on an Azure RDMA instance.
setup_ipoib() {
    bold "============= Performing IPoIB Setup ======================"

    sudo sed -i -e 's/# OS.EnableRDMA=y/OS.EnableRDMA=y/g' /etc/waagent.conf
    sudo systemctl restart walinuxagent

    bold "============= Reboot machine to Complete IPoIB setup ======"
}

# Install Mellanox OFED and setup IPoIB.
download_install
setup_ipoib
