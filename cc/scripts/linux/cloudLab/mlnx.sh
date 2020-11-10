#!/bin/bash
#
# This script sets up a Mellanox NIC on a CloudLab box for sofaster.

# For printing output in bold
bold=$(tput bold)
normal=$(tput sgr0)

# Prints to terminal in bold font
echo_term_bold() {
    echo -e "$bold$1$normal"
}

# Downloads and installs Mellanox OFED.
download_install() {
    echo_term_bold "============= Downloading and Installing OFED ============="

    # Version, distribution, and architecture to design for.
    LINK="http://www.mellanox.com/downloads/ofed"
    VERS="4.7-1.0.0.1"
    DIST="ubuntu16.04"
    ARCH="x86_64"

    sudo wget $LINK/MLNX_OFED-$VERS/MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz
    sudo tar -zxvf MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz

    # Install the OFED, avoid the firmware update.
    cd MLNX_OFED_LINUX-$VERS-$DIST-$ARCH
    sudo ./mlnxofedinstall --without-fw-update --force

    # Restart drivers.
    sudo /etc/init.d/openibd restart
}

# Sets up IPoIB over a Mellanox infiniband port.
setup_ipoib() {
    echo_term_bold "============= Performing IPoIB Setup ======================"

    let host=0+$(echo $HOSTNAME | cut -c 9-10)

    sudo ifconfig ib0 192.168.0.$host
}

# Install Mellanox OFED and setup IPoIB.
download_install
setup_ipoib
