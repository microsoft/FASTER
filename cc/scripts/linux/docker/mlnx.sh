# This script installs MLNX OFED for sofaster.

# Downloads and installs Mellanox OFED.
download_install() {
    echo "============= Downloading and Installing OFED ============="

    # Version, distribution, and architecture to install for.
    LINK="http://www.mellanox.com/downloads/ofed"
    VERS="4.7-1.0.0.1"
    DIST="ubuntu18.04"
    ARCH="x86_64"

    wget $LINK/MLNX_OFED-$VERS/MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz
    tar -zxvf MLNX_OFED_LINUX-$VERS-$DIST-$ARCH.tgz

    # Install the OFED, avoid the firmware update.
    cd MLNX_OFED_LINUX-$VERS-$DIST-$ARCH
    ./mlnxofedinstall --without-fw-update --user-space-only --force -q
}

download_install
