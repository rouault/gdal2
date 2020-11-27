#!/bin/sh

set -e

apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    docker.io sudo wget tzdata iproute2

docker images
df -k
docker rmi node:10 || /bin/true
docker rmi node:10-alpine || /bin/true
docker rmi node:12 || /bin/true
docker rmi buildpack-deps:stretch || /bin/true
docker rmi buildpack-deps:buster || /bin/true
docker rmi jekyll/builder:latest || /bin/true
df -k

TRAVIS=yes
export TRAVIS

LANG=en_US.UTF-8
export LANG
apt-get install -y locales && \
    echo "$LANG UTF-8" > /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=$LANG

IP=$(ip route show 0.0.0.0/0 dev eth0 | cut -d\  -f3)
export IP
echo "External IP: $IP"

USER=root
export USER

cd "$WORK_DIR"

if test -f "$WORK_DIR/ccache.tar.gz"; then
    echo "Restoring ccache..."
    (cd $HOME && tar xzf "$WORK_DIR/ccache.tar.gz")
fi

.github/workflows/ubuntu_18.04/before_install.sh
.github/workflows/ubuntu_18.04/install.sh
.github/workflows/ubuntu_18.04/script.sh

echo "Saving ccache..."
rm -f "$WORK_DIR/ccache.tar.gz"
(cd $HOME && tar czf "$WORK_DIR/ccache.tar.gz" .ccache)

mkdir -p coverage
lcov --no-external --capture --directory gdal --output-file coverage/lcov.info
