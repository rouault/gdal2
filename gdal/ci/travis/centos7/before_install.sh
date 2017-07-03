#!/bin/sh

set -e

sudo docker run -v `pwd`:/gdal:ro centos:centos7 /bin/bash /gdal/gdal/ci/travis/centos7/before_install_in_centos.sh
