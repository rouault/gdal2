#!/bin/sh

set -e

sudo docker run -v `pwd`:/gdal:ro centos:centos6 /bin/bash /gdal/gdal/ci/travis/centos6/before_install_in_centos.sh
