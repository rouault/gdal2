#!/usr/bin/env bash

set -e

NPROC=$(($(nproc)+1))

echo "" > /etc/yum.repos.d/epel.repo
echo "[epel]" >> /etc/yum.repos.d/epel.repo
echo "name=Extra Packages for Enterprise Linux 7 - \$basearch" >> /etc/yum.repos.d/epel.repo
echo "mirrorlist=https://mirrors.fedoraproject.org/metalink?repo=epel-7&arch=\$basearch" >> /etc/yum.repos.d/epel.repo
echo "failovermethod=priority" >> /etc/yum.repos.d/epel.repo
echo "enabled=1" >> /etc/yum.repos.d/epel.repo
echo "gpgcheck=0" >> /etc/yum.repos.d/epel.repo

yum install -y geos-devel wget gcc-c++ make expat-devel curl-devel sqlite-devel jasper-devel libpng-devel python-devel xerces-c-devel libtiff-devel poppler-cpp-devel libwebp-devel giflib-devel numpy

echo "Copy /gdal to /tmp"
mkdir /tmp/gdal
cp -r /gdal/gdal /tmp/gdal
cp -r /gdal/autotest /tmp/gdal

cd /tmp
wget http://download.osgeo.org/proj/proj-4.9.3.tar.gz
tar xzf proj-4.9.3.tar.gz
cd proj-4.9.3
cd nad
wget http://download.osgeo.org/proj/proj-datumgrid-1.5.tar.gz
tar xvzf proj-datumgrid-1.5.tar.gz
cd ..
./configure --prefix=/usr
make -j${NPROC}
make install
ldconfig

cd /tmp/gdal/gdal
./configure --without-libtool --enable-debug --with-poppler --with-static-proj4
make -j${NPROC} USER_DEFS="-Werror"
cd apps
make test_ogrsf -s USER_DEFS="-Werror"
cd ..
cd swig/python
python setup.py build
cd ../..

export PATH=$PWD/apps:$PATH
export LD_LIBRARY_PATH=$PWD
export GDAL_DATA=$PWD/data
export PYTHONPATH=$PWD/swig/python/build/lib.linux-x86_64-2.7

cd ../autotest
mv ogr/ogr_gft.py ogr/ogr_gft.py.dis
python run_all.py
