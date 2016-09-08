#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############################################################################
# $Id$
#
# Project:  GDAL/OGR Test Suite
# Purpose:  GMLAS driver testing.
# Author:   Even Rouault, <even dot rouault at spatialys dot com>
#
# Initial developement funded by the European Environment Agency
#
#******************************************************************************
# Copyright (c) 2016, Even Rouault, <even dot rouault at spatialys dot com>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
###############################################################################

import os
import sys

sys.path.append( '../pymod' )

import gdaltest
from osgeo import ogr

###############################################################################
#

def ogr_gmlas_1():

    if ogr.GetDriverByName('GMLAS') is None:
        return 'skip'

    ds = ogr.Open('GMLAS:data/gmlas_test1.xml')
    if ds is None:
        gdaltest.post_reason('fail')
        return 'fail'
    ds = None

    import test_cli_utilities

    if test_cli_utilities.get_ogrinfo_path() is None:
        return 'skip'

    ret = gdaltest.runexternal(test_cli_utilities.get_ogrinfo_path() + ' -ro -al GMLAS:data/gmlas_test1.xml')
    ret = ret.replace('\r\n', '\n')
    expected = open('data/gmlas_test1.txt', 'rb').read().decode('utf-8')
    expected = ret.replace('\r\n', '\n')
    if ret != expected:
        gdaltest.post_reason('fail')
        print('Got:')
        print(ret)
        open('tmp/ogr_gmlas_1.txt', 'wb').write(ret.encode('utf-8'))
        print('Diff:')
        os.system('diff -u data/gmlas_test1.txt tmp/ogr_gmlas_1.txt')
        os.unlink('tmp/ogr_gmlas_1.txt')
        return 'fail'

    return 'success'

###############################################################################
#  Cleanup

def ogr_gmlas_cleanup():

    if ogr.GetDriverByName('GMLAS') is None:
        return 'skip'

    return 'success'


gdaltest_list = [
    ogr_gmlas_1,
    ogr_gmlas_cleanup ]

if __name__ == '__main__':

    gdaltest.setup_run( 'ogr_gmlas' )

    gdaltest.run_tests( gdaltest_list )

    gdaltest.summarize()
