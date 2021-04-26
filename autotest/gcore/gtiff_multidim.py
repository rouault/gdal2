#!/usr/bin/env pytest
###############################################################################
# $Id$
#
# Project:  GDAL/OGR Test Suite
# Purpose:  Test multidimensional support in GTiff driver
# Author:   Even Rouault <even.rouault@spatialys.com>
#
###############################################################################
# Copyright (c) 2021, Even Rouault <even.rouault@spatialys.com>
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

import array
import struct

from osgeo import gdal
from osgeo import osr

import gdaltest
import pytest

# All tests will be skipped if numpy is unavailable
numpy = pytest.importorskip('numpy')


def _test_basic_2D(blockSize, arraySize, dt):

    filename = '/vsimem/mdim.tif'

    def write():
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimY = rg.CreateDimension("dimY", None, None, arraySize[0])
        dimX = rg.CreateDimension("dimX", None, None, arraySize[1])

        ar = rg.CreateMDArray("myarray", [dimY, dimX],
                              gdal.ExtendedDataType.Create(dt),
                              ['BLOCKSIZE=%d,%d' % (blockSize[0], blockSize[1])])
        assert ar.GetBlockSize() == [x for x in blockSize]
        numpy_ar = numpy.reshape(numpy.arange(0, dimY.GetSize() * dimX.GetSize(), dtype=numpy.uint16),
                                 (dimY.GetSize(), dimX.GetSize()))
        if dt == gdal.GDT_Byte:
            numpy_ar = numpy.clip(numpy_ar, 0, 255)
        assert ar.Write(numpy_ar) == gdal.CE_None
        return numpy_ar

    ref_ar = write()

    ds = gdal.Open(filename)
    assert ds.GetRasterBand(1).XSize == arraySize[1]
    assert ds.GetRasterBand(1).YSize == arraySize[0]
    assert ds.GetRasterBand(1).DataType == dt
    assert ds.GetRasterBand(1).GetBlockSize() == [blockSize[1], blockSize[0]]
    assert numpy.array_equal(ds.ReadAsArray(), ref_ar)
    assert ds.GetMetadata() == {
        'DIMENSION_0_BLOCK_SIZE': '%d' % blockSize[0],
        'DIMENSION_0_NAME': 'dimY',
        'DIMENSION_0_SIZE': '%d' % arraySize[0],
        'DIMENSION_1_BLOCK_SIZE': '%d' % blockSize[1],
        'DIMENSION_1_NAME': 'dimX',
        'DIMENSION_1_SIZE': '%d' % arraySize[1],
        'VARIABLE_NAME': 'myarray'}
    assert ds.GetSpatialRef() is None
    assert ds.GetGeoTransform(can_return_null=True) is None
    ds = None

    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert ar.GetDataType() == gdal.ExtendedDataType.Create(dt)
    assert [x.GetSize() for x in ar.GetDimensions()] == [x for x in arraySize]
    assert ar.GetBlockSize() == [x for x in blockSize]
    assert ar.GetSpatialRef() is None
    assert numpy.array_equal(ar.ReadAsArray(), ref_ar)

    assert numpy.array_equal(ar[2,3].ReadAsArray(), ref_ar[2,3])

    count_y = 2
    count_x = 4
    step_y = 1
    step_x = 1
    start_y = min(blockSize[0] - 2, arraySize[0] - count_y * step_y)
    start_x = min(blockSize[1] - 3, arraySize[1] - count_x * step_x)
    assert numpy.array_equal(ar.ReadAsArray([start_y, start_x],
                                            [count_y, count_x],
                                            [step_y, step_x]),
                             ref_ar[start_y:start_y+count_y*step_y:step_y,
                                    start_x:start_x+count_x*step_x:step_x])

    count_y = 2
    count_x = 4
    step_y = 3
    step_x = 2
    start_y = min(blockSize[0] - 2, arraySize[0] - count_y * step_y)
    start_x = min(blockSize[1] - 3, arraySize[1] - count_x * step_x)
    assert numpy.array_equal(ar.ReadAsArray([start_y, start_x],
                                            [count_y, count_x],
                                            [step_y, step_x]),
                             ref_ar[start_y:start_y+count_y*step_y:step_y,
                                    start_x:start_x+count_x*step_x:step_x])

    count_y = 2
    count_x = 4
    step_y = -2
    step_x = -3
    start_y = arraySize[0] - 1
    start_x = arraySize[1] - 1
    assert numpy.array_equal(ar.ReadAsArray([start_y, start_x],
                                            [count_y, count_x],
                                            [step_y, step_x]),
                             ref_ar[start_y:start_y+count_y*step_y:step_y,
                                    start_x:start_x+count_x*step_x:step_x])

    gdal.Unlink(filename)


@pytest.mark.parametrize("blockSize,arraySize", [[(16, 32), (32, 64)],  # raster size exact multiple of block size
                                                 # just one truncated block
                                                 [(16, 32), (15, 31)],
                                                 # three truncated blocks
                                                 [(16, 32), (33, 65)],
                                                 ])
def test_gtiff_mdim_basic_2D_blocksize(blockSize, arraySize):
    _test_basic_2D(blockSize, arraySize, gdal.GDT_UInt16)


@pytest.mark.parametrize("datatype", [gdal.GDT_Byte,
                                      gdal.GDT_Int16,
                                      gdal.GDT_UInt16,
                                      gdal.GDT_Int32,
                                      gdal.GDT_UInt32,
                                      gdal.GDT_Float32,
                                      gdal.GDT_Float64,
                                      gdal.GDT_CInt16,
                                      gdal.GDT_CInt32,
                                      gdal.GDT_CFloat32,
                                      gdal.GDT_CFloat64, ], ids=gdal.GetDataTypeName)
def test_gtiff_mdim_basic_2D_datatype(datatype):
    _test_basic_2D((16, 32), (16, 32), datatype)


@pytest.mark.parametrize("blockSize,arraySize", [[(1, 16, 32), (1, 32, 64)],
                                                 [(1, 16, 32), (2, 32, 64)],
                                                 [(2, 16, 32), (2, 32, 64)],
                                                 [(2, 16, 32), (5, 32, 64)],
                                                 [(3, 16, 32), (2, 32, 64)]])
def test_gtiff_mdim_basic_3D(blockSize, arraySize):

    filename = '/vsimem/mdim.tif'
    dt = gdal.GDT_UInt16
    z_values = [i + 1 for i in range(arraySize[0])]

    def write():
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimZ = rg.CreateDimension("dimZ", 'a', 'b', arraySize[0])
        dimY = rg.CreateDimension("dimY", None, None, arraySize[1])
        dimX = rg.CreateDimension("dimX", None, None, arraySize[2])

        dimZVar = rg.CreateMDArray("dimZ", [dimZ], gdal.ExtendedDataType.Create(
            gdal.GDT_Int32), ['IS_INDEXING_VARIABLE=YES'])
        dimZ.SetIndexingVariable(dimZVar)
        dimZVar.Write(array.array('f', z_values))

        ar = rg.CreateMDArray("myarray", [dimZ, dimY, dimX],
                              gdal.ExtendedDataType.Create(dt),
                              ['BLOCKSIZE=%d,%d,%d' % (blockSize[0], blockSize[1], blockSize[2])])
        numpy_ar = numpy.reshape(numpy.arange(0, dimZ.GetSize() * dimY.GetSize() * dimX.GetSize(), dtype=numpy.uint16),
                                 (dimZ.GetSize(), dimY.GetSize(), dimX.GetSize()))
        if dt == gdal.GDT_Byte:
            numpy_ar = numpy.clip(numpy_ar, 0, 255)
        assert ar.Write(numpy_ar) == gdal.CE_None
        return numpy_ar

    ref_ar = write()

    # Iterate over IFDs
    for idx in range(arraySize[0]):
        ds = gdal.Open('GTIFF_DIR:%d:%s' % (idx+1, filename))
        assert ds.GetRasterBand(1).XSize == arraySize[-1]
        assert ds.GetRasterBand(1).YSize == arraySize[-2]
        assert ds.GetRasterBand(1).DataType == dt
        assert ds.GetRasterBand(1).GetBlockSize() == [
            blockSize[-1], blockSize[-2]]
        assert numpy.array_equal(ds.ReadAsArray(), ref_ar[idx])
        # Minimum details beyond first IFD
        expected_md = {
            'DIMENSION_0_NAME': 'dimZ',
            'DIMENSION_0_IDX': '%d' % idx,
            'DIMENSION_0_VAL': '%d' % z_values[idx],
            'VARIABLE_NAME': 'myarray'
        }
        if idx == 0:
            # Full details for first IFD
            expected_md.update({
                'DIMENSION_0_BLOCK_SIZE': '%d' % blockSize[0],
                'DIMENSION_0_DATATYPE': 'Int32',
                'DIMENSION_0_DIRECTION': 'b',
                'DIMENSION_0_SIZE': '%d' % arraySize[0],
                'DIMENSION_0_TYPE': 'a',
                'DIMENSION_0_VALUES': ','.join(str(v) for v in z_values),
                'DIMENSION_1_BLOCK_SIZE': '%d' % blockSize[1],
                'DIMENSION_1_NAME': 'dimY',
                'DIMENSION_1_SIZE': '%d' % arraySize[1],
                'DIMENSION_2_BLOCK_SIZE': '%d' % blockSize[2],
                'DIMENSION_2_NAME': 'dimX',
                'DIMENSION_2_SIZE': '%d' % arraySize[2]})
        assert ds.GetMetadata() == expected_md

    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert ar.GetDataType() == gdal.ExtendedDataType.Create(dt)
    assert [x.GetSize() for x in ar.GetDimensions()] == [x for x in arraySize]
    assert ar.GetBlockSize() == [x for x in blockSize]
    assert ar.GetSpatialRef() is None
    dims = ar.GetDimensions()
    assert dims[0].GetName() == 'dimZ'
    assert dims[0].GetType() == 'a'
    assert dims[0].GetDirection() == 'b'
    indexingVar = dims[0].GetIndexingVariable()
    assert indexingVar is not None
    assert indexingVar.GetDataType() == gdal.ExtendedDataType.Create(gdal.GDT_Int32)
    assert [x for x in struct.unpack('i' * arraySize[0], indexingVar.Read())] == z_values
    assert numpy.array_equal(ar.ReadAsArray(), ref_ar)

    gdal.Unlink(filename)


@pytest.mark.parametrize("blockSize,arraySize", [  # [(1, 1, 16, 32), (1, 1, 32, 64)],
    [(2, 3, 16, 32), (5, 8, 32, 64)],
])
def test_gtiff_mdim_basic_4D(blockSize, arraySize):

    filename = '/vsimem/mdim.tif'
    dt = gdal.GDT_UInt16
    t_values = [i + 1 for i in range(arraySize[0])]
    z_values = [i + 1 for i in range(arraySize[1])]

    def write():
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimT = rg.CreateDimension("dimT", None, None, arraySize[0])
        dimZ = rg.CreateDimension("dimZ", 'a', 'b', arraySize[1])
        dimY = rg.CreateDimension("dimY", None, None, arraySize[2])
        dimX = rg.CreateDimension("dimX", None, None, arraySize[3])

        dimTVar = rg.CreateMDArray("dimT", [dimT], gdal.ExtendedDataType.Create(
            gdal.GDT_Int32), ['IS_INDEXING_VARIABLE=YES'])
        dimT.SetIndexingVariable(dimTVar)
        dimTVar.Write(array.array('f', t_values))

        dimZVar = rg.CreateMDArray("dimZ", [dimZ], gdal.ExtendedDataType.Create(
            gdal.GDT_Int32), ['IS_INDEXING_VARIABLE=YES'])
        dimZ.SetIndexingVariable(dimZVar)
        dimZVar.Write(array.array('f', z_values))

        ar = rg.CreateMDArray("myarray", [dimT, dimZ, dimY, dimX],
                              gdal.ExtendedDataType.Create(dt),
                              ['BLOCKSIZE=%d,%d,%d,%d' % (blockSize[0], blockSize[1], blockSize[2], blockSize[3])])
        numpy_ar = numpy.reshape(numpy.arange(0, dimT.GetSize() * dimZ.GetSize() * dimY.GetSize() * dimX.GetSize(), dtype=numpy.uint16),
                                 (dimT.GetSize(), dimZ.GetSize(), dimY.GetSize(), dimX.GetSize()))
        if dt == gdal.GDT_Byte:
            numpy_ar = numpy.clip(numpy_ar, 0, 255)
        assert ar.Write(numpy_ar) == gdal.CE_None
        return numpy_ar

    ref_ar = write()

    # Iterate over IFDs
    for idx_t in range(arraySize[0]):
        for idx_z in range(arraySize[1]):
            idx = idx_t * arraySize[1] + idx_z

            ds = gdal.Open('GTIFF_DIR:%d:%s' % (idx+1, filename))
            assert ds.GetRasterBand(1).XSize == arraySize[-1]
            assert ds.GetRasterBand(1).YSize == arraySize[-2]
            assert ds.GetRasterBand(1).DataType == dt
            assert ds.GetRasterBand(1).GetBlockSize() == [
                blockSize[-1], blockSize[-2]]
            assert numpy.array_equal(ds.ReadAsArray(), ref_ar[idx_t][idx_z])
            # Minimum details beyond first IFD
            expected_md = {
                'DIMENSION_0_NAME': 'dimT',
                'DIMENSION_0_IDX': '%d' % idx_t,
                'DIMENSION_0_VAL': '%d' % t_values[idx_t],

                'DIMENSION_1_NAME': 'dimZ',
                'DIMENSION_1_IDX': '%d' % idx_z,
                'DIMENSION_1_VAL': '%d' % z_values[idx_z],

                'VARIABLE_NAME': 'myarray'
            }
            if idx == 0:
                # Full details for first IFD
                expected_md.update({
                    'DIMENSION_0_BLOCK_SIZE': '%d' % blockSize[0],
                    'DIMENSION_0_DATATYPE': 'Int32',
                    'DIMENSION_0_SIZE': '%d' % arraySize[0],
                    'DIMENSION_0_VALUES': ','.join(str(v) for v in t_values),

                    'DIMENSION_1_BLOCK_SIZE': '%d' % blockSize[1],
                    'DIMENSION_1_DATATYPE': 'Int32',
                    'DIMENSION_1_DIRECTION': 'b',
                    'DIMENSION_1_SIZE': '%d' % arraySize[1],
                    'DIMENSION_1_TYPE': 'a',
                    'DIMENSION_1_VALUES': ','.join(str(v) for v in z_values),

                    'DIMENSION_2_BLOCK_SIZE': '%d' % blockSize[2],
                    'DIMENSION_2_NAME': 'dimY',
                    'DIMENSION_2_SIZE': '%d' % arraySize[2],

                    'DIMENSION_3_BLOCK_SIZE': '%d' % blockSize[3],
                    'DIMENSION_3_NAME': 'dimX',
                    'DIMENSION_3_SIZE': '%d' % arraySize[3]})
            assert ds.GetMetadata() == expected_md


    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert numpy.array_equal(ar.ReadAsArray(), ref_ar)
    ds = None

    gdal.Unlink(filename)


def test_gtiff_mdim_array_attributes_scale_offset_nodata():

    filename = '/vsimem/mdim.tif'

    def write():
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimY = rg.CreateDimension("dimY", None, None, 1)
        dimX = rg.CreateDimension("dimX", None, None, 1)

        ar = rg.CreateMDArray("myarray", [dimY, dimX],
                              gdal.ExtendedDataType.Create(gdal.GDT_Byte))

        att = ar.CreateAttribute(
            'att_text', [], gdal.ExtendedDataType.CreateString())
        assert att
        assert att.Write('foo') == gdal.CE_None

        att = ar.CreateAttribute(
            'att_text_null', [], gdal.ExtendedDataType.CreateString())
        assert att

        att = ar.CreateAttribute(
            'att_text_multiple', [2], gdal.ExtendedDataType.CreateString())
        assert att
        assert att.Write(['foo', 'bar']) == gdal.CE_None

        att = ar.CreateAttribute(
            'att_int', [], gdal.ExtendedDataType.Create(gdal.GDT_Int32))
        assert att
        assert att.Write(123456789) == gdal.CE_None

        att = ar.CreateAttribute(
            'att_int_multiple', [2], gdal.ExtendedDataType.Create(gdal.GDT_Int32))
        assert att
        assert att.Write([123456789, 23]) == gdal.CE_None

        assert ar.SetOffset(1.25) == gdal.CE_None
        assert ar.SetScale(3.25) == gdal.CE_None
        assert ar.SetUnit('my unit') == gdal.CE_None

        assert ar.SetNoDataValueDouble(23) == gdal.CE_None

    write()

    ds = gdal.Open(filename)
    assert ds.GetMetadataItem('att_text') == 'foo'
    assert ds.GetMetadataItem('att_text_null') == 'null'
    assert ds.GetMetadataItem('att_text_multiple') == 'foo,bar'
    assert ds.GetMetadataItem('att_int') == '123456789'
    assert ds.GetMetadataItem('att_int_multiple') == '123456789,23'
    assert ds.GetRasterBand(1).GetOffset() == 1.25
    assert ds.GetRasterBand(1).GetScale() == 3.25
    assert ds.GetRasterBand(1).GetUnitType() == 'my unit'
    assert ds.GetRasterBand(1).GetNoDataValue() == 23
    ds = None

    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert ar.GetOffset() == 1.25
    assert ar.GetScale() == 3.25
    assert ar.GetUnit() == 'my unit'
    assert ar.GetNoDataValueAsDouble() == 23.0
    attrs = ar.GetAttributes()
    attr_map = {}
    for attr in attrs:
        attr_map[attr.GetName()] = attr.ReadAsString()
    assert attr_map == {
        'att_int': '123456789',
        'att_int_multiple': '123456789,23',
        'att_text': 'foo',
        'att_text_multiple': 'foo,bar',
        'att_text_null': 'null'
    }

    gdal.Unlink(filename)


@pytest.mark.parametrize("codec,options,compression_level", [('DEFLATE', ['COMPRESS=DEFLATE'], None),
                                           ('DEFLATE', ['COMPRESS=DEFLATE'], 'ZLEVEL=9'),
                                           ('LZW', ['COMPRESS=LZW'], None),
                                           ('LZW', ['COMPRESS=LZW', 'PREDICTOR=2'], None),
                                           ('ZSTD', ['COMPRESS=ZSTD'], 'ZSTD_LEVEL=16'),
                                           ('LZMA', ['COMPRESS=LZMA'], 'LZMA_PRESET=1'),
                                           ('LERC', ['COMPRESS=LERC'], None),
                                           ('LERC_DEFLATE', ['COMPRESS=LERC_DEFLATE'], 'ZLEVEL=9'),
                                           ('LERC_ZSTD', ['COMPRESS=LERC_ZSTD'], 'ZSTD_LEVEL=16'),
                                           ])
def test_gtiff_mdim_array_compression(codec, options, compression_level):

    md = gdal.GetDriverByName('GTiff').GetMetadata()
    if codec not in md['DMD_CREATIONOPTIONLIST']:
        pytest.skip('%s codec not available' % codec)

    filename = '/vsimem/mdim.tif'
    dt = gdal.GDT_Byte
    arraySize = (2, 32, 64)
    z_values = [i + 1 for i in range(arraySize[0])]

    def write(extra_options = []):
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimZ = rg.CreateDimension("dimZ", 'a', 'b', arraySize[0])
        dimY = rg.CreateDimension("dimY", None, None, arraySize[1])
        dimX = rg.CreateDimension("dimX", None, None, arraySize[2])

        dimZVar = rg.CreateMDArray("dimZ", [dimZ], gdal.ExtendedDataType.Create(
            gdal.GDT_Int32), ['IS_INDEXING_VARIABLE=YES'])
        dimZ.SetIndexingVariable(dimZVar)
        dimZVar.Write(array.array('f', z_values))

        ar = rg.CreateMDArray("myarray", [dimZ, dimY, dimX],
                              gdal.ExtendedDataType.Create(dt),
                              options + extra_options)
        numpy_ar = numpy.reshape(numpy.arange(0, dimZ.GetSize() * dimY.GetSize() * dimX.GetSize(), dtype=numpy.uint16),
                                 (dimZ.GetSize(), dimY.GetSize(), dimX.GetSize()))
        if dt == gdal.GDT_Byte:
            numpy_ar = numpy.clip(numpy_ar, 0, 255)
        assert ar.Write(numpy_ar) == gdal.CE_None

    with gdaltest.config_option('GDAL_TIFF_DEFLATE_SUBCODEC', 'ZLIB'):
        write()

    filesize = gdal.VSIStatL(filename).size

    ds = gdal.Open(filename)
    assert ds.GetMetadataItem('COMPRESSION', 'IMAGE_STRUCTURE') == codec
    if 'PREDICTOR=2' in options:
        assert ds.GetMetadataItem('PREDICTOR', 'IMAGE_STRUCTURE') == '2'
    ds = None

    if compression_level is not None:
        with gdaltest.config_option('GDAL_TIFF_DEFLATE_SUBCODEC', 'ZLIB'):
            write([compression_level])

        if codec == 'LZMA':
            # Too fragile depending on the version of ZSTD or ZLIB
            assert gdal.VSIStatL(filename).size != filesize

    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert ar.GetStructuralInfo()['COMPRESSION'] == codec, ar.GetStructuralInfo()
    if 'PREDICTOR=2' in options:
        assert ar.GetStructuralInfo()['PREDICTOR'] == '2'
    ds = None

    gdal.Unlink(filename)

@pytest.mark.parametrize("y_incr", [-0.2, 0.2])
def test_gtiff_mdim_array_geotiff_tags(y_incr):

    filename = '/vsimem/mdim.tif'
    x_incr = 0.1

    x_ar = [2 + 0.5 * x_incr + x_incr * i for i in range(10)]
    y_ar = [49 + 0.5 * y_incr + y_incr * i for i in range(5)]

    def write():
        ds = gdal.GetDriverByName('GTiff').CreateMultiDimensional(filename)
        rg = ds.GetRootGroup()
        dimY = rg.CreateDimension("dimY", None, None, 5)
        dimX = rg.CreateDimension("dimX", None, None, 10)

        dimXVar = rg.CreateMDArray("dimX", [dimX], gdal.ExtendedDataType.Create(
            gdal.GDT_Float64), ['IS_INDEXING_VARIABLE=YES'])
        dimX.SetIndexingVariable(dimXVar)
        dimXVar.Write(array.array('d', x_ar))

        dimYVar = rg.CreateMDArray("dimY", [dimY], gdal.ExtendedDataType.Create(
            gdal.GDT_Float64), ['IS_INDEXING_VARIABLE=YES'])
        dimY.SetIndexingVariable(dimYVar)
        dimYVar.Write(array.array('d', y_ar))

        ar = rg.CreateMDArray("myarray", [dimY, dimX],
                              gdal.ExtendedDataType.Create(gdal.GDT_Byte))
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        ar.SetSpatialRef(srs)


    write()

    ds = gdal.Open(filename)
    assert ds.GetSpatialRef().GetAuthorityCode(None) == '4326'
    assert ds.GetGeoTransform() == pytest.approx([2, x_incr, 0, 49, 0, y_incr])
    assert ds.GetMetadataItem('AREA_OR_POINT') == 'Point'
    ds = None

    ds = gdal.OpenEx(filename, gdal.OF_MULTIDIM_RASTER)
    rg = ds.GetRootGroup()
    ar = rg.OpenMDArray('myarray')
    assert ar.GetSpatialRef().GetAuthorityCode(None) == '4326'
    dims = ar.GetDimensions()

    assert dims[0].GetName() == 'dimY'
    assert dims[0].GetType() == 'HORIZONTAL_Y'
    assert dims[0].GetDirection() == 'NORTH'
    y_var = dims[0].GetIndexingVariable()
    assert y_var is not None
    assert [x for x in struct.unpack('d' * 5, y_var.Read())] == pytest.approx(y_ar)

    assert dims[1].GetName() == 'dimX'
    assert dims[1].GetType() == 'HORIZONTAL_X'
    assert dims[1].GetDirection() == 'EAST'
    x_var = dims[1].GetIndexingVariable()
    assert x_var is not None
    assert [x for x in struct.unpack('d' * 10, x_var.Read())] == pytest.approx(x_ar)
    ds = None

    gdal.Unlink(filename)
