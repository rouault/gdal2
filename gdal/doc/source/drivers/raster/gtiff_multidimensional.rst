.. _raster.gtiff_multidimensional:

================================================================================
Multidimensional TIFF/GeoTIFF support
================================================================================

.. warning:: This is experimental work. The specification of the format might change.

The driver supports reading and writing a single multidimensional GDALMDArray
per file, and the information about the 1D array that are the indexing variables
of its dimensions. The 2 fastest varying dimensions are generally used to be
for horizontal geospatial axis, with dim N-2 being the Y/latitude axis and
dim N-1 the X/longitude axis (N being the number of dimensions). If there are
more than 2 dimensions, then there are as many TIFF IFDs as 2D slices along those
extra dimensions, that is they are

.. math::

    \prod_{i=0}^{N-3}dimSize[i]

IFD where dimSize[] contains the number of samples along each dimensions.


The index of the IFD in the IFD list is linked to the indexing of sample
as following:

- IFD 0:                   dimIdx[] = {0, 0, ..., 0, 0 }
- IFD 1:                   dimIdx[] = {0, 0, ..., 0, 1 }
- ...
- IFD dimSize[N-3]-1:      dimIdx[] = {0, 0, ..., 0, dimSize[N-3]-1 }
- IFD dimSize[N-3]:        dimIdx[] = {0, 0, ..., 1, 0 }
- ...
- IFD 2*dimSize[N-3]-2:    dimIdx[] = {0, 0, ..., 1, dimSize[N-3]-1 }
- ...
- IFD :math:`\prod_{i=0}^{N-2}dimSize[i]-1`: {dimSize[0]-1, dimSize[1]-1, ..., dimSize[N-4]-1, dimSize[N-3]-1 }

More generally, for a slice indexed by dimIdx[], the corresponding IFD index is
given with the following recursive formula:

::

    IFD_index( dimIdx[0..N-1], dimSize[0..N-1] ) =
        if N = 1, dimIdx[0]
        if N > 1, IFD_index( dimIdx[0..N-2], dimSize[0..N-2] ) * dimSize[N-1] + dimIdx[N-1]

IFD headers should be put at the beginning of the file, by increasing order of
their indices. It is recommended that the values of the TileByteCounts and
TileOffsets array are put after all the IFD headers, and before the first tile
of data.

A N-dimensional block of data contains :math:`\prod_{i=0}^{N-1}dimBlockSize[i]`
samples. The block size for the 2 fastest varying dimensions corresponds to the
TIFF 2D tiling mechanism, that is to say dimBlockSize[N-2] is equal to the value
of the TileHeight tag, and dimBlockSize[N-1].
An array contains :math:`\prod_{i=0}^{N-1}ceil(\frac{dimSize[i]}{dimBlockSize[i]})`
blocks.
The last block along each dimension should be in full, and the
:math:`{dimSize[i]}\mathbin{\%}{dimBlockSize[i]}` remaining samples will be ignored by
a reader.
For dimensions before the last 2 fastest varying one, the block size is used
typically to interleave data amongst IFD so that a reader can potentially fetch
the data for a block in a single read operation.

Let's take the example of an array of size dimSize[] = {4,512,256} and block size
dimBlockSize[] = {2,256,256}.
This will define 4 IFDs, each with 2 256x256 tiles. There will be 2*2*1 blocks.
The first block corresponds to samples indexed by {0..1,0..255,0..255}. Its data
is split in 2 tiles: one tile of index (0,0) for IFD 0 and one tile of index (0,0)
for IFD 1. To take into accout the block size hint, a writer should write the
data of those 2 blocks in a consecutive way in the file.
The second block corresponds to samples indexed by {0..1,256..511,0..255}. Its data
is split in 2 tiles: one tile of index (1,0) for IFD 0 and one tile of index (1,0)
for IFD 1, that should be written in a consecutive way in the file.
The third block corresponds to samples indexed by {2..3,0..255,0..255}. Its data
is split in 2 tiles: one tile of index (0,0) for IFD 2 and one tile of index (0,0)
for IFD 3, that should be written in a consecutive way in the file.
The fourth and last block corresponds to samples indexed by {2..3,256..511,0..255}. Its data
is split in 2 tiles: one tile of index (1,0) for IFD 2 and one tile of index (1,0)
for IFD 3, that should be written in a consecutive way in the file.


The following restrictions currently apply for multidimensional TIFF/GeoTIFF files:

* Only the following combinations of values for the BitsPerSample and SampleFormat tags are allowed:

      =============    =============   ==============
      BitsPerSample    SampleFormat    GDAL data type
      =============    =============   ==============
      8                UInt            Byte
      16               UInt            UInt16
      16               Int             Int16
      32               UInt            UInt32
      32               Int             Int32
      32               IEEEFP          Float32
      64               IEEEFP          Float64
      32               ComplexInt      CInt16
      64               ComplexInt      CInt32
      64               ComplexIEEEFP   CFloat32
      128              ComplexIEEEFP   CFloat64
      =============    =============   ==============

  That is to say, that non power-of-two BitsPerSample are not supported currently,
  contrary to the 2D classic GTiff driver.

* Only SamplesPerPixel = 1 is supported.

  .. note:: The extension to SamplesPerPixel > 1 could likely be done by using
            a compound data type. This would be restricted however to components
            of the same data type.

* Only tiled TIFF files are supported.

* When an array has more than 2 dimensions, each IFD representing a 2D slice must
  have the same characteristics, that is use the same values for the tags
  ImageWidth, ImageHeight, TileWidth, TileHeight, SamplesPerPixel, BitsPerSample,
  SampleFormat, PlanarConfig, Compression (and tags related to compression)

The NoData value, when present, should be encoded in the
`GDAL_NODATA <https://www.awaresystems.be/imaging/tiff/tifftags/gdal_nodata.html>`_ tag.

Information on the multidimensional structure should be encoded in the
`GDAL_METADATA <https://www.awaresystems.be/imaging/tiff/tifftags/gdal_metadata.html>`_ tag.
This is a set of key/value pairs, where the key is encoded as the ``name`` attribute
of a ``Item`` element, and the value as the text child node of that ``Item`` element

IFD may contain at least the following items ({i} must be evaluated
for each integer in the [0,N-1] range):

* ``VARIABLE_NAME``: String: name of the array.
* ``DIMENSION_{i}_NAME``: String: name of the i(th) dimension
* ``DIMENSION_{i}_SIZE``: integer: size of the i(th) dimension, that is number
  of samples along that dimension. Must be greater or equal to 1. For the 2
  fastest varying dimensions, the value must be consistent with the ImageHeight
  and ImageWidth TIFF tags.
* ``DIMENSION_{i}_BLOCK_SIZE``: Integer: size in number of samples of a block
  along the i(th) dimension. Must be in the [1,dimSize(i)] range. For the 2
  fastest varying dimensions, the value must be consistent with the TileHeight
  and TileWidth TIFF tags.
* ``DIMENSION_{i}_TYPE`` (except for the 2 fastest varying dimensions) :
  String: type of the axis of the i(th) dimension. Predefined values
  are ``HORIZONTAL_X``, ``HORIZONTAL_Y``, ``VERTICAL``, ``TEMPORAL``, ``PARAMETRIC``.
  Other values might be used. Empty string value means unknown.
* ``DIMENSION_{i}_DIRECTION`` (except for the 2 fastest varying dimensions):
  String: direction of the axis of the i(th) dimension. Predefined values
  are ``EAST``, ``WEST``, ``SOUTH``, ``NORTH``, ``UP``, ``DOWN``, ``FUTURE``, ``PAST``.
  Other values might be used. Empty string value means unknown.
* ``DIMENSION_{i}_DATATYPE``: (except for the 2 fastest varying dimensions)
  Enumerated value among ``Byte``, ``UInt16``, ``Int16``,
  ``UInt32``, ``Int32``, ``Float32``, ``Float64`` and ``String``. Represents the data type
  of values in the ``DIMENSION_{i}_VALUES`` item.
* ``DIMENSION_{i}_VALUES`` (except for the 2 fastest varying dimensions):
  comma-separated list of values, representing the values
  along the axis indexing the i(th) dimension. There should be as many values as
  the value of ``DIMENSION_{i}_SIZE``.
* ``DIMENSION_{i}_IDX`` (except for the 2 fastest varying dimensions): integer:
  index of the samples present in that IFD format
  i(th) dimension. Must be in the [0,dimSize(i)-1] range. The value of that
  item is linked to the index of the IFD with the above IFD_index() formula.
  For the first IFD, ``DIMENSION_{i}_IDX`` always evaluate to 0.

For the first IFD, the following items are required: ``VARIABLE_NAME``,
``DIMENSION_{i}_NAME``, ``DIMENSION_{i}_SIZE``, ``DIMENSION_{i}_BLOCK_SIZE``,
``DIMENSION_{i}_IDX`` (except for the 2 fastest varying dimensions).

The other mentionned above items (``DIMENSION_{i}_TYPE``, ``DIMENSION_{i}_DIRECTION``n
``DIMENSION_{i}_DATATYPE``, ``DIMENSION_{i}_VALUES``) should generally be provided
for a complete description of the non-2D dimensions.

For all IFDs, the following items are required: ``VARIABLE_NAME``,
``DIMENSION_{i}_NAME``, ``DIMENSION_{i}_IDX``. Other items may be present, but
if so, should be consistent with the values given in the first IFD.

With those rules, a reader should be able by reading only the first IFD to infer
the characteristics of the array described in the TIFF file.

Example of metadata for the first IFD of a 3D array:

.. code-block:: xml

    <GDALMetadata>
      <Item name="VARIABLE_NAME">myarray</Item>
      <Item name="DIMENSION_0_NAME">dimZ</Item>
      <Item name="DIMENSION_0_SIZE">5</Item>
      <Item name="DIMENSION_0_BLOCK_SIZE">2</Item>
      <Item name="DIMENSION_0_TYPE">a</Item>
      <Item name="DIMENSION_0_DIRECTION">b</Item>
      <Item name="DIMENSION_0_IDX">0</Item>
      <Item name="DIMENSION_0_DATATYPE">Int32</Item>
      <Item name="DIMENSION_0_VALUES">1,2,3,4,5</Item>
      <Item name="DIMENSION_0_VAL">1</Item>
      <Item name="DIMENSION_1_NAME">dimY</Item>
      <Item name="DIMENSION_1_SIZE">257</Item>
      <Item name="DIMENSION_1_BLOCK_SIZE">256</Item>
      <Item name="DIMENSION_2_NAME">dimX</Item>
      <Item name="DIMENSION_2_SIZE">280</Item>
      <Item name="DIMENSION_2_BLOCK_SIZE">256</Item>
    </GDALMetadata>


Example of metadata for the second IFD of a 3D array:

.. code-block:: xml

    <GDALMetadata>
        <Item name="VARIABLE_NAME">myarray</Item>
        <Item name="DIMENSION_0_NAME">dimZ</Item>
        <Item name="DIMENSION_0_IDX">1</Item>
        <Item name="DIMENSION_0_VAL">2</Item>
    </GDALMetadata>

The GeoTIFF tags may be used to encode georeferencing of the horizontal dimensions.
