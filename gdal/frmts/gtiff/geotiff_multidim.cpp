/******************************************************************************
 *
 * Project:  GeoTIFF Driver
 * Purpose:  GDAL GeoTIFF support.
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2021, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

// We need to do those includes in that order to have PRIxxx macros working
// on x86_64-w64-mingw32-g++ 7.3-win32 of ubuntu 18.04
#include <cinttypes>

#include "cpl_minixml.h"
#include "cpl_vsi.h"

#include "tiffio.h"
#include "geotiff.h"
#include "geo_normalize.h"
#include "geovalues.h"
#include "xtiffio.h"

#include "geotiff_multidim.h"
#include "memmultidim.h"
#include "gtiff.h"
#include "tifvsi.h"
#include "gt_wkt_srs.h"

#include "ogr_proj_p.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <vector>

#if TIFFLIB_VERSION > 20181110 // > 4.0.10
#define SUPPORTS_GET_OFFSET_BYTECOUNT
#endif

namespace osgeo {
namespace gdal {
namespace gtiff {

class Array;

/************************************************************************/
/*                         MultiDimSharedResource                       */
/************************************************************************/

class MultiDimSharedResources
{
    friend class MultiDimDataset;

    std::string   osFilename{};
    VSILFILE*     fpL = nullptr;
    bool          bNewFile = true;
    TIFF*         hTIFF = nullptr;
    bool          bWritable = false;
    CPLStringList aosCreationOptions{};
    std::set<Array*> oSetArrays{};

    bool Crystalize(Array* array);

public:
    ~MultiDimSharedResources();

    bool IsWritable() const { return bWritable; }
    void RegisterArray(Array* array) { oSetArrays.insert(array); }
    bool Crystalize();
    TIFF* GetTIFFHandle() const { return hTIFF; }
    bool IsNewFile() const { return bNewFile; }
    void UnsetNewFile() { bNewFile = false; }
    const CPLStringList& GetCreationOptions() const { return aosCreationOptions; }
    const std::string& GetFilename() const { return osFilename; }
};

/************************************************************************/
/*                                Group                                 */
/************************************************************************/

class Group final: public GDALGroup
{
    friend class MultiDimDataset;
    std::shared_ptr<MultiDimSharedResources> m_poShared;
    std::map<CPLString, std::shared_ptr<GDALDimension>> m_oMapDimensions{};
    std::map<CPLString, std::shared_ptr<GDALMDArray>> m_oMapMDArrays{};

public:
    Group(const std::string& osParentName, const char* pszName):
        GDALGroup(osParentName, pszName ? pszName : ""),
        m_poShared(std::make_shared<MultiDimSharedResources>())
    {}

    std::shared_ptr<GDALDimension> CreateDimension(const std::string&,
                                                   const std::string&,
                                                   const std::string&,
                                                   GUInt64,
                                                   CSLConstList papszOptions) override;

    std::vector<std::string> GetMDArrayNames(CSLConstList) const override;

    std::shared_ptr<GDALMDArray> CreateMDArray(
               const std::string& osName,
               const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
               const GDALExtendedDataType& oDataType,
               CSLConstList papszOptions) override;

};

/************************************************************************/
/*                                Dimension                             */
/************************************************************************/

class Dimension final: public GDALDimension
{
    std::weak_ptr<GDALMDArray> m_poIndexingVariable{};

public:
    Dimension(const std::string& osParentName,
                 const std::string& osName,
                 const std::string& osType,
                 const std::string& osDirection,
                 GUInt64 nSize);

    // TODO
    std::shared_ptr<GDALMDArray> GetIndexingVariable() const override { return m_poIndexingVariable.lock(); }

    bool SetIndexingVariable(std::shared_ptr<GDALMDArray> poIndexingVariable) override;
};

/************************************************************************/
/*                                Array                                 */
/************************************************************************/

class Array final: public GDALMDArray
{
    Array(const Array&) = delete;
    Array(Array &&) = delete;
    Array& operator= (const Array&) = delete;
    Array& operator= (Array &&) = delete;

    std::shared_ptr<MultiDimSharedResources> m_poShared;
    std::vector<std::shared_ptr<GDALDimension>> m_aoDims;
    GDALExtendedDataType m_oType;
    std::map<CPLString, std::shared_ptr<GDALAttribute>> m_oMapAttributes{};

    // To be sure we keep an extra reference to indexing arrays, in odd cases
    // where we won't crystalize in Array::IWrite()
    std::vector<std::shared_ptr<GDALMDArray>> m_oVectorIndexingArrays{};

    std::vector<TIFF*> m_ahTIFF{}; // size should be m_nIFDCount
    size_t                  m_nIFDCount = 0;
    std::vector<uint32_t> m_anBlockSize{}; // size should be the same as m_aoDims

    std::string m_osUnit{};
    std::shared_ptr<OGRSpatialReference> m_poSRS{};
    GByte* m_pabyNoData = nullptr;
    double m_dfScale = 1.0;
    double m_dfOffset = 0.0;
    bool m_bHasScale = false;
    bool m_bHasOffset = false;
    GDALDataType m_eOffsetStorageType = GDT_Unknown;
    GDALDataType m_eScaleStorageType = GDT_Unknown;

    int      m_nCompression = COMPRESSION_NONE;
    uint16_t m_nPredictor = PREDICTOR_NONE;

    signed char m_nZLevel = -1;
    signed char m_nLZMAPreset = -1;
    signed char m_nZSTDLevel = -1;
    double      m_dfMaxZError = 0.0;
    int         m_nLercSubcodec = LERC_ADD_COMPRESSION_NONE;

    bool Crystalize();

    std::vector<size_t> IFDIndexToDimIdx(size_t nIFDIndex) const;
    char*               GenerateMetadata(bool mainIFD, const std::vector<size_t>& anDimIdx) const;
    void                WriteGeoTIFFTags( TIFF* hTIFF );

protected:
    bool IRead(const GUInt64* arrayStartIdx,
               const size_t* count,
               const GInt64* arrayStep,
               const GPtrDiff_t* bufferStride,
               const GDALExtendedDataType& bufferDataType,
               void* pDstBuffer) const override;

    bool IWrite(const GUInt64* arrayStartIdx,
                const size_t* count,
                const GInt64* arrayStep,
                const GPtrDiff_t* bufferStride,
                const GDALExtendedDataType& bufferDataType,
                const void* pSrcBuffer) override;

    Array(const std::shared_ptr<MultiDimSharedResources>& poShared,
          const std::string& osParentName,
          const std::string& osName,
          const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
          const GDALExtendedDataType& oType,
          const std::vector<uint32_t>& anBlockSize);

    friend class MultiDimSharedResources;
    bool CrystalizeFromSharedResources();

public:
    ~Array();

    static std::shared_ptr<Array> Create(
               const std::shared_ptr<MultiDimSharedResources>& poShared,
               const std::string& osParentName,
               const std::string& osName,
               const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
               const GDALExtendedDataType& oType,
               const std::vector<uint32_t>& anBlockSize,
               const std::map<std::string, std::string>& options);

    const std::string& GetFilename() const override { return m_poShared->GetFilename(); }

    const std::vector<std::shared_ptr<GDALDimension>>& GetDimensions() const override { return m_aoDims; }

    std::vector<GUInt64> GetBlockSize() const override {
        return std::vector<GUInt64>( m_anBlockSize.begin(), m_anBlockSize.end() ); }

    const GDALExtendedDataType& GetDataType() const override { return m_oType; }

    bool IsWritable() const override { return m_poShared->IsWritable(); }

    std::shared_ptr<GDALAttribute> GetAttribute(const std::string& osName) const override;

    std::vector<std::shared_ptr<GDALAttribute>> GetAttributes(CSLConstList papszOptions) const override;

    std::shared_ptr<GDALAttribute> CreateAttribute(
        const std::string& osName,
        const std::vector<GUInt64>& anDimensions,
        const GDALExtendedDataType& oDataType,
        CSLConstList papszOptions) override;

    const std::string& GetUnit() const override { return m_osUnit; }

    bool SetUnit(const std::string& osUnit) override {
        m_osUnit = osUnit; return true; }

    bool SetSpatialRef(const OGRSpatialReference* poSRS) override {
        m_poSRS.reset(poSRS ? poSRS->Clone() : nullptr); return true; }

    std::shared_ptr<OGRSpatialReference> GetSpatialRef() const override { return m_poSRS; }

    const void* GetRawNoDataValue() const override;

    bool SetRawNoDataValue(const void*) override;

    double GetOffset(bool* pbHasOffset, GDALDataType* peStorageType) const override
    {
        if( pbHasOffset) *pbHasOffset = m_bHasOffset;
        if( peStorageType ) *peStorageType = m_eOffsetStorageType;
        return m_dfOffset;
    }

    double GetScale(bool* pbHasScale, GDALDataType* peStorageType) const override
    {
        if( pbHasScale) *pbHasScale = m_bHasScale;
        if( peStorageType ) *peStorageType = m_eScaleStorageType;
        return m_dfScale;
    }

    bool SetOffset(double dfOffset, GDALDataType eStorageType) override
    { m_bHasOffset = true; m_dfOffset = dfOffset; m_eOffsetStorageType = eStorageType; return true; }

    bool SetScale(double dfScale, GDALDataType eStorageType) override
    { m_bHasScale = true; m_dfScale = dfScale; m_eScaleStorageType = eStorageType; return true; }
};

/************************************************************************/
/*                        Dimension::Dimension()                        */
/************************************************************************/

Dimension::Dimension(const std::string& osParentName,
                           const std::string& osName,
                           const std::string& osType,
                           const std::string& osDirection,
                           GUInt64 nSize):
    GDALDimension(osParentName, osName, osType, osDirection, nSize)
{
}


/************************************************************************/
/*                           SetIndexingVariable()                      */
/************************************************************************/

// cppcheck-suppress passedByValue
bool Dimension::SetIndexingVariable(std::shared_ptr<GDALMDArray> poIndexingVariable)
{
    if( dynamic_cast<Array*>(poIndexingVariable.get()) )
        return false;
    if( poIndexingVariable->GetDimensions().size() != 1 ||
        poIndexingVariable->GetDimensions().front().get() != this )
        return false;
    m_poIndexingVariable = poIndexingVariable;
    return true;
}

/************************************************************************/
/*                          Array::Array()                              */
/************************************************************************/

Array::Array(const std::shared_ptr<MultiDimSharedResources>& poShared,
             const std::string& osParentName,
             const std::string& osName,
             const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
             const GDALExtendedDataType& oType,
             const std::vector<uint32_t>& anBlockSize):
    GDALAbstractMDArray(osParentName, osName),
    GDALMDArray(osParentName, osName),
    m_poShared(poShared),
    m_aoDims(aoDimensions),
    m_oType(oType),
    m_anBlockSize(anBlockSize)
{
    const auto nDims = m_aoDims.size();
    assert( m_anBlockSize.size() == nDims );
    m_nIFDCount = 1;
    for( size_t i = 0; i < nDims - 2; ++i )
    {
        m_nIFDCount *= static_cast<size_t>(m_aoDims[i]->GetSize());
    }

    for( const auto& poDim: aoDimensions )
    {
        m_oVectorIndexingArrays.emplace_back(poDim->GetIndexingVariable());
    }
}

/************************************************************************/
/*                               ~Array()                               */
/************************************************************************/

Array::~Array()
{
    Crystalize();
    for( const auto& hTIFF: m_ahTIFF )
    {
        XTIFFClose(hTIFF);
    }

    if( m_pabyNoData )
    {
        m_oType.FreeDynamicMemory(&m_pabyNoData[0]);
        CPLFree(m_pabyNoData);
    }
}

/************************************************************************/
/*                            Crystalize()                              */
/************************************************************************/

bool Array::Crystalize()
{
    return m_poShared->Crystalize();
}

/************************************************************************/
/*                          IFDIndexToDimIdx()                          */
/************************************************************************/

std::vector<size_t> Array::IFDIndexToDimIdx(size_t nIFDIndex) const
{
    const auto nDims = m_aoDims.size();
    std::vector<size_t> ret(nDims - 2);
    for( size_t i = nDims - 2; i > 0; )
    {
        --i;
        const size_t nDimSize = static_cast<size_t>(m_aoDims[i]->GetSize());
        ret[i] = nIFDIndex % nDimSize;
        nIFDIndex /= nDimSize;
    }
    return ret;
}

/************************************************************************/
/*                       Array::GenerateMetadata()                      */
/************************************************************************/

char* Array::GenerateMetadata(bool mainIFD, const std::vector<size_t>& anDimIdx) const
{
    const auto nDims = static_cast<int>(m_aoDims.size());
    auto psRoot = CPLCreateXMLNode( nullptr, CXT_Element, "GDALMetadata" );
    CPLXMLNode* psTail = nullptr;

    GTiffAppendMetadataItem( &psRoot, &psTail, "VARIABLE_NAME", GetName().c_str());

    for( int j = 0; j < nDims; ++j )
    {
        const auto& poDim = m_aoDims[j];

        if( mainIFD || j < nDims - 2 )
        {
            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_NAME", j),
                                poDim->GetName().c_str() );
        }

        if( mainIFD )
        {
            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_SIZE", j),
                                CPLSPrintf("%" PRIu64,
                                   static_cast<uint64_t>(poDim->GetSize())) );
            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_BLOCK_SIZE", j),
                                CPLSPrintf("%u", m_anBlockSize[j]) );
            if( !poDim->GetType().empty() )
            {
                GTiffAppendMetadataItem( &psRoot, &psTail,
                                    CPLSPrintf("DIMENSION_%d_TYPE", j),
                                    poDim->GetType().c_str() );
            }
            if( !poDim->GetDirection().empty() )
            {
                GTiffAppendMetadataItem( &psRoot, &psTail,
                                    CPLSPrintf("DIMENSION_%d_DIRECTION", j),
                                    poDim->GetDirection().c_str() );
            }
        }

        if( j < nDims - 2 )
        {
            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_IDX", j),
                                CPLSPrintf("%u",
                                   static_cast<unsigned>(anDimIdx[j])));

            const auto indexingVar = poDim->GetIndexingVariable();
            if( indexingVar )
            {
                const auto& oIndexingVarDT = indexingVar->GetDataType();
                const bool bIsInteger = CPL_TO_BOOL(GDALDataTypeIsInteger(
                                    oIndexingVarDT.GetNumericDataType()));
                if( mainIFD )
                {
                    const GUInt64 arrayStartIdx = 0;
                    const size_t count = static_cast<size_t>(poDim->GetSize());
                    const GInt64 arrayStep = 1;
                    const GPtrDiff_t bufferStride = 1;
                    if( oIndexingVarDT.GetClass() == GEDTC_STRING )
                    {
                        GTiffAppendMetadataItem( &psRoot, &psTail,
                            CPLSPrintf("DIMENSION_%d_DATATYPE", j),
                            "String");

                        std::vector<char*> apszValues(count);
                        if( indexingVar->Read(&arrayStartIdx, &count,
                                              &arrayStep, &bufferStride,
                                              oIndexingVarDT, &apszValues[0]) )
                        {
                            std::string values;
                            for( char* pszStr: apszValues )
                            {
                                if( !values.empty() )
                                    values += ',';
                                if( pszStr )
                                {
                                    values += pszStr;
                                    CPLFree(pszStr);
                                }
                            }
                            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_VALUES", j),
                                values.c_str());
                        }
                    }
                    else
                    {
                        GTiffAppendMetadataItem( &psRoot, &psTail,
                            CPLSPrintf("DIMENSION_%d_DATATYPE", j),
                            GDALGetDataTypeName(oIndexingVarDT.GetNumericDataType()) );

                        std::vector<double> adfValues(count);
                        if( indexingVar->Read(&arrayStartIdx, &count,
                                              &arrayStep, &bufferStride,
                                              GDALExtendedDataType::Create(GDT_Float64),
                                              &adfValues[0]) )
                        {
                            std::string values;
                            for( double dfVal: adfValues )
                            {
                                if( !values.empty() )
                                    values += ',';
                                values += bIsInteger ?
                                    CPLSPrintf("%" PRId64, static_cast<int64_t>(dfVal)) :
                                    CPLSPrintf("%f", dfVal);
                            }
                            GTiffAppendMetadataItem( &psRoot, &psTail,
                                CPLSPrintf("DIMENSION_%d_VALUES", j),
                                values.c_str());
                        }
                    }
                }

                const GUInt64 arrayStartIdx = anDimIdx[j];
                const size_t count = 1;
                const GInt64 arrayStep = 1;
                const GPtrDiff_t bufferStride = 0;
                if( oIndexingVarDT.GetClass() == GEDTC_STRING )
                {
                    char* str = nullptr;
                    indexingVar->Read(&arrayStartIdx, &count, &arrayStep, &bufferStride,
                                      oIndexingVarDT, &str);
                    if( str )
                    {
                        GTiffAppendMetadataItem( &psRoot, &psTail,
                                            CPLSPrintf("DIMENSION_%d_VAL", j),
                                            str);
                        CPLFree(str);
                    }
                }
                else
                {
                    double val = 0;
                    if( indexingVar->Read(&arrayStartIdx, &count, &arrayStep, &bufferStride,
                                          GDALExtendedDataType::Create(GDT_Float64), &val) )
                    {
                        GTiffAppendMetadataItem( &psRoot, &psTail,
                            CPLSPrintf("DIMENSION_%d_VAL", j),
                            bIsInteger ?
                                CPLSPrintf("%" PRId64, static_cast<int64_t>(val)) :
                                CPLSPrintf("%f", val) );
                    }
                }
            }
        }
    }

    // Serialize attributes
    for( const auto& kv: m_oMapAttributes )
    {
        const auto& attr = kv.second;
        const auto klass = attr->GetDataType().GetClass();
        std::string osVal;
        const auto& dims = attr->GetDimensionsSize();
        const GUInt64 arrayStartIdx = 0;
        const size_t count = dims.empty() ? 1 : static_cast<size_t>(dims[0]);
        const GInt64 arrayStep = 1;
        const GPtrDiff_t bufferStride = 1;
        if( klass == GEDTC_STRING )
        {
            std::vector<char*> apszStr(count);
            attr->Read(&arrayStartIdx, &count, &arrayStep, &bufferStride,
                       attr->GetDataType(), &apszStr[0]);
            for( size_t i = 0; i < count; ++i )
            {
                if( !osVal.empty() )
                    osVal += ',';
                osVal += apszStr[i] ? apszStr[i] : "null";
                CPLFree(apszStr[i]);
            }
        }
        else if ( klass == GEDTC_NUMERIC )
        {
            std::vector<double> adfVals(count);
            attr->Read(&arrayStartIdx, &count, &arrayStep, &bufferStride,
                       GDALExtendedDataType::Create(GDT_Float64),
                       &adfVals[0]);
            for( size_t i = 0; i < count; ++i )
            {
                if( !osVal.empty() )
                    osVal += ',';
                osVal += CPLSPrintf("%.18g", adfVals[i]);
            }
        }
        if( !osVal.empty() )
        {
            GTiffAppendMetadataItem( &psRoot, &psTail, attr->GetName().c_str(),
                                osVal.c_str() );
        }
    }

    if( m_bHasOffset )
    {
        GTiffAppendMetadataItem( &psRoot, &psTail, "OFFSET",
                            CPLSPrintf("%.18g", m_dfOffset), 1,
                            "offset");
    }

    if( m_bHasScale )
    {
        GTiffAppendMetadataItem( &psRoot, &psTail, "SCALE",
                            CPLSPrintf("%.18g", m_dfScale), 1,
                            "scale");
    }

    if( !m_osUnit.empty() )
    {
        GTiffAppendMetadataItem( &psRoot, &psTail, "UNITTYPE",
                            m_osUnit.c_str(), 1,
                            "unittype", "" );
    }

    char *pszXML_MD = CPLSerializeXMLTree( psRoot );

    // Strip off formatting spaces from XML to save space.
    int iDst = 0;
    for( int iSrc = 0; pszXML_MD[iSrc] != '\0'; ++iSrc, ++iDst )
    {
        pszXML_MD[iDst] = pszXML_MD[iSrc];
        if( pszXML_MD[iSrc] == '>' )
        {
            while ( pszXML_MD[iSrc+1] == ' ' || pszXML_MD[iSrc+1] == '\n' )
                iSrc ++;
        }
    }
    pszXML_MD[iDst] = '\0';

    CPLDestroyXMLNode(psRoot);
    return pszXML_MD;
}

/************************************************************************/
/*               GTiffDatasetLibGeotiffErrorCallback()                  */
/************************************************************************/

static void GTiffDatasetLibGeotiffErrorCallback(GTIF*,
                                                int level,
                                                const char* pszMsg, ...)
{
    va_list ap;
    va_start(ap, pszMsg);
    CPLErrorV( (level == LIBGEOTIFF_WARNING ) ? CE_Warning : CE_Failure,
               CPLE_AppDefined, pszMsg, ap );
    va_end(ap);
}

/************************************************************************/
/*                           GTiffDatasetGTIFNew()                      */
/************************************************************************/

static GTIF* GTiffDatasetGTIFNew( TIFF* hTIFF )
{
    GTIF* gtif = GTIFNewEx(hTIFF, GTiffDatasetLibGeotiffErrorCallback, nullptr);
    if( gtif )
    {
        GTIFAttachPROJContext(gtif, OSRGetProjTLSContext());
    }
    return gtif;
}

/************************************************************************/
/*                          WriteGeoTIFFTags()                          */
/************************************************************************/

void Array::WriteGeoTIFFTags( TIFF* hTIFF )
{
    const auto nDims = static_cast<int>(m_aoDims.size());
    const size_t nDimX = nDims - 1;
    const size_t nDimY = nDims - 2;
    double adfGeoTransform[6] = {0,0,0,0,0,0};
    constexpr bool bPixelIsPoint = true;
    const bool bGotGT = GuessGeoTransform(nDimX, nDimY, bPixelIsPoint,
                                          adfGeoTransform);
    if( bGotGT )
    {
        if( adfGeoTransform[5] < 0 )
        {
            const double adfPixelScale[3] = {
                adfGeoTransform[1], -adfGeoTransform[5], 0.0 };
            TIFFSetField( hTIFF, TIFFTAG_GEOPIXELSCALE, 3, adfPixelScale );

            const double adfTiePoints[6] = {
                0.0, 0.0, 0.0, adfGeoTransform[0], adfGeoTransform[3], 0.0 };

            TIFFSetField( hTIFF, TIFFTAG_GEOTIEPOINTS, 6, adfTiePoints );
        }
        else
        {
            double adfMatrix[16] = {};

            adfMatrix[0] = adfGeoTransform[1];
            adfMatrix[1] = adfGeoTransform[2];
            adfMatrix[3] = adfGeoTransform[0];
            adfMatrix[4] = adfGeoTransform[4];
            adfMatrix[5] = adfGeoTransform[5];
            adfMatrix[7] = adfGeoTransform[3];
            adfMatrix[15] = 1.0;

            TIFFSetField( hTIFF, TIFFTAG_GEOTRANSMATRIX, 16, adfMatrix );
        }
    }

    if( bGotGT || m_poSRS )
    {
        GTIF *psGTIF = GTiffDatasetGTIFNew( hTIFF );

        // Set according to coordinate system.
        if( m_poSRS )
        {
            char* pszProjection = nullptr;
            {
                CPLErrorStateBackuper oErrorStateBackuper;
                CPLErrorHandlerPusher oErrorHandler(CPLQuietErrorHandler);
                m_poSRS->exportToWkt(&pszProjection);
            }

            const  GTIFFKeysFlavorEnum eGeoTIFFKeysFlavor = GEOTIFF_KEYS_STANDARD;
            const GeoTIFFVersionEnum eGeoTIFFVersion = GEOTIFF_VERSION_AUTO;
            if( pszProjection && pszProjection[0] &&
                strstr(pszProjection, "custom_proj4") == nullptr )
            {
                GTIFSetFromOGISDefnEx( psGTIF,
                                       OGRSpatialReference::ToHandle(m_poSRS.get()),
                                       eGeoTIFFKeysFlavor,
                                       eGeoTIFFVersion );
            }
            CPLFree(pszProjection);
        }

        /* if( bPixelIsPoint ) */
        {
            GTIFKeySet(psGTIF, GTRasterTypeGeoKey, TYPE_SHORT, 1,
                       RasterPixelIsPoint);
        }

        GTIFWriteKeys( psGTIF );
        GTIFFree( psGTIF );
    }
}

/************************************************************************/
/*                Array::CrystalizeFromSharedResources()                */
/************************************************************************/

bool Array::CrystalizeFromSharedResources()
{
    GDALDataType eType;
    if( m_oType.GetClass() == GEDTC_NUMERIC )
        eType = m_oType.GetNumericDataType();
    else
        eType = m_oType.GetComponents().front()->GetType().GetNumericDataType();

    const auto nDims = static_cast<int>(m_aoDims.size());
    const auto& poDimX = m_aoDims[nDims-1];
    const auto& poDimY = m_aoDims[nDims-2];
    const uint32_t nXSize = static_cast<uint32_t>(poDimX->GetSize());
    const uint32_t nYSize = static_cast<uint32_t>(poDimY->GetSize());

    uint16_t nSampleFormat;
    if( eType == GDT_Int16 || eType == GDT_Int32 )
        nSampleFormat = SAMPLEFORMAT_INT;
    else if( eType == GDT_CInt16 || eType == GDT_CInt32 )
        nSampleFormat = SAMPLEFORMAT_COMPLEXINT;
    else if( eType == GDT_Float32 || eType == GDT_Float64 )
        nSampleFormat = SAMPLEFORMAT_IEEEFP;
    else if( eType == GDT_CFloat32 || eType == GDT_CFloat64 )
        nSampleFormat = SAMPLEFORMAT_COMPLEXIEEEFP;
    else
        nSampleFormat = SAMPLEFORMAT_UINT;

    const uint16_t nBitsPerPixel = static_cast<uint16_t>(
                                                GDALGetDataTypeSize(eType));

    m_ahTIFF.reserve(m_nIFDCount);
    for( size_t i = 0; i < m_nIFDCount; i++ )
    {
        TIFF* hTIFFMaster = m_poShared->GetTIFFHandle();
        // Due to how libtiff works, it is not really possible to create a
        // child TIFF handle until the first directory has been materialized
        TIFF* hTIFF = m_poShared->IsNewFile() ? hTIFFMaster:
                                                VSI_TIFFOpenChild(hTIFFMaster);

        // This is a bit of a hack to cause (*tif->tif_cleanup)(tif); to be called.
        // See https://trac.osgeo.org/gdal/ticket/2055
        TIFFSetField( hTIFF, TIFFTAG_COMPRESSION, COMPRESSION_NONE );
        TIFFFreeDirectory( hTIFF );

        TIFFCreateDirectory( hTIFF );

        TIFFSetField( hTIFF, TIFFTAG_IMAGEWIDTH, nXSize );

        TIFFSetField( hTIFF, TIFFTAG_IMAGELENGTH, nYSize );

        TIFFSetField( hTIFF, TIFFTAG_TILEWIDTH, m_anBlockSize[nDims-1] );

        TIFFSetField( hTIFF, TIFFTAG_TILELENGTH, m_anBlockSize[nDims-2] );

        TIFFSetField( hTIFF, TIFFTAG_BITSPERSAMPLE, nBitsPerPixel );

        TIFFSetField( hTIFF, TIFFTAG_SAMPLEFORMAT, nSampleFormat );

        TIFFSetField( hTIFF, TIFFTAG_COMPRESSION, m_nCompression );

        if( m_nCompression == COMPRESSION_LZW ||
            m_nCompression == COMPRESSION_ADOBE_DEFLATE ||
            m_nCompression == COMPRESSION_ZSTD )
        {
            TIFFSetField( hTIFF, TIFFTAG_PREDICTOR, m_nPredictor );
        }
        else if( m_nCompression == COMPRESSION_LERC )
        {
            TIFFSetField(hTIFF, TIFFTAG_LERC_ADD_COMPRESSION, m_nLercSubcodec);
        }

        WriteGeoTIFFTags( hTIFF );

        const bool mainIFD = (i == 0);
        const auto anDimIdx = IFDIndexToDimIdx(i);
        char* pszXML_MD = GenerateMetadata(mainIFD, anDimIdx);
        TIFFSetField( hTIFF, TIFFTAG_GDAL_METADATA, pszXML_MD );
        CPLFree(pszXML_MD);

        if( m_pabyNoData )
        {
            CPLString osVal;
            double dfNoData;
            GDALExtendedDataType::CopyValue( m_pabyNoData, m_oType,
                                             &dfNoData,
                                             GDALExtendedDataType::Create(GDT_Float64) );
            if( std::isnan(dfNoData) )
                osVal = "nan";
            else
                osVal.Printf("%.18g", dfNoData);
            TIFFSetField( hTIFF, TIFFTAG_GDAL_NODATA, osVal.c_str() );
        }

#ifdef SUPPORTS_GET_OFFSET_BYTECOUNT
        TIFFDeferStrileArrayWriting( hTIFF );
#endif

        if( TIFFWriteCheck( hTIFF, TIFFIsTiled(hTIFF), "Crystalize") == 0 )
        {
            if( hTIFF != hTIFFMaster )
                XTIFFClose(hTIFF);
            return false;
        }

        const auto ifdPos = (VSI_TIFFSeek( hTIFF, 0, SEEK_END ) + 1) & ~1;

        // TODO? extend libtiff API to provide a hint where the last IFD is
        if( TIFFWriteDirectory( hTIFF ) == 0 )
        {
            if( hTIFF != hTIFFMaster )
                XTIFFClose(hTIFF);
            return false;
        }

        if( m_poShared->IsNewFile() )
        {
            hTIFF = VSI_TIFFOpenChild(hTIFFMaster);
            m_poShared->UnsetNewFile();
        }

        TIFFSetSubDirectory( hTIFF, ifdPos );

        // Set transient tags
        if(m_nZLevel > 0 && (m_nCompression == COMPRESSION_ADOBE_DEFLATE ||
                             m_nCompression == COMPRESSION_LERC) )
        {
#if defined(TIFFTAG_DEFLATE_SUBCODEC) && defined(LIBDEFLATE_SUPPORT)
            // Mostly for strict reproducibility purposes
            if( m_nCompression == COMPRESSION_ADOBE_DEFLATE &&
                EQUAL(CPLGetConfigOption("GDAL_TIFF_DEFLATE_SUBCODEC", ""), "ZLIB") )
            {
                TIFFSetField(hTIFF, TIFFTAG_DEFLATE_SUBCODEC,
                             DEFLATE_SUBCODEC_ZLIB);
            }
#endif
            TIFFSetField(hTIFF, TIFFTAG_ZIPQUALITY, m_nZLevel);
        }
        if(m_nLZMAPreset > 0 && m_nCompression == COMPRESSION_LZMA)
        {
            TIFFSetField(hTIFF, TIFFTAG_LZMAPRESET, m_nLZMAPreset);
        }
        if( m_nZSTDLevel > 0 && (m_nCompression == COMPRESSION_ZSTD ||
                                 m_nCompression == COMPRESSION_LERC) )
        {
            TIFFSetField(hTIFF, TIFFTAG_ZSTD_LEVEL, m_nZSTDLevel);
        }
        if( m_nCompression == COMPRESSION_LERC )
        {
            TIFFSetField(hTIFF, TIFFTAG_LERC_MAXZERROR, m_dfMaxZError);
        }

        m_ahTIFF.emplace_back(hTIFF);

    }

#ifdef SUPPORTS_GET_OFFSET_BYTECOUNT
    for( const auto& hTIFF: m_ahTIFF )
    {
        TIFFForceStrileArrayWriting(hTIFF);
    }
#endif

    return true;
}

/************************************************************************/
/*                          GetRawNoDataValue()                         */
/************************************************************************/

const void* Array::GetRawNoDataValue() const
{
    return m_pabyNoData;
}

/************************************************************************/
/*                          SetRawNoDataValue()                         */
/************************************************************************/

bool Array::SetRawNoDataValue(const void* pNoData)
{
    if( m_pabyNoData )
    {
        m_oType.FreeDynamicMemory(&m_pabyNoData[0]);
    }

    if( pNoData == nullptr )
    {
        CPLFree(m_pabyNoData);
        m_pabyNoData = nullptr;
    }
    else
    {
        const auto nSize = m_oType.GetSize();
        if( m_pabyNoData == nullptr )
        {
            m_pabyNoData = static_cast<GByte*>(CPLMalloc(nSize));
        }
        memset(m_pabyNoData, 0, nSize);
        GDALExtendedDataType::CopyValue( pNoData, m_oType, m_pabyNoData, m_oType );
    }
    return true;
}

/************************************************************************/
/*                        Array::Create()                               */
/************************************************************************/

std::shared_ptr<Array> Array::Create(
           const std::shared_ptr<MultiDimSharedResources>& poShared,
           const std::string& osParentName,
           const std::string& osName,
           const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
           const GDALExtendedDataType& oType,
           const std::vector<uint32_t>& anBlockSize,
           const std::map<std::string, std::string>& options)
{
    auto array(std::shared_ptr<Array>(
        new Array(poShared, osParentName, osName, aoDimensions, oType, anBlockSize)));

    const auto GetValueOption = [&options](const std::string& name) {
        const auto iter = options.find(name);
        if( iter != options.end() )
            return iter->second;
        return std::string();
    };

    int nCompression = COMPRESSION_NONE;
    std::string osCompression = GetValueOption("COMPRESS");
    if( !osCompression.empty() )
    {
        nCompression = GTIFFGetCompressionMethod(osCompression.c_str(), "COMPRESS");
        if( nCompression < 0 )
            return nullptr;
        const int supportedMethods[] = { COMPRESSION_ADOBE_DEFLATE,
                                         COMPRESSION_LZW,
                                         COMPRESSION_LZMA,
                                         COMPRESSION_ZSTD,
                                         COMPRESSION_LERC };
        if( std::find(std::begin(supportedMethods), std::end(supportedMethods),
                                    nCompression) == std::end(supportedMethods) )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "COMPRESS=%s not handled currently",
                     osCompression.c_str());
            return nullptr;
        }

        if( nCompression == COMPRESSION_LERC )
        {
            if( EQUAL(osCompression.c_str() , "LERC_DEFLATE" ) )
                array->m_nLercSubcodec = LERC_ADD_COMPRESSION_DEFLATE;
            else if( EQUAL(osCompression.c_str() , "LERC_ZSTD" ) )
                array->m_nLercSubcodec = LERC_ADD_COMPRESSION_ZSTD;
        }
    }

    array->m_nCompression = nCompression;

    const std::string osPredictor = GetValueOption("PREDICTOR");
    if( !osPredictor.empty() )
        array->m_nPredictor = static_cast<uint16_t>(atoi( osPredictor.c_str() ));

    CPLStringList aosOptions;
    for( const auto& kv: options )
    {
        aosOptions.SetNameValue(kv.first.c_str(), kv.second.c_str());
    }
    array->m_nZLevel = GTiffGetZLevel(aosOptions.List());
    array->m_nLZMAPreset = GTiffGetLZMAPreset(aosOptions.List());
    array->m_nZSTDLevel = GTiffGetZSTDPreset(aosOptions.List());
    array->m_dfMaxZError = GTiffGetLERCMaxZError(aosOptions.List());

    array->SetSelf(array);
    return array;
}

/************************************************************************/
/*                            GetAttribute()                            */
/************************************************************************/

std::shared_ptr<GDALAttribute> Array::GetAttribute(const std::string& osName) const
{
    auto oIter = m_oMapAttributes.find(osName);
    if( oIter != m_oMapAttributes.end() )
        return oIter->second;
    return nullptr;
}

/************************************************************************/
/*                             GetAttributes()                          */
/************************************************************************/

std::vector<std::shared_ptr<GDALAttribute>> Array::GetAttributes(CSLConstList) const
{
    std::vector<std::shared_ptr<GDALAttribute>> oRes;
    for( const auto& oIter: m_oMapAttributes )
    {
        oRes.push_back(oIter.second);
    }
    return oRes;
}

/************************************************************************/
/*                            CreateAttribute()                         */
/************************************************************************/

std::shared_ptr<GDALAttribute> Array::CreateAttribute(
        const std::string& osName,
        const std::vector<GUInt64>& anDimensions,
        const GDALExtendedDataType& oDataType,
        CSLConstList)
{
    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty attribute name not supported");
        return nullptr;
    }
    if( m_oMapAttributes.find(osName) != m_oMapAttributes.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "An attribute with same name already exists");
        return nullptr;
    }
    if( anDimensions.size() > 1 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Only 0 or 1-dimensional attribute are supported");
        return nullptr;
    }
    auto newAttr(MEMAttribute::Create(GetFullName(), osName, anDimensions, oDataType));
    if( !newAttr )
        return nullptr;
    m_oMapAttributes[osName] = newAttr;
    return newAttr;
}

/************************************************************************/
/*                         Array::IRead()                               */
/************************************************************************/

bool Array::IRead(const GUInt64* arrayStartIdx,
                  const size_t* count,
                  const GInt64* arrayStep,
                  const GPtrDiff_t* bufferStride,
                  const GDALExtendedDataType& bufferDataType,
                  void* pDstBuffer) const
{
    // FIXME
    (void)arrayStartIdx;
    (void)count;
    (void)arrayStep;
    (void)bufferStride;
    (void)bufferDataType;
    (void)pDstBuffer;
    return false;
}

/************************************************************************/
/*                         Array::IWrite()                              */
/************************************************************************/

bool Array::IWrite(const GUInt64* arrayStartIdx,
                   const size_t* count,
                   const GInt64* arrayStep,
                   const GPtrDiff_t* bufferStride,
                   const GDALExtendedDataType& bufferDataType,
                   const void* pSrcBuffer)
{
    if( !Crystalize() )
        return false;

    const size_t nDims = m_aoDims.size();
    for( size_t i = 0; i < nDims; ++i )
    {
        if( (arrayStartIdx[i] % m_anBlockSize[i]) != 0 )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Only writes aligned with block size are supported.");
            return false;
        }
        if( ((count[i] % m_anBlockSize[i]) != 0) &&
            arrayStartIdx[i] + count[i] != m_aoDims[i]->GetSize() )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Only writes covering the full blocks are supported.");
            return false;
        }
        if( arrayStep[i] != 1 )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Only writes with arrayStep[i] == 1 are supported.");
            return false;
        }
    }
    if( bufferDataType.GetClass() != GEDTC_NUMERIC )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Incompatible buffer data type.");
        return false;
    }

    const auto eBufferDT = bufferDataType.GetNumericDataType();
    const auto nBufferDTSize = static_cast<int>(bufferDataType.GetSize());

    const size_t iDimY = nDims - 2;
    const size_t iDimX = nDims - 1;

    // Figure out which 2D tiles intersects the window written
    const uint32_t iYLastTile = static_cast<uint32_t>(
        (arrayStartIdx[iDimY] + count[iDimY] - 1) / m_anBlockSize[iDimY]);
    const uint32_t iXLastTile = static_cast<uint32_t>(
        (arrayStartIdx[iDimX] + count[iDimX] - 1) / m_anBlockSize[iDimX]);
    const bool bPartialTileX =
        (iXLastTile == static_cast<uint32_t>(
            (m_aoDims[iDimX]->GetSize() - 1) / m_anBlockSize[iDimX])) &&
        (m_aoDims[iDimX]->GetSize() % m_anBlockSize[iDimX]) != 0;
    const bool bPartialTileY =
        (iYLastTile == static_cast<uint32_t>(
            (m_aoDims[iDimY]->GetSize() - 1) / m_anBlockSize[iDimY])) &&
        (m_aoDims[iDimY]->GetSize() % m_anBlockSize[iDimY]) != 0;

    // Reserve a buffer for 2D tile content
    const auto nDTSize = m_oType.GetSize();
    std::vector<GByte> abyTileData;
    try
    {
        abyTileData.resize( m_anBlockSize[iDimX] * m_anBlockSize[iDimY] * nDTSize );
    }
    catch( const std::bad_alloc& e )
    {
        CPLError(CE_Failure, CPLE_OutOfMemory, "%s", e.what());
        return false;
    }
    const tmsize_t nTileDataSize = static_cast<tmsize_t>(abyTileData.size());

    // Number of tiles along X direction
    const uint32_t nTileXCount = static_cast<uint32_t>(
        (m_aoDims[iDimX]->GetSize() + m_anBlockSize[iDimX] - 1) / m_anBlockSize[iDimX]);

    const auto nSrcXIncLarge = bufferStride[iDimX] * nBufferDTSize;
    if( nSrcXIncLarge > std::numeric_limits<int>::max() ||
        nSrcXIncLarge < std::numeric_limits<int>::min() )
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Too large buffer stride");
        return false;
    }
    const int nSrcXInc = static_cast<int>(nSrcXIncLarge);
    const GDALDataType eDT = m_oType.GetNumericDataType();
    const size_t nDstYInc = m_anBlockSize[iDimX] * nDTSize;

    size_t dimIdx = 0;
    std::vector<size_t> indicesOuterLoop(nDims);
    std::vector<size_t> indicesInnerLoop(nDims - 2);
    std::vector<const GByte*> srcPtrStackOuterLoop(nDims + 1);
    srcPtrStackOuterLoop[0] = static_cast<const GByte*>(pSrcBuffer);
    std::vector<const GByte*> srcPtrStackInnerLoop(nDims - 2 + 1);
    std::vector<size_t> anIFDIndex(nDims - 2 + 1);

    // We will iterate over dimensions two times:
    // - an outer loop that iterates over indices, using the block size as the
    //   increment
    // - an inner loop that iterates over indices, of the non-spatial dims,
    //   by increment of one.

lbl_next_depth:
    if( dimIdx == nDims )
    {
        size_t dimIdxSubLoop = 0;
        srcPtrStackInnerLoop[0] = srcPtrStackOuterLoop[nDims];
        anIFDIndex[0] = 0;
lbl_next_depth_inner_loop:
        if( dimIdxSubLoop == iDimY )
        {
            // Now we have a 2D tile !
            const size_t nIFDIndex = anIFDIndex[iDimY];
            assert( nIFDIndex < m_ahTIFF.size() );
            TIFF* hTIFF = m_ahTIFF[nIFDIndex];
            const uint32_t iYTile = static_cast<uint32_t>(
                                    indicesOuterLoop[iDimY] /  m_anBlockSize[iDimY]);
            const uint32_t iXTile = static_cast<uint32_t>(
                                    indicesOuterLoop[iDimX] /  m_anBlockSize[iDimX]);
            const uint32_t nAfterRowY = (iYTile == iYLastTile && bPartialTileY ) ?
                static_cast<uint32_t>(m_aoDims[iDimY]->GetSize() % m_anBlockSize[iDimY]):
                static_cast<uint32_t>(m_anBlockSize[iDimY]);
            const uint32_t nAfterColX = (iXTile == iXLastTile && bPartialTileX ) ?
                    static_cast<uint32_t>(m_aoDims[iDimX]->GetSize() % m_anBlockSize[iDimX]):
                    static_cast<uint32_t>(m_anBlockSize[iDimX]);
            const GByte* src_ptr_row = srcPtrStackInnerLoop[dimIdxSubLoop];
            GByte* dst_ptr_row = &abyTileData[0];
            for( uint32_t y = 0; y < nAfterRowY; ++y )
            {
                GDALCopyWords64(src_ptr_row, eBufferDT, nSrcXInc,
                                dst_ptr_row, eDT, static_cast<int>(nDTSize),
                                static_cast<GPtrDiff_t>(nAfterColX));

                // For right-most partial tiles, fill padding colmuns with
                // the content of the last valid one. Will help for JPEG
                // compression.
                if( nAfterColX < m_anBlockSize[iDimX] )
                {
                    GDALCopyWords64(dst_ptr_row + (nAfterColX - 1) * nDTSize,
                                    eDT, 0,
                                    dst_ptr_row + nAfterColX * nDTSize,
                                    eDT, static_cast<int>(nDTSize),
                                    static_cast<GPtrDiff_t>(
                                        m_anBlockSize[iDimX] - nAfterColX));
                }

                src_ptr_row += bufferStride[iDimY] * nBufferDTSize;
                dst_ptr_row += nDstYInc;
            }

            // For bottom-most partial tiles, fill padding lines with the
            // content of the last valid one. Will help for JPEG compression
            src_ptr_row = dst_ptr_row - nDstYInc;
            for( uint32_t y = nAfterRowY; y < m_anBlockSize[iDimY]; ++y )
            {
                memcpy(dst_ptr_row, src_ptr_row, nDstYInc);
                dst_ptr_row += nDstYInc;
            }

            const uint32_t nTile = iYTile * nTileXCount + iXTile;
            if( TIFFWriteEncodedTile(hTIFF, nTile, &abyTileData[0],
                                     nTileDataSize) != nTileDataSize )
            {
                return false;
            }
        }
        else
        {
            // This level op loop loops over individual samples, within a
            // block, in the non-2D indices.
            indicesInnerLoop[dimIdxSubLoop] = indicesOuterLoop[dimIdxSubLoop];
            anIFDIndex[dimIdxSubLoop] *=
                static_cast<size_t>(m_aoDims[dimIdxSubLoop]->GetSize());
            anIFDIndex[dimIdxSubLoop] += indicesOuterLoop[dimIdxSubLoop];
            while(true)
            {
                dimIdxSubLoop ++;
                anIFDIndex[dimIdxSubLoop] = anIFDIndex[dimIdxSubLoop-1];
                srcPtrStackInnerLoop[dimIdxSubLoop] = srcPtrStackInnerLoop[dimIdxSubLoop-1];
                goto lbl_next_depth_inner_loop;
lbl_return_to_caller_inner_loop:
                dimIdxSubLoop --;
                indicesInnerLoop[dimIdxSubLoop] ++;
                if( indicesInnerLoop[dimIdxSubLoop] ==
                        indicesOuterLoop[dimIdxSubLoop] + m_anBlockSize[dimIdxSubLoop] ||
                    indicesInnerLoop[dimIdxSubLoop] ==
                        m_aoDims[dimIdxSubLoop]->GetSize() )
                {
                    break;
                }
                srcPtrStackInnerLoop[dimIdxSubLoop] +=
                    bufferStride[dimIdxSubLoop] * nBufferDTSize;
                anIFDIndex[dimIdxSubLoop] ++;
            }
        }
        if( dimIdxSubLoop > 0 )
            goto lbl_return_to_caller_inner_loop;
    }
    else
    {
        // This level op loop loops over blocks
        indicesOuterLoop[dimIdx] = static_cast<size_t>(arrayStartIdx[dimIdx]);
        while(true)
        {
            dimIdx ++;
            srcPtrStackOuterLoop[dimIdx] = srcPtrStackOuterLoop[dimIdx - 1];
            goto lbl_next_depth;
lbl_return_to_caller:
            dimIdx --;
            indicesOuterLoop[dimIdx] += m_anBlockSize[dimIdx];
            if( indicesOuterLoop[dimIdx] >= arrayStartIdx[dimIdx] + count[dimIdx] )
                break;
            srcPtrStackOuterLoop[dimIdx] += static_cast<GPtrDiff_t>(
                m_anBlockSize[dimIdx]) * bufferStride[dimIdx] * nBufferDTSize;
        }
    }
    if( dimIdx > 0 )
        goto lbl_return_to_caller;

    return true;
}

/************************************************************************/
/*                      Group::CreateDimension()                        */
/************************************************************************/

std::shared_ptr<GDALDimension> Group::CreateDimension(const std::string& osName,
                                                         const std::string& osType,
                                                         const std::string& osDirection,
                                                         GUInt64 nSize,
                                                         CSLConstList)
{
    if( !m_poShared->IsWritable() )
    {
        CPLError(CE_Failure, CPLE_NotSupported, "Non-writeable dataset");
        return nullptr;
    }
    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty dimension name not supported");
        return nullptr;
    }
    if( m_oMapDimensions.find(osName) != m_oMapDimensions.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "A dimension with same name already exists");
        return nullptr;
    }
    auto newDim(std::make_shared<Dimension>(GetFullName(), osName, osType, osDirection, nSize));
    m_oMapDimensions[osName] = newDim;
    return newDim;
}

/************************************************************************/
/*                           GetMDArrayNames()                          */
/************************************************************************/

std::vector<std::string> Group::GetMDArrayNames(CSLConstList) const
{
    std::vector<std::string> names;
    for( const auto& iter: m_oMapMDArrays )
        names.push_back(iter.first);
    return names;
}

/************************************************************************/
/*                        Group::CreateMDArray()                        */
/************************************************************************/

std::shared_ptr<GDALMDArray> Group::CreateMDArray(
               const std::string& osName,
               const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
               const GDALExtendedDataType& oDataType,
               CSLConstList papszOptions)
{
    if( !m_poShared->IsWritable() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Non-writeable dataset");
        return nullptr;
    }

    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty array name not supported");
        return nullptr;
    }
    if( m_oMapMDArrays.find(osName) != m_oMapMDArrays.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "An array with same name already exists.");
        return nullptr;
    }

    const size_t nDims = aoDimensions.size();
    if( nDims == 1 )
    {
        if( CPLTestBool(CSLFetchNameValueDef(papszOptions,
                                             "IS_INDEXING_VARIABLE", "NO")) )
        {
            auto array = MEMMDArray::Create(std::string(), osName, aoDimensions, oDataType);
            if( !array->Init() )
                return nullptr;
            m_oMapMDArrays[osName] = array;
            return array;
        }
        CPLError(CE_Failure, CPLE_NotSupported,
                 "1D array can be created only if they are the indexing "
                 "variable of a dimension, and with the option "
                 "IS_INDEXING_VARIABLE=YES set.");
        return nullptr;
    }
    else if( nDims == 0 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Only 2D or more arrays are supported.");
        return nullptr;
    }

    const auto eDTClass = oDataType.GetClass();
    if( eDTClass == GEDTC_STRING )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Arrays with data of type String are not supported");
        return nullptr;
    }
    else if( eDTClass == GEDTC_COMPOUND )
    {
        const auto& components = oDataType.GetComponents();
        assert( !components.empty() ); // normally enforced in GDALExtendedDataType constructor
        const auto eFirstDT = components.front()->GetType().GetNumericDataType();
        for( const auto& comp: components )
        {
            const auto& compType = comp->GetType();
            if( compType.GetClass() != GEDTC_NUMERIC )
            {
                CPLError(CE_Failure, CPLE_NotSupported,
                         "Arrays with data of type Compound can only "
                         "contain numeric components.");
                return nullptr;
            }
            if( compType.GetNumericDataType() != eFirstDT )
            {
                CPLError(CE_Failure, CPLE_NotSupported,
                         "Arrays with data of type Compound can only "
                         "contain components of the same numeric data type.");
                return nullptr;
            }
        }
    }

    if( !aoDimensions[nDims-1]->GetType().empty() &&
        aoDimensions[nDims-1]->GetType() != GDAL_DIM_TYPE_HORIZONTAL_X )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "dim[n-1].type should be HORIZONTAL_X (found %s).",
                 aoDimensions[nDims-1]->GetType().c_str());
        return nullptr;
    }
    if( !aoDimensions[nDims-2]->GetType().empty() &&
        aoDimensions[nDims-2]->GetType() != GDAL_DIM_TYPE_HORIZONTAL_Y )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "dim[n-2].type should be HORIZONTAL_Y (found %s).",
                 aoDimensions[nDims-2]->GetType().c_str());
        return nullptr;
    }

    // We could potentially go to uint32 max, but GDAL 2D is limited to int32
    if( aoDimensions[nDims-1]->GetSize() >
            static_cast<GUInt64>(std::numeric_limits<int>::max()) )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Too many samples along X dimension");
        return nullptr;
    }
    if( aoDimensions[nDims-2]->GetSize() >
            static_cast<GUInt64>(std::numeric_limits<int>::max()) )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Too many samples along Y dimension");
        return nullptr;
    }

    GUInt64 nExtraDimSamples = 1;
    constexpr uint32_t LIMIT_SAMPLES_ABOVE_2D = 1000 * 1000;
    for( size_t iDimIdx = 0; iDimIdx < nDims - 2; ++iDimIdx )
    {
        if( aoDimensions[iDimIdx]->GetSize() > LIMIT_SAMPLES_ABOVE_2D )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Too many samples along dimension %d",
                     static_cast<int>(iDimIdx));
            return nullptr;
        }
        nExtraDimSamples *= aoDimensions[iDimIdx]->GetSize();
        if( nExtraDimSamples > LIMIT_SAMPLES_ABOVE_2D )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Too many samples along dimensions > 2D");
            return nullptr;
        }
    }

    std::vector<uint32_t> anBlockSize(nDims, 1);
    anBlockSize[nDims-2] = 256;
    anBlockSize[nDims-1] = 256;

    const char* pszBlockSize = CSLFetchNameValue(papszOptions, "BLOCKSIZE");
    if( pszBlockSize )
    {
        const auto aszTokens(CPLStringList(CSLTokenizeString2(pszBlockSize, ",", 0)));
        if( static_cast<size_t>(aszTokens.size()) != nDims )
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Invalid number of values in BLOCKSIZE");
            return nullptr;
        }
        for( size_t i = 0; i < nDims; ++i )
        {
            anBlockSize[i] = static_cast<uint32_t>(CPLAtoGIntBig(aszTokens[i]));
        }
    }

    std::map<std::string, std::string> options;

    // Order matters: we want array specific options to override generic ones
    for( auto papszList: { m_poShared->GetCreationOptions().List(), papszOptions } )
    {
        for( auto papszIter = papszList; papszIter && *papszIter; ++papszIter )
        {
            char* pszKey = nullptr;
            const char* pszValue = CPLParseNameValue(*papszIter, &pszKey);
            if( pszKey && pszValue )
                options[CPLString(pszKey).toupper()] = pszValue;
            CPLFree(pszKey);
        }
    }

    auto array = Array::Create(m_poShared, std::string(), osName,
                               aoDimensions, oDataType, anBlockSize, options);
    if( array )
    {
        m_oMapMDArrays[osName] = array;
        m_poShared->RegisterArray(array.get());
    }
    return array;
}

/************************************************************************/
/*                        ~MultiDimSharedResources()                    */
/************************************************************************/

MultiDimSharedResources::~MultiDimSharedResources()
{
    Crystalize();
    if( hTIFF )
    {
        XTIFFClose(hTIFF);
    }
    if( fpL )
    {
        VSIFCloseL(fpL);
    }
}

/************************************************************************/
/*                 MultiDimSharedResources::Crystalize()                */
/************************************************************************/

bool MultiDimSharedResources::Crystalize()
{
    for (auto it = oSetArrays.cbegin(), next_it = it;
                             it != oSetArrays.cend(); it = next_it)
    {
        ++next_it;
        const bool ok = Crystalize(*it);
        oSetArrays.erase(it);
        if( !ok )
        {
            return false;
        }
    }
    return true;
}

/************************************************************************/
/*                 MultiDimSharedResources::Crystalize()                */
/************************************************************************/

bool MultiDimSharedResources::Crystalize(Array* array)
{
    if( hTIFF == nullptr )
    {
        hTIFF = VSI_TIFFOpen(osFilename.c_str(), "w+", fpL);
        if( hTIFF == nullptr )
            return false;
    }

    return array->CrystalizeFromSharedResources();
}

/************************************************************************/
/*                          GetRootGroup()                              */
/************************************************************************/

std::shared_ptr<GDALGroup> MultiDimDataset::GetRootGroup() const
{
    return m_poRootGroup;
}

/************************************************************************/
/*                        CreateMultiDim()                              */
/************************************************************************/

GDALDataset* MultiDimDataset::CreateMultiDim(const char * pszFilename,
                                             CSLConstList /* papszRootGroupOptions */,
                                             CSLConstList papszOptions )
{
    if( !GTiffOneTimeInit() )
        return nullptr;

    auto poDS = std::unique_ptr<MultiDimDataset>(new MultiDimDataset());

    poDS->SetDescription(pszFilename);
    auto poRG = std::make_shared<Group>(std::string(), nullptr);
    poRG->m_poShared->osFilename = pszFilename;
    poRG->m_poShared->fpL = VSIFOpenL(pszFilename, "wb+");
    if( !poRG->m_poShared->fpL )
    {
        CPLError( CE_Failure, CPLE_OpenFailed,
                  "Cannot create file `%s': %s",
                  pszFilename, VSIStrerror(errno) );
        return nullptr;
    }
    poDS->m_poRootGroup = poRG;
    poRG->m_poShared->bWritable = true;
    poRG->m_poShared->aosCreationOptions = papszOptions;

    return poDS.release();
}

} // namespace gtiff
} // namespace gdal
} // namespace osgeo
