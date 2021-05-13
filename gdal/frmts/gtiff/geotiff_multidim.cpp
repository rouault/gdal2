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

#include "cpl_mem_cache.h"
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
#include "gt_wkt_srs_priv.h"

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
    bool          bHasOptimizedReadMultiRange = false;
    CPLStringList aosCreationOptions{};
    std::set<Array*> oSetArrays{};

    bool Crystalize(Array* array);

public:
    ~MultiDimSharedResources();

    bool IsWritable() const { return bWritable; }
    bool HasOptimizedReadMultiRange() const { return bHasOptimizedReadMultiRange; }
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

    std::vector<std::shared_ptr<GDALDimension>> GetDimensions(CSLConstList papszOptions = nullptr) const override;

    std::vector<std::string> GetMDArrayNames(CSLConstList) const override;
    std::shared_ptr<GDALMDArray> OpenMDArray(const std::string& osName,
                                             CSLConstList papszOptions = nullptr) const override;

    std::shared_ptr<GDALMDArray> CreateMDArray(
               const std::string& osName,
               const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
               const GDALExtendedDataType& oDataType,
               CSLConstList papszOptions) override;

    bool OpenIFD(TIFF* hTIFF);
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
    friend class Group;

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

    mutable std::vector<TIFF*> m_ahTIFF{}; // size should be m_nIFDCount
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

    CPLStringList m_aosStructuralInfo{};

#ifdef SUPPORTS_GET_OFFSET_BYTECOUNT
    mutable lru11::Cache<uint64_t, std::pair<vsi_l_offset, vsi_l_offset>> m_oCacheStrileToOffsetByteCount{1024};
#endif
    mutable std::vector<GByte> m_abyCachedRangeBuffer{};

    bool Crystalize() const;

    std::vector<size_t> IFDIndexToDimIdx(size_t nIFDIndex) const;
    char*               GenerateMetadata(bool mainIFD, const std::vector<size_t>& anDimIdx) const;
    void                WriteGeoTIFFTags( TIFF* hTIFF );
    bool                ValidateIFDConsistency(TIFF* hTIFF,
                                               size_t nIFDIndex,
                                               const std::vector<size_t>& indices) const;
    bool                GetTileOffsetSize( uint32_t nIFDIndex,
                                           uint32_t nBlockId,
                                           vsi_l_offset& nOffset,
                                           vsi_l_offset& nSize ) const;
    void                PreloadData(const std::vector<size_t>& indicesOuterLoop,
                                    std::vector<size_t>& indicesInnerLoop,
                                    std::vector<size_t>& anIFDIndex,
                                    const GUInt64* arrayStartIdx,
                                    const size_t* count,
                                    const GInt64* arrayStep) const;

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

    static std::shared_ptr<Array> CreateFromOpen(
               const std::shared_ptr<MultiDimSharedResources>& poShared,
               const std::string& osParentName,
               const std::string& osName,
               const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
               const GDALExtendedDataType& oType,
               const std::vector<uint32_t>& anBlockSize,
               TIFF* hTIFF);

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

    CSLConstList GetStructuralInfo() const override { return m_aosStructuralInfo.List(); }
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
        if( hTIFF )
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

bool Array::Crystalize() const
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
/*                      Array::CreateFromOpen()                         */
/************************************************************************/

std::shared_ptr<Array> Array::CreateFromOpen(
           const std::shared_ptr<MultiDimSharedResources>& poShared,
           const std::string& osParentName,
           const std::string& osName,
           const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
           const GDALExtendedDataType& oType,
           const std::vector<uint32_t>& anBlockSize,
           TIFF* hTIFF)
{
    auto array(std::shared_ptr<Array>(
        new Array(poShared, osParentName, osName, aoDimensions, oType, anBlockSize)));

    array->m_ahTIFF.resize(array->m_nIFDCount);

    uint16_t nCompression = COMPRESSION_NONE;
    TIFFGetField( hTIFF, TIFFTAG_COMPRESSION, &nCompression);
    array->m_nCompression = nCompression;

    if( nCompression != COMPRESSION_NONE &&
        !TIFFIsCODECConfigured(nCompression) )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Cannot open TIFF file due to missing codec." );
        return nullptr;
    }

    switch( nCompression )
    {
        case COMPRESSION_LZW:
            array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "LZW");
            break;

        case COMPRESSION_DEFLATE:
        case COMPRESSION_ADOBE_DEFLATE:
            array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "DEFLATE");
            break;

        case COMPRESSION_LZMA:
            array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "LZMA");
            break;

        case COMPRESSION_ZSTD:
            array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "ZSTD");
            break;

        case COMPRESSION_LERC:
        {
            array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "LERC");

            uint32_t nAddVersion = LERC_ADD_COMPRESSION_NONE;
            if( TIFFGetField( hTIFF, TIFFTAG_LERC_ADD_COMPRESSION, &nAddVersion ) &&
                nAddVersion != LERC_ADD_COMPRESSION_NONE )
            {
                if( nAddVersion == LERC_ADD_COMPRESSION_DEFLATE )
                {
                    array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "LERC_DEFLATE");
                }
                else if( nAddVersion == LERC_ADD_COMPRESSION_ZSTD )
                {
                    array->m_aosStructuralInfo.SetNameValue("COMPRESSION", "LERC_ZSTD");
                }
            }

            uint32_t nLercVersion = LERC_VERSION_2_4;
            if( TIFFGetField( hTIFF, TIFFTAG_LERC_VERSION, &nLercVersion) )
            {
                if( nLercVersion == LERC_VERSION_2_4 )
                {
                    array->m_aosStructuralInfo.SetNameValue("LERC_VERSION", "2.4");
                }
                else
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "Unknown Lerc version: %d", nLercVersion);
                }
            }
            break;
        }

        default:
            break;
    }

    if( array->m_nCompression == COMPRESSION_LZW ||
        array->m_nCompression == COMPRESSION_ADOBE_DEFLATE ||
        array->m_nCompression == COMPRESSION_ZSTD )
    {
        TIFFGetField( hTIFF, TIFFTAG_PREDICTOR, &array->m_nPredictor);
        if( array->m_nPredictor != PREDICTOR_NONE )
        {
            array->m_aosStructuralInfo.SetNameValue(
                        "PREDICTOR", CPLSPrintf("%d", array->m_nPredictor));
        }
    }

/* -------------------------------------------------------------------- */
/*      Check for NODATA                                                */
/* -------------------------------------------------------------------- */
    char *pszText = nullptr;
    if( TIFFGetField( hTIFF, TIFFTAG_GDAL_NODATA, &pszText ) &&
        !EQUAL(pszText, "") )
    {
        double dfNoDataValue = CPLAtofM( pszText );
        if( array->m_oType.GetNumericDataType() == GDT_Float32 )
        {
            dfNoDataValue = GDALAdjustNoDataCloseToFloatMax(dfNoDataValue);
        }
        if( array->m_pabyNoData )
        {
            array->m_oType.FreeDynamicMemory(&array->m_pabyNoData[0]);
            CPLFree(array->m_pabyNoData);
        }
        array->m_pabyNoData = static_cast<GByte*>(CPLMalloc(array->m_oType.GetSize()));
        GDALExtendedDataType::CopyValue( &dfNoDataValue,
                                         GDALExtendedDataType::Create(GDT_Float64),
                                         array->m_pabyNoData,
                                         array->m_oType );
    }

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
/*                          GTIFFGetDataType()                          */
/************************************************************************/

static GDALDataType GTIFFGetDataType(TIFF* hTIFF)
{
    uint16_t nBitsPerSample = 1;
    TIFFGetField(hTIFF, TIFFTAG_BITSPERSAMPLE, &nBitsPerSample );

    uint16_t nSampleFormat = SAMPLEFORMAT_UINT;
    TIFFGetField( hTIFF, TIFFTAG_SAMPLEFORMAT, &nSampleFormat );

    GDALDataType eDataType = GDT_Unknown;
    if( nBitsPerSample <= 8 )
    {
        if( nSampleFormat == SAMPLEFORMAT_UINT )
            eDataType = GDT_Byte;
    }
    else if( nBitsPerSample <= 16 )
    {
        if( nSampleFormat == SAMPLEFORMAT_INT )
            eDataType = GDT_Int16;
        else
            eDataType = GDT_UInt16;
    }
    else if( nBitsPerSample == 32 )
    {
        if( nSampleFormat == SAMPLEFORMAT_COMPLEXINT )
            eDataType = GDT_CInt16;
        else if( nSampleFormat == SAMPLEFORMAT_IEEEFP )
            eDataType = GDT_Float32;
        else if( nSampleFormat == SAMPLEFORMAT_INT )
            eDataType = GDT_Int32;
        else
            eDataType = GDT_UInt32;
    }
    else if( nBitsPerSample == 64 )
    {
        if( nSampleFormat == SAMPLEFORMAT_IEEEFP )
            eDataType = GDT_Float64;
        else if( nSampleFormat == SAMPLEFORMAT_COMPLEXIEEEFP )
            eDataType = GDT_CFloat32;
        else if( nSampleFormat == SAMPLEFORMAT_COMPLEXINT )
            eDataType = GDT_CInt32;
    }
    else if( nBitsPerSample == 128 )
    {
        if( nSampleFormat == SAMPLEFORMAT_COMPLEXIEEEFP )
            eDataType = GDT_CFloat64;
    }

    return eDataType;
}

/************************************************************************/
/*                   Array::ValidateIFDConsistency()                    */
/************************************************************************/

bool Array::ValidateIFDConsistency(TIFF* hTIFF,
                                   size_t nIFDIndex,
                                   const std::vector<size_t>& indices) const
{

    uint16_t nSamplesPerPixel = 1;
    TIFFGetField(hTIFF, TIFFTAG_SAMPLESPERPIXEL, &nSamplesPerPixel);
    if( nSamplesPerPixel != 1 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "SamplesPerPixel != 1 not supported in multidimensional API");
        return false;
    }

    uint16_t nBitsPerSample = 1;
    TIFFGetField(hTIFF, TIFFTAG_BITSPERSAMPLE, &nBitsPerSample );
    if( nBitsPerSample != 8 && nBitsPerSample != 16 && nBitsPerSample != 32 &&
        nBitsPerSample != 64 && nBitsPerSample != 128 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "BitsPerSample != 8, 16, 32, 64 and 128 not supported "
                 "in multidimensional API");
        return false;
    }

    const auto eDataType = GTIFFGetDataType(hTIFF);
    if( eDataType == GDT_Unknown )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Unsupported combination of BitsPerSample and SampleFormat");
        return false;
    }
    if( eDataType != m_oType.GetNumericDataType() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Data type of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    if( !TIFFIsTiled(hTIFF) )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Only tiled files supported in multidimensional API");
        return false;
    }

    const auto nDims = m_aoDims.size();

    uint32_t nYSize = 0;
    TIFFGetField( hTIFF, TIFFTAG_IMAGELENGTH, &nYSize );
    if( nYSize != m_aoDims[nDims-2]->GetSize() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Image height of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    uint32_t nBlockYSize = 0;
    TIFFGetField( hTIFF, TIFFTAG_TILELENGTH, &nBlockYSize );
    if( nBlockYSize != m_anBlockSize[nDims-2] )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Block height of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    uint32_t nXSize = 0;
    TIFFGetField( hTIFF, TIFFTAG_IMAGEWIDTH, &nXSize );
    if( nXSize != m_aoDims[nDims-1]->GetSize() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Image width of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    uint32_t nBlockXSize = 0;
    TIFFGetField( hTIFF, TIFFTAG_TILEWIDTH, &nBlockXSize );
    if( nBlockXSize != m_anBlockSize[nDims-1] )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Block width of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    std::map<std::string, std::string> oMapDimMetadata;
    std::string osVarName;

    char* pszText = nullptr; /* not to be freed */
    if( TIFFGetField( hTIFF, TIFFTAG_GDAL_METADATA, &pszText ) )
    {
        CPLXMLNode *psRoot = CPLParseXMLString( pszText );
        CPLXMLNode *psItem = nullptr;

        if( psRoot != nullptr && psRoot->eType == CXT_Element
            && EQUAL(psRoot->pszValue,"GDALMetadata") )
            psItem = psRoot->psChild;

        for( ; psItem != nullptr; psItem = psItem->psNext )
        {

            if( psItem->eType != CXT_Element
                || !EQUAL(psItem->pszValue,"Item") )
                continue;

            const char *pszKey = CPLGetXMLValue( psItem, "name", nullptr );
            const char *pszValue = CPLGetXMLValue( psItem, nullptr, nullptr );
            const char *pszDomain = CPLGetXMLValue( psItem, "domain", "" );
            if( !EQUAL(pszDomain, "") )
                continue;

            if( pszKey == nullptr || pszValue == nullptr )
                continue;

            // Note: this un-escaping should not normally be done, as the deserialization
            // of the tree from XML also does it, so we end up width double XML escaping,
            // but keep it for backward compatibility.
            char *pszUnescapedValue =
                CPLUnescapeString( pszValue, nullptr, CPLES_XML );

            if( EQUAL(pszKey, "VARIABLE_NAME") )
            {
                osVarName = pszUnescapedValue;
            }
            else
            {
                if( STARTS_WITH_CI(pszKey, "DIMENSION_") )
                {
                    oMapDimMetadata[pszKey] = pszValue;
                }
            }

            CPLFree( pszUnescapedValue );
        }

        CPLDestroyXMLNode( psRoot );
    }

    if( osVarName != GetName() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "VARIABLE_NAME of IFD %u not consistent with main one",
                 static_cast<unsigned>(nIFDIndex));
        return false;
    }

    for( int i = 0; i < static_cast<int>(nDims) - 2; ++i )
    {
        std::string osKeyPrefix("DIMENSION_");
        osKeyPrefix += std::to_string(i);
        osKeyPrefix += '_';

        const auto osDimName = oMapDimMetadata[osKeyPrefix + "NAME"];
        if( osDimName.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s in IFD %u",
                     (osKeyPrefix + "NAME").c_str(),
                     static_cast<unsigned>(nIFDIndex));
            return false;
        }
        if( osDimName != m_aoDims[i]->GetName() )
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "DIMENSION_%d_NAME of IFD %u not consistent with main one",
                     i,
                     static_cast<unsigned>(nIFDIndex));
            return false;
        }

        const auto osDimIdx = oMapDimMetadata[osKeyPrefix + "IDX"];
        if( osDimIdx.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s in IFD %u",
                     (osKeyPrefix + "IDX").c_str(),
                     static_cast<unsigned>(nIFDIndex));
            return false;
        }

        const auto nGotDimIdx = static_cast<unsigned>(atoi(osDimIdx.c_str()));
        if( nGotDimIdx != indices[i] )
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "DIMENSION_%d_IDX=%u in IFD %u instead of %u",
                     i,
                     nGotDimIdx,
                     static_cast<unsigned>(nIFDIndex),
                     static_cast<unsigned>(indices[i]));
            return false;
        }
    }

    return true;
}

/************************************************************************/
/*                        GetTileOffsetSize()                           */
/************************************************************************/

bool Array::GetTileOffsetSize( uint32_t nIFDIndex,
                               uint32_t nBlockId,
                               vsi_l_offset& nOffset,
                               vsi_l_offset& nSize ) const

{
    TIFF* hTIFF = m_ahTIFF[nIFDIndex];
    CPLAssert(hTIFF);

#ifdef SUPPORTS_GET_OFFSET_BYTECOUNT
    // Optimization to avoid fetching the whole Strip/TileCounts and
    // Strip/TileOffsets arrays.
    std::pair<vsi_l_offset, vsi_l_offset> oPair;
    const uint64_t key = (static_cast<uint64_t>(nIFDIndex) << 32) | nBlockId;
    if( m_oCacheStrileToOffsetByteCount.tryGet(key, oPair) )
    {
        nOffset = oPair.first;
        nSize = oPair.second;
        return oPair.first != 0;
    }

    int nErrOccurred = 0;
    nSize = TIFFGetStrileByteCountWithErr(hTIFF, nBlockId, &nErrOccurred);
    if( nErrOccurred )
        return false;
    nOffset = TIFFGetStrileOffsetWithErr(hTIFF, nBlockId, &nErrOccurred);
    if( nErrOccurred )
        return false;

    m_oCacheStrileToOffsetByteCount.insert(
                key,
                std::pair<vsi_l_offset, vsi_l_offset>(nOffset, nSize));
    return true;
#else
    toff_t *panByteCounts = nullptr;
    toff_t *panOffsets = nullptr;
    const bool bIsTiled = CPL_TO_BOOL( TIFFIsTiled(hTIFF) );

    if( ( bIsTiled
          && TIFFGetField( hTIFF, TIFFTAG_TILEBYTECOUNTS, &panByteCounts )
          && TIFFGetField( hTIFF, TIFFTAG_TILEOFFSETS, &panOffsets ) )
        || ( !bIsTiled
          && TIFFGetField( hTIFF, TIFFTAG_STRIPBYTECOUNTS, &panByteCounts )
          && TIFFGetField( hTIFF, TIFFTAG_STRIPOFFSETS, &panOffsets ) ) )
    {
        if( panByteCounts == nullptr || panOffsets == nullptr )
        {
            return false;
        }

        const uint32_t nBlockCount =
            bIsTiled ? TIFFNumberOfTiles(hTIFF) : TIFFNumberOfStrips(hTIFF);
        assert( nBlockId < nBlockCount );

        nOffset = panOffsets[nBlockId];
        nSize = panByteCounts[nBlockId];
        return true;
    }

    return false;
#endif
}

/************************************************************************/
/*                       Array::PreloadData()                           */
/************************************************************************/

void Array::PreloadData(const std::vector<size_t>& indicesOuterLoop,
                        std::vector<size_t>& indicesInnerLoop,
                        std::vector<size_t>& anIFDIndex,
                        const GUInt64* arrayStartIdx,
                        const size_t* count,
                        const GInt64* arrayStep) const
{
    const auto nDims = GetDimensionCount();
    const size_t iDimY = nDims - 2;
    const size_t iDimX = nDims - 1;
    const uint32_t nTileXCount = static_cast<uint32_t>(
        (m_aoDims[iDimX]->GetSize() + m_anBlockSize[iDimX] - 1) / m_anBlockSize[iDimX]);
    size_t dimIdxSubLoop = 0;
    anIFDIndex[0] = 0;
    std::map<vsi_l_offset, std::pair<size_t, size_t>> oMapOffsetToIFDAndSize;
    size_t nAccBufferOffset = 0;
    constexpr size_t MAX_SIZE_TO_CACHE = 100 * 1024 * 1024;

#ifdef DEBUG_VERBOSE
    CPLDebug("GDAL", "PreloadData()");
#endif

    // First step: build a list of (ifd_index, file_offset, size) chunks we
    // will need (in oMapOffsetToIFDAndSize)

lbl_next_depth_inner_loop:
    if( dimIdxSubLoop == iDimY )
    {
        // Now we have a 2D tile !
        const size_t nIFDIndex = anIFDIndex[iDimY];
        assert( nIFDIndex < m_ahTIFF.size() );
        if( m_ahTIFF[nIFDIndex] == nullptr )
        {
            TIFF* hTIFF = VSI_TIFFOpenChild(m_poShared->GetTIFFHandle());
            if( hTIFF == nullptr )
                return;
            if( TIFFSetDirectory(hTIFF, static_cast<uint16_t>(nIFDIndex)) == 0 ||
                !ValidateIFDConsistency(hTIFF, nIFDIndex, indicesInnerLoop) )
            {
                XTIFFClose(hTIFF);
                return;
            }
            m_ahTIFF[nIFDIndex] = hTIFF;
        }

        const uint32_t iYTile = static_cast<uint32_t>(
                            indicesOuterLoop[iDimY] / m_anBlockSize[iDimY]);
        const uint32_t iXTile = static_cast<uint32_t>(
                            indicesOuterLoop[iDimX] / m_anBlockSize[iDimX]);
        const uint32_t nTileID = iYTile * nTileXCount + iXTile;
        vsi_l_offset nOffset = 0;
        vsi_l_offset nSize = 0;
        if( !GetTileOffsetSize(static_cast<uint32_t>(nIFDIndex), nTileID,
                               nOffset, nSize) )
        {
            return;
        }

#ifdef DEBUG_VERBOSE
        CPLDebug("GDAL", "IFD %u, tile=(%u,%u), offset=" CPL_FRMT_GUIB ", size=%u",
                 static_cast<unsigned>(nIFDIndex), iYTile, iXTile,
                 static_cast<GUIntBig>(nOffset),
                 static_cast<unsigned>(nSize));
#endif

        // Limit the total size to read
        if( nSize > MAX_SIZE_TO_CACHE - nAccBufferOffset )
        {
            goto after_loop;
        }
        nAccBufferOffset += static_cast<size_t>(nSize);

        if( nSize != 0 )
        {
            oMapOffsetToIFDAndSize[nOffset] = std::pair<size_t, size_t>(
                                        nIFDIndex, static_cast<size_t>(nSize));
        }
    }
    else
    {
        // This level of loop loops over individual samples, within a
        // block, in the non-2D indices.
        indicesInnerLoop[dimIdxSubLoop] = indicesOuterLoop[dimIdxSubLoop];
        anIFDIndex[dimIdxSubLoop] *=
            static_cast<size_t>(m_aoDims[dimIdxSubLoop]->GetSize());
        anIFDIndex[dimIdxSubLoop] += indicesOuterLoop[dimIdxSubLoop];
        while(true)
        {
            dimIdxSubLoop ++;
            anIFDIndex[dimIdxSubLoop] = anIFDIndex[dimIdxSubLoop-1];
            goto lbl_next_depth_inner_loop;
lbl_return_to_caller_inner_loop:
            dimIdxSubLoop --;
            if( arrayStep[dimIdxSubLoop] == 0 )
            {
                break;
            }
            const auto oldBlock = indicesOuterLoop[dimIdxSubLoop] / m_anBlockSize[dimIdxSubLoop];
            indicesInnerLoop[dimIdxSubLoop] = static_cast<size_t>(
                indicesInnerLoop[dimIdxSubLoop] + arrayStep[dimIdxSubLoop]);
            if( (indicesInnerLoop[dimIdxSubLoop] /
                    m_anBlockSize[dimIdxSubLoop]) != oldBlock ||
                indicesInnerLoop[dimIdxSubLoop] >
                    arrayStartIdx[dimIdxSubLoop] + (count[dimIdxSubLoop]-1) * arrayStep[dimIdxSubLoop] )
            {
                break;
            }
            anIFDIndex[dimIdxSubLoop] = static_cast<size_t>(
                anIFDIndex[dimIdxSubLoop] + arrayStep[dimIdxSubLoop]);
        }
    }
    if( dimIdxSubLoop > 0 )
        goto lbl_return_to_caller_inner_loop;
after_loop:

    // Clear existing cached ranges
    for( auto& hTIFF: m_ahTIFF )
    {
        if( hTIFF )
        {
            VSI_TIFFSetCachedRanges( TIFFClientdata( hTIFF ),
                                     0, nullptr, nullptr, nullptr );
        }
    }

    try
    {
        m_abyCachedRangeBuffer.resize(nAccBufferOffset);
    }
    catch( const std::exception& )
    {
        return;
    }

    // Iterate over our map to aggregate consecutive ranges of data

    // RMM stands for ReadMultiRange
    std::vector<vsi_l_offset> anFileOffsetsForRMM;
    std::vector<size_t> anSizesForRMM;
    std::vector<void*> apDataForRMM;

    nAccBufferOffset = 0;
    for( const auto& kv: oMapOffsetToIFDAndSize )
    {
        const auto nOffset = kv.first;
        const auto nSize = kv.second.second;

        if( !anFileOffsetsForRMM.empty() &&
            anFileOffsetsForRMM.back() + anSizesForRMM.back() == nOffset )
        {
            anSizesForRMM.back() += nSize;
        }
        else
        {
            anFileOffsetsForRMM.push_back(nOffset);
            anSizesForRMM.push_back(nSize);
            apDataForRMM.push_back( &m_abyCachedRangeBuffer[nAccBufferOffset] );
        }
        nAccBufferOffset += nSize;
    }

    // Read from the file
    VSILFILE* fp = VSI_TIFFGetVSILFile(TIFFClientdata( m_poShared->GetTIFFHandle() ));
    const int nRet =
        VSIFReadMultiRangeL( static_cast<int>(apDataForRMM.size()),
                             apDataForRMM.data(),
                             anFileOffsetsForRMM.data(),
                             anSizesForRMM.data(), fp );
    if( nRet != 0 )
        return;

    // Dispatch the ranges among IFDs
    struct RangesForTIFF_VSI
    {
        std::vector<vsi_l_offset> anFileOffsets{};
        std::vector<size_t> anSizes{};
        std::vector<void*> apData{};
    };
    std::map<size_t, RangesForTIFF_VSI> oMapIFDIndexToRanges;

    nAccBufferOffset = 0;
    for( const auto& kv: oMapOffsetToIFDAndSize )
    {
        const auto nOffset = kv.first;
        const auto nIFDIndex = kv.second.first;
        const auto nSize = kv.second.second;

        auto iter = oMapIFDIndexToRanges.find(nIFDIndex);

        if( iter == oMapIFDIndexToRanges.end() )
        {
            oMapIFDIndexToRanges[nIFDIndex].anFileOffsets.push_back(nOffset);
            oMapIFDIndexToRanges[nIFDIndex].anSizes.push_back(nSize);
            oMapIFDIndexToRanges[nIFDIndex].apData.push_back(
                                &m_abyCachedRangeBuffer[nAccBufferOffset] );
        }
        else if( iter->second.anFileOffsets.back() +
                        iter->second.anSizes.back() == nOffset )
        {
            iter->second.anSizes.back() += nSize;
        }
        else
        {
            iter->second.anFileOffsets.push_back(nOffset);
            iter->second.anSizes.push_back(nSize);
            iter->second.apData.push_back(
                                &m_abyCachedRangeBuffer[nAccBufferOffset] );
        }

        nAccBufferOffset += nSize;
    }

    // Push the data to the VSI_TIFF layer
    for( const auto& kv: oMapIFDIndexToRanges )
    {
        const auto nIFDIndex = kv.first;
        TIFF* hTIFF = m_ahTIFF[nIFDIndex];
        const auto& ranges = kv.second;
        VSI_TIFFSetCachedRanges( TIFFClientdata( hTIFF ),
                                 static_cast<int>(ranges.anFileOffsets.size()),
                                 ranges.apData.data(),
                                 ranges.anFileOffsets.data(),
                                 ranges.anSizes.data());
    }
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
    if( !Crystalize() )
        return false;

    if( bufferDataType.GetClass() != GEDTC_NUMERIC )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Incompatible buffer data type.");
        return false;
    }

    // Need to be kept in top-level scope
    std::vector<GUInt64> arrayStartIdxMod;
    std::vector<GInt64> arrayStepMod;
    std::vector<GPtrDiff_t> bufferStrideMod;

    const size_t nDims = m_aoDims.size();
    bool negativeStep = false;
    for( size_t i = 0; i < nDims; ++i )
    {
        if( arrayStep[i] < 0 )
        {
            negativeStep = true;
            break;
        }
    }

    const auto eBufferDT = bufferDataType.GetNumericDataType();
    const auto nBufferDTSize = static_cast<int>(bufferDataType.GetSize());

    // Make sure that arrayStep[i] are positive for sake of simplicity
    if( negativeStep )
    {
        arrayStartIdxMod.resize(nDims);
        arrayStepMod.resize(nDims);
        bufferStrideMod.resize(nDims);
        for( size_t i = 0; i < nDims; ++i )
        {
            if( arrayStep[i] < 0 )
            {
                arrayStartIdxMod[i] = arrayStartIdx[i] - (count[i] - 1) * (-arrayStep[i]);
                arrayStepMod[i] = -arrayStep[i];
                bufferStrideMod[i] = -bufferStride[i];
                pDstBuffer = static_cast<GByte*>(pDstBuffer) +
                    bufferStride[i] * static_cast<GPtrDiff_t>(nBufferDTSize * (count[i] - 1));
            }
            else
            {
                arrayStartIdxMod[i] = arrayStartIdx[i];
                arrayStepMod[i] = arrayStep[i];
                bufferStrideMod[i] = bufferStride[i];
            }
        }
        arrayStartIdx = arrayStartIdxMod.data();
        arrayStep = arrayStepMod.data();
        bufferStride = bufferStrideMod.data();
    }

    const size_t iDimY = nDims - 2;
    const size_t iDimX = nDims - 1;

    const auto nDstXIncLarge = bufferStride[iDimX] * nBufferDTSize;
    if( nDstXIncLarge > std::numeric_limits<int>::max() ||
        nDstXIncLarge < std::numeric_limits<int>::min() )
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Too large buffer stride");
        return false;
    }
    const int nDstXInc = static_cast<int>(nDstXIncLarge);

    std::vector<size_t> indicesOuterLoop(nDims);
    std::vector<GByte*> dstPtrStackOuterLoop(nDims + 1);

    std::vector<size_t> indicesInnerLoop(nDims - 2);
    std::vector<GByte*> dstPtrStackInnerLoop(nDims - 2 + 1);
    std::vector<size_t> anIFDIndex(nDims - 2 + 1);

    // Reserve a buffer for 2D tile content
    const GDALDataType eDT = m_oType.GetNumericDataType();
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

    size_t dimIdx = 0;
    dstPtrStackOuterLoop[0] = static_cast<GByte*>(pDstBuffer);
lbl_next_depth:
    if( dimIdx == nDims )
    {
        if( m_poShared->HasOptimizedReadMultiRange() )
        {
            PreloadData(indicesOuterLoop, indicesInnerLoop, anIFDIndex,
                        arrayStartIdx, count, arrayStep);
        }

        size_t dimIdxSubLoop = 0;
        dstPtrStackInnerLoop[0] = dstPtrStackOuterLoop[nDims];
        anIFDIndex[0] = 0;
lbl_next_depth_inner_loop:
        if( dimIdxSubLoop == iDimY )
        {
            // Now we have a 2D tile !
            const size_t nIFDIndex = anIFDIndex[iDimY];
            assert( nIFDIndex < m_ahTIFF.size() );
            TIFF* hTIFF = m_ahTIFF[nIFDIndex];
            if( hTIFF == nullptr )
            {
                hTIFF = VSI_TIFFOpenChild(m_poShared->GetTIFFHandle());
                if( hTIFF == nullptr )
                    return false;
                if( TIFFSetDirectory(hTIFF, static_cast<uint16_t>(nIFDIndex)) == 0 ||
                    !ValidateIFDConsistency(hTIFF, nIFDIndex, indicesInnerLoop) )
                {
                    XTIFFClose(hTIFF);
                    return false;
                }
                m_ahTIFF[nIFDIndex] = hTIFF;
            }

            const uint32_t iYTile = static_cast<uint32_t>(
                                indicesOuterLoop[iDimY] / m_anBlockSize[iDimY]);
            const uint32_t iXTile = static_cast<uint32_t>(
                                indicesOuterLoop[iDimX] / m_anBlockSize[iDimX]);

            const uint32_t nTile = iYTile * nTileXCount + iXTile;
            if( TIFFReadEncodedTile(hTIFF, nTile, &abyTileData[0],
                                    nTileDataSize) != nTileDataSize )
            {
                return false;
            }

            const uint32_t nYOffsetInTile = static_cast<uint32_t>(
                            indicesOuterLoop[iDimY] % m_anBlockSize[iDimY]);
            const uint32_t nYNextTile = static_cast<uint32_t>(
                indicesOuterLoop[iDimY] + m_anBlockSize[iDimY] - nYOffsetInTile);
            const auto nLastYCoord = static_cast<uint32_t>(arrayStartIdx[iDimY] + (count[iDimY]-1) * arrayStep[iDimY]);
            const auto arrayStepY = arrayStep[iDimY] ? arrayStep[iDimY] : 1;
            const uint32_t nYIters =
                static_cast<uint32_t>((std::min(nYNextTile, nLastYCoord+1) -
                    indicesOuterLoop[iDimY] + arrayStepY - 1) / arrayStepY);
            //assert( indicesOuterLoop[iDimY] + (nYIters - 1) * arrayStep[iDimY] <= nLastYCoord );
            //assert( indicesOuterLoop[iDimY] + (nYIters - 1) * arrayStep[iDimY] < nYNextTile );

            const uint32_t nXOffsetInTile = static_cast<uint32_t>(
                            indicesOuterLoop[iDimX] % m_anBlockSize[iDimX]);
            const uint32_t nXNextTile = static_cast<uint32_t>(
                indicesOuterLoop[iDimX] + m_anBlockSize[iDimX] - nXOffsetInTile);
            const auto nLastXCoord = static_cast<uint32_t>(arrayStartIdx[iDimX] + (count[iDimX]-1) * arrayStep[iDimX]);
            const auto arrayStepX = arrayStep[iDimX] ? arrayStep[iDimX] : 1;
            const uint32_t nXIters =
                static_cast<uint32_t>((std::min(nXNextTile, nLastXCoord+1) -
                    indicesOuterLoop[iDimX] + arrayStepX - 1) / arrayStepX);
            //assert( indicesOuterLoop[iDimX] + (nXIters - 1) * arrayStep[iDimX] <= nLastXCoord );
            //assert( indicesOuterLoop[iDimX] + (nXIters - 1) * arrayStep[iDimX] < nXNextTile );

            GByte* dst_ptr_row = dstPtrStackInnerLoop[dimIdxSubLoop];
            const GByte* src_ptr_row = abyTileData.data() +
                (nYOffsetInTile * m_anBlockSize[iDimX] + nXOffsetInTile) * nDTSize;

            for( uint32_t y = 0; y < nYIters; ++y )
            {
                GDALCopyWords64(src_ptr_row, eDT,
                                static_cast<int>(nDTSize * arrayStep[iDimX]),
                                dst_ptr_row, eBufferDT, nDstXInc,
                                static_cast<GPtrDiff_t>(nXIters));

                dst_ptr_row += bufferStride[iDimY] * nBufferDTSize;
                src_ptr_row += arrayStep[iDimY] * m_anBlockSize[iDimX] * nDTSize;
            }
        }
        else
        {
            // This level of loop loops over individual samples, within a
            // block, in the non-2D indices.
            indicesInnerLoop[dimIdxSubLoop] = indicesOuterLoop[dimIdxSubLoop];
            anIFDIndex[dimIdxSubLoop] *=
                static_cast<size_t>(m_aoDims[dimIdxSubLoop]->GetSize());
            anIFDIndex[dimIdxSubLoop] += indicesOuterLoop[dimIdxSubLoop];
            while(true)
            {
                dimIdxSubLoop ++;
                anIFDIndex[dimIdxSubLoop] = anIFDIndex[dimIdxSubLoop-1];
                dstPtrStackInnerLoop[dimIdxSubLoop] = dstPtrStackInnerLoop[dimIdxSubLoop-1];
                goto lbl_next_depth_inner_loop;
lbl_return_to_caller_inner_loop:
                dimIdxSubLoop --;
                if( arrayStep[dimIdxSubLoop] == 0 )
                {
                    break;
                }
                const auto oldBlock = indicesOuterLoop[dimIdxSubLoop] / m_anBlockSize[dimIdxSubLoop];
                indicesInnerLoop[dimIdxSubLoop] = static_cast<size_t>(
                    indicesInnerLoop[dimIdxSubLoop] + arrayStep[dimIdxSubLoop]);
                if( (indicesInnerLoop[dimIdxSubLoop] /
                        m_anBlockSize[dimIdxSubLoop]) != oldBlock ||
                    indicesInnerLoop[dimIdxSubLoop] >
                        arrayStartIdx[dimIdxSubLoop] + (count[dimIdxSubLoop]-1) * arrayStep[dimIdxSubLoop] )
                {
                    break;
                }
                dstPtrStackInnerLoop[dimIdxSubLoop] +=
                    bufferStride[dimIdxSubLoop] * static_cast<GPtrDiff_t>(nBufferDTSize);
                anIFDIndex[dimIdxSubLoop] = static_cast<size_t>(
                    anIFDIndex[dimIdxSubLoop] + arrayStep[dimIdxSubLoop]);
            }
        }
        if( dimIdxSubLoop > 0 )
            goto lbl_return_to_caller_inner_loop;
    }
    else
    {
        // This level of loop loops over blocks
        indicesOuterLoop[dimIdx] = static_cast<size_t>(arrayStartIdx[dimIdx]);
        while(true)
        {
            dimIdx ++;
            dstPtrStackOuterLoop[dimIdx] = dstPtrStackOuterLoop[dimIdx - 1];
            goto lbl_next_depth;
lbl_return_to_caller:
            dimIdx --;
            if( count[dimIdx] == 1 )
                break;

            size_t nIncr;
            if( arrayStep[dimIdx] < m_anBlockSize[dimIdx] )
            {
                // Compute index at next block boundary
                auto newIdx = indicesOuterLoop[dimIdx] +
                    (m_anBlockSize[dimIdx] - (indicesOuterLoop[dimIdx] % m_anBlockSize[dimIdx]));
                // And round up compared to arrayStartIdx, arrayStep
                nIncr = static_cast<size_t>(
                    (newIdx - indicesOuterLoop[dimIdx] + arrayStep[dimIdx] - 1) / arrayStep[dimIdx]);
            }
            else
            {
                nIncr = 1;
            }
            indicesOuterLoop[dimIdx] = static_cast<size_t>(
                indicesOuterLoop[dimIdx] + nIncr * arrayStep[dimIdx]);
            if( indicesOuterLoop[dimIdx] > arrayStartIdx[dimIdx] + (count[dimIdx]-1) * arrayStep[dimIdx] )
                break;
            dstPtrStackOuterLoop[dimIdx] += bufferStride[dimIdx] * static_cast<GPtrDiff_t>(nIncr * nBufferDTSize);
        }
    }
    if( dimIdx > 0 )
        goto lbl_return_to_caller;

    return true;
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
/*                            GetDimensions()                           */
/************************************************************************/

std::vector<std::shared_ptr<GDALDimension>> Group::GetDimensions(CSLConstList) const
{
    std::vector<std::shared_ptr<GDALDimension>> oRes;
    for( const auto& oIter: m_oMapDimensions )
    {
        oRes.push_back(oIter.second);
    }
    return oRes;
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
/*                             OpenMDArray()                            */
/************************************************************************/

std::shared_ptr<GDALMDArray> Group::OpenMDArray(const std::string& osName,
                                                CSLConstList) const
{
    auto oIter = m_oMapMDArrays.find(osName);
    if( oIter != m_oMapMDArrays.end() )
        return oIter->second;
    return nullptr;
}

/************************************************************************/
/*                         Group::OpenIFD()                             */
/************************************************************************/

bool Group::OpenIFD(TIFF* hTIFF)
{
    std::string osVarName( CPLGetBasename(m_poShared->GetFilename().c_str()) );

    uint16_t nSamplesPerPixel = 1;
    TIFFGetField(hTIFF, TIFFTAG_SAMPLESPERPIXEL, &nSamplesPerPixel);
    if( nSamplesPerPixel != 1 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "SamplesPerPixel != 1 not supported in multidimensional API");
        return false;
    }

    uint16_t nBitsPerSample = 1;
    TIFFGetField(hTIFF, TIFFTAG_BITSPERSAMPLE, &nBitsPerSample );
    if( nBitsPerSample != 8 && nBitsPerSample != 16 && nBitsPerSample != 32 &&
        nBitsPerSample != 64 && nBitsPerSample != 128 )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "BitsPerSample != 8, 16, 32, 64 and 128 not supported "
                 "in multidimensional API");
        return false;
    }

    const auto eDataType = GTIFFGetDataType(hTIFF);
    if( eDataType == GDT_Unknown )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Unsupported combination of BitsPerSample and SampleFormat");
        return false;
    }

    if( !TIFFIsTiled(hTIFF) )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Only tiled files supported in multidimensional API");
        return false;
    }

    std::map<std::string, std::string> oMapDimMetadata;
    std::map<std::string, std::string> oMapAttributesStr;
    int nDimensions = 2;
    bool bHasScale = false;
    double dfScale = 1.0;
    bool bHasOffset = false;
    double dfOffset = 0.0;
    std::string osUnitType;

    char* pszText = nullptr; /* not to be freed */
    if( TIFFGetField( hTIFF, TIFFTAG_GDAL_METADATA, &pszText ) )
    {
        CPLXMLNode *psRoot = CPLParseXMLString( pszText );
        CPLXMLNode *psItem = nullptr;

        if( psRoot != nullptr && psRoot->eType == CXT_Element
            && EQUAL(psRoot->pszValue,"GDALMetadata") )
            psItem = psRoot->psChild;

        for( ; psItem != nullptr; psItem = psItem->psNext )
        {

            if( psItem->eType != CXT_Element
                || !EQUAL(psItem->pszValue,"Item") )
                continue;

            const char *pszKey = CPLGetXMLValue( psItem, "name", nullptr );
            const char *pszValue = CPLGetXMLValue( psItem, nullptr, nullptr );
            const char *pszRole = CPLGetXMLValue( psItem, "role", "" );
            const char *pszDomain = CPLGetXMLValue( psItem, "domain", "" );
            if( !EQUAL(pszDomain, "") )
                continue;

            if( pszKey == nullptr || pszValue == nullptr )
                continue;

            // Note: this un-escaping should not normally be done, as the deserialization
            // of the tree from XML also does it, so we end up width double XML escaping,
            // but keep it for backward compatibility.
            char *pszUnescapedValue =
                CPLUnescapeString( pszValue, nullptr, CPLES_XML );

            if( EQUAL(pszRole,"scale") )
            {
                bHasScale = true;
                dfScale = CPLAtofM(pszUnescapedValue);
            }
            else if( EQUAL(pszRole,"offset") )
            {
                bHasOffset = true;
                dfOffset = CPLAtofM(pszUnescapedValue);
            }
            else if( EQUAL(pszRole,"unittype") )
            {
                osUnitType = pszUnescapedValue;
            }
            else if( !EQUAL(pszRole, "") )
            {
                // do nothing
            }
            else if( EQUAL(pszKey, "VARIABLE_NAME") )
            {
                osVarName = pszUnescapedValue;
            }
            else
            {
                if( STARTS_WITH_CI(pszKey, "DIMENSION_") )
                {
                    nDimensions = std::max(nDimensions,
                                           1 + atoi(pszKey + strlen("DIMENSION_")));
                    oMapDimMetadata[pszKey] = pszValue;
                }
                else
                {
                    oMapAttributesStr[pszKey] = pszValue;
                }
            }

            CPLFree( pszUnescapedValue );
        }

        CPLDestroyXMLNode( psRoot );
    }

    std::vector<std::shared_ptr<GDALDimension>> aoDimensions;
    std::vector<uint32_t> anBlockSize;
    std::vector<std::shared_ptr<GDALMDArray>> apoIndexingVars;

    for( int i = 0; i < nDimensions - 2; ++i )
    {
        std::string osKeyPrefix("DIMENSION_");
        osKeyPrefix += std::to_string(i);
        osKeyPrefix += '_';

        const auto osDimName = oMapDimMetadata[osKeyPrefix + "NAME"];
        if( osDimName.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                     (osKeyPrefix + "NAME").c_str());
            return false;
        }

        const auto osDimSize = oMapDimMetadata[osKeyPrefix + "SIZE"];
        if( osDimSize.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                     (osKeyPrefix + "SIZE").c_str());
            return false;
        }

        const GUInt64 nDimSize = static_cast<GUInt64>(
                                            CPLAtoGIntBig(osDimSize.c_str()));
        if( nDimSize == 0 )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid value for %s",
                     (osKeyPrefix + "SIZE").c_str());
            return false;
        }

        const auto osDimIdx = oMapDimMetadata[osKeyPrefix + "IDX"];
        if( osDimIdx.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                     (osKeyPrefix + "IDX").c_str());
            return false;
        }

        if( osDimIdx != "0" )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid value for %s",
                     (osKeyPrefix + "IDX").c_str());
            return false;
        }

        const auto osDimDataType = oMapDimMetadata[osKeyPrefix + "DATATYPE"];
        const auto osDimDirection = oMapDimMetadata[osKeyPrefix + "DIRECTION"];
        const auto osDimType = oMapDimMetadata[osKeyPrefix + "TYPE"];
        const auto osDimValues = oMapDimMetadata[osKeyPrefix + "VALUES"];

        const auto osDimBlockSize = oMapDimMetadata[osKeyPrefix + "BLOCK_SIZE"];
        if( osDimBlockSize.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                     (osKeyPrefix + "BLOCK_SIZE").c_str());
            return false;
        }
        const uint32_t nDimBlockSize = static_cast<uint32_t>(
                                        CPLAtoGIntBig(osDimBlockSize.c_str()));

        // Create dimension
        auto poDim = std::make_shared<Dimension>(GetFullName(), osDimName,
                                                 osDimType, osDimDirection,
                                                 nDimSize);
        aoDimensions.emplace_back(poDim);
        anBlockSize.emplace_back(nDimBlockSize);

        // Create indexing variable

        if( !osDimDataType.empty() || !osDimValues.empty() )
        {
            if( osDimDataType.empty() )
            {
                CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                         (osKeyPrefix + "DATATYPE").c_str());
                return false;
            }

            const auto eDimDT = GDALGetDataTypeByName( osDimDataType.c_str() );
            if( eDimDT == GDT_Unknown )
            {
                CPLError(CE_Failure, CPLE_AppDefined, "Invalid value for %s",
                         (osKeyPrefix + "DATATYPE").c_str());
                return false;
            }

            if( osDimValues.empty() )
            {
                CPLError(CE_Failure, CPLE_AppDefined, "Missing %s",
                         (osKeyPrefix + "VALUES").c_str());
                return false;
            }

            const CPLStringList aosValues(
                CSLTokenizeString2(osDimValues.c_str(), ",", 0));
            if( static_cast<GUInt64>(aosValues.size()) != nDimSize )
            {
                CPLError(CE_Failure, CPLE_AppDefined,
                         "Inconsistent number of values in %s w.r.t %s",
                         (osKeyPrefix + "VALUES").c_str(),
                         (osKeyPrefix + "SIZE").c_str());
                return false;
            }

            auto poIndexingVariable = MEMMDArray::Create(
                std::string(), osDimName, {poDim},
                 GDALExtendedDataType::Create(eDimDT));
            if( !poIndexingVariable || !poIndexingVariable->Init() )
                return false;

            std::vector<double> adfValues;
            for( int j = 0; j < aosValues.size(); ++j )
            {
                adfValues.emplace_back(CPLAtof(aosValues[j]));
            }
            const GUInt64 arrayStartIdx = 0;
            const size_t count = aosValues.size();
            const GInt64 arrayStep = 1;
            const GPtrDiff_t bufferStride = 1;
            poIndexingVariable->Write(
                &arrayStartIdx, &count, &arrayStep, &bufferStride,
                GDALExtendedDataType::Create(GDT_Float64),
                adfValues.data());

            // Attach indexing variable to dimension
            apoIndexingVars.emplace_back(poIndexingVariable);
            poDim->SetIndexingVariable(poIndexingVariable);
        }
    }

    double *padfScale = nullptr;
    double *padfMatrix = nullptr;
    uint16_t nCount = 0;
    double dfXStart = 0;
    double dfXSpacing = 0;
    double dfYStart = 0;
    double dfYSpacing = 0;
    bool bHasGeoTransform = false;
    if( TIFFGetField(hTIFF, TIFFTAG_GEOPIXELSCALE, &nCount, &padfScale )
        && nCount >= 2
        && padfScale[0] != 0.0 && padfScale[1] != 0.0 )
    {
        dfXSpacing = padfScale[0];
        dfYSpacing = -padfScale[1];

        nCount = 0;
        double *padfTiePoints = nullptr;
        if( TIFFGetField(hTIFF, TIFFTAG_GEOTIEPOINTS,
                         &nCount, &padfTiePoints )
            && nCount >= 6 )
        {
            dfXStart = padfTiePoints[3];
            dfYStart = padfTiePoints[4];
            bHasGeoTransform = true;
        }
    }
    else
    {
        nCount = 0;
        if( TIFFGetField(hTIFF, TIFFTAG_GEOTRANSMATRIX,
                          &nCount, &padfMatrix )
           && nCount == 16 )
        {
            if( padfMatrix[1] == 0.0 && padfMatrix[4] == 0.0 )
            {
                dfXStart = padfMatrix[3];
                dfXSpacing = padfMatrix[0];
                dfYStart = padfMatrix[7];
                dfYSpacing = padfMatrix[5];
                bHasGeoTransform = true;
            }
            else
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                         "GeotransMatrix with rotation/shearing terms non supported");
            }
        }
    }

    std::shared_ptr<OGRSpatialReference> poSRS;

    GTIF *psGTIF = GTiffDatasetGTIFNew( hTIFF );
    if( psGTIF )
    {
        GTIFDefn *psGTIFDefn = GTIFAllocDefn();
        if( GTIFGetDefn( psGTIF, psGTIFDefn ) )
        {
            OGRSpatialReferenceH hSRS = GTIFGetOGISDefnAsOSR( psGTIF, psGTIFDefn );
            if( hSRS )
            {
                poSRS.reset(OGRSpatialReference::FromHandle(hSRS));
                poSRS->SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
            }
        }

        if( bHasGeoTransform )
        {
            unsigned short nRasterType = 0;
            if( GDALGTIFKeyGetSHORT(psGTIF, GTRasterTypeGeoKey,
                                    &nRasterType, 0, 1 ) == 1
                && nRasterType !=
                   static_cast<short>(RasterPixelIsPoint) )
            {
                dfXStart += dfXSpacing / 2;
                dfYStart += dfYSpacing / 2;
            }
        }

        GTIFFreeDefn(psGTIFDefn);
        GTIFFree( psGTIF );
    }

    std::string osTypeY;
    std::string osDirectionY;
    std::string osTypeX;
    std::string osDirectionX;
    if( poSRS && poSRS->GetAxesCount() == 2 )
    {
        auto mapping = poSRS->GetDataAxisToSRSAxisMapping();
        OGRAxisOrientation eOrientation1 = OAO_Other;
        poSRS->GetAxis(nullptr, 0,  &eOrientation1 );
        OGRAxisOrientation eOrientation2 = OAO_Other;
        poSRS->GetAxis(nullptr, 1,  &eOrientation2 );

        if( mapping == std::vector<int>{2,1} )
        {
            std::swap(mapping[0], mapping[1]);
            std::swap(eOrientation1, eOrientation2);
        }

        if( eOrientation1 == OAO_East && eOrientation2 == OAO_North &&
            mapping == std::vector<int>{1,2} )
        {
            osTypeY = GDAL_DIM_TYPE_HORIZONTAL_Y;
            osDirectionY = "NORTH";
            osTypeX = GDAL_DIM_TYPE_HORIZONTAL_X;
            osDirectionX = "EAST";
        }
    }

    {
        std::string osKeyPrefix("DIMENSION_");
        osKeyPrefix += std::to_string(nDimensions - 2);
        osKeyPrefix += '_';

        uint32_t nYSize = 0;
        TIFFGetField( hTIFF, TIFFTAG_IMAGELENGTH, &nYSize );

        // Create dimension
        std::string osDimName = oMapDimMetadata[osKeyPrefix + "NAME"];
        if( osDimName.empty() )
            osDimName = "dimY";
        auto poDim = std::make_shared<Dimension>(GetFullName(), osDimName,
                                                 osTypeY,
                                                 osDirectionY,
                                                 nYSize);
        aoDimensions.emplace_back(poDim);

        if( bHasGeoTransform )
        {
            auto poIndexingVariable = std::make_shared<GDALMDArrayRegularlySpaced>(
                std::string(), poDim->GetName(), poDim, dfYStart, dfYSpacing, 0.0);
            apoIndexingVars.emplace_back(poIndexingVariable);
            poDim->SetIndexingVariable(poIndexingVariable);
        }

        uint32_t nBlockYSize = 0;
        TIFFGetField( hTIFF, TIFFTAG_TILELENGTH, &nBlockYSize );
        anBlockSize.emplace_back(nBlockYSize);
    }

    {
        std::string osKeyPrefix("DIMENSION_");
        osKeyPrefix += std::to_string(nDimensions - 1);
        osKeyPrefix += '_';

        uint32_t nXSize = 0;
        TIFFGetField( hTIFF, TIFFTAG_IMAGEWIDTH, &nXSize );

        // Create dimension
        std::string osDimName = oMapDimMetadata[osKeyPrefix + "NAME"];
        if( osDimName.empty() )
            osDimName = "dimX";
        auto poDim = std::make_shared<Dimension>(GetFullName(), osDimName,
                                                 osTypeX,
                                                 osDirectionX,
                                                 nXSize);
        aoDimensions.emplace_back(poDim);

        if( bHasGeoTransform )
        {
            auto poIndexingVariable = std::make_shared<GDALMDArrayRegularlySpaced>(
                std::string(), poDim->GetName(), poDim, dfXStart, dfXSpacing, 0.0);
            apoIndexingVars.emplace_back(poIndexingVariable);
            poDim->SetIndexingVariable(poIndexingVariable);
        }

        uint32_t nBlockXSize = 0;
        TIFFGetField( hTIFF, TIFFTAG_TILEWIDTH, &nBlockXSize );
        anBlockSize.emplace_back(nBlockXSize);
    }

    size_t nIFDCount = 1;
    for( int i = 0; i < nDimensions - 2; ++i )
    {
        if( aoDimensions[i]->GetSize() >= 65536 / nIFDCount )
        {
            CPLError(CE_Failure, CPLE_NotSupported,
                     "Too many IFDs");
            return false;
        }
        nIFDCount *= static_cast<size_t>(aoDimensions[i]->GetSize());
    }

    auto array = Array::CreateFromOpen(m_poShared, std::string(), osVarName,
                                       aoDimensions,
                                       GDALExtendedDataType::Create(eDataType),
                                       anBlockSize, hTIFF);
    if( array )
    {
        m_oMapMDArrays[osVarName] = array;

        for( auto& poDim: aoDimensions )
        {
            m_oMapDimensions[poDim->GetName()] = poDim;
            auto poIndexingVariable = poDim->GetIndexingVariable();
            if( poIndexingVariable )
                m_oMapMDArrays[poIndexingVariable->GetName()] = poIndexingVariable;
        }

        for( const auto& kv: oMapAttributesStr )
        {
            array->m_oMapAttributes[kv.first] = std::make_shared<
                GDALAttributeString>(array->GetName(), kv.first, kv.second);
        }

        array->m_bHasScale = bHasScale;
        array->m_dfScale = dfScale;
        array->m_eScaleStorageType = GDT_Float64;
        array->m_bHasOffset = bHasOffset;
        array->m_dfOffset = dfOffset;
        array->m_eOffsetStorageType = GDT_Float64;
        array->m_osUnit = osUnitType;
        array->m_poSRS = poSRS;
    }

    return array != nullptr;
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

/************************************************************************/
/*                              Open()                                  */
/************************************************************************/

GDALDataset *MultiDimDataset::Open( GDALOpenInfo * poOpenInfo )
{
    if( !GTiffOneTimeInit() || poOpenInfo->fpL == nullptr )
        return nullptr;

    auto poDS = std::unique_ptr<MultiDimDataset>(new MultiDimDataset());

    poDS->SetDescription(poOpenInfo->pszFilename);
    auto poRG = std::make_shared<Group>(std::string(), nullptr);
    poRG->m_poShared->osFilename = poOpenInfo->pszFilename;
    poRG->m_poShared->fpL = poOpenInfo->fpL;
    poOpenInfo->fpL = nullptr;

    const char* pszFlag =
        (poOpenInfo->nOpenFlags & GDAL_OF_UPDATE) != 0  ? "r+D" : "rDO";
    poRG->m_poShared->hTIFF = VSI_TIFFOpen(
        poOpenInfo->pszFilename, pszFlag, poRG->m_poShared->fpL);
    if( poRG->m_poShared->hTIFF == nullptr )
    {
        CPLError( CE_Failure, CPLE_OpenFailed,
                  "File `%s' is not a valid TIFF file",
                  poOpenInfo->pszFilename );
        return nullptr;
    }

    poDS->m_poRootGroup = poRG;
    poRG->m_poShared->bWritable = (poOpenInfo->nOpenFlags & GDAL_OF_UPDATE) != 0;
    poRG->m_poShared->UnsetNewFile();
    poRG->m_poShared->bHasOptimizedReadMultiRange = CPL_TO_BOOL(
        VSIHasOptimizedReadMultiRange(poOpenInfo->pszFilename));

    if( !poRG->OpenIFD(poRG->m_poShared->hTIFF) )
        return nullptr;

    return poDS.release();
}

} // namespace gtiff
} // namespace gdal
} // namespace osgeo
