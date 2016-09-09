/******************************************************************************
 * $Id$
 *
 * Project:  OGR
 * Purpose:  OGRGMLASDriver implementation
 * Author:   Even Rouault, <even dot rouault at spatialys dot com>
 *
 * Initial developement funded by the European Environment Agency
 *
 ******************************************************************************
 * Copyright (c) 2016, Even Rouault, <even dot rouault at spatialys dot com>
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

#include "ogr_gmlas.h"

#include "ogr_mem.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                          OGRGMLASDataSource()                        */
/************************************************************************/

OGRGMLASDataSource::OGRGMLASDataSource()
{
    // FIXME
    XMLPlatformUtils::Initialize();
    m_bExposeMetadataLayers = false;

    m_poFieldsMetadataLayer = new OGRMemLayer
                                    ("_ogr_fields_metadata", NULL, wkbNone );
    {
        OGRFieldDefn oFieldDefn("layer_name", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_name", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_xpath", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_type", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_min_occurs", OFTInteger);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_max_occurs", OFTInteger);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_default_value", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_fixed_value", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_category", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_related_layer", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("field_documentation", OFTString);
        m_poFieldsMetadataLayer->CreateField(&oFieldDefn);
    }

    m_poRelationshipsLayer = new OGRMemLayer("_ogr_layer_relationships",
                                             NULL, wkbNone );
    {
        OGRFieldDefn oFieldDefn("relation_name", OFTString);
        m_poRelationshipsLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("parent_layer", OFTString);
        m_poRelationshipsLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("parent_field", OFTString);
        m_poRelationshipsLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("child_layer", OFTString);
        m_poRelationshipsLayer->CreateField(&oFieldDefn);
    }
    {
        OGRFieldDefn oFieldDefn("child_field", OFTString);
        m_poRelationshipsLayer->CreateField(&oFieldDefn);
    }
}

/************************************************************************/
/*                         ~OGRGMLASDataSource()                        */
/************************************************************************/

OGRGMLASDataSource::~OGRGMLASDataSource()
{
    for(size_t i=0;i<m_apoLayers.size();i++)
        delete m_apoLayers[i];
    delete m_poFieldsMetadataLayer;
    delete m_poRelationshipsLayer;

    // FIXME
    XMLPlatformUtils::Terminate();
}

/************************************************************************/
/*                            GetLayerCount()                           */
/************************************************************************/

int         OGRGMLASDataSource::GetLayerCount()
{
    return static_cast<int>(m_apoLayers.size() +
                            (m_bExposeMetadataLayers ? 2 : 0));
}

/************************************************************************/
/*                                GetLayer()                            */
/************************************************************************/

OGRLayer    *OGRGMLASDataSource::GetLayer(int i)
{
    if( m_bExposeMetadataLayers && i == static_cast<int>(m_apoLayers.size()) )
        return m_poFieldsMetadataLayer;
    if( m_bExposeMetadataLayers && i == 1 + static_cast<int>(m_apoLayers.size()) )
        return m_poRelationshipsLayer;

    if( i < 0 || i >= static_cast<int>(m_apoLayers.size()) )
        return NULL;
    return m_apoLayers[i];
}

/************************************************************************/
/*                             GetLayerByName()                         */
/************************************************************************/

OGRLayer    *OGRGMLASDataSource::GetLayerByName(const char* pszName)
{
    OGRLayer* poLayer = GDALDataset::GetLayerByName(pszName);
    if( poLayer )
        return poLayer;

    if( EQUAL(pszName, m_poFieldsMetadataLayer->GetName()) )
        return m_poFieldsMetadataLayer;

    if( EQUAL(pszName, m_poRelationshipsLayer->GetName()) )
        return m_poRelationshipsLayer;

    return NULL;
}

/************************************************************************/
/*                           TranslateClasses()                         */
/************************************************************************/

void OGRGMLASDataSource::TranslateClasses( OGRGMLASLayer* poParentLayer,
                                           const GMLASFeatureClass& oFC )
{
    const std::vector<GMLASFeatureClass>& aoClasses = oFC.GetNestedClasses();

    //CPLDebug("GMLAS", "TranslateClasses(%s,%s)",
    //         oFC.GetName().c_str(), oFC.GetXPath().c_str());

    OGRGMLASLayer* poLayer = new OGRGMLASLayer(this, oFC, poParentLayer,
                                               !aoClasses.empty());
    m_apoLayers.push_back(poLayer);

    for( size_t i=0; i<aoClasses.size(); ++i )
    {
        TranslateClasses( poLayer, aoClasses[i] );
    }
}

/************************************************************************/
/*                         GMLASGuessXSDFilename                        */
/************************************************************************/

class GMLASGuessXSDFilename : public DefaultHandler
{
            std::vector<PairURIFilename>  m_aoFilenames;
            int         m_nStartElementCounter;
            bool        m_bFinish;

    public:
                        GMLASGuessXSDFilename();

                        virtual ~GMLASGuessXSDFilename() {}

        std::vector<PairURIFilename> Guess(const CPLString& osFilename,
                                     VSILFILE* fp);

        virtual void startElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const    qname,
            const   Attributes& attrs
        );

};

/************************************************************************/
/*                          GMLASGuessXSDFilename()                     */
/************************************************************************/

GMLASGuessXSDFilename::GMLASGuessXSDFilename()
    : m_nStartElementCounter(0),
        m_bFinish(false)
{
}

/************************************************************************/
/*                               Guess()                                */
/************************************************************************/

std::vector<PairURIFilename>
        GMLASGuessXSDFilename::Guess(const CPLString& osFilename,
                                                    VSILFILE* fp)
{
    SAX2XMLReader* poSAXReader = XMLReaderFactory::createXMLReader();

    poSAXReader->setFeature (XMLUni::fgSAX2CoreNameSpaces, true);
    poSAXReader->setFeature (XMLUni::fgSAX2CoreNameSpacePrefixes, true);

    poSAXReader->setContentHandler( this );
    poSAXReader->setLexicalHandler( this );
    poSAXReader->setDTDHandler( this );

    poSAXReader->setFeature (XMLUni::fgXercesLoadSchema, false);

    GMLASErrorHandler oErrorHandler;
    poSAXReader->setErrorHandler(&oErrorHandler);

    GMLASInputSource* poIS = new GMLASInputSource(osFilename, fp, false);

    try
    {
        XMLPScanToken     oToFill;
        if( poSAXReader->parseFirst( *poIS, oToFill ) )
        {
            while( !m_bFinish &&
                   poSAXReader->parseNext( oToFill ) )
            {
                // do nothing
            }
        }
    }
    catch (const XMLException& toCatch)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s",
                 transcode( toCatch.getMessage() ).c_str() );
    }
    catch (const SAXException& toCatch)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s",
                 transcode( toCatch.getMessage() ).c_str() );
    }

    delete poSAXReader;
    delete poIS;

    return m_aoFilenames;
}

/************************************************************************/
/*                             startElement()                           */
/************************************************************************/

void GMLASGuessXSDFilename::startElement(
                            const   XMLCh* const /*uri*/,
                            const   XMLCh* const /*localname*/,
                            const   XMLCh* const /*qname*/,
                            const   Attributes& attrs )
{
    m_nStartElementCounter ++;

    for(unsigned int i=0; i < attrs.getLength(); i++)
    {
        CPLString osAttrURIPrefix( transcode(attrs.getURI(i)) );
        CPLString osAttrLocalname( transcode(attrs.getLocalName(i)) );
        CPLString osAttrValue( transcode(attrs.getValue(i)) );

        if( osAttrURIPrefix == pszXSI_URI &&
            osAttrLocalname == "schemaLocation" )
        {
            CPLDebug("GMLAS", "schemaLocation=%s", osAttrValue.c_str());

            char** papszTokens = CSLTokenizeString2(osAttrValue, " ", 0 );
            int nTokens = CSLCount(papszTokens);
            if( (nTokens % 2) == 0 )
            {
                for( int j = 0; j < nTokens; j+= 2 )
                {
                    if( !STARTS_WITH(papszTokens[j], pszWFS_URI) &&
                        !STARTS_WITH(papszTokens[j], pszGML_URI) )
                    {
                        CPLDebug("GMLAS", "Schema to analyze: %s -> %s",
                                 papszTokens[j], papszTokens[j+1]);
                        m_aoFilenames.push_back( PairURIFilename(
                            papszTokens[j], papszTokens[j+1]) );
                    }
                }
            }
            CSLDestroy(papszTokens);
        }
    }

    if( m_nStartElementCounter == 1 )
        m_bFinish = true;
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

bool OGRGMLASDataSource::Open(GDALOpenInfo* poOpenInfo)
{
    GMLASSchemaAnalyzer oAnalyzer;
    oAnalyzer.SetAllowArrays(true);

    m_osGMLFilename = STARTS_WITH_CI(poOpenInfo->pszFilename, "GMLAS:") ?
        poOpenInfo->pszFilename + strlen("GMLAS:") : poOpenInfo->pszFilename;

    CPLString osXSDFilename = CSLFetchNameValueDef(poOpenInfo->papszOpenOptions,
                                                   "XSD", "");

    VSILFILE* fpGML = NULL;
    if( !m_osGMLFilename.empty() )
    {
        fpGML = VSIFOpenL(m_osGMLFilename, "rb");
        if( fpGML == NULL )
        {
            CPLError(CE_Failure, CPLE_FileIO, "Cannot open %s",
                     m_osGMLFilename.c_str());
            return false;
        }
    }
    else if( osXSDFilename.empty() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "XSD open option must be provided when no XML data file is passed");
        return false;
    }

    std::vector<PairURIFilename> aoXSDs;
    if( osXSDFilename.empty() )
    {
        GMLASGuessXSDFilename guesser;
        aoXSDs = guesser.Guess(m_osGMLFilename, fpGML);
    }
    else
    {
        char** papszTokens = CSLTokenizeString2(osXSDFilename," ,",0);
        for( int i=0; papszTokens != NULL && papszTokens[i] != NULL; i++ )
        {
            aoXSDs.push_back(PairURIFilename("",papszTokens[i]));
        }
        CSLDestroy(papszTokens);
    }
    if( fpGML )
        VSIFCloseL(fpGML);

    if( aoXSDs.empty() )
    {
        if( osXSDFilename.empty() )
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                    "No schema locations found when analyzing data file: XSD open "
                    "option must be provided");
        }
        else
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "No schema locations found");
        }
        return false;
    }
    bool bRet = oAnalyzer.Analyze( CPLGetDirname(m_osGMLFilename), aoXSDs );
    if( !bRet )
    {
        return false;
    }

    m_bExposeMetadataLayers = CPLTestBool(
        CSLFetchNameValueDef(poOpenInfo->papszOpenOptions,
                             "EXPOSE_METADATA_LAYERS", "NO") );

    const std::vector<GMLASFeatureClass>& aoClasses = oAnalyzer.GetClasses();

    // First "standard" tables
    for( size_t i=0; i<aoClasses.size(); ++i )
    {
        if( aoClasses[i].GetParentXPath().empty() )
            TranslateClasses( NULL, aoClasses[i] );
    }
    // Then junction tables
    for( size_t i=0; i<aoClasses.size(); ++i )
    {
        if( !aoClasses[i].GetParentXPath().empty() )
            TranslateClasses( NULL, aoClasses[i] );
    }

    // And now do initialization since we need to have instanciated everything
    // to be able to do cross-layer links
    for( size_t i = 0; i < m_apoLayers.size(); i++ )
    {
        m_apoLayers[i]->PostInit();
    }

    m_oMapURIToPrefix = oAnalyzer.GetMapURIToPrefix();

    return true;
}

/************************************************************************/
/*                          GetLayerByXPath()                           */
/************************************************************************/

OGRGMLASLayer* OGRGMLASDataSource::GetLayerByXPath( const CPLString& osXPath )
{
    for(size_t i = 0; i < m_apoLayers.size(); i++ )
    {
        if( m_apoLayers[i]->GetFeatureClass().GetXPath() == osXPath )
        {
            return m_apoLayers[i];
        }
    }
    return NULL;
}
