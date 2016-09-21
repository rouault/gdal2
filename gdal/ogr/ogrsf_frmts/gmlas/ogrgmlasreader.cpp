/******************************************************************************
 * Project:  OGR
 * Purpose:  OGRGMLASDriver implementation
 * Author:   Even Rouault, <even dot rouault at spatialys dot com>
 *
 * Initial development funded by the European Earth observation programme
 * Copernicus
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
#include "ogr_p.h"

#include <json.h>

CPL_CVSID("$Id$");

/************************************************************************/
/*                        GMLASBinInputStream                           */
/************************************************************************/

class GMLASBinInputStream : public BinInputStream
{
    VSILFILE*         m_fp;

public :

             GMLASBinInputStream(VSILFILE* fp);
    virtual ~GMLASBinInputStream();

    virtual XMLFilePos curPos() const;
    virtual XMLSize_t readBytes(XMLByte* const toFill, const XMLSize_t maxToRead);
    virtual const XMLCh* getContentType() const ;
};

/************************************************************************/
/*                        GMLASBinInputStream()                         */
/************************************************************************/

GMLASBinInputStream::GMLASBinInputStream(VSILFILE* fp)
{
    m_fp = fp;
    VSIFSeekL(fp, 0, SEEK_SET);
}

/************************************************************************/
/*                       ~GMLASBinInputStream()                         */
/************************************************************************/

GMLASBinInputStream::~ GMLASBinInputStream()
{
}

/************************************************************************/
/*                                curPos()                              */
/************************************************************************/

XMLFilePos GMLASBinInputStream::curPos() const
{
    return (XMLFilePos)VSIFTellL(m_fp);
}

/************************************************************************/
/*                               readBytes()                            */
/************************************************************************/

XMLSize_t GMLASBinInputStream::readBytes(XMLByte* const toFill,
                                         const XMLSize_t maxToRead)
{
    return (XMLSize_t)VSIFReadL(toFill, 1, maxToRead, m_fp);
}

/************************************************************************/
/*                            getContentType()                          */
/************************************************************************/

const XMLCh* GMLASBinInputStream::getContentType() const
{
    return NULL;
}



/************************************************************************/
/*                          GMLASInputSource()                          */
/************************************************************************/

GMLASInputSource::GMLASInputSource(const char* pszFilename,
                                   VSILFILE* fp,
                                   bool bOwnFP,
                                   MemoryManager* const manager)
    : InputSource(manager)
{
    m_fp = fp;
    m_bOwnFP = bOwnFP;
    m_osFilename = pszFilename;
    XMLCh* pFilename = XMLString::transcode(pszFilename);
    setPublicId(pFilename);
    setSystemId(pFilename);
    XMLString::release( &pFilename );
    m_nCounter = 0;
    m_pnCounter = &m_nCounter;
    m_cbk = NULL;
}

/************************************************************************/
/*                        SetClosingCallback()                          */
/************************************************************************/

void GMLASInputSource::SetClosingCallback( IGMLASInputSourceClosing* cbk )
{
    m_cbk = cbk;
}

/************************************************************************/
/*                         ~GMLASInputSource()                          */
/************************************************************************/

GMLASInputSource::~GMLASInputSource()
{
    if( m_cbk )
        m_cbk->notifyClosing( m_osFilename );
    if( m_bOwnFP && m_fp )
        VSIFCloseL(m_fp);
}

/************************************************************************/
/*                              makeStream()                            */
/************************************************************************/

BinInputStream* GMLASInputSource::makeStream() const
{
    // This is a lovely cheating around the const qualifier of this method !
    // We cannot modify m_nCounter directly, but we can change the value
    // pointed by m_pnCounter...
    if( *m_pnCounter != 0 )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "makeStream() called several times on same GMLASInputSource");
        return NULL;
    }
    (*m_pnCounter) ++;
    if( m_fp == NULL )
        return NULL;
    return new GMLASBinInputStream(m_fp);
}

/************************************************************************/
/*                            warning()                                 */
/************************************************************************/

void GMLASErrorHandler::warning (const SAXParseException& e)
{
    handle (e, CE_Warning);
}

/************************************************************************/
/*                             error()                                  */
/************************************************************************/

void GMLASErrorHandler::error (const SAXParseException& e)
{
    m_bFailed = true;
    handle (e, CE_Failure);
}

/************************************************************************/
/*                          fatalError()                                */
/************************************************************************/

void GMLASErrorHandler::fatalError (const SAXParseException& e)
{
    m_bFailed = true;
    handle (e, CE_Failure);
}

/************************************************************************/
/*                            handle()                                  */
/************************************************************************/

void GMLASErrorHandler::handle (const SAXParseException& e, CPLErr eErr)
{
    const XMLCh* resourceId (e.getPublicId());

    if ( resourceId == NULL || resourceId[0] == 0 )
        resourceId = e.getSystemId();

    CPLError(eErr, CPLE_AppDefined, "%s:%d:%d %s",
             transcode(resourceId).c_str(),
             static_cast<int>(e.getLineNumber()),
             static_cast<int>(e.getColumnNumber()),
             transcode(e.getMessage()).c_str());
}

/************************************************************************/
/*                     GMLASBaseEntityResolver()                        */
/************************************************************************/

GMLASBaseEntityResolver::GMLASBaseEntityResolver(const CPLString& osBasePath,
                                                 GMLASResourceCache& oCache) 
    : m_oCache(oCache)
{
    m_aosPathStack.push_back(osBasePath);
}

/************************************************************************/
/*                    ~GMLASBaseEntityResolver()                        */
/************************************************************************/

GMLASBaseEntityResolver::~GMLASBaseEntityResolver()
{
    CPLAssert( m_aosPathStack.size() == 1 );
}

/************************************************************************/
/*                            notifyClosing()                           */
/************************************************************************/

/* Called by GMLASInputSource destructor. This is useful for use to */
/* know where a .xsd has been finished from processing. Note that we */
/* strongly depend on Xerces behaviour here... */
void GMLASBaseEntityResolver::notifyClosing(const CPLString& osFilename )
{
    CPLDebug("GMLAS", "Closing %s", osFilename.c_str());

    CPLAssert( m_aosPathStack.back() ==
                                CPLString(CPLGetDirname(osFilename)) );
    m_aosPathStack.pop_back();
}

/************************************************************************/
/*                            SetBasePath()                             */
/************************************************************************/

void GMLASBaseEntityResolver::SetBasePath(const CPLString& osBasePath)
{
    CPLAssert( m_aosPathStack.size() == 1 );
    m_aosPathStack[0] = osBasePath;
}

/************************************************************************/
/*                         DoExtraSchemaProcessing()                    */
/************************************************************************/

void GMLASBaseEntityResolver::DoExtraSchemaProcessing(
                                             const CPLString& /*osFilename*/,
                                             VSILFILE* /*fp*/)
{
}

/************************************************************************/
/*                         resolveEntity()                              */
/************************************************************************/

InputSource* GMLASBaseEntityResolver::resolveEntity(
                                                const XMLCh* const /*publicId*/,
                                                const XMLCh* const systemId)
{
    CPLString osSystemId(transcode(systemId));

    CPLString osNewPath;
    VSILFILE* fp = m_oCache.Open(osSystemId,
                                 m_aosPathStack.back(),
                                 osNewPath);

    if( fp != NULL )
    {
        CPLDebug("GMLAS", "Opening %s", osNewPath.c_str());
        DoExtraSchemaProcessing( osNewPath, fp );
    }

    m_aosPathStack.push_back( CPLGetDirname(osNewPath) );
    GMLASInputSource* poIS = new GMLASInputSource(osNewPath, fp, true);
    poIS->SetClosingCallback(this);
    return poIS;
}

/************************************************************************/
/*                             Dump()                                   */
/************************************************************************/

void GMLASReader::Context::Dump()
{
    CPLDebug("GMLAS", "Context");
    CPLDebug("GMLAS", "  m_nLevel = %d", m_nLevel);
    CPLDebug("GMLAS", "  m_poFeature = %p", m_poFeature);
    const char* pszDebug = CPLGetConfigOption("CPL_DEBUG", "OFF");
    if( EQUAL(pszDebug, "ON") || EQUAL(pszDebug, "GMLAS") )
    {
        if( m_poFeature )
            m_poFeature->DumpReadable(stderr);
    }
    CPLDebug("GMLAS", "  m_poLayer = %p (%s)",
             m_poLayer, m_poLayer ? m_poLayer->GetName() : "");
    CPLDebug("GMLAS", "  m_poGroupLayer = %p (%s)", 
             m_poGroupLayer, m_poGroupLayer ? m_poGroupLayer->GetName() : "");
    CPLDebug("GMLAS", "  m_nGroupLayerLevel = %d", m_nGroupLayerLevel);
    CPLDebug("GMLAS", "  m_nLastFieldIdxGroupLayer = %d",
             m_nLastFieldIdxGroupLayer);
    CPLDebug("GMLAS", "  m_osCurSubXPath = %s",
             m_osCurSubXPath.c_str());
}

/************************************************************************/
/*                             GMLASReader()                            */
/************************************************************************/

GMLASReader::GMLASReader(GMLASResourceCache& oCache,
                         const GMLASXPathMatcher& oIgnoredXPathMatcher)
    : m_oCache(oCache)
    , m_oIgnoredXPathMatcher(oIgnoredXPathMatcher)
{
    m_bParsingError = false;
    m_poSAXReader = NULL;
    m_fp = NULL;
    m_GMLInputSource = NULL;
    m_bFirstIteration = true;
    m_bEOF = false;
    m_nLevel = 0;
    m_oCurCtxt.m_nLevel = 0;
    m_oCurCtxt.m_poLayer = NULL;
    m_oCurCtxt.m_poGroupLayer = NULL;
    m_oCurCtxt.m_nGroupLayerLevel = -1;
    m_oCurCtxt.m_nLastFieldIdxGroupLayer = -1;
    m_oCurCtxt.m_poFeature = NULL;
    m_nCurFieldIdx = -1;
    m_nCurGeomFieldIdx = -1;
    m_nCurFieldLevel = 0;
    m_bIsXMLBlob = false;
    m_bIsXMLBlobIncludeUpper = false;
    m_nTextContentListEstimatedSize = 0;
    m_poLayerOfInterest = NULL;
    m_nMaxLevel = atoi(CPLGetConfigOption("GMLAS_XML_MAX_LEVEL", "100"));
    m_nMaxContentSize = static_cast<size_t>(
          atoi(CPLGetConfigOption("GMLAS_XML_MAX_CONTENT_SIZE", "512000000")));
    m_bValidate = false;
    m_poEntityResolver = NULL;
}

/************************************************************************/
/*                            ~GMLASReader()                            */
/************************************************************************/

GMLASReader::~GMLASReader()
{
    delete m_poSAXReader;
    delete m_GMLInputSource;
    if( m_oCurCtxt.m_poFeature != NULL && !m_aoStackContext.empty() &&
        m_oCurCtxt.m_poFeature != m_aoStackContext.back().m_poFeature )
    {
        delete m_oCurCtxt.m_poFeature;
    }
    for( size_t i = 0; i < m_aoStackContext.size(); i++ )
    {
        delete m_aoStackContext[i].m_poFeature;
    }
    for( size_t i = 0; i < m_aoFeaturesReady.size(); i++ )
    {
        delete m_aoFeaturesReady[i].first;
    }
    if( !m_apsXMLNodeStack.empty() )
    {
        CPLDestroyXMLNode(m_apsXMLNodeStack[0].psNode);
    }
    delete m_poEntityResolver;
}

/************************************************************************/
/*                          SetLayerOfInterest()                        */
/************************************************************************/

void GMLASReader::SetLayerOfInterest( OGRGMLASLayer* poLayer )
{
    m_poLayerOfInterest = poLayer;
}

/************************************************************************/
/*                          LoadXSDInParser()                           */
/************************************************************************/

bool GMLASReader::LoadXSDInParser( SAX2XMLReader* poParser,
                                   GMLASResourceCache& oCache,
                                   GMLASBaseEntityResolver& oXSDEntityResolver,
                                   const CPLString& osBaseDirname,
                                   const CPLString& osXSDFilename,
                                   Grammar** ppoGrammar )
{
    if( ppoGrammar != NULL )
        *ppoGrammar = NULL;

    const CPLString osModifXSDFilename( 
        (osXSDFilename.find("http://") != 0 &&
        osXSDFilename.find("https://") != 0 &&
        CPLIsFilenameRelative(osXSDFilename)) ?
            CPLString(CPLFormFilename(osBaseDirname, osXSDFilename, NULL)) :
            osXSDFilename );
    CPLString osResolvedFilename;
    VSILFILE* fpXSD = oCache.Open( osModifXSDFilename, CPLString(),
                                   osResolvedFilename );
    if( fpXSD == NULL )
    {
        return false;
    }

    // Install a temporary entity resolved based on the current XSD
    CPLString osXSDDirname( CPLGetDirname(osModifXSDFilename) );
    if( osXSDFilename.find("http://") == 0 || 
        osXSDFilename.find("https://") == 0 )
    {
        osXSDDirname = CPLGetDirname(("/vsicurl_streaming/" +
                                     osModifXSDFilename).c_str());
    }
    oXSDEntityResolver.SetBasePath(osXSDDirname);
    oXSDEntityResolver.DoExtraSchemaProcessing( osResolvedFilename, fpXSD );

    EntityResolver* poOldEntityResolver = poParser->getEntityResolver();
    poParser->setEntityResolver( &oXSDEntityResolver );

    // Install a temporary error handler
    GMLASErrorHandler oErrorHandler;
    ErrorHandler* poOldErrorHandler = poParser->getErrorHandler();
    poParser->setErrorHandler( &oErrorHandler);

    GMLASInputSource oSource(osResolvedFilename, fpXSD, false);
    const bool bCacheGrammar = true;
    Grammar* poGrammar = poParser->loadGrammar(oSource,
                                            Grammar::SchemaGrammarType,
                                            bCacheGrammar);

    // Restore previous handlers
    poParser->setEntityResolver( poOldEntityResolver );
    poParser->setErrorHandler( poOldErrorHandler );
    VSIFCloseL(fpXSD);

    if( poGrammar == NULL )
    {
        CPLError(CE_Failure, CPLE_AppDefined, "loadGrammar failed");
        return false;
    }
    if( oErrorHandler.hasFailed() )
    {
        return false;
    }

    if( ppoGrammar != NULL )
        *ppoGrammar = poGrammar;

    return true;
}

/************************************************************************/
/*                                  Init()                              */
/************************************************************************/

bool GMLASReader::Init(const char* pszFilename,
                       VSILFILE* fp,
                       const std::map<CPLString, CPLString>& oMapURIToPrefix,
                       std::vector<OGRGMLASLayer*>* papoLayers,
                       bool bValidate,
                       const std::vector<PairURIFilename>& aoXSDs)
{
    m_oMapURIToPrefix = oMapURIToPrefix;
    m_papoLayers = papoLayers;
    m_bValidate = bValidate;

    m_poSAXReader = XMLReaderFactory::createXMLReader();

    // Commonly useful configuration.
    //
    m_poSAXReader->setFeature (XMLUni::fgSAX2CoreNameSpaces, true);
    m_poSAXReader->setFeature (XMLUni::fgSAX2CoreNameSpacePrefixes, true);

    m_poSAXReader->setContentHandler( this );
    m_poSAXReader->setLexicalHandler( this );
    m_poSAXReader->setDTDHandler( this );

    m_poSAXReader->setErrorHandler(&m_oErrorHandler);

    if( bValidate )
    {
        // Enable validation.
        m_poSAXReader->setFeature (XMLUni::fgSAX2CoreValidation, true);
        m_poSAXReader->setFeature (XMLUni::fgXercesSchema, true);
        m_poSAXReader->setFeature (XMLUni::fgXercesSchemaFullChecking, true);

        // We want all errors to be reported
        m_poSAXReader->setFeature (XMLUni::fgXercesValidationErrorAsFatal, false);

        CPLString osBaseDirname( CPLGetDirname(pszFilename) );

        // In the case the schemas are explicitly passed, we must do special
        // processing
        if( !aoXSDs.empty() )
        {
            GMLASBaseEntityResolver oXSDEntityResolver( CPLString(), m_oCache );
            for( size_t i = 0; i < aoXSDs.size(); i++ )
            {
                const CPLString osXSDFilename(aoXSDs[i].second);
                if( !LoadXSDInParser( m_poSAXReader, m_oCache,
                                      oXSDEntityResolver,
                                      osBaseDirname, osXSDFilename ) )
                {
                    return false;
                }
            }

            // Make sure our previously loaded schemas are used
            m_poSAXReader->setFeature (XMLUni::fgXercesUseCachedGrammarInParse,
                                       true);

            // Don't load schemas from any other source (e.g., from XML document's
            // xsi:schemaLocation attributes).
            //
            m_poSAXReader->setFeature (XMLUni::fgXercesLoadSchema, false);
        }

        // Install entity resolver based on XML file
        m_poEntityResolver = new GMLASBaseEntityResolver(
                                                osBaseDirname,
                                                m_oCache );
        m_poSAXReader->setEntityResolver( m_poEntityResolver );

    }
    else
    {
        // Don't load schemas from any other source (e.g., from XML document's
        // xsi:schemaLocation attributes).
        //
        m_poSAXReader->setFeature (XMLUni::fgXercesLoadSchema, false);
        m_poSAXReader->setEntityResolver( this );
    }

    m_fp = fp;
    m_GMLInputSource = new GMLASInputSource(pszFilename, fp, false);

    return true;
}

/************************************************************************/
/*                                SetField()                            */
/************************************************************************/

void GMLASReader::SetField( OGRFeature* poFeature,
                            OGRGMLASLayer* poLayer,
                            int nAttrIdx,
                            const CPLString& osAttrValue )
{
    const OGRFieldType eType(poFeature->GetFieldDefnRef(nAttrIdx)->GetType());
    if( osAttrValue.empty() )
    {
        if( eType == OFTString &&
            !poFeature->GetFieldDefnRef(nAttrIdx)->IsNullable() )
        {
            poFeature->SetField( nAttrIdx, "" );
        }
    }
    else if( eType == OFTDateTime )
    {
        OGRField sField;
        if( OGRParseXMLDateTime( osAttrValue.c_str(), &sField ) )
        {
            poFeature->SetField( nAttrIdx, &sField );
        }
    }
    // Transform boolean values to something that OGR understands
    else if( eType == OFTInteger && 
             poFeature->GetFieldDefnRef(nAttrIdx)->GetSubType() == OFSTBoolean )
    {
        if( osAttrValue == "true" )
            poFeature->SetField( nAttrIdx, TRUE );
        else
            poFeature->SetField( nAttrIdx, FALSE );
    }
    else if( eType == OFTBinary )
    {
        const int nFCFieldIdx =
            poLayer->GetFCFieldIndexFromOGRFieldIdx(nAttrIdx);
        if( nFCFieldIdx >= 0 )
        {
            const GMLASField& oField(
                poLayer->GetFeatureClass().GetFields()[nFCFieldIdx]);
            if( oField.GetType() == GMLAS_FT_BASE64BINARY )
            {
                GByte* pabyBuffer = reinterpret_cast<GByte*>(
                                                    CPLStrdup(osAttrValue));
                int nBytes = CPLBase64DecodeInPlace(pabyBuffer);
                poFeature->SetField( nAttrIdx, nBytes, pabyBuffer );
                CPLFree(pabyBuffer);
            }
            else
            {
                int nBytes = 0;
                GByte* pabyBuffer = CPLHexToBinary( osAttrValue, &nBytes );
                poFeature->SetField( nAttrIdx, nBytes, pabyBuffer );
                CPLFree(pabyBuffer);
            }
        }
    }
    else
    {
        poFeature->SetField( nAttrIdx, osAttrValue.c_str() );
    }
}

/************************************************************************/
/*                          PushFeatureReady()                          */
/************************************************************************/

void GMLASReader::PushFeatureReady( OGRFeature* poFeature,
                                    OGRGMLASLayer* poLayer )
{
    m_aoFeaturesReady.push_back(
        std::pair<OGRFeature*, OGRGMLASLayer*>(poFeature, poLayer) );
}

/************************************************************************/
/*                          CreateNewFeature                            */
/************************************************************************/

void GMLASReader::CreateNewFeature(const CPLString& osLocalname)
{
    m_oCurCtxt.m_poFeature = new OGRFeature(
                m_oCurCtxt.m_poLayer->GetLayerDefn() );

    // Assign FID (1, ...). Only for OGR compliance, but definitely
    // not a unique ID among datasets with the same schema
    ++m_oMapGlobalCounter[m_oCurCtxt.m_poLayer];
    const int nGlobalCounter =
                    m_oMapGlobalCounter[m_oCurCtxt.m_poLayer];
    m_oCurCtxt.m_poFeature->SetFID(nGlobalCounter);

    // Find parent ID
    CPLString osParentId;
    if( !m_aoStackContext.empty() &&
        m_oCurCtxt.m_poLayer->GetParentIDFieldIdx() >= 0 )
    {
        CPLAssert(m_aoStackContext.back().
                            m_poLayer->GetIDFieldIdx() >= 0 );
        osParentId = m_aoStackContext.back().m_poFeature->
            GetFieldAsString(
            m_aoStackContext.back().m_poLayer->GetIDFieldIdx() );
        m_oCurCtxt.m_poFeature->SetField(
            m_oCurCtxt.m_poLayer->GetParentIDFieldIdx(),
            osParentId.c_str() );
    }

    // Should we generate a unique (child) ID from the parent ID ?
    if( m_oCurCtxt.m_poLayer->IsGeneratedIDField() )
    {
        // Local IDs (ie related to a parent feature are fine, but when
        // we might have cycles, that doesn't work anymore
        /*
        ++m_oCurCtxt.m_oMapCounter[m_oCurCtxt.m_poLayer];
        const int nCounter =
            m_oCurCtxt.m_oMapCounter[m_oCurCtxt.m_poLayer];*/
        const int nCounter = nGlobalCounter;

        // TODO: take into account case where osParentId is empty. Use UUID
        CPLString osGeneratedID = osParentId + "_" + osLocalname +
                                    CPLSPrintf("_%d", nCounter);
        m_oCurCtxt.m_poFeature->SetField(
                        m_oCurCtxt.m_poLayer->GetIDFieldIdx(),
                        osGeneratedID.c_str() );
    }

    m_nCurFieldIdx = -1;
}

/************************************************************************/
/*                             IsArrayType()                            */
/************************************************************************/

static bool IsArrayType( OGRFieldType eType )
{
    return eType == OFTIntegerList ||
           eType == OFTInteger64List ||
           eType == OFTRealList ||
           eType == OFTStringList;
}

/************************************************************************/
/*                         AttachAsLastChild()                          */
/************************************************************************/

/* Attach element as the last child of its parent */
void GMLASReader::AttachAsLastChild(CPLXMLNode* psNode)
{
    NodeLastChild& sNodeLastChild = m_apsXMLNodeStack.back();
    CPLXMLNode* psLastChildParent = sNodeLastChild.psLastChild;

    if (psLastChildParent == NULL)
    {
        CPLAssert( sNodeLastChild.psNode );
        sNodeLastChild.psNode->psChild = psNode;
    }
    else
    {
        psLastChildParent->psNext = psNode;
    }
    sNodeLastChild.psLastChild = psNode;
}

/************************************************************************/
/*                         BuildXMLBlobStartElement()                   */
/************************************************************************/

void GMLASReader::BuildXMLBlobStartElement(const CPLString& osNSPrefix,
                                           const CPLString& osLocalname,
                                           const  Attributes& attrs)
{
    m_osTextContent += "<";
    CPLString osElementName;
    if( !osNSPrefix.empty() )
    {
        osElementName += osNSPrefix + ":";
    }
    osElementName += osLocalname;
    m_osTextContent += osElementName;

    CPLXMLNode* psNode = NULL;
    if( m_nCurGeomFieldIdx >= 0 )
    {
        psNode = CPLCreateXMLNode( NULL, CXT_Element, osElementName );
        if( !m_apsXMLNodeStack.empty() )
        {
            AttachAsLastChild(psNode);
        }
    }

    CPLXMLNode* psLastChild = NULL;
    for(unsigned int i=0; i < attrs.getLength(); i++)
    {
        CPLString osAttrNSPrefix( m_oMapURIToPrefix[
                                        transcode( attrs.getURI(i) ) ] );
        CPLString osAttrLocalname( transcode(attrs.getLocalName(i)) );
        CPLString osAttrValue( transcode(attrs.getValue(i)) );
        CPLString osAttributeName;
        if( !osAttrNSPrefix.empty() )
        {
            osAttributeName += osAttrNSPrefix + ":";
        }
        osAttributeName += osAttrLocalname;

        if( psNode != NULL )
        {
            CPLXMLNode* psAttrNode = CPLCreateXMLNode( NULL, CXT_Attribute,
                                                       osAttributeName );
            CPLCreateXMLNode(psAttrNode, CXT_Text, osAttrValue);

            if( psLastChild == NULL )
            {
                psNode->psChild = psAttrNode;
            }
            else
            {
                psLastChild->psNext = psAttrNode;
            }
            psLastChild = psAttrNode;
        }

        m_osTextContent += " ";
        m_osTextContent += osAttributeName;
        m_osTextContent += "=\"";
        char* pszEscaped = CPLEscapeString( osAttrValue.c_str(),
                                    static_cast<int>(osAttrValue.size()),
                                    CPLES_XML );
        m_osTextContent += pszEscaped;
        CPLFree(pszEscaped);
        m_osTextContent += '"';
    }
    m_osTextContent += ">";

    if( psNode != NULL )
    {
        /* Push the element on the stack */
        NodeLastChild sNewNodeLastChild;
        sNewNodeLastChild.psNode = psNode;
        sNewNodeLastChild.psLastChild = psLastChild;
        m_apsXMLNodeStack.push_back(sNewNodeLastChild);
    }

    if( m_osTextContent.size() > m_nMaxContentSize )
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                "Too much data in a single element");
        m_bParsingError = true;
    }
}



/************************************************************************/
/*                          GetLayerByXPath()                           */
/************************************************************************/

OGRGMLASLayer* GMLASReader::GetLayerByXPath( const CPLString& osXPath )
{
    for(size_t i = 0; i < m_papoLayers->size(); i++ )
    {
        if( (*m_papoLayers)[i]->GetFeatureClass().GetXPath() == osXPath )
        {
            return (*m_papoLayers)[i];
        }
    }
    return NULL;
}

/************************************************************************/
/*                             startElement()                           */
/************************************************************************/

/* <xs:group ref="somegroup" maxOccurs="unbounded"/> are particularly hard to
   deal with since we cannot easily know when the corresponding subfeature
   is exactly terminated.

   Let's consider:

        <xs:group name="somegroup">
            <xs:choice>
                <xs:element name="first_elt_of_group" type="xs:string"/>
                <xs:element name="second_elt_of_group" type="xs:string"/>
            </xs:choice>
        </xs:group>

        <xs:group name="another_group">
            <xs:choice>
                <xs:element name="first_elt_of_another_group" type="xs:string"/>
            </xs:choice>
        </xs:group>


   There are different cases :
    *
              <first_elt_of_group>...</first_elt_of_group>
              <second_elt_of_group>...</first_elt_of_group>
              <first_elt_of_group>  <!-- we are here at startElement() -->
                ...
              </first_elt_of_group> 

    *
              <first_elt_of_group>...</first_elt_of_group>
              <first_elt_of_group>  <!-- we are here at startElement() -->
                ...</first_elt_of_group>

    *
              <first_elt_of_group>...</first_elt_of_group>
              <first_elt_of_another_group>  <!-- we are here at startElement() -->
                ...</first_elt_of_group>

    *
              <first_elt_of_group>...</first_elt_of_group>
              <some_other_elt>  <!-- we are here at startElement() -->
                ...</some_other_elt>

    *
            <first_elt>...</first_elt>
            <second_elt><sub>...</sub></second_elt>
            <first_elt> <-- here -->
                ...</first_elt>
    *
                <first_elt_of_group>...</first_elt_of_group>
            </end_of_enclosing_element>   <!-- we are here at endElement() -->


*/
void GMLASReader::startElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const
#ifdef DEBUG_VERBOSE
                                    qname
#endif
            , const   Attributes& attrs
        )
{
    CPLString osLocalname( transcode(localname) );
    CPLString osNSPrefix( m_oMapURIToPrefix[ transcode(uri) ] );
    CPLString osXPath = (osNSPrefix.empty()) ? osLocalname : osNSPrefix + ":" + osLocalname;
#ifdef DEBUG_VERBOSE
    CPLDebug("GMLAS", "startElement(%s / %s)", transcode(qname).c_str(), osXPath.c_str());
#endif
    m_anStackXPathLength.push_back(osXPath.size());
    if( !m_osCurXPath.empty() )
        m_osCurXPath += "/";
    m_osCurXPath += osXPath;

    CPLString osSubXPathBefore(m_osCurSubXPath);
    if( !m_osCurSubXPath.empty() )
    {
        m_osCurSubXPath += "/";
        m_osCurSubXPath += osXPath;
    }

    // Deal with XML content
    if( m_bIsXMLBlob )
    {
        BuildXMLBlobStartElement(osNSPrefix, osLocalname, attrs);
        m_nLevel ++;
        return;
    }

    if( m_nLevel == m_nMaxLevel )
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Too deeply nested XML content");
        m_bParsingError = true;
        return;
    }

    CPLAssert(m_aoFeaturesReady.empty());

    // Look which layer might match the current XPath
    for(size_t i = 0; i < m_papoLayers->size(); i++ )
    {
        // Are we entering a group ?
        const bool bIsRepeatedSequenceLayer =
            ((*m_papoLayers)[i]->GetFeatureClass().IsRepeatedSequence() &&
             m_oCurCtxt.m_poLayer != NULL &&
             m_oCurCtxt.m_poLayer != (*m_papoLayers)[i] &&
             m_oCurCtxt.m_poLayer->GetFeatureClass().GetXPath() ==
                    (*m_papoLayers)[i]->GetFeatureClass().GetXPath() &&
             (*m_papoLayers)[i]->GetOGRFieldIndexFromXPath(m_osCurSubXPath) >= 0 );

        if( // Case where we haven't yet entered the top-level element, which may
            // be in container elements
            (m_osCurSubXPath.empty() &&
             (*m_papoLayers)[i]->GetFeatureClass().GetXPath() == osXPath) ||

            // Case where we are a sub-element of a top-level feature
            (!m_osCurSubXPath.empty() &&
             (*m_papoLayers)[i]->GetFeatureClass().GetXPath() == m_osCurSubXPath) ||

            // Case where we are a sub-element of a (repeated) group of a
            //top-level feature
            bIsRepeatedSequenceLayer ||

            // Case where we go back from a sub-element of a (repeated) group
            // of a top-level feature to a regular sub-element of that top-level
            // feature
            (m_oCurCtxt.m_poGroupLayer != NULL &&
             (*m_papoLayers)[i]->GetOGRFieldIndexFromXPath(m_osCurSubXPath) >= 0) )
        {
#ifdef DEBUG_VERBOSE
            CPLDebug("GMLAS", "Matches layer %s (%s)",
                     (*m_papoLayers)[i]->GetName(),
                     (*m_papoLayers)[i]->GetFeatureClass().GetXPath().c_str());
#endif

            if( (*m_papoLayers)[i]->GetParent() != NULL &&
                (*m_papoLayers)[i]->GetParent()->GetFeatureClass().IsRepeatedSequence() &&
                m_oCurCtxt.m_poGroupLayer != (*m_papoLayers)[i]->GetParent() )
            {
                // Yuck! Simulate top-level element of a group if we directly jump
                // into a nested class of it !
                /* Something like 
                    <xs:group name="group">
                        <xs:sequence>
                            <xs:element name="optional_elt" type="xs:string" minOccurs="0"/>
                            <xs:element name="elt">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="subelt"  type="xs:dateTime" maxOccurs="unbounded"/>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:group>

                    <top_element>
                        <elt><subelt>...</subelt></elt>
                    </top_element>
                */
                m_oCurCtxt.m_poLayer = (*m_papoLayers)[i]->GetParent();
                m_oCurCtxt.m_poGroupLayer = m_oCurCtxt.m_poLayer;
                m_oCurCtxt.m_nLevel = m_nLevel;
                m_oCurCtxt.m_nLastFieldIdxGroupLayer = -1;
                CreateNewFeature( m_oCurCtxt.m_poLayer->GetName() );
            }

            bool bPushNewState = true;
            if( bIsRepeatedSequenceLayer )
            {
                int nFieldIdx =
                    (*m_papoLayers)[i]->GetOGRFieldIndexFromXPath(m_osCurSubXPath);
                bool bPushNewFeature = false;
                if( m_oCurCtxt.m_poGroupLayer == NULL )
                {
                    m_oCurCtxt.m_poFeature = NULL;
                }
                else if ( m_oCurCtxt.m_nGroupLayerLevel == m_nLevel &&
                          m_oCurCtxt.m_poGroupLayer != (*m_papoLayers)[i] )
                {
                    /* Case like:
                            <first_elt_of_group>...</first_elt_of_group>
                            <first_elt_of_another_group>  <!-- we are here at startElement() -->
                                ...</first_elt_of_group>
                    */
                    bPushNewFeature = true;
                }
                else if( m_oCurCtxt.m_nGroupLayerLevel == m_nLevel &&
                         m_oCurCtxt.m_poGroupLayer == (*m_papoLayers)[i] &&
                         nFieldIdx == m_oCurCtxt.m_nLastFieldIdxGroupLayer &&
                         !IsArrayType(m_oCurCtxt.m_poFeature->
                                        GetFieldDefnRef(nFieldIdx)->GetType()))
                {
                    /* Case like:
                        <first_elt>...</first_elt>
                        <first_elt> <-- here -->
                    */
                    bPushNewFeature = true;
                }
                else if ( m_oCurCtxt.m_nGroupLayerLevel == m_nLevel &&
                          nFieldIdx < m_oCurCtxt.m_nLastFieldIdxGroupLayer )
                {
                    /* Case like:
                            <first_elt_of_group>...</first_elt_of_group>
                            <second_elt_of_group>...</first_elt_of_group>
                            <first_elt_of_group>  <!-- we are here at startElement() -->
                                ...
                            </first_elt_of_group> 
                    */
                    bPushNewFeature = true;
                }
                else if ( m_oCurCtxt.m_nGroupLayerLevel == m_nLevel + 1 &&
                          m_oCurCtxt.m_poGroupLayer == (*m_papoLayers)[i] )
                {
                    /* Case like:
                        <first_elt>...</first_elt>
                        <second_elt><sub>...</sub></second_elt>
                        <first_elt> <-- here -->
                            ...</first_elt>
                    */
                    bPushNewFeature = true;
                }
                if( bPushNewFeature )
                {
                    CPLAssert( m_oCurCtxt.m_poFeature );
                    CPLAssert( m_oCurCtxt.m_poGroupLayer );
                    //CPLDebug("GMLAS", "Feature ready");
                    PushFeatureReady(m_oCurCtxt.m_poFeature,
                                     m_oCurCtxt.m_poGroupLayer);
                    m_oCurCtxt.m_poFeature = NULL;
                    m_nCurFieldIdx = -1;
                }
                m_oCurCtxt.m_poLayer = (*m_papoLayers)[i];
                m_oCurCtxt.m_poGroupLayer = (*m_papoLayers)[i];
                m_oCurCtxt.m_nGroupLayerLevel = m_nLevel;
                m_oCurCtxt.m_nLastFieldIdxGroupLayer = nFieldIdx;
            }
            else
            {
                if( m_oCurCtxt.m_nGroupLayerLevel == m_nLevel &&
                    (*m_papoLayers)[i] == m_aoStackContext.back().m_poLayer )
                {
                    // This is the case where we switch from an element that was
                    // in a group to a regular element of the same level
                    // Cf group_case_C in above doc

                    // Push group feature as ready
                    CPLAssert( m_oCurCtxt.m_poFeature );

                    //CPLDebug("GMLAS", "Feature ready");
                    PushFeatureReady(m_oCurCtxt.m_poFeature,
                                     m_oCurCtxt.m_poGroupLayer);

                    // Restore "top-level" context
                    CPLAssert( !m_aoStackContext.empty() );
                    m_oCurCtxt = m_aoStackContext.back();
                    bPushNewState = false;
                }
                else
                {
                    if( m_oCurCtxt.m_poGroupLayer )
                    {
                        Context oContext;
                        oContext = m_oCurCtxt;
                        oContext.m_nLevel = -1;
                        oContext.Dump();
                        m_aoStackContext.push_back( oContext );
                    }

                    m_oCurCtxt.m_poFeature = NULL;
                    m_oCurCtxt.m_poGroupLayer = NULL;
                    m_oCurCtxt.m_nGroupLayerLevel = -1;
                    m_oCurCtxt.m_nLastFieldIdxGroupLayer = -1;
                    m_oCurCtxt.m_poLayer = (*m_papoLayers)[i];
                    if( m_aoStackContext.empty() )
                        m_osCurSubXPath = osXPath;
                }
            }

            if( m_oCurCtxt.m_poFeature == NULL )
            {
                CPLAssert( bPushNewState );

                CreateNewFeature(osLocalname);
            }

            if( bPushNewState )
            {
                Context oContext;
                oContext = m_oCurCtxt;
                oContext.m_nLevel = m_nLevel;
                m_aoStackContext.push_back( oContext );
                m_oCurCtxt.m_oMapCounter.clear();
            }
            break;
        }
    }

    if( m_oCurCtxt.m_poLayer )
    {
#ifdef DEBUG_VERBOSE
        CPLDebug("GMLAS", "Current layer: %s", m_oCurCtxt.m_poLayer->GetName() );
#endif

        // Find if we can match this element with one of our fields
        int idx = m_oCurCtxt.m_poLayer->
                            GetOGRFieldIndexFromXPath(m_osCurSubXPath);
        int geom_idx = m_oCurCtxt.m_poLayer->
                            GetOGRGeomFieldIndexFromXPath(m_osCurSubXPath);
        if( idx >= 0 || geom_idx >= 0 )
        {

            bool bPushNewFeature = false;
            const int nFCFieldIdx = (idx >= 0) ?
                m_oCurCtxt.m_poLayer->GetFCFieldIndexFromOGRFieldIdx(idx) :
                m_oCurCtxt.m_poLayer->GetFCFieldIndexFromOGRGeomFieldIdx(geom_idx);

            /* For cases like
                    <xs:element name="element_compound">
                        <xs:complexType>
                            <xs:sequence maxOccurs="unbounded">
                                <xs:element name="subelement1" type="xs:string"/>
                                <xs:element name="subelement2" type="xs:string"/>
                            </xs:sequence>
                        </xs:complexType>
                    </xs:element>

                    <element_compound>
                        <subelement1>a</subelement>
                        <subelement2>b</subelement>
                        <subelement1>c</subelement>
                        <subelement2>d</subelement>
                    </element_compound>
            */
            if( idx >= 0 && idx < m_nCurFieldIdx )
            {
                bPushNewFeature = true;
            }

            /* For cases like
                    <xs:element name="element_compound">
                        <xs:complexType>
                            <xs:sequence maxOccurs="unbounded">
                                <xs:element name="subelement" type="xs:dateTime"/>
                            </xs:sequence>
                        </xs:complexType>
                    </xs:element>

                    <element_compound>
                        <subelement>2012-01-01T12:34:56Z</subelement>
                        <subelement>2012-01-02T12:34:56Z</subelement>
                    </element_compound>
            */
            else if( idx >= 0 && idx == m_nCurFieldIdx &&
                     !IsArrayType(m_oCurCtxt.m_poFeature->
                                GetFieldDefnRef(m_nCurFieldIdx)->GetType()) &&
                     // Make sure this isn't a repeated geometry as well
                     !( geom_idx >= 0 && nFCFieldIdx >= 0 &&
                        m_oCurCtxt.m_poLayer->GetFeatureClass().GetFields()[
                                        nFCFieldIdx].GetMaxOccurs() > 1 ) )
            {
                bPushNewFeature = true;
            }

            // Make sure we are in a repeated sequence, otherwise this is
            // invalid XML
            if( bPushNewFeature &&
                !m_oCurCtxt.m_poLayer->GetFeatureClass().IsRepeatedSequence() )
            {
                bPushNewFeature = false;
                CPLError(CE_Warning, CPLE_AppDefined,
                            "Unexpected element %s",
                            m_osCurSubXPath.c_str());
            }

            if( bPushNewFeature )
            {
                //CPLDebug("GMLAS", "Feature ready");
                PushFeatureReady(m_oCurCtxt.m_poFeature, m_oCurCtxt.m_poLayer);
                Context oContext = m_aoStackContext.back();
                m_aoStackContext.pop_back();
                CreateNewFeature(osLocalname);
                oContext.m_poFeature = m_oCurCtxt.m_poFeature;
                m_aoStackContext.push_back( oContext );
                m_oCurCtxt.m_oMapCounter.clear();
            }

            if( m_nCurFieldIdx != idx )
            {
                m_osTextContentList.Clear();
                m_nTextContentListEstimatedSize = 0;
            }
            m_nCurFieldIdx = idx;
            m_nCurGeomFieldIdx = geom_idx;
            m_nCurFieldLevel = m_nLevel + 1;
            m_osTextContent.clear();
            m_bIsXMLBlob = false;
            m_bIsXMLBlobIncludeUpper = false;

#ifdef DEBUG_VERBOSE
            if( idx >= 0 )
            {
                CPLDebug("GMLAS", "Matches field %s", m_oCurCtxt.m_poLayer->
                         GetLayerDefn()->GetFieldDefn(idx)->GetNameRef() );
            }
            if( geom_idx >= 0 )
            {
                CPLDebug("GMLAS", "Matches geometry field %s", m_oCurCtxt.m_poLayer->
                         GetLayerDefn()->GetGeomFieldDefn(geom_idx)->GetNameRef() );
            }
#endif
            if( nFCFieldIdx >= 0 )
            {
                const GMLASField& oField(
                    m_oCurCtxt.m_poLayer->GetFeatureClass().GetFields()[
                                                                nFCFieldIdx]);
                m_bIsXMLBlob = (oField.GetType() == GMLAS_FT_ANYTYPE ||
                                m_nCurGeomFieldIdx != -1 );
                m_bIsXMLBlobIncludeUpper = m_bIsXMLBlob &&
                                            oField.GetIncludeThisEltInBlob();
                if( m_bIsXMLBlobIncludeUpper )
                {
                    BuildXMLBlobStartElement(osNSPrefix, osLocalname, attrs);
                    m_nLevel ++;
                    return;
                }

                // Figure out if it is an element that calls for a related
                // top-level feature (but without junction table)
                if( oField.GetCategory() ==
                                GMLASField::PATH_TO_CHILD_ELEMENT_WITH_LINK )
                {
                    const CPLString& osNestedXPath(oField.GetRelatedClassXPath());
                    CPLAssert( !osNestedXPath.empty() );
                    OGRGMLASLayer* poSubLayer = GetLayerByXPath(osNestedXPath);
                    if( poSubLayer )
                    {
                        int nOldCurFieldIdx = m_nCurFieldIdx;
                        OGRFeature* poOldCurFeature = m_oCurCtxt.m_poFeature;
                        OGRGMLASLayer* poOldLayer = m_oCurCtxt.m_poLayer;
                        m_oCurCtxt.m_poLayer = poSubLayer;
                        CreateNewFeature(osLocalname);
                        CPLString osChildId(
                            m_oCurCtxt.m_poFeature->GetFieldAsString(
                                    m_oCurCtxt.m_poLayer->GetIDFieldIdx()));
                        SetField( poOldCurFeature,
                                  poOldLayer,
                                  nOldCurFieldIdx,
                                  osChildId );

                        Context oContext;
                        oContext = m_oCurCtxt;
                        oContext.m_nLevel = m_nLevel;
                        oContext.m_osCurSubXPath = m_osCurSubXPath;
                        m_osCurSubXPath = osNestedXPath;
#ifdef DEBUG_VERBOSE
                        CPLDebug("GMLAS",
                                 "Installing new m_osCurSubXPath from %s to %s",
                                 oContext.m_osCurSubXPath.c_str(),
                                 m_osCurSubXPath.c_str());
#endif
                        m_aoStackContext.push_back( oContext );
                        m_oCurCtxt.m_oMapCounter.clear();
                    }
                }

            }
        }

#if 0
        // Case where we have an abstract type and don't know its realizations
        else if ( idx != IDX_COMPOUND_FOLDED &&
            (idx = m_oCurCtxt.m_poLayer->GetOGRFieldIndexFromXPath(
                                    osSubXPathBefore + "/" + "*")) >= 0 &&
            m_oCurCtxt.m_poGroupLayer == NULL )
        {
            m_nCurFieldIdx = idx;
            m_nCurFieldLevel = m_nLevel + 1;
            m_osTextContent.clear();
            m_bIsXMLBlob = true;
            m_bIsXMLBlobIncludeUpper = true;
            BuildXMLBlobStartElement(osNSPrefix, osLocalname, attrs);
            m_nLevel ++;
            return;
        }
#endif

        else if( m_nLevel > m_aoStackContext.back().m_nLevel )
        {
            // Figure out if it is an element that calls from a related
            // top-level feature with a junction table
            const std::vector<GMLASField>& aoFields =
                    m_oCurCtxt.m_poLayer->GetFeatureClass().GetFields();
            for( size_t i = 0; i < aoFields.size(); ++i )
            {
                if( aoFields[i].GetCategory() ==
                        GMLASField::PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE &&
                    aoFields[i].GetXPath() == m_osCurSubXPath )
                {
                    const CPLString& osAbstractElementXPath(
                                        aoFields[i].GetAbstractElementXPath());
                    const CPLString& osNestedXPath(
                                        aoFields[i].GetRelatedClassXPath());
                    CPLAssert( !osAbstractElementXPath.empty() );
                    CPLAssert( !osNestedXPath.empty() );

                    OGRGMLASLayer* poJunctionLayer = GetLayerByXPath(
                                osAbstractElementXPath + "|" + osNestedXPath);
                    OGRGMLASLayer* poSubLayer = GetLayerByXPath(osNestedXPath);

                    if( poSubLayer && poJunctionLayer )
                    {
                        CPLString osParentId(
                            m_oCurCtxt.m_poFeature->GetFieldAsString(
                                    m_oCurCtxt.m_poLayer->GetIDFieldIdx()));

                        // Create child feature
                        m_oCurCtxt.m_poLayer = poSubLayer;
                        CreateNewFeature(osLocalname);
                        CPLString osChildId(
                            m_oCurCtxt.m_poFeature->GetFieldAsString(
                                    m_oCurCtxt.m_poLayer->GetIDFieldIdx()));

                        // Create junction feature
                        OGRFeature* poJunctionFeature =
                                new OGRFeature(poJunctionLayer->GetLayerDefn());

                        ++m_oMapGlobalCounter[poJunctionLayer];
                        const int nGlobalCounter =
                                        m_oMapGlobalCounter[poJunctionLayer];

                        ++m_oCurCtxt.m_oMapCounter[poJunctionLayer];
                        const int nCounter =
                            m_oCurCtxt.m_oMapCounter[poJunctionLayer];

                        poJunctionFeature->SetFID(nGlobalCounter);
                        poJunctionFeature->SetField("occurence", nCounter);
                        poJunctionFeature->SetField("parent_pkid", osParentId);
                        poJunctionFeature->SetField("child_pkid", osChildId);
                        PushFeatureReady(poJunctionFeature, poJunctionLayer);

                        Context oContext;
                        oContext = m_oCurCtxt;
                        oContext.m_nLevel = m_nLevel;
                        oContext.m_osCurSubXPath = m_osCurSubXPath;
                        m_osCurSubXPath = osNestedXPath;
#ifdef DEBUG_VERBOSE
                        CPLDebug("GMLAS",
                                 "Installing new m_osCurSubXPath from %s to %s",
                                 oContext.m_osCurSubXPath.c_str(),
                                 m_osCurSubXPath.c_str());
#endif
                        m_aoStackContext.push_back( oContext );
                        m_oCurCtxt.m_oMapCounter.clear();
                    }
                    idx = IDX_COMPOUND_FOLDED;

                    break;
                }
            }

            m_nCurFieldIdx = -1;
            m_nCurGeomFieldIdx = -1;
            if( idx != IDX_COMPOUND_FOLDED )
            {
                CPLString osMatchedXPath;
                if( m_oIgnoredXPathMatcher.MatchesRefXPath(
                                        m_osCurSubXPath, m_oMapURIToPrefix,
                                        osMatchedXPath) )
                {
                    if( m_oMapIgnoredXPathToWarn[osMatchedXPath] )
                    {
                        CPLError(CE_Warning, CPLE_AppDefined,
                                 "Element with xpath=%s found in document but "
                                 "ignored according to configuration",
                                 m_osCurSubXPath.c_str());
                    }
                    else
                    {
                        CPLDebug("GMLAS",
                                 "Element with xpath=%s found in document but "
                                 "ignored according to configuration",
                                 m_osCurSubXPath.c_str());
                    }
                }
                else
                {
                    CPLDebug("GMLAS",
                             "Unexpected element with xpath=%s (subxpath=%s) found",
                             m_osCurXPath.c_str(),
                             m_osCurSubXPath.c_str());
                }
            }
        }
        else
        {
            m_nCurFieldIdx = -1;
            m_nCurGeomFieldIdx = -1;
        }

        // Browse through attributes and match them with one of our fields
        const int nWildcardAttrIdx =
            m_oCurCtxt.m_poLayer->GetOGRFieldIndexFromXPath(m_osCurSubXPath + "/@*");
        json_object* poWildcard = NULL;

        for(unsigned int i=0; i < attrs.getLength(); i++)
        {
            CPLString osAttrNSPrefix( m_oMapURIToPrefix[
                                            transcode( attrs.getURI(i) ) ] );
            CPLString osAttrLocalname( transcode(attrs.getLocalName(i)) );
            CPLString osAttrValue( transcode(attrs.getValue(i)) );
            CPLString osAttrXPath = m_osCurSubXPath + "/@";
            if( !osAttrNSPrefix.empty() )
                osAttrXPath += osAttrNSPrefix + ":";
            osAttrXPath += osAttrLocalname;
            int nAttrIdx = m_oCurCtxt.m_poLayer->GetOGRFieldIndexFromXPath(osAttrXPath);
            if( nAttrIdx >= 0 )
            {
                SetField( m_oCurCtxt.m_poFeature,
                          m_oCurCtxt.m_poLayer,
                          nAttrIdx, osAttrValue );

                // If we are a xlink:href attribute, and that the link value is
                // a internal link, then find if we have
                // a field that does a relation to a targetElement
                if( osAttrNSPrefix == "xlink" &&
                    osAttrLocalname == "href" &&
                    !osAttrValue.empty() && osAttrValue[0] == '#' )
                {
                    int nAttrIdx2 =
                        m_oCurCtxt.m_poLayer->GetOGRFieldIndexFromXPath(
                            GMLASField::MakePKIDFieldFromXLinkHrefXPath(
                                                                osAttrXPath));
                    if( nAttrIdx2 >= 0 )
                    {
                        SetField( m_oCurCtxt.m_poFeature,
                                  m_oCurCtxt.m_poLayer,
                                  nAttrIdx2, osAttrValue.substr(1) );
                    }
                }
            }

            else if( osAttrNSPrefix != "xmlns" &&
                     !(osAttrNSPrefix == "xsi" &&
                            osAttrLocalname == "schemaLocation") &&
                     !(osAttrNSPrefix == "xsi" &&
                            osAttrLocalname == "noNamespaceSchemaLocation") &&
                     !(osAttrNSPrefix == "xsi" &&
                            osAttrLocalname == "nil") &&
                     // Do not warn about fixed attributes on geometry properties
                     !(m_nCurGeomFieldIdx >= 0 && (
                        (osAttrNSPrefix == "xlink" && osAttrLocalname == "type") ||
                        (osAttrNSPrefix == "" && osAttrLocalname == "owns"))) )
            {
                CPLString osMatchedXPath;
                if( nWildcardAttrIdx >= 0 )
                {
                    if( poWildcard == NULL )
                        poWildcard = json_object_new_object();
                    CPLString osKey;
                    if( !osAttrNSPrefix.empty() )
                        osKey = osAttrNSPrefix + ":" + osAttrLocalname;
                    else
                        osKey = osAttrLocalname;
                    json_object_object_add(poWildcard,
                        osKey,
                        json_object_new_string(osAttrValue));
                }
                else if( m_oIgnoredXPathMatcher.MatchesRefXPath(
                                            osAttrXPath, m_oMapURIToPrefix,
                                            osMatchedXPath) )
                {
                    if( m_oMapIgnoredXPathToWarn[osMatchedXPath] )
                    {
                        CPLError(CE_Warning, CPLE_AppDefined,
                                "Attribute with xpath=%s found in document but "
                                "ignored according to configuration",
                                osAttrXPath.c_str());
                    }
                    else
                    {
                        CPLDebug("GMLAS",
                                "Attribute with xpath=%s found in document but "
                                "ignored according to configuration",
                                osAttrXPath.c_str());
                    }
                }
                else
                {
                    // Emit debug message if unexpected attribute
                    CPLDebug("GMLAS",
                                "Unexpected attribute with xpath=%s found",
                            osAttrXPath.c_str());
                }
            }
        }

        // Store wildcard attributes
        if( poWildcard != NULL )
        {
            SetField( m_oCurCtxt.m_poFeature,
                      m_oCurCtxt.m_poLayer,
                      nWildcardAttrIdx,
                      json_object_get_string(poWildcard) );
            json_object_put(poWildcard);
        }

        // Process fixed and default values
        const int nFieldCount = m_oCurCtxt.m_poFeature->GetFieldCount();
        const std::vector<GMLASField>& aoFields =
                        m_oCurCtxt.m_poLayer->GetFeatureClass().GetFields();
        for( int i=0; i < nFieldCount; i++ )
        {
            const int nFCIdx =
                    m_oCurCtxt.m_poLayer->GetFCFieldIndexFromOGRFieldIdx(i);
            if( nFCIdx >= 0 &&
                aoFields[nFCIdx].GetXPath().find('@') != std::string::npos )
            {
                // We process fixed as default. In theory, to be XSD compliant,
                // the user shouldn't have put a different value than the fixed
                // one, but just in case he did, then honour it instead of
                // overwriting it.
                CPLString osFixedDefaultValue = aoFields[nFCIdx].GetFixedValue();
                if( osFixedDefaultValue.empty() )
                    osFixedDefaultValue = aoFields[nFCIdx].GetDefaultValue();
                if( !osFixedDefaultValue.empty() &&
                    !m_oCurCtxt.m_poFeature->IsFieldSet(i) )
                {
                    SetField( m_oCurCtxt.m_poFeature,
                              m_oCurCtxt.m_poLayer,
                              i, osFixedDefaultValue);
                }
            }
        }
    }
    else
    {
        m_nCurFieldIdx = -1;
        m_nCurGeomFieldIdx = -1;
    }

    m_nLevel ++;
}

/************************************************************************/
/*                              endElement()                            */
/************************************************************************/

void GMLASReader::endElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const    /*qname*/
        )
{
    m_nLevel --;

    // Make sure to set field only if we are at the expected nesting level
    if( (m_nCurFieldIdx >= 0 || m_nCurGeomFieldIdx >= 0) &&
            m_nLevel == m_nCurFieldLevel - 1 )
    {
        const OGRFieldType eType(
            m_nCurFieldIdx >= 0 ?
                m_oCurCtxt.m_poFeature->GetFieldDefnRef(m_nCurFieldIdx)->GetType() :
            OFTString );

        // Transform boolean values to something that OGR understands
        if( (eType == OFTIntegerList || eType == OFTInteger) && 
             m_oCurCtxt.m_poFeature->GetFieldDefnRef(m_nCurFieldIdx)->GetSubType() ==
                                                                   OFSTBoolean )
        {
            if( m_osTextContent == "true" )
                m_osTextContent = "1";
            else
                m_osTextContent = "0";
        }

        // Assign XML content to field value
        if( IsArrayType(eType) )
        {
            if( m_nTextContentListEstimatedSize > m_nMaxContentSize )
            {
                CPLError(CE_Failure, CPLE_OutOfMemory,
                         "Too much repeated data in a single element");
                m_bParsingError = true;
            }
            else
            {
                m_osTextContentList.AddString( m_osTextContent );
                // 16 is an arbitrary number for the cost of a new entry in the
                // string list
                m_nTextContentListEstimatedSize += 16 + m_osTextContent.size();
                m_oCurCtxt.m_poFeature->SetField( m_nCurFieldIdx,
                                        m_osTextContentList.List() );
            }
        }
        else 
        {
            if( m_bIsXMLBlobIncludeUpper )
            {
                CPLString osLocalname( transcode(localname) );
                CPLString osNSPrefix( m_oMapURIToPrefix[ transcode(uri) ] );

                m_osTextContent += "</";
                if( !osNSPrefix.empty() )
                {
                    m_osTextContent += osNSPrefix + ":";
                }
                m_osTextContent += osLocalname;
                m_osTextContent += ">";
            }

            if( m_nCurFieldIdx >= 0 )
            {
                SetField( m_oCurCtxt.m_poFeature,
                          m_oCurCtxt.m_poLayer,
                          m_nCurFieldIdx, m_osTextContent );
            }
        }

        if( m_nCurGeomFieldIdx >= 0 )
        {
            if( m_bIsXMLBlobIncludeUpper )
            {
                m_apsXMLNodeStack.pop_back();
            }

            if( !m_apsXMLNodeStack.empty() )
            {
                CPLAssert( m_apsXMLNodeStack.size() == 1 );

                ProcessGeometry();
            }
            m_nCurGeomFieldIdx = -1;
        }
        m_bIsXMLBlob = false;
        m_bIsXMLBlobIncludeUpper = false;
    }

    if( m_bIsXMLBlob )
    {
        if( m_nCurGeomFieldIdx >= 0 )
        {
            if( m_nLevel >= m_nCurFieldLevel + 1 )
                m_apsXMLNodeStack.pop_back();
        }

        CPLString osLocalname( transcode(localname) );
        CPLString osNSPrefix( m_oMapURIToPrefix[ transcode(uri) ] );

        m_osTextContent += "</";
        if( !osNSPrefix.empty() )
        {
            m_osTextContent += osNSPrefix + ":";
        }
        m_osTextContent += osLocalname;
        m_osTextContent += ">";

        if( m_osTextContent.size() > m_nMaxContentSize )
        {
            CPLError(CE_Failure, CPLE_OutOfMemory,
                    "Too much data in a single element");
            m_bParsingError = true;
        }
    }
    else
    {
        m_osTextContent.clear();
    }

    if( !m_aoStackContext.empty() &&
        m_aoStackContext.back().m_nLevel == m_nLevel )
    {
        std::map<OGRLayer*, int> oMapCounter = m_aoStackContext.back().m_oMapCounter;
        if( !m_aoStackContext.back().m_osCurSubXPath.empty() )
        {
#ifdef DEBUG_VERBOSE
            CPLDebug("GMLAS", "Restoring m_osCurSubXPath from %s to %s",
                     m_osCurSubXPath.c_str(),
                     m_aoStackContext.back().m_osCurSubXPath.c_str());
#endif
            m_osCurSubXPath = m_aoStackContext.back().m_osCurSubXPath;
        }

        if( m_oCurCtxt.m_poGroupLayer == m_oCurCtxt.m_poLayer )
        {
            m_aoStackContext.pop_back();
            CPLAssert( !m_aoStackContext.empty() );
            m_oCurCtxt.m_poLayer = m_aoStackContext.back().m_poLayer;
        }
        else
        {
            if( m_oCurCtxt.m_poGroupLayer )
            {
                /* Case like
                        <first_elt_of_group>...</first_elt_of_group>
                    </end_of_enclosing_element>   <!-- we are here at endElement() -->
                */

                //CPLDebug("GMLAS", "Feature ready");
                PushFeatureReady(m_oCurCtxt.m_poFeature,
                                 m_oCurCtxt.m_poGroupLayer);
                //CPLDebug("GMLAS", "Feature ready");
                PushFeatureReady(m_aoStackContext.back().m_poFeature,
                                 m_aoStackContext.back().m_poLayer);
            }
            else
            {
                //CPLDebug("GMLAS", "Feature ready");
                PushFeatureReady(m_oCurCtxt.m_poFeature,
                                 m_oCurCtxt.m_poLayer);
            }
            m_aoStackContext.pop_back();
            if( !m_aoStackContext.empty() )
            {
                m_oCurCtxt = m_aoStackContext.back();
                m_oCurCtxt.m_osCurSubXPath.clear();
                if( m_oCurCtxt.m_nLevel < 0 )
                {
                    m_aoStackContext.pop_back();
                    CPLAssert( !m_aoStackContext.empty() );
                    m_oCurCtxt.m_poLayer = m_aoStackContext.back().m_poLayer;
                }
            }
            else
            {
                m_oCurCtxt.m_poFeature = NULL;
                m_oCurCtxt.m_poLayer = NULL;
                m_oCurCtxt.m_poGroupLayer = NULL;
                m_oCurCtxt.m_nGroupLayerLevel = -1;
                m_oCurCtxt.m_nLastFieldIdxGroupLayer = -1;
            }
            m_nCurFieldIdx = -1;
        }
        m_oCurCtxt.m_oMapCounter = oMapCounter;
    }

    size_t nLastXPathLength = m_anStackXPathLength.back();
    m_anStackXPathLength.pop_back();
    if( m_anStackXPathLength.empty())
        m_osCurXPath.clear();
    else
        m_osCurXPath.resize( m_osCurXPath.size() - 1 - nLastXPathLength);

    if( m_osCurSubXPath.size() >= 1 + nLastXPathLength )
        m_osCurSubXPath.resize( m_osCurSubXPath.size() - 1 - nLastXPathLength);
    else if( m_osCurSubXPath.size() == nLastXPathLength )
         m_osCurSubXPath.clear();
}

/************************************************************************/
/*                            ProcessGeometry()                         */
/************************************************************************/

void GMLASReader::ProcessGeometry()
{
    CPLXMLNode* psInterestNode = m_apsXMLNodeStack.back().psNode;
    m_apsXMLNodeStack.pop_back();

#ifdef DEBUG_VERBOSE
    {
        char* pszXML = CPLSerializeXMLTree(psInterestNode);
        CPLDebug("GML", "geometry = %s", pszXML);
        CPLFree(pszXML);
    }
#endif

    OGRGeometry* poGeom = reinterpret_cast<OGRGeometry*>
                    (OGR_G_CreateFromGMLTree( psInterestNode ));
    if( poGeom != NULL )
    {
        OGRGeomFieldDefn* poGeomFieldDefn =
            m_oCurCtxt.m_poFeature->GetGeomFieldDefnRef(
                                        m_nCurGeomFieldIdx);


        const char* pszSRSName = CPLGetXMLValue(psInterestNode,
                                                "srsName", NULL);
        bool bSwapXY = false;
        if( pszSRSName != NULL )
        {
            // If we are doing a first pass, store the SRS of the geometry
            // column
            if( !m_oSetGeomFieldsWithUnknownSRS.empty() &&
                 m_oSetGeomFieldsWithUnknownSRS.find(poGeomFieldDefn) !=
                        m_oSetGeomFieldsWithUnknownSRS.end() )
            {
                OGRSpatialReference* poSRS = 
                                new OGRSpatialReference();
                if( poSRS->SetFromUserInput( pszSRSName ) == OGRERR_NONE )
                {
                    OGR_SRSNode *poGEOGCS = poSRS->GetAttrNode( "GEOGCS" );
                    if( poGEOGCS != NULL )
                        poGEOGCS->StripNodes( "AXIS" );

                    OGR_SRSNode *poPROJCS = poSRS->GetAttrNode( "PROJCS" );
                    if (poPROJCS != NULL &&
                        poSRS->EPSGTreatsAsNorthingEasting())
                    {
                        poPROJCS->StripNodes( "AXIS" );
                    }

                    m_oMapGeomFieldDefnToSRSName[poGeomFieldDefn] = pszSRSName;
                    poGeomFieldDefn->SetSpatialRef(poSRS);
                }
                poSRS->Release();
                m_oSetGeomFieldsWithUnknownSRS.erase(poGeomFieldDefn);
            }

            // Check if the srsName indicates unusual axis order,
            // and if so swap x and y coordinates.
            std::map<CPLString, bool>::iterator oIter =
                m_oMapSRSNameToInvertedAxis.find(pszSRSName);
            if( oIter == m_oMapSRSNameToInvertedAxis.end() )
            {
                OGRSpatialReference oSRS;
                oSRS.SetFromUserInput( pszSRSName );
                bSwapXY = CPL_TO_BOOL(oSRS.EPSGTreatsAsLatLong()) ||
                            CPL_TO_BOOL(oSRS.EPSGTreatsAsNorthingEasting());
                m_oMapSRSNameToInvertedAxis[ pszSRSName ] = bSwapXY;
            }
            else
            {
                bSwapXY = oIter->second;
            }
        }
        if( bSwapXY )
            poGeom->swapXY();

        if( !m_oSetGeomFieldsWithUnknownSRS.empty() )
        {
            delete poGeom;
            poGeom = NULL;
        }
        // Do we need to do reprojection ?
        else if( pszSRSName != NULL && poGeomFieldDefn->GetSpatialRef() != NULL &&
            m_oMapGeomFieldDefnToSRSName[poGeomFieldDefn] != pszSRSName )
        {
            bool bReprojectionOK = false;
            OGRSpatialReference oSRS;
            if( oSRS.SetFromUserInput( pszSRSName ) == OGRERR_NONE )
            {
                OGRCoordinateTransformation* poCT =
                    OGRCreateCoordinateTransformation( &oSRS,
                                            poGeomFieldDefn->GetSpatialRef() );
                if( poCT != NULL )
                {
                    bReprojectionOK = (poGeom->transform( poCT ) == OGRERR_NONE);
                    delete poCT;
                }
            }
            if( !bReprojectionOK )
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                         "Reprojection fom %s to %s failed",
                         pszSRSName,
                         m_oMapGeomFieldDefnToSRSName[poGeomFieldDefn].c_str());
                delete poGeom;
                poGeom = NULL;
            }
#ifdef DEBUG_VERBOSE
            else
            {
                CPLDebug("GMLAS", "Reprojected geometry from %s to %s",
                         pszSRSName,
                         m_oMapGeomFieldDefnToSRSName[poGeomFieldDefn].c_str());
            }
#endif
        }

        if( poGeom != NULL )
        {
            // Deal with possibly repeated geometries by building
            // a geometry collection. We could also create a
            // nested table, but that would probably be less
            // convenient to use.
            OGRGeometry* poPrevGeom = m_oCurCtxt.m_poFeature->
                                    StealGeometry(m_nCurGeomFieldIdx);
            if( poPrevGeom != NULL )
            {
                if( poPrevGeom->getGeometryType() ==
                                        wkbGeometryCollection )
                {
                    reinterpret_cast<OGRGeometryCollection*>(
                        poPrevGeom)->addGeometryDirectly(poGeom);
                    poGeom = poPrevGeom;
                }
                else
                {
                    OGRGeometryCollection* poGC =
                                    new OGRGeometryCollection();
                    poGC->addGeometryDirectly(poPrevGeom);
                    poGC->addGeometryDirectly(poGeom);
                    poGeom = poGC;
                }
            }
            poGeom->assignSpatialReference(
                                poGeomFieldDefn->GetSpatialRef());
            m_oCurCtxt.m_poFeature->SetGeomFieldDirectly(
                m_nCurGeomFieldIdx, poGeom );
        }
    }
    else
    {
        char* pszXML = CPLSerializeXMLTree(psInterestNode);
        CPLDebug("GMLAS", "Non-recognized geometry: %s",
                    pszXML);
        CPLFree(pszXML);
    }
    CPLDestroyXMLNode( psInterestNode );
}

/************************************************************************/
/*                              characters()                            */
/************************************************************************/

void GMLASReader::characters( const XMLCh *const chars,
                              const XMLSize_t length )
{
    if( m_bIsXMLBlob )
    {
        const CPLString osText( transcode(chars, static_cast<int>(length) ) );

        if( m_nCurGeomFieldIdx >= 0 &&
            // Check the stack is not empty in case of space chars before the
            // starting node
            !m_apsXMLNodeStack.empty() )
        {
            // Merge content in current text node if it exists
            NodeLastChild& sNodeLastChild = m_apsXMLNodeStack.back();
            if( sNodeLastChild.psLastChild != NULL &&
                sNodeLastChild.psLastChild->eType == CXT_Text )
            {
                CPLXMLNode* psNode = sNodeLastChild.psLastChild;
                const size_t nOldLength = strlen(psNode->pszValue);
                char* pszNewValue = reinterpret_cast<char*>(VSIRealloc(
                        psNode->pszValue, nOldLength + osText.size() + 1));
                if( pszNewValue )
                {
                    psNode->pszValue = pszNewValue;
                    memcpy( pszNewValue + nOldLength, osText.c_str(),
                            osText.size() + 1);
                }
                else
                {
                    CPLError(CE_Failure, CPLE_OutOfMemory, "Out of memory");
                    m_bParsingError = true;
                }
            }
            // Otherwise create a new text node
            else
            {
                AttachAsLastChild(
                        CPLCreateXMLNode(NULL, CXT_Text, osText) );
            }
        }

        char* pszEscaped = CPLEscapeString( osText.c_str(),
                                        static_cast<int>(osText.size()),
                                        CPLES_XML );
        try
        {
            m_osTextContent += pszEscaped;
        }
        catch( const std::bad_alloc& )
        {
            CPLError(CE_Failure, CPLE_OutOfMemory, "Out of memory");
            m_bParsingError = true;
        }
        CPLFree(pszEscaped);
    }
    // Make sure to set content only if we are at the expected nesting level
    else if( m_nLevel == m_nCurFieldLevel )
    {
        const CPLString osText( transcode(chars, static_cast<int>(length) ) );
        try
        {
            m_osTextContent += osText;
        }
        catch( const std::bad_alloc& )
        {
            CPLError(CE_Failure, CPLE_OutOfMemory, "Out of memory");
            m_bParsingError = true;
        }
    }

    if( m_osTextContent.size() > m_nMaxContentSize )
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                 "Too much data in a single element");
        m_bParsingError = true;
    }
}

/************************************************************************/
/*                            GetNextFeature()                          */
/************************************************************************/

OGRFeature* GMLASReader::GetNextFeature( OGRLayer** ppoBelongingLayer )
{
    // In practice we will never have more than 2 features
    while( !m_aoFeaturesReady.empty() )
    {
        OGRFeature* m_poFeatureReady = m_aoFeaturesReady[0].first;
        OGRGMLASLayer* m_poFeatureReadyLayer = m_aoFeaturesReady[0].second;
        m_aoFeaturesReady.erase( m_aoFeaturesReady.begin() );

        if( m_poLayerOfInterest == NULL ||
            m_poLayerOfInterest == m_poFeatureReadyLayer )
        {
            if( ppoBelongingLayer )
                *ppoBelongingLayer = m_poFeatureReadyLayer;
            return m_poFeatureReady;
        }
        delete m_poFeatureReady;
    }

    if( m_bEOF )
        return NULL;

    try
    {
        if( m_bFirstIteration )
        {
            m_bFirstIteration = false;
            if( !m_poSAXReader->parseFirst( *m_GMLInputSource, m_oToFill ) )
            {
                m_bParsingError = true;
                m_bEOF = true;
                return NULL;
            }
        }

        while( m_poSAXReader->parseNext( m_oToFill ) )
        {
            if( m_bParsingError )
                break;

            // In practice we will never have more than 2 features
            while( !m_aoFeaturesReady.empty() )
            {
                OGRFeature* m_poFeatureReady = m_aoFeaturesReady[0].first;
                OGRGMLASLayer* m_poFeatureReadyLayer =
                                               m_aoFeaturesReady[0].second;
                m_aoFeaturesReady.erase( m_aoFeaturesReady.begin() );

                if( m_poLayerOfInterest == NULL ||
                    m_poLayerOfInterest == m_poFeatureReadyLayer )
                {
                    if( ppoBelongingLayer )
                        *ppoBelongingLayer = m_poFeatureReadyLayer;
                    return m_poFeatureReady;
                }
                delete m_poFeatureReady;
            }
        }

        m_bEOF = true;
    }
    catch (const XMLException& toCatch)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s",
                 transcode( toCatch.getMessage() ).c_str() );
        m_bParsingError = true;
        m_bEOF = true;
    }
    catch (const SAXException& toCatch)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s",
                 transcode( toCatch.getMessage() ).c_str() );
        m_bParsingError = true;
        m_bEOF = true;
    }

    return NULL;
}

/************************************************************************/
/*                              RunFirstPass()                          */
/************************************************************************/

void GMLASReader::RunFirstPass()
{
    // Store in m_oSetGeomFieldsWithUnknownSRS the geometry fields
    for(size_t i=0; i < m_papoLayers->size(); i++ )
    {
        OGRFeatureDefn* poFDefn = (*m_papoLayers)[i]->GetLayerDefn();
        for(int j=0; j< poFDefn->GetGeomFieldCount(); j++ )
        {
            m_oSetGeomFieldsWithUnknownSRS.insert(
                                            poFDefn->GetGeomFieldDefn(j));
        }
    }

    CPLDebug("GMLAS", "Start of first pass");

    // Loop on features until we have determined the SRS of all geometry
    // columns. If we do validation, parse the whole file
    OGRFeature* poFeature;
    while( (m_bValidate || !m_oSetGeomFieldsWithUnknownSRS.empty()) &&
           (poFeature = GetNextFeature(NULL)) != NULL )
    {
        delete poFeature;
    }

    CPLDebug("GMLAS", "End of first pass");

    // Clear the set even if we didn't manage to determine all the SRS
    m_oSetGeomFieldsWithUnknownSRS.clear();
}
