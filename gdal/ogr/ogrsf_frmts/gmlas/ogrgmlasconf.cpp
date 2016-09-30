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
#include "cpl_minixml.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                          GMLASConfiguration()                        */
/************************************************************************/

GMLASConfiguration::GMLASConfiguration()
    : m_bAllowRemoteSchemaDownload(ALLOW_REMOTE_SCHEMA_DOWNLOAD_DEFAULT)
    , m_bAlwaysGenerateOGRId(ALWAYS_GENERATE_OGR_ID_DEFAULT)
    , m_bRemoveUnusedLayers(REMOVE_UNUSED_LAYERS_DEFAULT)
    , m_bRemoveUnusedFields(REMOVE_UNUSED_FIELDS_DEFAULT)
    , m_bUseArrays(USE_ARRAYS_DEFAULT)
    , m_bIncludeGeometryXML(INCLUDE_GEOMETRY_XML_DEFAULT)
    , m_bInstantiateGMLFeaturesOnly(INSTANTIATE_GML_FEATURES_ONLY_DEFAULT)
    , m_bAllowXSDCache(ALLOW_XSD_CACHE_DEFAULT)
    , m_bValidate(VALIDATE_DEFAULT)
    , m_bFailIfValidationError(FAIL_IF_VALIDATION_ERROR_DEFAULT)
    , m_bExposeMetadataLayers(WARN_IF_EXCLUDED_XPATH_FOUND_DEFAULT)
{
}

/************************************************************************/
/*                         ~GMLASConfiguration()                        */
/************************************************************************/

GMLASConfiguration::~GMLASConfiguration()
{
}

/************************************************************************/
/*                              Finalize()                              */
/************************************************************************/

void GMLASConfiguration::Finalize()
{
    if( m_bAllowXSDCache && m_osCacheDirectory.empty() )
    {
#ifdef WIN32
        const char* pszHome = CPLGetConfigOption("USERPROFILE", NULL);
#else
        const char* pszHome = CPLGetConfigOption("HOME", NULL);
#endif
        if( pszHome != NULL )
        {
            m_osCacheDirectory = CPLFormFilename( pszHome, ".gdal", NULL) ;
            m_osCacheDirectory = CPLFormFilename( m_osCacheDirectory,
                                                  "gmlas_xsd_cache", NULL) ;
            CPLDebug("GMLAS", "XSD cache directory: %s",
                     m_osCacheDirectory.c_str());
        }
        else
        {
            const char *pszDir = CPLGetConfigOption( "CPL_TMPDIR", NULL );

            if( pszDir == NULL )
                pszDir = CPLGetConfigOption( "TMPDIR", NULL );

            if( pszDir == NULL )
                pszDir = CPLGetConfigOption( "TEMP", NULL );

            const char* pszUsername = CPLGetConfigOption("USERNAME", NULL);
            if( pszUsername == NULL )
                pszUsername = CPLGetConfigOption("USER", NULL);

            if( pszDir != NULL && pszUsername != NULL )
            {
                m_osCacheDirectory = CPLFormFilename( pszDir,
                        CPLSPrintf(".gdal_%s", pszUsername), NULL) ;
                m_osCacheDirectory = CPLFormFilename( m_osCacheDirectory,
                                                    "gmlas_xsd_cache", NULL) ;
                CPLDebug("GMLAS", "XSD cache directory: %s",
                          m_osCacheDirectory.c_str());
            }
            else
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                         "Could not determine a directory for GMLAS XSD cache");
            }
        }
    }
}

/************************************************************************/
/*                          CPLGetXMLBoolValue()                        */
/************************************************************************/

static bool CPLGetXMLBoolValue(CPLXMLNode* psNode,
                               const char* pszKey,
                               bool bDefault)
{
    const char* pszVal = CPLGetXMLValue(psNode, pszKey, NULL);
    if( pszVal )
        return CPLTestBool(pszVal);
    else
        return bDefault;
}

/************************************************************************/
/*                            IsValidXPath()                            */
/************************************************************************/

static bool IsValidXPath(const CPLString& osXPath )
{
    // Check that the XPath syntax belongs to the subset we
    // understand
    bool bOK = !osXPath.empty();
    for(size_t i = 0; i < osXPath.size(); ++i )
    {
        const char chCur = osXPath[i];
        if( chCur == '/' )
        {
            // OK
        }
        else if( chCur == '@' &&
                 (i == 0 || osXPath[i-1] == '/') &&
                 i < osXPath.size()-1 &&
                 isalpha( static_cast<int>(osXPath[i+1]) ) )
        {
            // OK
        }
        else if( chCur == '_' ||
                    isalpha( static_cast<int>(chCur) ) )
        {
            // OK
        }
        else if( isdigit( static_cast<int>(chCur) ) &&
                    i > 0 &&
                    (isalnum( static_cast<int>(osXPath[i-1]) ) ||
                    osXPath[i-1] == '_') )
        {
            // OK
        }
        else if( chCur == ':' &&
                 i > 0 &&
                 (isalnum( static_cast<int>(osXPath[i-1]) ) ||
                  osXPath[i-1] == '_') &&
                 i < osXPath.size()-1 &&
                 isalpha( static_cast<int>(osXPath[i+1]) ) )
        {
            // OK
        }
        else
        {
            bOK = false;
            break;
        }
    }
    return bOK;
}

/************************************************************************/
/*                                 Load()                               */
/************************************************************************/

bool GMLASConfiguration::Load(const char* pszFilename)
{
    // Allow configuration to be inlined
    CPLXMLNode* psRoot = STARTS_WITH(pszFilename, "<Configuration>") ?
                                CPLParseXMLString(pszFilename) :
                                CPLParseXMLFile(pszFilename);
    if( psRoot == NULL )
    {
        Finalize();
        return false;
    }
    CPLXMLTreeCloser oCloser(psRoot);

    m_bAllowRemoteSchemaDownload = CPLGetXMLBoolValue(psRoot,
                                "=Configuration.AllowRemoteSchemaDownload",
                                ALLOW_REMOTE_SCHEMA_DOWNLOAD_DEFAULT );

    m_bAllowXSDCache = CPLGetXMLBoolValue( psRoot,
                                           "=Configuration.SchemaCache.enabled",
                                           ALLOW_XSD_CACHE_DEFAULT );
    if( m_bAllowXSDCache )
    {
        m_osCacheDirectory =
            CPLGetXMLValue(psRoot, "=Configuration.SchemaCache.Directory",
                           "");
    }

    m_bValidate = CPLGetXMLBoolValue( psRoot,
                                      "=Configuration.Validation.enabled",
                                      VALIDATE_DEFAULT );

    if( m_bValidate )
    {
        m_bFailIfValidationError = CPLGetXMLBoolValue(psRoot,
                                        "=Configuration.Validation.FailIfError",
                                        FAIL_IF_VALIDATION_ERROR_DEFAULT );
    }

    m_bExposeMetadataLayers = CPLGetXMLBoolValue( psRoot,
                                      "=Configuration.ExposeMetadataLayers",
                                      EXPOSE_METADATA_LAYERS_DEFAULT );

    m_bAlwaysGenerateOGRId = CPLGetXMLBoolValue( psRoot,
                                "=Configuration.LayerBuildingRules.AlwaysGenerateOGRId",
                                ALWAYS_GENERATE_OGR_ID_DEFAULT );

    m_bRemoveUnusedLayers = CPLGetXMLBoolValue( psRoot,
                                "=Configuration.LayerBuildingRules.RemoveUnusedLayers",
                                REMOVE_UNUSED_LAYERS_DEFAULT );

    m_bRemoveUnusedFields = CPLGetXMLBoolValue( psRoot,
                                "=Configuration.LayerBuildingRules.RemoveUnusedFields",
                                REMOVE_UNUSED_FIELDS_DEFAULT );

    m_bUseArrays = CPLGetXMLBoolValue( psRoot,
                                "=Configuration.LayerBuildingRules.UseArrays",
                                USE_ARRAYS_DEFAULT );
    m_bIncludeGeometryXML = CPLGetXMLBoolValue( psRoot,
                       "=Configuration.LayerBuildingRules.GML.IncludeGeometryXML",
                       INCLUDE_GEOMETRY_XML_DEFAULT );
    m_bInstantiateGMLFeaturesOnly = CPLGetXMLBoolValue( psRoot,
                "=Configuration.LayerBuildingRules.GML.InstantiateGMLFeaturesOnly",
                INSTANTIATE_GML_FEATURES_ONLY_DEFAULT );

    CPLXMLNode* psIgnoredXPaths = CPLGetXMLNode(psRoot,
                                            "=Configuration.IgnoredXPaths");
    if( psIgnoredXPaths )
    {
        const bool bGlobalWarnIfIgnoredXPathFound = CPLGetXMLBoolValue(
                       psIgnoredXPaths,
                       "WarnIfIgnoredXPathFoundInDocInstance",
                       WARN_IF_EXCLUDED_XPATH_FOUND_DEFAULT );

        CPLXMLNode* psNamespaces = CPLGetXMLNode(psIgnoredXPaths,
                                                 "Namespaces");
        if( psNamespaces != NULL )
        {
            for( CPLXMLNode* psIter = psNamespaces->psChild;
                             psIter != NULL;
                             psIter = psIter->psNext )
            {
                if( psIter->eType == CXT_Element &&
                    EQUAL(psIter->pszValue, "Namespace") )
                {
                    CPLString osPrefix = CPLGetXMLValue(psIter, "prefix", "");
                    CPLString osURI = CPLGetXMLValue(psIter, "uri", "");
                    if( !osPrefix.empty() && !osURI.empty() )
                    {
                        if( m_oMapPrefixToURIIgnoredXPaths.find(osPrefix) ==
                            m_oMapPrefixToURIIgnoredXPaths.end() )
                        {
                            m_oMapPrefixToURIIgnoredXPaths[osPrefix] = osURI;
                        }
                        else
                        {
                            CPLError(CE_Warning, CPLE_AppDefined,
                                     "Prefix %s was already mapped to %s. "
                                     "Attempt to map it to %s ignored",
                                     osPrefix.c_str(),
                                     m_oMapPrefixToURIIgnoredXPaths[osPrefix].
                                                                        c_str(),
                                     osURI.c_str());
                        }
                    }
                }
            }
        }

        for( CPLXMLNode* psIter = psIgnoredXPaths->psChild;
                         psIter != NULL;
                         psIter = psIter->psNext )
        {
            if( psIter->eType == CXT_Element &&
                EQUAL(psIter->pszValue, "XPath") )
            {
                const CPLString& osXPath( CPLGetXMLValue(psIter, "", "") );
                if( IsValidXPath(osXPath) )
                {
                    m_aosIgnoredXPaths.push_back( osXPath );

                    const bool bWarnIfIgnoredXPathFound = CPLGetXMLBoolValue(
                                psIter,
                                "warnIfIgnoredXPathFoundInDocInstance",
                                bGlobalWarnIfIgnoredXPathFound );
                    m_oMapIgnoredXPathToWarn[ osXPath ] = bWarnIfIgnoredXPathFound;
                }
                else
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "XPath syntax %s not supported",
                             osXPath.c_str());
                }
            }
        }
    }

    Finalize();

    return true;
}
