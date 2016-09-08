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

CPL_CVSID("$Id$");

/************************************************************************/
/*                        IsCompatibleOfArray()                         */
/************************************************************************/

static bool IsCompatibleOfArray( GMLASFieldType eType )
{
    return eType == GMLAS_FT_STRING ||
           eType == GMLAS_FT_BOOLEAN ||
           eType == GMLAS_FT_SHORT ||
           eType == GMLAS_FT_INT32 ||
           eType == GMLAS_FT_INT64 ||
           eType == GMLAS_FT_FLOAT ||
           eType == GMLAS_FT_DOUBLE ||
           eType == GMLAS_FT_DECIMAL ||
           eType == GMLAS_FT_ANYURI;
}


/************************************************************************/
/*                       GMLASPrefixMappingHander                       */
/************************************************************************/

class GMLASPrefixMappingHander: public DefaultHandler
{
        std::map<CPLString, CPLString>& m_oMapURIToPrefix;
  public:
        GMLASPrefixMappingHander(
                        std::map<CPLString, CPLString>& oMapURIToPrefix) :
            m_oMapURIToPrefix( oMapURIToPrefix )
        {}

        virtual void startPrefixMapping(const XMLCh* const prefix,
                                        const XMLCh* const uri);
};

/************************************************************************/
/*                         startPrefixMapping()                         */
/************************************************************************/

void GMLASPrefixMappingHander::startPrefixMapping(const XMLCh* const prefix,
                                                  const XMLCh* const uri)
{
    const CPLString osURI( transcode(uri) );
    const CPLString osPrefix( transcode(prefix) );
    if( !osPrefix.empty() )
    {
        std::map<CPLString, CPLString>::iterator oIter = 
                    m_oMapURIToPrefix.find( osURI );
        if( oIter == m_oMapURIToPrefix.end() )
        {
            m_oMapURIToPrefix[ osURI ] = osPrefix;
            CPLDebug("GMLAS", "Registering prefix=%s for uri=%s",
                     osPrefix.c_str(), osURI.c_str());
        }
        else if( oIter->second != osPrefix )
        {
            CPLDebug("GMLAS",
                     "Existing prefix=%s for uri=%s (new prefix %s not used)",
                    oIter->second.c_str(), osURI.c_str(), osPrefix.c_str());
        }
    }
}

/************************************************************************/
/*                        CollectNamespacePrefixes()                    */
/************************************************************************/

static
void CollectNamespacePrefixes(const char* pszXSDFilename,
                              VSILFILE* fpXSD,
                              std::map<CPLString, CPLString>& oMapURIToPrefix)
{
    GMLASInputSource oSource(pszXSDFilename, fpXSD, false);
    // This is a bit silly but the startPrefixMapping() callback only gets
    // called when using SAX2XMLReader::parse(), and not when using
    // loadGrammar(), so we have to parse the doc twice.
    SAX2XMLReader* poReader = XMLReaderFactory::createXMLReader ();

    GMLASPrefixMappingHander contentHandler(oMapURIToPrefix);
    poReader->setContentHandler(&contentHandler);

    GMLASErrorHandler oErrorHandler;
    poReader->setErrorHandler(&oErrorHandler);

    poReader->parse(oSource);
    delete poReader;
}

/************************************************************************/
/*                         GMLASEntityResolver                          */
/************************************************************************/

class GMLASEntityResolver: public EntityResolver,
                           public IGMLASInputSourceClosing
{
        std::vector<CPLString> m_aosPathStack;
        std::map<CPLString, CPLString>& m_oMapURIToPrefix;
        GMLASResourceCache& m_oCache;

  public:
        GMLASEntityResolver(const CPLString& osBasePath,
                            std::map<CPLString, CPLString>& oMapURIToPrefix,
                            GMLASResourceCache& oCache)
            : m_oMapURIToPrefix(oMapURIToPrefix)
            , m_oCache(oCache)
        {
            m_aosPathStack.push_back(osBasePath);
        }

        virtual ~GMLASEntityResolver()
        {
            CPLAssert( m_aosPathStack.size() == 1 );
        }

        /* Called by GMLASInputSource destructor. This is useful for use to */
        /* know where a .xsd has been finished from processing. Note that we */
        /* strongly depend on Xerces behaviour here... */
        virtual void notifyClosing(const CPLString& osFilename )
        {
            CPLDebug("GMLAS", "Closing %s", osFilename.c_str());

            CPLAssert( m_aosPathStack.back() ==
                                        CPLString(CPLGetDirname(osFilename)) );
            m_aosPathStack.pop_back();
        }

        virtual InputSource* resolveEntity( const XMLCh* const publicId,
                                            const XMLCh* const systemId);
};

/************************************************************************/
/*                         resolveEntity()                              */
/************************************************************************/

InputSource* GMLASEntityResolver::resolveEntity( const XMLCh* const /*publicId*/,
                                                 const XMLCh* const systemId)
{
    CPLString osSystemId(transcode(systemId));

    CPLString osNewPath;
    VSILFILE* fp = m_oCache.Open(osSystemId,
                                 m_aosPathStack.back(),
                                 osNewPath);
    if( fp == NULL )
    {
        return NULL;
    }
    CPLDebug("GMLAS", "Opening %s", osNewPath.c_str());

    CollectNamespacePrefixes(osNewPath, fp, m_oMapURIToPrefix);
    VSIFSeekL(fp, 0, SEEK_SET);

    m_aosPathStack.push_back( CPLGetDirname(osNewPath) );
    GMLASInputSource* poIS = new GMLASInputSource(osNewPath, fp, true);
    poIS->SetClosingCallback(this);
    return poIS;
}

/************************************************************************/
/*                        GMLASSchemaAnalyzer()                         */
/************************************************************************/

GMLASSchemaAnalyzer::GMLASSchemaAnalyzer()
    : m_bAllowArrays(false)
{
    // A few hardcoded namespace uri->prefix mappings
    m_oMapURIToPrefix[ pszXMLNS_URI ] = "xmlns";
    m_oMapURIToPrefix[ pszXSI_URI ] = "xsi";
}

/************************************************************************/
/*                               MakeXPath()                            */
/************************************************************************/

CPLString GMLASSchemaAnalyzer::MakeXPath( const CPLString& osNamespace,
                                          const CPLString& osName )
{
    if( osNamespace.size() == 0 )
        return osName;
    std::map<CPLString,CPLString>::const_iterator oIter =
                                        m_oMapURIToPrefix.find(osNamespace);
    if( oIter != m_oMapURIToPrefix.end() )
        return oIter->second + ":" + osName;
    else
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Cannot find prefix for ns='%s' (name='%s')",
                 osNamespace.c_str(), osName.c_str());
        return osName;
    }
}

/************************************************************************/
/*                         GetNSOfLastXPathComponent()                  */
/************************************************************************/

// Return the namespace (if any) of the last component of the XPath
static CPLString GetNSOfLastXPathComponent(const CPLString& osXPath )
{
    size_t nPos = osXPath.rfind('@');
    if( nPos != std::string::npos )
        nPos ++;
    else
    {
        nPos = osXPath.rfind('/');
        if( nPos != std::string::npos )
            nPos ++;
        else
            nPos = 0;
    }
    size_t nPosColumn = osXPath.find(':', nPos);
    if( nPosColumn == std::string::npos )
        return CPLString();
    return CPLString(osXPath.substr(nPos, nPosColumn - nPos));
}

/************************************************************************/
/*                         FixDuplicatedFieldNames()                    */
/************************************************************************/

// Make sure that field names are unique within the class
void GMLASSchemaAnalyzer::FixDuplicatedFieldNames( GMLASFeatureClass& oClass )
{
    // Duplicates can happen if a class has both an element and an attribute
    // with same name, and/or attributes/elements with same name in different
    // namespaces.
    bool bHasDoneSomeRenaming = false;
    do
    {
        bHasDoneSomeRenaming = false;

        // Detect duplicated field names
        std::map<CPLString, std::vector<int> > oSetNames;
        std::vector<GMLASField>& aoFields = oClass.GetFields();
        for(size_t i=0; i<aoFields.size();i++)
        {
            if( !aoFields[i].IsAbstract() )
            {
                oSetNames[ aoFields[i].GetName() ].push_back(i ) ;
            }
        }

        // Iterate over the unique names
        std::map<CPLString, std::vector<int> >::const_iterator
                oIter = oSetNames.begin();
        for(; oIter != oSetNames.end(); ++oIter)
        {
            // Has it duplicates ?
            const size_t nOccurences = oIter->second.size();
            if( nOccurences > 1 )
            {
                const CPLString oClassNS =
                        GetNSOfLastXPathComponent(oClass.GetXPath());
                bool bHasDoneRemnamingForThatCase = false;

                for(size_t i=0; i<nOccurences;i++)
                {
                    GMLASField& oField = aoFields[oIter->second[i]];
                    // CPLDebug("GMLAS", "%s", oField.GetXPath().c_str() );
                    const CPLString oNS(
                                GetNSOfLastXPathComponent(oField.GetXPath()));
                    // If the field has a namespace that is not the one of its
                    // class, then prefix its name with its namespace
                    if( oNS.size() && oNS != oClassNS &&
                        !STARTS_WITH(oField.GetName(), (oNS + "_").c_str() ) )
                    {
                        bHasDoneSomeRenaming = true;
                        bHasDoneRemnamingForThatCase = true;
                        oField.SetName( oNS + "_" + oField.GetName() );
                        break;
                    }
                    // If it is an attribute without a particular namespace,
                    // then suffix with _attr
                    else if( oNS.size() == 0 &&
                             oField.GetXPath().find('@') != std::string::npos &&
                             oField.GetName().find("_attr") == std::string::npos )
                    {
                        bHasDoneSomeRenaming = true;
                        bHasDoneRemnamingForThatCase = true;
                        oField.SetName( oField.GetName() + "_attr" );
                        break;
                    }
                }

                // If none of the above renaming strategies have worked, then
                // append a counter to the duplicates.
                if( !bHasDoneRemnamingForThatCase )
                {
                    for(size_t i=0; i<nOccurences;i++)
                    {
                        GMLASField& oField = aoFields[oIter->second[i]];
                        if( i > 0 )
                        {
                            bHasDoneSomeRenaming = true;
                            oField.SetName( oField.GetName() +
                                CPLSPrintf("%d", static_cast<int>(i)+1) );
                        }
                    }
                }
            }
        }
    }
    // As renaming could have created new duplicates (hopefully not!), loop
    // until no renaming has been done.
    while( bHasDoneSomeRenaming );

    // Recursively process nested classes
    std::vector<GMLASFeatureClass> aoNestedClasses = oClass.GetNestedClasses();
    for(size_t i=0; i<aoNestedClasses.size();i++)
    {
        FixDuplicatedFieldNames( aoNestedClasses[i] );
    }
}

/************************************************************************/
/*                       GMLASUniquePtr()                               */
/************************************************************************/

// Poor-man std::unique_ptr
template<class T> class GMLASUniquePtr
{
        T* m_p;

        GMLASUniquePtr(const GMLASUniquePtr&);
        GMLASUniquePtr& operator=(const GMLASUniquePtr&);

    public:
        GMLASUniquePtr(T* p): m_p(p) {}
       ~GMLASUniquePtr() { delete m_p; }

       T* operator->() const { CPLAssert(m_p); return m_p; }

       T* get () const { return m_p; }
       T* release() { T* ret = m_p; m_p = NULL; return ret; }
};

/************************************************************************/
/*                   GetTopElementDeclarationFromXPath()                */
/************************************************************************/

XSElementDeclaration* GMLASSchemaAnalyzer::GetTopElementDeclarationFromXPath(
                                                    const CPLString& osXPath,
                                                    XSModel* poModel)
{
    const char* pszTypename = osXPath.c_str();
    const char* pszName = strrchr(pszTypename, ':');
    if( pszName )
        pszName ++;
    XSElementDeclaration* poEltDecl = NULL;
    if( pszName != NULL )
    {
        CPLString osNSPrefix = pszTypename;
        osNSPrefix.resize( pszName - 1 - pszTypename );
        CPLString osName = pszName;
        CPLString osNSURI;

        std::map<CPLString, CPLString>::const_iterator oIterNS =
                                            m_oMapURIToPrefix.begin();
        for( ; oIterNS != m_oMapURIToPrefix.end(); ++oIterNS)
        {
            const CPLString& osIterNSURI(oIterNS->first);
            const CPLString& osIterNSPrefix(oIterNS->second);
            if( osNSPrefix == osIterNSPrefix )
            {
                osNSURI = osIterNSURI;
                break;
            }
        }
        XMLCh* xmlNS = XMLString::transcode(osNSURI);
        XMLCh* xmlName = XMLString::transcode(osName);
        poEltDecl = poModel->getElementDeclaration(xmlName, xmlNS);
        XMLString::release( &xmlNS );
        XMLString::release( &xmlName );
    }
    else
    {
        XMLCh* xmlName = XMLString::transcode(pszTypename);
        poEltDecl = poModel->getElementDeclaration(xmlName, NULL);
        XMLString::release( &xmlName );
    }
    return poEltDecl;
}

/************************************************************************/
/*                               Analyze()                              */
/************************************************************************/

bool GMLASSchemaAnalyzer::Analyze(const CPLString& osBaseDirname,
                                  const std::vector<PairURIFilename>& aoXSDs)
{
    GMLASResourceCache oCache;

    GMLASUniquePtr<XMLGrammarPool> poGrammarPool(
         (new XMLGrammarPoolImpl(XMLPlatformUtils::fgMemoryManager)));

    std::vector<CPLString> aoNamespaces;
    for( size_t i = 0; i < aoXSDs.size(); i++ )
    {
        const CPLString osURI(aoXSDs[i].first);
        const CPLString osOriFilename(aoXSDs[i].second);
        const CPLString osXSDFilename( 
            (osOriFilename.find("http://") != 0 &&
             osOriFilename.find("https://") != 0 &&
             CPLIsFilenameRelative(osOriFilename)) ?
                CPLString(CPLFormFilename(osBaseDirname, osOriFilename, NULL)) :
                osOriFilename );
        CPLString osResolvedFilename;
        VSILFILE* fpXSD = oCache.Open( osXSDFilename, CPLString(),
                                       osResolvedFilename );
        if( fpXSD == NULL )
        {
            return FALSE;
        }

        CollectNamespacePrefixes(osResolvedFilename, fpXSD, m_oMapURIToPrefix);

        GMLASUniquePtr<SAX2XMLReader> poParser(
                    XMLReaderFactory::createXMLReader(
                                    XMLPlatformUtils::fgMemoryManager,
                                    poGrammarPool.get()));

        // Commonly useful configuration.
        //
        poParser->setFeature (XMLUni::fgSAX2CoreNameSpaces, true);
        poParser->setFeature (XMLUni::fgSAX2CoreNameSpacePrefixes, true);
        poParser->setFeature (XMLUni::fgSAX2CoreValidation, true);

        // Enable validation.
        //
        poParser->setFeature (XMLUni::fgXercesSchema, true);
        poParser->setFeature (XMLUni::fgXercesSchemaFullChecking, true);
        poParser->setFeature (XMLUni::fgXercesValidationErrorAsFatal, true);

        // Use the loaded grammar during parsing.
        //
        poParser->setFeature (XMLUni::fgXercesUseCachedGrammarInParse, true);

        // Don't load schemas from any other source (e.g., from XML document's
        // xsi:schemaLocation attributes).
        //
        poParser->setFeature (XMLUni::fgXercesLoadSchema, false);

        GMLASErrorHandler oErrorHandler;
        poParser->setErrorHandler(&oErrorHandler);

        CPLString osXSDDirname( CPLGetDirname(osXSDFilename) );
        if( osOriFilename.find("http://") == 0 || 
            osOriFilename.find("https://") == 0 )
        {
            osXSDDirname = CPLGetDirname(("/vsicurl_streaming/" + osXSDFilename).c_str());
        }
        GMLASEntityResolver oEntityResolver( osXSDDirname,
                                             m_oMapURIToPrefix,
                                             oCache );
        poParser->setEntityResolver(&oEntityResolver);

        GMLASInputSource oSource(osResolvedFilename, fpXSD, false);
        const bool bCacheGrammar = true;
        Grammar* poGrammar = poParser->loadGrammar(oSource,
                                                   Grammar::SchemaGrammarType,
                                                   bCacheGrammar);
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

        // Some .xsd like
        // http://www.opengis.net/gwml-main/2.1 -> https://wfspoc.brgm-rec.fr/constellation/WS/wfs/BRGM:GWML2?request=DescribeFeatureType&version=2.0.0&service=WFS&namespace=xmlns(ns1=http://www.opengis.net/gwml-main/2.1)&typenames=ns1:GW_Aquifer
        // do not have a declared targetNamespace, so use the one of the
        // schemaLocation if the grammar returns an empty namespace.
        CPLString osGrammarURI( transcode(poGrammar->getTargetNamespace()) );
        if( osGrammarURI.empty() )
            aoNamespaces.push_back( osURI );
        else
            aoNamespaces.push_back( osGrammarURI );
    }

    bool changed;
    XSModel* poModel = poGrammarPool->getXSModel(changed);
    CPLAssert(poModel); // should not be null according to doc

#if 0
    XSNamespaceItem* nsItem = poModel->getNamespaceItem(
                                        loadedGrammar->getTargetNamespace());
    if( nsItem == NULL )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "getNamespaceItem(%s) failed",
                 transcode(loadedGrammar->getTargetNamespace()).c_str());
        return false;
    }
#endif

    // Initial pass, in all namespaces, to figure out inheritance relationships
    // and group models that have names
    std::map<CPLString, CPLString>::const_iterator oIterNS =
                                                    m_oMapURIToPrefix.begin();
    for( ; oIterNS != m_oMapURIToPrefix.end(); ++oIterNS)
    {
        const CPLString& osNSURI(oIterNS->first);
        if( osNSURI == pszXS_URI ||
            osNSURI == pszXSI_URI ||
            osNSURI == pszXMLNS_URI ||
            osNSURI == pszXLINK_URI )
        {
            continue;
        }

        XMLCh* xmlNamespace = XMLString::transcode(osNSURI.c_str());

        XSNamedMap<XSObject>* poMapModelGroupDefinition =
            poModel->getComponentsByNamespace(XSConstants::MODEL_GROUP_DEFINITION,
                                            xmlNamespace);

        // Remember group models that have names
        for(XMLSize_t i = 0; poMapModelGroupDefinition != NULL &&
                             i <  poMapModelGroupDefinition->getLength(); i++ )
        {
            XSModelGroupDefinition* modelGroupDefinition =
                reinterpret_cast<XSModelGroupDefinition*>(
                                            poMapModelGroupDefinition->item(i));
            m_oMapModelGroupDefinitionToName[modelGroupDefinition->getModelGroup()]
                            = transcode(modelGroupDefinition->getName());
        }

        CPLDebug("GMLAS", "Discovering substitutions of %s (%s)",
                 oIterNS->second.c_str(), osNSURI.c_str());

        XSNamedMap<XSObject>* poMapElements = poModel->getComponentsByNamespace(
                            XSConstants::ELEMENT_DECLARATION, xmlNamespace);

        for(XMLSize_t i = 0; poMapElements != NULL &&
                             i < poMapElements->getLength(); i++ )
        {
            XSElementDeclaration* poEltDecl =
                reinterpret_cast<XSElementDeclaration*>(poMapElements->item(i));
            XSElementDeclaration* poSubstGroup =
                            poEltDecl->getSubstitutionGroupAffiliation();
            if( poSubstGroup )
            {
                CPLString osParentType(
                            transcode(poSubstGroup->getNamespace()) + ":" +
                            transcode(poSubstGroup->getName()));
                m_oMapParentTypeToChildTypes[osParentType].push_back(poEltDecl);
#ifdef DEBUG_VERBOSE
                CPLString osChildType(
                            transcode(poEltDecl->getNamespace()) + ":" +
                            transcode(poEltDecl->getName()));
                CPLDebug("GMLAS", "%s is a substitution for %s",
                        osChildType.c_str(),
                        osParentType.c_str());
#endif
            }
        }

        XMLString::release(&xmlNamespace);
    }

    // Find which elements must be top levels (because referenced several
    // times)
    std::set<XSElementDeclaration*> oSetVisitedEltDecl;
    std::set<XSModelGroup*> oSetVisitedModelGroups;
    for( size_t iNS = 0; iNS < aoNamespaces.size(); iNS++ )
    {
        XMLCh* xmlNamespace = XMLString::transcode(aoNamespaces[iNS].c_str());

        XSNamedMap<XSObject>* poMapElements = poModel->getComponentsByNamespace(
            XSConstants::ELEMENT_DECLARATION, xmlNamespace);

        for(XMLSize_t i = 0; poMapElements != NULL &&
                             i < poMapElements->getLength(); i++ )
        {
            XSElementDeclaration* poEltDecl =
                reinterpret_cast<XSElementDeclaration*>(poMapElements->item(i));
            XSTypeDefinition* poTypeDef = poEltDecl->getTypeDefinition();
            if( poTypeDef->getTypeCategory() == XSTypeDefinition::COMPLEX_TYPE &&
                !poEltDecl->getAbstract() &&
                transcode(poEltDecl->getName()) != "FeatureCollection" )
            {
                XSComplexTypeDefinition* poCT =
                            reinterpret_cast<XSComplexTypeDefinition*>(poTypeDef);
                if( poCT->getContentType() ==
                                    XSComplexTypeDefinition::CONTENTTYPE_ELEMENT )
                {
                    FindElementsWithMustBeToLevel(
                            poCT->getParticle()->getModelGroupTerm(),
                            0,
                            oSetVisitedEltDecl,
                            oSetVisitedModelGroups,
                            poModel);
                }
            }
        }
    }

    // Iterate over top level elements of namespaces of interest to instanciate
    // classes
    for( size_t iNS = 0; iNS < aoNamespaces.size(); iNS++ )
    {
        XMLCh* xmlNamespace = XMLString::transcode(aoNamespaces[iNS].c_str());

        XSNamedMap<XSObject>* poMapElements = poModel->getComponentsByNamespace(
            XSConstants::ELEMENT_DECLARATION, xmlNamespace);

        for(XMLSize_t i = 0; poMapElements != NULL &&
                             i < poMapElements->getLength(); i++ )
        {
            XSElementDeclaration* poEltDecl =
                reinterpret_cast<XSElementDeclaration*>(poMapElements->item(i));
            bool bError = false;
            InstantiateClassFromEltDeclaration(poEltDecl, poModel, bError);
            if( bError )
            {
                XMLString::release(&xmlNamespace);
                return false;
            }
        }

        XMLString::release(&xmlNamespace);
    }

    // Make sure that all needed typenames are instanciated
    // This can happen if the main schema references abstract types of other
    // namespaces
    while( true )
    {
        bool bChangeMade = false;
        // Make a local copy of m_oSetNeededTypenames since it might be
        // modified by InstantiateClassFromEltDeclaration()
        std::set<XSElementDeclaration*> oSetNeeded(m_oSetNeededTypenames);
        std::set<XSElementDeclaration*>::iterator oIter = oSetNeeded.begin();
        for(; oIter != oSetNeeded.end(); ++oIter )
        {
            XSElementDeclaration* poEltDecl = *oIter;
            if( m_oSetTypenames.find(poEltDecl) != m_oSetTypenames.end() )
                continue;

            bool bError = false;
            bool bResolvedType = InstantiateClassFromEltDeclaration(poEltDecl,
                                                                    poModel,
                                                                    bError);
            if( bError )
            {
                return false;
            }
            if( bResolvedType )
            {
                bChangeMade = true;
            }
            else
            {
                CPLString osTypenameXPath( MakeXPath(
                                    transcode(poEltDecl->getNamespace()),
                                    transcode(poEltDecl->getName()) ) );
                CPLError(CE_Failure, CPLE_AppDefined,
                         "Couldn't resolve %s", osTypenameXPath.c_str());
                return false;
            }
        }
        if( !bChangeMade )
            break;
    }

    return true;
}

/************************************************************************/
/*                  InstantiateClassFromEltDeclaration()                */
/************************************************************************/

bool GMLASSchemaAnalyzer::InstantiateClassFromEltDeclaration(
                                                XSElementDeclaration* poEltDecl,
                                                XSModel* poModel,
                                                bool& bError)
{
    bError = false;
    XSTypeDefinition* poTypeDef = poEltDecl->getTypeDefinition();
    if( poTypeDef->getTypeCategory() == XSTypeDefinition::COMPLEX_TYPE &&
        !poEltDecl->getAbstract() &&
        transcode(poEltDecl->getName()) != "FeatureCollection" )
    {
        XSComplexTypeDefinition* poCT =
                    reinterpret_cast<XSComplexTypeDefinition*>(poTypeDef);
        if( poCT->getContentType() ==
                            XSComplexTypeDefinition::CONTENTTYPE_ELEMENT )
        {
            GMLASFeatureClass oClass;
            oClass.SetName( transcode(poEltDecl->getName()) );

            CPLString osXPath( MakeXPath(
                                    transcode(poEltDecl->getNamespace()),
                                    transcode(poEltDecl->getName()) ) );
#ifdef DEBUG_VERBOSE
            CPLDebug("GMLAS", "Instantiating element %s", osXPath.c_str());
#endif
            oClass.SetXPath( osXPath );
            oClass.SetIsTopLevel(
                GetTopElementDeclarationFromXPath(osXPath, poModel) != NULL );

            m_oSetTypenames.insert( poEltDecl );
            std::set<XSModelGroup*> oSetVisitedModelGroups;
            if( !ExploreModelGroup(
                                poCT->getParticle()->getModelGroupTerm(),
                                poCT->getAttributeUses(),
                                oClass,
                                0,
                                oSetVisitedModelGroups ) )
            {
                bError = true;
                return false;
            }

            FixDuplicatedFieldNames( oClass );

            m_aoClasses.push_back(oClass);
            return true;
        }
    }
    return false;
}

/************************************************************************/
/*                 SetFieldTypeAndWidthFromDefinition()                 */
/************************************************************************/

void GMLASSchemaAnalyzer::SetFieldTypeAndWidthFromDefinition(
                                                 XSSimpleTypeDefinition* poST,
                                                 GMLASField& oField )
{
    int nMaxLength = INT_MAX;
    while( poST->getBaseType() != poST &&
            poST->getBaseType()->getTypeCategory() ==
                                        XSTypeDefinition::SIMPLE_TYPE &&
            !XMLString::equals(poST->getNamespace(),
                               PSVIUni::fgNamespaceXmlSchema) )
    {
        const XMLCh* maxLength = poST->getLexicalFacetValue(
                                    XSSimpleTypeDefinition::FACET_LENGTH );
        if( maxLength == NULL )
        {
            maxLength = poST->getLexicalFacetValue(
                                XSSimpleTypeDefinition::FACET_MAXLENGTH );
        }
        if( maxLength != NULL )
            nMaxLength = MIN(nMaxLength, atoi( transcode(maxLength) ) );
        poST = reinterpret_cast<XSSimpleTypeDefinition*>(poST->getBaseType());
    }

    if( XMLString::equals(poST->getNamespace(), PSVIUni::fgNamespaceXmlSchema) )
    {
        CPLString osType( transcode(poST->getName()) );
        oField.SetType( GMLASField::GetTypeFromString(osType), osType );
    }
    else
    {
        CPLError(CE_Warning, CPLE_AppDefined, "Base type is not a xs: one ???");
    }

    if( nMaxLength < INT_MAX )
        oField.SetWidth( nMaxLength );
}

/************************************************************************/
/*                              IsSame()                                */
/*                                                                      */
/* The objects returned by different PSVI API are not always the same   */
/* so do content inspection to figure out if they are equivalent.       */
/************************************************************************/

bool GMLASSchemaAnalyzer::IsSame( const XSModelGroup* poModelGroup1,
                                  const XSModelGroup* poModelGroup2 )
{
    if( poModelGroup1->getCompositor() != poModelGroup2->getCompositor() )
        return false;

    const XSParticleList* poParticleList1 = poModelGroup1->getParticles();
    const XSParticleList* poParticleList2 = poModelGroup2->getParticles();
    if( poParticleList1->size() != poParticleList2->size() )
        return false;

    for(size_t i = 0; i < poParticleList1->size(); ++i )
    {
        const XSParticle* poParticle1 = poParticleList1->elementAt(i);
        const XSParticle* poParticle2 = poParticleList2->elementAt(i);
        if( poParticle1->getTermType() != poParticle2->getTermType() ||
            poParticle1->getMinOccurs() != poParticle2->getMinOccurs() ||
            poParticle1->getMaxOccurs() != poParticle2->getMaxOccurs() ||
            poParticle1->getMaxOccursUnbounded() !=
                                        poParticle2->getMaxOccursUnbounded() )
        {
            return false;
        }
        switch( poParticle1->getTermType() )
        {
            case XSParticle::TERM_EMPTY:
                break;

            case XSParticle::TERM_ELEMENT:
            {
                const XSElementDeclaration* poElt1 =
                    const_cast<XSParticle*>(poParticle1)->getElementTerm();
                const XSElementDeclaration* poElt2 =
                    const_cast<XSParticle*>(poParticle2)->getElementTerm();
                // Pointer comparison works here
                if( poElt1 != poElt2 )
                    return false;
                break;
            }

            case XSParticle::TERM_MODELGROUP:
            {
                const XSModelGroup* psSubGroup1 =
                    const_cast<XSParticle*>(poParticle1)->getModelGroupTerm();
                const XSModelGroup* psSubGroup2 =
                    const_cast<XSParticle*>(poParticle2)->getModelGroupTerm();
                if( !IsSame(psSubGroup1, psSubGroup2) )
                    return false;
                break;
            }

            case XSParticle::TERM_WILDCARD:
            {
                // TODO: check that pointer comparison works
                const XSWildcard* psWildcard1 =
                    const_cast<XSParticle*>(poParticle1)->getWildcardTerm();
                const XSWildcard* psWildcard2 =
                    const_cast<XSParticle*>(poParticle2)->getWildcardTerm();
                if( psWildcard1 != psWildcard2 )
                    return false;
                break;
            }

            default:
            {
                CPLAssert(FALSE);
                return false;
            }
        }
    }

    return true;
}

/************************************************************************/
/*                           GetGroupName()                             */
/*                                                                      */
/*  The model group object returned when exploring a high level model   */
/*  group isn't the same object as the one returned by model group      */
/*  definitions and has no name. So we have to investigate the content  */
/*  of model groups to figure out if they are the same.                 */
/************************************************************************/

CPLString GMLASSchemaAnalyzer::GetGroupName( const XSModelGroup* poModelGroup )
{
    std::map< XSModelGroup*, CPLString>::const_iterator oIter =
        m_oMapModelGroupDefinitionToName.begin();
    for(; oIter != m_oMapModelGroupDefinitionToName.end(); ++oIter )
    {
        const XSModelGroup* psIterModelGroup = oIter->first;
        if( IsSame(poModelGroup, psIterModelGroup) )
        {
            return oIter->second;
        }
    }

    return CPLString();
}

/************************************************************************/
/*                              IsAnyType()                             */
/************************************************************************/

static bool IsAnyType(XSComplexTypeDefinition* poType)
{
    XSModelGroup* poGroupTerm = NULL;
    XSParticle* poParticle = NULL;
    XSParticleList* poParticles = NULL;
    return XMLString::equals(poType->getBaseType()->getNamespace(),
                             PSVIUni::fgNamespaceXmlSchema) &&
        transcode( poType->getBaseType()->getName() ) == "anyType" &&
        (poParticle = poType->getParticle()) != NULL &&
        (poGroupTerm = poParticle->getModelGroupTerm()) != NULL &&
        (poParticles = poGroupTerm->getParticles()) != NULL &&
        poParticles->size() == 1 &&
        poParticles->elementAt(0)->getTermType() == XSParticle::TERM_WILDCARD;
}

/************************************************************************/
/*                       SetFieldFromAttribute()                        */
/************************************************************************/

void GMLASSchemaAnalyzer::SetFieldFromAttribute(
                                  GMLASField& oField,
                                  XSAttributeUse* poAttr,
                                  const CPLString& osXPathPrefix,
                                  const CPLString& osNamePrefix)
{
    XSAttributeDeclaration* poAttrDecl = poAttr->getAttrDeclaration();
    XSSimpleTypeDefinition* poAttrType = poAttrDecl->getTypeDefinition();

    SetFieldTypeAndWidthFromDefinition(poAttrType, oField);

    CPLString osNS(transcode(poAttrDecl->getNamespace()));
    CPLString osName(transcode(poAttrDecl->getName()));

    if( osNamePrefix.empty() )
        oField.SetName( osName );
    else
        oField.SetName( osNamePrefix + "_" + osName );

    oField.SetXPath( osXPathPrefix + "@" +
                        MakeXPath( osNS, osName) );
    if( poAttr->getRequired() )
    {
        oField.SetNotNullable( true );
    }
    oField.SetMinOccurs( poAttr->getRequired() ? 1 : 0 );
    oField.SetMaxOccurs( 1 );
    if( poAttr->getConstraintType() ==
                            XSConstants::VALUE_CONSTRAINT_FIXED )
    {
        oField.SetFixedValue(
                    transcode(poAttr->getConstraintValue()) );
    }
    else if( poAttr->getConstraintType() ==
                            XSConstants::VALUE_CONSTRAINT_DEFAULT )
    {
        oField.SetDefaultValue(
                    transcode(poAttr->getConstraintValue()) );
    }
}

/************************************************************************/
/*                      GetConcreteImplementationTypes()                */
/************************************************************************/

void GMLASSchemaAnalyzer::GetConcreteImplementationTypes(
                                XSElementDeclaration* poParentElt,
                                std::vector<XSElementDeclaration*>& apoSubEltList)
{
    CPLString osParentType(transcode(poParentElt->getNamespace()) + ":" +
                           transcode(poParentElt->getName()));
    tMapParentTypeToChildTypes::const_iterator oIter =
        m_oMapParentTypeToChildTypes.find( osParentType );
    if( oIter == m_oMapParentTypeToChildTypes.end() )
        return;

    for( size_t j = 0; j < oIter->second.size(); j++ )
    {
        XSElementDeclaration* poSubElt = oIter->second[j];
        XSTypeDefinition* poSubEltType =
                                poSubElt->getTypeDefinition();
        XSComplexTypeDefinition* poCT;
        if( poSubEltType->getTypeCategory() ==
                            XSTypeDefinition::COMPLEX_TYPE &&
            (poCT = reinterpret_cast<XSComplexTypeDefinition*>(
                                            poSubEltType))
                    ->getContentType() ==
                XSComplexTypeDefinition::CONTENTTYPE_ELEMENT )
        {
            if( !poSubElt->getAbstract() )
                apoSubEltList.push_back(poSubElt);
            GetConcreteImplementationTypes(poSubElt, apoSubEltList);
        }
    }
}

/************************************************************************/
/*                        IsGMLGeometryProperty()                       */
/************************************************************************/

static bool IsGMLGeometryProperty( XSTypeDefinition* poTypeDef )
{
    CPLString osName(transcode(poTypeDef->getName()));
    return osName == "GeometryPropertyType" ||
           osName == "PointPropertyType" ||
           osName == "PolygonPropertyType" ||
           osName == "LineStringPropertyType" ||
           osName == "MultiPointPropertyType" ||
           osName == "MultiLineStringPropertyType" ||
           osName == "MultiPolygonPropertyType" ||
           osName == "MultiGeometryPropertyType" ||

           osName == "MultiCurvePropertyType" ||
           osName == "MultiSurfacePropertyType" ||
           osName == "MultiSolidPropertyType" ||
           /*osName == "GeometryArrayPropertyType" ||
           osName == "GeometricPrimitivePropertyType" || */
           osName == "CurvePropertyType" ||
           /* osName == "CurveArrayPropertyType" || */
           osName == "SurfacePropertyType" ||
           /*osName == "SurfaceArrayPropertyType" || */
           /*osName == "AbstractRingPropertyType" || */
           /*osName == "LinearRingPropertyType" || */
           osName == "CompositeCurvePropertyType" ||
           osName == "CompositeSurfacePropertyType" ||
           osName == "CompositeSolidPropertyType" ||
           osName == "GeometricComplexPropertyType";

#if 0
  <complexType name="CurveSegmentArrayPropertyType">
  <complexType name="KnotPropertyType">
  <complexType name="SurfacePatchArrayPropertyType">
  <complexType name="RingPropertyType">
  <complexType name="PolygonPatchArrayPropertyType">
  <complexType name="TrianglePatchArrayPropertyType">
  <complexType name="LineStringSegmentArrayPropertyType">
  <complexType name="SolidPropertyType">
  <complexType name="SolidArrayPropertyType">
#endif
}

/************************************************************************/
/*                      CreateNonNestedRelationship()                  */
/************************************************************************/

void GMLASSchemaAnalyzer::CreateNonNestedRelationship(
                        XSElementDeclaration* poElt,
                        std::vector<XSElementDeclaration*>& apoSubEltList,
                        GMLASFeatureClass& oClass,
                        int nMaxOccurs,
                        bool bForceJunctionTable )
{
    const CPLString osOnlyElementXPath(
                    MakeXPath(transcode(poElt->getNamespace()),
                            transcode(poElt->getName())) );
    const CPLString osElementXPath( oClass.GetXPath() + "/" +
                                    osOnlyElementXPath );

    if( !poElt->getAbstract() )
    {
        apoSubEltList.insert(apoSubEltList.begin(), poElt);
    }

    if( nMaxOccurs == 1 && !bForceJunctionTable )
    {
        // If the field isn't repeated, then we can link to each
        // potential realization types with a field
        for( size_t j = 0; j < apoSubEltList.size(); j++ )
        {
            XSElementDeclaration* poSubElt = apoSubEltList[j];
            const CPLString osSubEltXPath(
                MakeXPath(transcode(poSubElt->getNamespace()),
                            transcode(poSubElt->getName())) );

            GMLASField oField;
            if( apoSubEltList.size() > 1 )
            {
                oField.SetName( transcode(poElt->getName()) + "_" +
                            transcode(poSubElt->getName()) + "_pkid" );
            }
            else
            {
                oField.SetName( transcode(poElt->getName()) + "_pkid" );
            }
            oField.SetXPath( oClass.GetXPath() + "/" +
                                                osSubEltXPath);
            oField.SetMinOccurs( 0 );
            oField.SetMaxOccurs( nMaxOccurs );
            oField.SetNestedClassXPath(osSubEltXPath);
            oField.SetType( GMLAS_FT_STRING, "string" );
            oClass.AddField( oField );

            // Make sure we will instanciate the referenced element
            if( m_oSetTypenames.find( poSubElt ) == 
                        m_oSetTypenames.end() )
            {
#ifdef DEBUG_VERBOSE
                CPLDebug("GMLAS", "Adding %s as needed type",
                         osSubEltXPath.c_str() );
#endif
                m_oSetNeededTypenames.insert( poSubElt );
            }
        }
    }
    else
    {
        // If the field is repeated, we need to use junction
        // tables
        for( size_t j = 0; j < apoSubEltList.size(); j++ )
        {
            XSElementDeclaration* poSubElt = apoSubEltList[j];
            const CPLString osSubEltXPath(
                MakeXPath(transcode(poSubElt->getNamespace()),
                            transcode(poSubElt->getName())) );

            // Instantiate a junction table
            GMLASFeatureClass oJunctionTable;
            oJunctionTable.SetName( oClass.GetName() + "_" +
                    transcode(poElt->getName()) + "_" +
                    transcode(poSubElt->getName()) );
            oJunctionTable.SetXPath( osElementXPath + "|" +
                                        osSubEltXPath );
            oJunctionTable.SetParentXPath( oClass.GetXPath() );
            oJunctionTable.SetChildXPath( osSubEltXPath );
            m_aoClasses.push_back(oJunctionTable);

            // Add an abstract field
            GMLASField oField;
            oField.SetXPath( oClass.GetXPath() + "/" +
                                                osSubEltXPath);
            oField.SetMinOccurs( 0 );
            oField.SetMaxOccurs( nMaxOccurs );
            oField.SetAbstractElementXPath(osElementXPath);
            oField.SetNestedClassXPath(osSubEltXPath);
            oField.SetAbstract( true );
            oClass.AddField( oField );

            // Make sure we will instanciate the referenced element
            if( m_oSetTypenames.find( poSubElt ) == 
                        m_oSetTypenames.end() )
            {
#ifdef DEBUG_VERBOSE
                CPLDebug("GMLAS", "Adding %s as needed type",
                         osSubEltXPath.c_str() );
#endif
                m_oSetNeededTypenames.insert( poSubElt );
            }
        }

    }

#if 0
    GMLASField oField;
    oField.SetName( transcode(poElt->getName()) );
    oField.SetXPath( osElementXPath );
    oField.SetMinOccurs( poParticle->getMinOccurs() );
    oField.SetMaxOccurs( poParticle->getMaxOccursUnbounded() ?
        MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );

    for( size_t j = 0; j < apoSubEltList.size(); j++ )
    {
        XSElementDeclaration* poSubElt = apoSubEltList[j];
        XSTypeDefinition* poSubEltType =
                                    poSubElt->getTypeDefinition();
        XSComplexTypeDefinition* poCT =
            reinterpret_cast<XSComplexTypeDefinition*>(poSubEltType);

        GMLASFeatureClass oNestedClass;
        oNestedClass.SetName( oClass.GetName() + "_" +
                    transcode(poSubElt->getName()) );
        oNestedClass.SetXPath( oClass.GetXPath() + "/" +
            MakeXPath(transcode(poSubElt->getNamespace()),
                        transcode(poSubElt->getName())) );

        std::set<XSModelGroup*>
            oSetNewVisitedModelGroups(oSetVisitedModelGroups);
        if( !ExploreModelGroup(
                poCT->getParticle()->getModelGroupTerm(),
                NULL,
                oNestedClass,
                nRecursionCounter + 1,
                oSetNewVisitedModelGroups ) )
        {
            return false;
        }

        oClass.AddNestedClass( oNestedClass );
    }

    if( !apoSubEltList.empty() )
    {
        oField.SetAbstract(true);
    }
    else
    {
        oField.SetType( GMLAS_FT_ANYTYPE, "anyType" );
        oField.SetXPath( oClass.GetXPath() + "/" + "*" );
        oField.SetIncludeThisEltInBlob( true );
    }
    oClass.AddField( oField );
#endif
}

bool GMLASSchemaAnalyzer::FindElementsWithMustBeToLevel(
                            XSModelGroup* poModelGroup,
                            int nRecursionCounter,
                            std::set<XSElementDeclaration*>& oSetVisitedEltDecl,
                            std::set<XSModelGroup*>& oSetVisitedModelGroups,
                            XSModel* poModel)
{
    if( oSetVisitedModelGroups.find(poModelGroup) !=
                                                oSetVisitedModelGroups.end() )
    {
        return true;
    }

    oSetVisitedModelGroups.insert(poModelGroup);

    if( nRecursionCounter == 100 )
    {
        // Presumably an hostile schema
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Schema analysis failed due to too deeply nested model");
        return false;
    }

    XSParticleList* poParticles = poModelGroup->getParticles();
    for(size_t i = 0; i < poParticles->size(); ++i )
    {
        XSParticle* poParticle = poParticles->elementAt(i);
        if( poParticle->getTermType() == XSParticle::TERM_ELEMENT )
        {
            XSElementDeclaration* poElt = poParticle->getElementTerm();
            XSTypeDefinition* poTypeDef = poElt->getTypeDefinition();
            const CPLString osXPath(
                                MakeXPath(transcode(poElt->getNamespace()),
                                            transcode(poElt->getName())) );

            if( !poElt->getAbstract() &&
                poTypeDef->getTypeCategory() == XSTypeDefinition::COMPLEX_TYPE )
            {
                XSComplexTypeDefinition* poEltCT =
                        reinterpret_cast<XSComplexTypeDefinition*>(poTypeDef);
                if( poEltCT->getContentType() == 
                            XSComplexTypeDefinition::CONTENTTYPE_ELEMENT )
                {
                    if( oSetVisitedEltDecl.find(poElt) !=
                                    oSetVisitedEltDecl.end() )
                    {
                        if( m_oSetTopLevelElement.find(poElt) ==
                                                    m_oSetTopLevelElement.end() )
                        {
#ifdef DEBUG_VERBOSE
                            CPLDebug("GMLAS", "%s must be exposed as top-level",
                                     osXPath.c_str());
#endif
                            m_oSetTopLevelElement.insert(poElt);
                            if( m_oSetTypenames.find( poElt ) == 
                                                        m_oSetTypenames.end() )
                            {
                                m_oSetNeededTypenames.insert( poElt );
                            }
                        }
                    }
                    else
                    {
                        oSetVisitedEltDecl.insert(poElt);
                    }
                }

                if( poEltCT->getParticle() != NULL )
                {
                    if( !FindElementsWithMustBeToLevel(
                                    poEltCT->getParticle()->getModelGroupTerm(),
                                    nRecursionCounter + 1,
                                    oSetVisitedEltDecl,
                                    oSetVisitedModelGroups,
                                    poModel ) )
                    {
                        return false;
                    }
                }
            }

        }
        else if( poParticle->getTermType() == XSParticle::TERM_MODELGROUP )
        {
            XSModelGroup* psSubModelGroup = poParticle->getModelGroupTerm();
            if( !FindElementsWithMustBeToLevel( psSubModelGroup,
                                    nRecursionCounter + 1,
                                    oSetVisitedEltDecl,
                                    oSetVisitedModelGroups,
                                    poModel ) )
            {
                return false;
            }
        }
    }

    return true;
}

/************************************************************************/
/*                         ExploreModelGroup()                          */
/************************************************************************/

bool GMLASSchemaAnalyzer::ExploreModelGroup(
                            XSModelGroup* poModelGroup,
                            XSAttributeUseList* poMainAttrList,
                            GMLASFeatureClass& oClass,
                            int nRecursionCounter,
                            std::set<XSModelGroup*>& oSetVisitedModelGroups )
{
    if( oSetVisitedModelGroups.find(poModelGroup) !=
                                                oSetVisitedModelGroups.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "%s already visited",
                 oClass.GetXPath().c_str());
        return false;
    }
    oSetVisitedModelGroups.insert(poModelGroup);

    if( nRecursionCounter == 100 )
    {
        // Presumably an hostile schema
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Schema analysis failed due to too deeply nested model");
        return false;
    }

    const size_t nMainAttrListSize = (poMainAttrList != NULL) ? 
                                                    poMainAttrList->size(): 0;
    for(size_t j=0; j < nMainAttrListSize; ++j )
    {
        GMLASField oField;
        XSAttributeUse* poAttr = poMainAttrList->elementAt(j);
        SetFieldFromAttribute(oField, poAttr, oClass.GetXPath());
        oClass.AddField(oField);
    }

    const bool bIsChoice = (poModelGroup->getCompositor() ==
                                            XSModelGroup::COMPOSITOR_CHOICE);

    XSParticleList* poParticles = poModelGroup->getParticles();
    int nGroup = 0;
    for(size_t i = 0; i < poParticles->size(); ++i )
    {
        XSParticle* poParticle = poParticles->elementAt(i);
        const bool bRepeatedParticle = poParticle->getMaxOccursUnbounded() ||
                                       poParticle->getMaxOccurs() > 1;
        if( poParticle->getTermType() == XSParticle::TERM_ELEMENT )
        {
            XSElementDeclaration* poElt = poParticle->getElementTerm();
            const CPLString osOnlyElementXPath(
                          MakeXPath(transcode(poElt->getNamespace()),
                                    transcode(poElt->getName())) );
            const CPLString osElementXPath( oClass.GetXPath() + "/" +
                                            osOnlyElementXPath );
#ifdef DEBUG_VERBOSE
            CPLDebug("GMLAS", "Iterating through %s", osElementXPath.c_str());
#endif
            XSTypeDefinition* poTypeDef = poElt->getTypeDefinition();

            std::vector<XSElementDeclaration*> apoSubEltList;
            GetConcreteImplementationTypes(poElt, apoSubEltList);

            // Special case for a GML geometry property
            if( transcode(poTypeDef->getNamespace()).find(pszGML_URI) == 0 &&
                IsGMLGeometryProperty(poTypeDef) )
            {
                GMLASField oField;
                oField.SetName( transcode(poElt->getName()) );
                oField.SetMinOccurs( poParticle->getMinOccurs() );
                oField.SetMaxOccurs( poParticle->getMaxOccursUnbounded() ?
                    MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );
                oField.SetType( GMLAS_FT_ANYTYPE, "anyType" );
                oField.SetXPath( osElementXPath );

                oClass.AddField( oField );
            }

            // Any GML abstract type
            else if( poElt->getAbstract() &&
                transcode(poElt->getNamespace()).find(pszGML_URI) == 0 )
            {
                GMLASField oField;
                oField.SetName( transcode(poElt->getName()) );
                oField.SetMinOccurs( poParticle->getMinOccurs() );
                oField.SetMaxOccurs( poParticle->getMaxOccursUnbounded() ?
                    MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );
                oField.SetType( GMLAS_FT_ANYTYPE, "anyType" );
                oField.SetIncludeThisEltInBlob( true );

                for( size_t j = 0; j < apoSubEltList.size(); j++ )
                {
                    XSElementDeclaration* poSubElt = apoSubEltList[j];
                    oField.AddAlternateXPath( oClass.GetXPath() + "/" +
                         MakeXPath(transcode(poSubElt->getNamespace()),
                                   transcode(poSubElt->getName())) );
                }

                oClass.AddField( oField );
            }

            // Are there substitution groups for this element ?
            else if( !apoSubEltList.empty() ||
                        m_oSetTopLevelElement.find(poElt) != m_oSetTopLevelElement.end() )
            {
                const int nMaxOccurs =
                        poParticle->getMaxOccursUnbounded() ?
                            MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs();
                CreateNonNestedRelationship(poElt,
                                            apoSubEltList,
                                            oClass,
                                            nMaxOccurs,
                                            false);
            }

            // Abstract element without realizations !
            else if ( poElt->getAbstract() )
            {
                // Do nothing with it since it cannot be instanciated
                // in a valid way.
            }

            // Simple type like string, int, etc...
            else
            if( poTypeDef->getTypeCategory() == XSTypeDefinition::SIMPLE_TYPE )
            {
                XSSimpleTypeDefinition* poST =
                            reinterpret_cast<XSSimpleTypeDefinition*>(poTypeDef);
                GMLASField oField;
                SetFieldTypeAndWidthFromDefinition(poST, oField);
                oField.SetMinOccurs( poParticle->getMinOccurs() );
                oField.SetMaxOccurs( poParticle->getMaxOccursUnbounded() ?
                    MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );

                bool bNeedAuxTable = false;
                if( m_bAllowArrays && bRepeatedParticle &&
                    IsCompatibleOfArray(oField.GetType()) )
                {
                    oField.SetArray( true );
                }
                else if( bRepeatedParticle )
                {
                    bNeedAuxTable = true;
                }
                if( bNeedAuxTable )
                {
                    GMLASFeatureClass oNestedClass;
                    oNestedClass.SetName( oClass.GetName() + "_" +
                                          transcode(poElt->getName()) );
                    oNestedClass.SetXPath( osElementXPath );
                    GMLASField oUniqueField;
                    oUniqueField.SetName("value");
                    oUniqueField.SetMinOccurs( 1 );
                    oUniqueField.SetMaxOccurs( 1 );
                    oUniqueField.SetXPath( osElementXPath );
                    oUniqueField.SetType( oField.GetType(),
                                          oField.GetTypeName() );
                    oNestedClass.AddField(oUniqueField);
                    oClass.AddNestedClass( oNestedClass );

                    oField.SetName( transcode(poElt->getName()) );
                    oField.SetXPath( osElementXPath );
                    oField.SetIsNestedClass(true);
                    oClass.AddField(oField);
                }
                else
                {
                    oField.SetName( transcode(poElt->getName()) );
                    oField.SetXPath( osElementXPath );
                    if( !bIsChoice && poParticle->getMinOccurs() > 0 &&
                        !poElt->getNillable() )
                    {
                        oField.SetNotNullable( true );
                    }
                    oClass.AddField(oField);

                    // If the element has minOccurs=0 and is nillable, then we
                    // need an extra field to be able to distinguish between the
                    // case of the missing element or the element with
                    // xsi:nil="true"
                    if( poParticle->getMinOccurs() == 0 &&
                        poElt->getNillable() )
                    {
                        GMLASField oFieldNil;
                        oFieldNil.SetName( transcode(poElt->getName()) +
                                           "_nil" );
                        oFieldNil.SetXPath( osElementXPath + "@xsi:nil" );
                        oFieldNil.SetType( GMLAS_FT_BOOLEAN, "boolean" );
                        oFieldNil.SetMinOccurs( 0 );
                        oFieldNil.SetMaxOccurs( 1 );
                        oClass.AddField(oFieldNil);
                    }
                }
            }

            // Complex type (element with attributes, composed element, etc...)
            else if( poTypeDef->getTypeCategory() == XSTypeDefinition::COMPLEX_TYPE )
            {
                XSComplexTypeDefinition* poEltCT =
                        reinterpret_cast<XSComplexTypeDefinition*>(poTypeDef);
                std::vector< GMLASField > aoFields;
                bool bNothingMoreToDo = false;
                std::vector<GMLASFeatureClass> aoNestedClasses;

                const bool bEltRepeatedParticle =
                    poEltCT->getParticle() != NULL &&
                    (poEltCT->getParticle()->getMaxOccursUnbounded() ||
                     poEltCT->getParticle()->getMaxOccurs() > 1);
                const bool bMoveNestedClassToTop =
                        !bRepeatedParticle && !bEltRepeatedParticle;

                // Process attributes
                XSAttributeUseList* poAttrList =
                                        poEltCT->getAttributeUses();
                const size_t nAttrListSize = (poAttrList != NULL) ? 
                                                    poAttrList->size(): 0;
                for(size_t j=0; j< nAttrListSize; ++j )
                {
                    XSAttributeUse* poAttr = poAttrList->elementAt(j);
                    GMLASField oField;
                    CPLString osNamePrefix( bMoveNestedClassToTop ?
                                transcode(poElt->getName()) : CPLString() );
                    SetFieldFromAttribute(oField, poAttr, osElementXPath,
                                          osNamePrefix);
                    aoFields.push_back(oField);
                }

                // Deal with anyAttributes (or any element that also imply it)
                XSWildcard* poAttrWildcard = poEltCT->getAttributeWildcard();
                if( poAttrWildcard != NULL )
                {
                    GMLASField oField;
                    oField.SetType( GMLASField::GetTypeFromString("string"),
                                    "json_dict" );
                    if( !bMoveNestedClassToTop )
                    {
                        oField.SetName( "anyAttributes" );
                    }
                    else
                    {
                        oField.SetName( transcode(poElt->getName()) +
                                                    "_anyAttributes" );
                    }
                    oField.SetXPath(  osElementXPath + "@*" );
                    aoFields.push_back(oField);
                }

                XSSimpleTypeDefinition* poST = poEltCT->getSimpleType();
                if( poST != NULL )
                {
                    /* Case of an element, generally with attributes */

                    GMLASField oField;
                    SetFieldTypeAndWidthFromDefinition(poST, oField);
                    if( bRepeatedParticle && nAttrListSize == 0 &&
                        m_bAllowArrays &&
                        IsCompatibleOfArray(oField.GetType()) )
                    {
                        /* We have a complex type, but no attributes, and */
                        /* compatible of arrays, so move it to top level! */
                        oField.SetName( transcode(poElt->getName()) );
                        oField.SetArray( true );
                        oField.SetMinOccurs( poParticle->getMinOccurs() );
                        oField.SetMaxOccurs( 
                            poParticle->getMaxOccursUnbounded() ?
                            MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );
                    }
                    else if( bRepeatedParticle )
                    {
                        oField.SetName( "value" );
                        oField.SetMinOccurs( 1 );
                        oField.SetMaxOccurs( 1 );
                        oField.SetNotNullable( true );
                    }
                    else
                    {
                        oField.SetName( transcode(poElt->getName()) );
                        oField.SetMinOccurs( poParticle->getMinOccurs() );
                        oField.SetMaxOccurs( 
                            poParticle->getMaxOccursUnbounded() ?
                            MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );
                    }
                    oField.SetXPath( osElementXPath );
                    aoFields.push_back(oField);
                    if( oField.IsArray() )
                    {
                        oClass.AddField( oField );
                        bNothingMoreToDo = true;
                    }
                }
                else if( IsAnyType(poEltCT) )
                {
                    GMLASField oField;
                    oField.SetType( GMLAS_FT_ANYTYPE, "anyType" );
                    if( bRepeatedParticle )
                    {
                        oField.SetName( "value" );
                        oField.SetMinOccurs( 1 );
                        oField.SetMaxOccurs( 1 );
                        oField.SetNotNullable( true );
                    }
                    else
                    {
                        oField.SetName( transcode(poElt->getName()) );
                        oField.SetMinOccurs( poParticle->getMinOccurs() );
                        oField.SetMaxOccurs( 
                            poParticle->getMaxOccursUnbounded() ?
                            MAXOCCURS_UNLIMITED : poParticle->getMaxOccurs() );
                    }
                    oField.SetXPath( osElementXPath );
                    aoFields.push_back(oField);
                }

                // Is it an element that we already visited ? (cycle)
                else if( poEltCT->getParticle() != NULL &&
                         oSetVisitedModelGroups.find(
                            poEltCT->getParticle()->getModelGroupTerm()) !=
                                                oSetVisitedModelGroups.end() )
                {
                    CreateNonNestedRelationship(poElt,
                                                apoSubEltList,
                                                oClass,
                                                bMoveNestedClassToTop ? 1 :
                                                        MAXOCCURS_UNLIMITED,
                                                true);

                    bNothingMoreToDo = true;
                }

                else 
                {
                    GMLASFeatureClass oNestedClass;
                    oNestedClass.SetName( oClass.GetName() + "_" +
                                          transcode(poElt->getName()) );
                    oNestedClass.SetXPath( osElementXPath );

                    // NULL can happen, for example for gml:ReferenceType
                    // that is an empty sequence with just attributes
                    if( poEltCT->getParticle() != NULL )
                    {
#ifdef DEBUG_VERBOSE
                        CPLDebug("GMLAS", "Exploring %s",
                                 osElementXPath.c_str());
#endif
                        std::set<XSModelGroup*>
                            oSetNewVisitedModelGroups(oSetVisitedModelGroups);
                        if( !ExploreModelGroup(
                                           poEltCT->getParticle()->
                                                            getModelGroupTerm(),
                                           NULL,
                                           oNestedClass,
                                           nRecursionCounter + 1,
                                           oSetNewVisitedModelGroups ) )
                        {
                            return false;
                        }
                    }

                    // Can we move the nested class(es) one level up ?
                    if( bMoveNestedClassToTop )
                    {
                        // Case of an element like
                        //   <xs:element name="foo">
                        //      <xs:complexType>
                        //          <xs:sequence>

                        const std::vector<GMLASField>& osNestedClassFields =
                                                    oNestedClass.GetFields();
                        for(size_t j = 0; j < osNestedClassFields.size(); j++ )
                        {
                            GMLASField oField(osNestedClassFields[j]);
                            oField.SetName( transcode(poElt->getName()) +
                                            "_" + oField.GetName() );
                            if( poEltCT->getParticle() != NULL &&
                                poEltCT->getParticle()->getMinOccurs() == 0 )
                            {
                                oField.SetMinOccurs(0);
                                oField.SetNotNullable(false);
                            }
                            aoFields.push_back( oField );
                        }

                        aoNestedClasses = oNestedClass.GetNestedClasses();
                    }
                    else
                    {
                        // Case of an element like
                        //   <xs:element name="foo">
                        //      <xs:complexType>
                        //          <xs:sequence maxOccurs="unbounded">
                        // or
                        //   <xs:element name="foo" maxOccurs="unbounded">
                        //      <xs:complexType>
                        //          <xs:sequence>
                        // or even
                        //   <xs:element name="foo" maxOccurs="unbounded">
                        //      <xs:complexType>
                        //          <xs:sequence maxOccurs="unbounded">
                        if( m_bAllowArrays && nAttrListSize == 0 &&
                            oNestedClass.GetNestedClasses().size() == 0 &&
                            oNestedClass.GetFields().size() == 1 &&
                            IsCompatibleOfArray(
                                    oNestedClass.GetFields()[0].GetType()) )
                        {
                            // In the case the sequence has a single element,
                            // compatible of array type, and no attribute and
                            // no nested classes, then add an array attribute
                            // at the top-level
                            GMLASField oField (oNestedClass.GetFields()[0] );
                            oField.SetName( transcode(poElt->getName()) + "_" +
                                            oField.GetName() );
                            oField.SetArray( true );
                            oClass.AddField( oField );
                        }
                        else
                        {
                            if( aoFields.size() && bEltRepeatedParticle)
                            {
                                // We have attributes and the sequence is
                                // repeated 
                                //   <xs:element name="foo" maxOccurs="unbounded">
                                //      <xs:complexType>
                                //          <xs:sequence maxOccurs="unbounded">
                                //              ...
                                //          </xs:sequence>
                                //          <xs:attribute .../>
                                //      </xs:complexType>
                                //   </xs:element>
                                // So we need to create an
                                // intermediate class to store them
                                GMLASFeatureClass oIntermNestedClass;
                                oIntermNestedClass.SetName(
                                        oClass.GetName() + "_" +
                                        transcode(poElt->getName()) );
                                oIntermNestedClass.SetXPath( osElementXPath );

                                oIntermNestedClass.PrependFields( aoFields );

                                oNestedClass.SetName( oClass.GetName() + "_" +
                                        transcode(poElt->getName()) + "_sequence" );
                                oNestedClass.SetIsGroup( true );

                                GMLASField oField;
                                oField.SetXPath( oNestedClass.GetXPath() );
                                oField.SetIsNestedClass(true);
                                oIntermNestedClass.AddField(oField);

                                oIntermNestedClass.AddNestedClass( oNestedClass );

                                oClass.AddNestedClass( oIntermNestedClass );
                            }
                            else
                            {
                                oNestedClass.PrependFields( aoFields );

                                oClass.AddNestedClass( oNestedClass );
                            }

                            GMLASField oField;
                            oField.SetName( transcode(poElt->getName()) );
                            oField.SetXPath( osElementXPath );
                            oField.SetIsNestedClass(true);
                            oClass.AddField(oField);
                        }

                        bNothingMoreToDo = true;
                    }
                }

                if( bNothingMoreToDo )
                {
                    // Nothing to do
                }
                else if( bRepeatedParticle )
                {
                    GMLASFeatureClass oNestedClass;
                    oNestedClass.SetName( oClass.GetName() + "_" +
                                        transcode(poElt->getName()) );
                    oNestedClass.SetXPath( osElementXPath );
                    oNestedClass.AppendFields( aoFields );
                    oClass.AddNestedClass( oNestedClass );

                    GMLASField oField;
                    oField.SetName( transcode(poElt->getName()) );
                    oField.SetXPath( osElementXPath );
                    oField.SetIsNestedClass(true);
                    oClass.AddField(oField);
                }
                else
                {
                    oClass.AppendFields( aoFields );
                    for(size_t j = 0; j < aoNestedClasses.size(); j++ )
                    {
                        oClass.AddNestedClass( aoNestedClasses[j] );
                    }
                }

            }
        }
        else if( poParticle->getTermType() == XSParticle::TERM_MODELGROUP )
        {
            XSModelGroup* psSubModelGroup = poParticle->getModelGroupTerm();
            if( bRepeatedParticle )
            {
                GMLASFeatureClass oNestedClass;
                CPLString osGroupName = GetGroupName(psSubModelGroup);
                if( !osGroupName.empty() )
                {
                    oNestedClass.SetName( oClass.GetName() + "_" + osGroupName);
                }
                else
                {
                    // Shouldn't happen normally
                    nGroup ++;
                    oNestedClass.SetName( oClass.GetName() +
                                          CPLSPrintf("_group%d", nGroup) );
                }
                oNestedClass.SetIsGroup(true);
                oNestedClass.SetXPath( oClass.GetXPath() );
                std::set<XSModelGroup*>
                    oSetNewVisitedModelGroups(oSetVisitedModelGroups);
                if( !ExploreModelGroup( psSubModelGroup,
                                        NULL,
                                        oNestedClass,
                                        nRecursionCounter + 1,
                                        oSetNewVisitedModelGroups ) )
                {
                    return false;
                }

                if( m_bAllowArrays &&
                    oNestedClass.GetFields().size() == 1 &&
                    IsCompatibleOfArray(oNestedClass.GetFields()[0].GetType()) )
                {
                    GMLASField oField(oNestedClass.GetFields()[0]);
                    oField.SetArray( true );
                    oClass.AddField( oField );
                }
                else
                {
                    oClass.AddNestedClass( oNestedClass );
                }
            }
            else
            {
                std::set<XSModelGroup*>
                    oSetNewVisitedModelGroups(oSetVisitedModelGroups);
                if( !ExploreModelGroup( psSubModelGroup,
                                        NULL,
                                        oClass,
                                        nRecursionCounter + 1,
                                        oSetNewVisitedModelGroups ) )
                {
                    return false;
                }
            }
        }
    }

    return true;
}
