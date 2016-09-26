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

#ifndef OGR_GMLAS_INCLUDED
#define OGR_GMLAS_INCLUDED

#include "xercesc_headers.h"

#include "gdal_priv.h"
#include "ogrsf_frmts.h"

#include <set>
#include <map>
#include <vector>

// Pseudo index to indicate that this xpath is a part of a more detailed
// xpath that is folded into the main type, hence we shouldn't warn about it
// to be unexpected
// Would for example be the case of "element_compound_simplifiable" for :
//             <xs:element name="element_compound_simplifiable">
//                <xs:complexType><xs:sequence>
//                        <xs:element name="subelement" type="xs:string"/>
//                </xs:sequence></xs:complexType>
//            </xs:element>

static const int IDX_COMPOUND_FOLDED = -2;

static const int MAXOCCURS_UNLIMITED = -2;

static const char* const pszXS_URI = "http://www.w3.org/2001/XMLSchema";
static const char* const pszXSI_URI =
                                "http://www.w3.org/2001/XMLSchema-instance";
static const char* const pszXMLNS_URI =
                                "http://www.w3.org/2000/xmlns/";
static const char* const pszXLINK_URI = "http://www.w3.org/1999/xlink";

static const char* const pszGML_URI = "http://www.opengis.net/gml";

static const char* const pszWFS_URI = "http://www.opengis.net/wfs";

typedef std::pair<CPLString, CPLString> PairURIFilename;

// General functions

namespace OGRGMLAS
{
    CPLString transcode( const XMLCh *panXMLString, int nLimitingChars = -1 );
}
using OGRGMLAS::transcode;

typedef enum
{
    GMLAS_SWAP_AUTO,
    GMLAS_SWAP_YES,
    GMLAS_SWAP_NO,
} GMLASSwapCoordinatesEnum;

/************************************************************************/
/*                          IGMLASInputSourceClosing                    */
/************************************************************************/

class IGMLASInputSourceClosing
{
    public:
        virtual ~IGMLASInputSourceClosing() {}

        virtual void notifyClosing(const CPLString& osFilename) = 0;
};

/************************************************************************/
/*                         GMLASResourceCache                           */
/************************************************************************/

class GMLASResourceCache
{
                bool            m_bHasCheckedCacheDirectory;
                CPLString       m_osCacheDirectory;
                bool            m_bRefresh;
                bool            m_bAllowRemoteSchemaDownload;
                std::set<CPLString> m_aoSetRefreshedFiles;

                static bool RecursivelyCreateDirectoryIfNeeded(
                                                const CPLString& osDirname);
                bool RecursivelyCreateDirectoryIfNeeded();

    public:
                             GMLASResourceCache();
                    virtual ~GMLASResourceCache();

                    void    SetCacheDirectory(const CPLString& osCacheDirectory);
                    void    SetRefreshMode(bool bRefresh)
                                            { m_bRefresh = bRefresh; }
                    void    SetAllowRemoteSchemaDownload(bool bVal)
                                    { m_bAllowRemoteSchemaDownload = bVal; }

                    VSILFILE* Open( const CPLString& osResource,
                                    const CPLString& osBasePath,
                                    CPLString& osOutFilename );
};


/************************************************************************/
/*                     GMLASBaseEntityResolver                          */
/************************************************************************/

class GMLASBaseEntityResolver: public EntityResolver,
                           public IGMLASInputSourceClosing
{
        std::vector<CPLString> m_aosPathStack;
        GMLASResourceCache& m_oCache;

  public:
        GMLASBaseEntityResolver(const CPLString& osBasePath,
                                GMLASResourceCache& oCache);
        virtual ~GMLASBaseEntityResolver();

        void SetBasePath(const CPLString& osBasePath);

        virtual void notifyClosing(const CPLString& osFilename );
        virtual InputSource* resolveEntity( const XMLCh* const publicId,
                                            const XMLCh* const systemId);

        virtual void DoExtraSchemaProcessing(const CPLString& osFilename,
                                             VSILFILE* fp);
};

/************************************************************************/
/*                          GMLASInputSource                            */
/************************************************************************/

class GMLASInputSource : public InputSource
{
    VSILFILE *m_fp;
    bool      m_bOwnFP;
    int       m_nCounter;
    int      *m_pnCounter;
    CPLString m_osFilename;
    IGMLASInputSourceClosing* m_cbk;

public:
             GMLASInputSource(const char* pszFilename,
                              VSILFILE* fp,
                              bool bOwnFP,
                              MemoryManager* const manager =
                                            XMLPlatformUtils::fgMemoryManager);
    virtual ~GMLASInputSource();

    virtual BinInputStream* makeStream() const;

    void    SetClosingCallback( IGMLASInputSourceClosing* cbk );
};


/************************************************************************/
/*                            GMLASErrorHandler                         */
/************************************************************************/

class GMLASErrorHandler: public ErrorHandler
{
    public:
        GMLASErrorHandler () : m_bFailed (false) {}

        bool hasFailed () const { return m_bFailed; }

        virtual void warning (const SAXParseException& e);
        virtual void error (const SAXParseException& e);
        virtual void fatalError (const SAXParseException& e);

        virtual void resetErrors () { m_bFailed = false; }

    private:
        bool m_bFailed;

        void handle (const SAXParseException& e, CPLErr eErr);
};

/************************************************************************/
/*                          GMLASConfiguration                          */
/************************************************************************/

class GMLASConfiguration
{
    public:
        // Note the default values mentionned here should be kept
        // consistant with what is documented in gmlasconf.xsd
        static const bool ALLOW_REMOTE_SCHEMA_DOWNLOAD_DEFAULT = true;
        static const bool ALWAYS_GENERATE_OGR_ID_DEFAULT = false;
        static const bool USE_ARRAYS_DEFAULT = true;
        static const bool INCLUDE_GEOMETRY_XML_DEFAULT = false;
        static const bool INSTANTIATE_GML_FEATURES_ONLY_DEFAULT = true;
        static const bool ALLOW_XSD_CACHE_DEFAULT = true;
        static const bool VALIDATE_DEFAULT = false;
        static const bool FAIL_IF_VALIDATION_ERROR_DEFAULT = false;
        static const bool EXPOSE_METADATA_LAYERS_DEFAULT = false;
        static const bool WARN_IF_EXCLUDED_XPATH_FOUND_DEFAULT = true;

        /** Whether remote schemas are allowed to be download. */
        bool            m_bAllowRemoteSchemaDownload;

        /** Whether a ogr_pkid attribute should always be generated. */
        bool            m_bAlwaysGenerateOGRId;

        /** Whether repeated strings, integers, reals should be in corresponding
            OGR array types. */
        bool            m_bUseArrays;

        /** Whether geometries should be stored as XML in a OGR string field. */
        bool            m_bIncludeGeometryXML;

        /** Whether, when dealing with schemas that import the
            GML namespace, and that at least one of them has
            elements that derive from gml:_Feature or
            gml:AbstractFeatureonly, only such elements should be
            instantiated as OGR layers, during the first pass that
            iterates over top level elements of the imported
            schemas. */
        bool            m_bInstantiateGMLFeaturesOnly;

        /** Whether remote XSD schemas should be locally cached. */
        bool            m_bAllowXSDCache;

        /** Cache directory for cached XSD schemas. */
        CPLString       m_osCacheDirectory;

        /** Whether validation of document against schema should be done.  */
        bool            m_bValidate;

        /** Whether a validation error should prevent dataset opening.  */
        bool            m_bFailIfValidationError;

        /** Whether technical layers should be exposed.  */
        bool            m_bExposeMetadataLayers;

        /** For ignored xpaths, map prefix namespace to its URI */
        std::map<CPLString, CPLString> m_oMapPrefixToURIIgnoredXPaths;

        /** Ignored xpaths */
        std::vector<CPLString> m_aosIgnoredXPaths;

        /** Whether a warning should be emitted when an element or attribute is
            found in the document parsed, but ignored because of the ignored
            XPath defined.  */
        std::map<CPLString, bool> m_oMapIgnoredXPathToWarn;

                                    GMLASConfiguration();
                    virtual        ~GMLASConfiguration();

                            bool    Load(const char* pszFilename);
                            void    Finalize();
};

/************************************************************************/
/*                           GMLASXPathMatcher                          */
/************************************************************************/

/** Object to compares a user provided XPath against a set of test XPaths */
class GMLASXPathMatcher
{
        class XPathComponent
        {
            public:
                    CPLString m_osValue;
                    bool      m_bDirectChild;
        };

        /** For reference xpaths, map prefix namespace to its URI */
        std::map<CPLString, CPLString> m_oMapPrefixToURIReferenceXPaths;

        /** Reference xpaths */
        std::vector<CPLString> m_aosReferenceXPathsUncompiled;

        /** Reference xpaths "compiled" */
        std::vector< std::vector<XPathComponent> > m_aosReferenceXPaths;

        bool MatchesRefXPath(
            const CPLString& osXPath,
            const std::vector<XPathComponent>& oRefXPath) const;

    public:
                                GMLASXPathMatcher();
                    virtual    ~GMLASXPathMatcher();

        void    SetRefXPaths(const std::map<CPLString, CPLString>&
                                    oMapPrefixToURIReferenceXPaths,
                                const std::vector<CPLString>& 
                                    aosReferenceXPaths);

        void    SetDocumentMapURIToPrefix(
                    const std::map<CPLString,CPLString>& oMapURIToPrefix );

        /** Return true if osXPath matches one of the XPath of
            m_aosReferenceXPaths */
        bool MatchesRefXPath(
            const CPLString& osXPath,
            CPLString& osOutMatchedXPath ) const;
};

/************************************************************************/
/*                            GMLASFieldType                            */
/************************************************************************/

/** Enumeration for XML primitive types */
typedef enum
{
    GMLAS_FT_STRING,
    GMLAS_FT_ID,
    GMLAS_FT_BOOLEAN,
    GMLAS_FT_SHORT,
    GMLAS_FT_INT32,
    GMLAS_FT_INT64,
    GMLAS_FT_FLOAT,
    GMLAS_FT_DOUBLE,
    GMLAS_FT_DECIMAL,
    GMLAS_FT_DATE,
    GMLAS_FT_TIME,
    GMLAS_FT_DATETIME,
    GMLAS_FT_BASE64BINARY,
    GMLAS_FT_HEXBINARY,
    GMLAS_FT_ANYURI,
    GMLAS_FT_ANYTYPE,
    GMLAS_FT_ANYSIMPLETYPE,
    GMLAS_FT_GEOMETRY, // this one isn't a XML primitive type.
} GMLASFieldType;

/************************************************************************/
/*                              GMLASField                              */
/************************************************************************/

class GMLASField
{
    public:
        typedef enum 
        {
            /** Field that is going to be instantiated as a OGR field */
            REGULAR,

            /** Non-instanciable field. The corresponding element to the XPath
                is stored in a child layer that will reference back to the
                main layer. */
            PATH_TO_CHILD_ELEMENT_NO_LINK,

            /** Field that will store the PKID of a child element */
            PATH_TO_CHILD_ELEMENT_WITH_LINK,

            /** Non-instanciable field. The corresponding element to the XPath
                is stored in a child layer. And the link between both will be
                done through a junction table. */
            PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE,

            /** Non-instanciable field. Corresponds to a group of an element. */
            GROUP
        } Category;

    private:
        CPLString m_osName;         /**< Field name */
        GMLASFieldType m_eType;     /**< Field type */
        OGRwkbGeometryType m_eGeomType; /**< Field geometry type */
        CPLString m_osTypeName;     /**< Original XSD type */
        int m_nWidth;               /**< Field width */
        bool m_bNotNullable;        /**< If the field is not nullable */
        bool m_bArray;              /**< If the field is an array */

        /** Category of the field. */
        Category m_eCategory;

        /** XPath of the field. */
        CPLString m_osXPath;

        /** Set of XPath that are linked to this field.
            This is used for cases where a gml:AbstractGeometry element is
            referenced. In which case all possible realizations of this
            element are listed. Will be used with eType == GMLAS_FT_ANYTYPE
            to store XML blob on parsing. */
        std::vector<CPLString> m_aosXPath;

        CPLString m_osFixedValue;       /**< Value of fixed='' attribute */
        CPLString m_osDefaultValue;     /**< Value of default='' attribute */

        /** Minimum number of occurrences. Might be -1 if unset */
        int  m_nMinOccurs;

        /** Maximum number of occurrences, or MAXOCCURS_UNLIMITED. Might be
            -1 if unset. */
        int  m_nMaxOccurs;

        /** In case of m_eType == GMLAS_FT_ANYTYPE whether the current element
            must be stored in the XML blob (if false, only its children) */
        bool m_bIncludeThisEltInBlob;

        /** Only used for PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE. The XPath
            of the abstract element (the concrete XPath is in m_osXPath).
            e.g myns:mainElt/myns:subEltAbstract whereas the concrete XPath
            is myns:mainElt/myns:subEltRealization */
        CPLString m_osAbstractElementXPath;

        /** Only used for PATH_TO_CHILD_ELEMENT_WITH_LINK and
            PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE (and also for
            PATH_TO_CHILD_ELEMENT_NO_LINK and GROUP but for metadata layers only).
            The XPath of the child element. */
        CPLString m_osRelatedClassXPath;

    public:
        GMLASField();

        void SetName(const CPLString& osName) { m_osName = osName; }
        void SetType(GMLASFieldType eType, const char* pszTypeName);
        void SetGeomType(OGRwkbGeometryType eGeomType)
                                   { m_eGeomType = eGeomType; }
        void SetWidth(int nWidth) { m_nWidth = nWidth; }
        void SetNotNullable(bool bNotNullable)
                                    { m_bNotNullable = bNotNullable; }
        void SetArray(bool bArray) { m_bArray = bArray; }
        void SetXPath(const CPLString& osXPath) { m_osXPath = osXPath; }
        void AddAlternateXPath(const CPLString& osXPath)
                                            { m_aosXPath.push_back(osXPath); }
        void SetFixedValue(const CPLString& osFixedValue)
                                    { m_osFixedValue = osFixedValue; }
        void SetDefaultValue(const CPLString& osDefaultValue)
                                    { m_osDefaultValue = osDefaultValue; }
        void SetCategory(Category eCategory) { m_eCategory = eCategory; }
        void SetMinOccurs(int nMinOccurs) { m_nMinOccurs = nMinOccurs; }
        void SetMaxOccurs(int nMaxOccurs) { m_nMaxOccurs = nMaxOccurs; }
        void SetIncludeThisEltInBlob(bool b) { m_bIncludeThisEltInBlob = b; }
        void SetAbstractElementXPath(const CPLString& osName)
                                            { m_osAbstractElementXPath = osName; }

        void SetRelatedClassXPath(const CPLString& osName)
                                            { m_osRelatedClassXPath = osName; }

        static CPLString MakePKIDFieldFromXLinkHrefXPath(
            const CPLString& osBaseXPath)
                            { return "{" + osBaseXPath + "}_pkid"; }


        const CPLString& GetName() const { return m_osName; }
        const CPLString& GetXPath() const { return m_osXPath; }
        const std::vector<CPLString>& GetAlternateXPaths() const
                                            { return m_aosXPath; }
        GMLASFieldType GetType() const { return m_eType; }
        OGRwkbGeometryType GetGeomType() const { return m_eGeomType; }
        const CPLString& GetTypeName() const { return m_osTypeName; }
        int GetWidth() const { return m_nWidth; }
        bool IsNotNullable() const { return m_bNotNullable; }
        bool IsArray() const { return m_bArray; }
        const CPLString& GetFixedValue() const { return m_osFixedValue; }
        const CPLString& GetDefaultValue() const { return m_osDefaultValue; }
        Category GetCategory() const { return m_eCategory; }
        int GetMinOccurs() const { return m_nMinOccurs; }
        int GetMaxOccurs() const { return m_nMaxOccurs; }
        bool GetIncludeThisEltInBlob() const { return m_bIncludeThisEltInBlob; }
        const CPLString& GetAbstractElementXPath() const
                                            { return m_osAbstractElementXPath; }
        const CPLString& GetRelatedClassXPath() const
                                                { return m_osRelatedClassXPath; }

        static GMLASFieldType GetTypeFromString( const CPLString& osType );
};

/************************************************************************/
/*                            GMLASFeatureClass                         */
/************************************************************************/

class GMLASFeatureClass
{
        /** User facing name */
        CPLString m_osName;

        /** XPath to the main element of the feature class */
        CPLString m_osXPath;

        /** List of fields */
        std::vector<GMLASField> m_aoFields;

        /** Child nested classes */
        std::vector<GMLASFeatureClass> m_aoNestedClasses;

        /** Whether this layer corresponds to a (multiple instantiated) xs:group
            or a repeated sequence */
        bool m_bIsRepeatedSequence;

        /** Only used for junction tables. The XPath to the parent table */
        CPLString m_osParentXPath;

        /** Only used for junction tables. The XPath to the child table */
        CPLString m_osChildXPath;

        /** Whether this corresponds to a top-level XSD element in the schema */
        bool m_bIsTopLevelElt;

    public:
        GMLASFeatureClass();

        void SetName(const CPLString& osName);
        void SetXPath(const CPLString& osXPath);
        void AddField( const GMLASField& oField );
        void PrependFields( const std::vector<GMLASField>& aoFields );
        void AppendFields( const std::vector<GMLASField>& aoFields );
        void AddNestedClass( const GMLASFeatureClass& oNestedClass );
        void SetIsRepeatedSequence( bool bIsRepeatedSequence )
                            { m_bIsRepeatedSequence = bIsRepeatedSequence; }
        void SetParentXPath(const CPLString& osXPath)
                                                { m_osParentXPath = osXPath; }
        void SetChildXPath(const CPLString& osXPath)
                                                { m_osChildXPath = osXPath; }
        void SetIsTopLevelElt(bool bIsTopLevelElt )
                                        { m_bIsTopLevelElt = bIsTopLevelElt; }

        const CPLString& GetName() const { return m_osName; }
        const CPLString& GetXPath() const { return m_osXPath; }
        const std::vector<GMLASField>& GetFields() const { return m_aoFields; }
        std::vector<GMLASField>& GetFields() { return m_aoFields; }
        const std::vector<GMLASFeatureClass>& GetNestedClasses() const
                                            { return m_aoNestedClasses; }
        std::vector<GMLASFeatureClass>& GetNestedClasses()
                                            { return m_aoNestedClasses; }
        bool IsRepeatedSequence() const { return m_bIsRepeatedSequence; }
        const CPLString& GetParentXPath() const { return m_osParentXPath; }
        const CPLString& GetChildXPath() const { return m_osChildXPath; }
        bool IsTopLevelElt() const { return m_bIsTopLevelElt; }
};

/************************************************************************/
/*                         GMLASSchemaAnalyzer                          */
/************************************************************************/

class GMLASSchemaAnalyzer
{
        GMLASXPathMatcher& m_oIgnoredXPathMatcher;

        /** Whether repeated strings, integers, reals should be in corresponding
            OGR array types. */
        bool m_bUseArrays;

        /** Whether, when dealing with schemas that import the
            GML namespace, and that at least one of them has
            elements that derive from gml:_Feature or
            gml:AbstractFeatureonly, only such elements should be
            instantiated as OGR layers, during the first pass that
            iterates over top level elements of the imported
            schemas. */
        bool            m_bInstantiateGMLFeaturesOnly;

        /** Vector of feature classes */
        std::vector<GMLASFeatureClass> m_aoClasses;

        /** Map from a namespace URI to the corresponding prefix */
        std::map<CPLString, CPLString> m_oMapURIToPrefix;

        typedef std::map<XSElementDeclaration*,
                                std::vector<XSElementDeclaration*> >
                                                    tMapParentEltToChildElt;
        /** Map from a base/parent element to a vector of derived/children
            elements that are substitutionGroup of it. The map only
            addresses the direct derived types, and not the 2nd level or more
            derived ones. For that recursion in the map must be used.*/
        tMapParentEltToChildElt m_oMapParentEltToChildElt;

        /** Map from a XSModelGroup* object to the name of its group. */
        std::map< XSModelGroup*, CPLString> m_oMapModelGroupDefinitionToName;

        /** Map from (non namespace prefixed) element names to the number of
            elements that share the same namespace (in different namespaces) */
        std::map<CPLString, int> m_oMapEltNamesToInstanceCount;

        /** Set of elements that match a OGR layer */
        std::set<XSElementDeclaration*> m_oSetEltsForTopClass;

        static bool IsSame( const XSModelGroup* poModelGroup1,
                                  const XSModelGroup* poModelGroup2 );
        CPLString GetGroupName( const XSModelGroup* poModelGroup );
        void SetFieldFromAttribute(GMLASField& oField,
                                   XSAttributeUse* poAttr,
                                   const CPLString& osXPathPrefix,
                                   const CPLString& osNamePrefix = CPLString());
        void GetConcreteImplementationTypes(
                                XSElementDeclaration* poParentElt,
                                std::vector<XSElementDeclaration*>& apoImplEltList);
        bool FindElementsWithMustBeToLevel(
                            const CPLString& osParentXPath,
                            XSModelGroup* poModelGroup,
                            int nRecursionCounter,
                            std::set<XSElementDeclaration*>& oSetVisitedEltDecl,
                            std::set<XSModelGroup*>& oSetVisitedModelGroups,
                            std::vector<XSElementDeclaration*>& oVectorEltsForTopClass,
                            XSModel* poModel);
        bool ExploreModelGroup( XSModelGroup* psMainModelGroup,
                                XSAttributeUseList* poMainAttrList,
                                GMLASFeatureClass& oClass,
                                int nRecursionCounter,
                                std::set<XSModelGroup*>& oSetVisitedModelGroups,
                                XSModel* poModel );
        void SetFieldTypeAndWidthFromDefinition( XSSimpleTypeDefinition* poST,
                                                 GMLASField& oField );
        CPLString MakeXPath( const CPLString& osNamespace,
                                          const CPLString& osName );
        void FixDuplicatedFieldNames( GMLASFeatureClass& oClass );

        XSElementDeclaration* GetTopElementDeclarationFromXPath(
                                                    const CPLString& osXPath,
                                                    XSModel* poModel);

        bool InstantiateClassFromEltDeclaration(XSElementDeclaration* poEltDecl,
                                                XSModel* poModel,
                                                bool& bError);
        void CreateNonNestedRelationship(
                        XSElementDeclaration* poElt,
                        std::vector<XSElementDeclaration*>& apoSubEltList,
                        GMLASFeatureClass& oClass,
                        int nMaxOccurs,
                        bool bForceJunctionTable);

        bool IsGMLNamespace(const CPLString& osURI);

        bool DerivesFromGMLFeature(XSElementDeclaration* poEltDecl);

        bool IsIgnoredXPath(const CPLString& osXPath);

    public:
        GMLASSchemaAnalyzer( GMLASXPathMatcher& oIgnoredXPathMatcher );
        void SetUseArrays(bool b) { m_bUseArrays = b; }
        void SetInstantiateGMLFeaturesOnly(bool b)
                                    { m_bInstantiateGMLFeaturesOnly = b; }

        bool Analyze(GMLASResourceCache& oCache,
                     const CPLString& osBaseDirname,
                     const std::vector<PairURIFilename>& aoXSDs);
        const std::vector<GMLASFeatureClass>& GetClasses() const
                { return m_aoClasses; }

        const std::map<CPLString, CPLString>& GetMapURIToPrefix() const
                    { return m_oMapURIToPrefix; }
};

/************************************************************************/
/*                           OGRGMLASDataSource                         */
/************************************************************************/

class OGRGMLASLayer;
class GMLASReader;

class OGRGMLASDataSource: public GDALDataset
{
        std::vector<OGRGMLASLayer*>    m_apoLayers;
        std::map<CPLString, CPLString> m_oMapURIToPrefix;
        CPLString                      m_osGMLFilename;
        bool                           m_bExposeMetadataLayers;
        OGRLayer                      *m_poFieldsMetadataLayer;
        OGRLayer                      *m_poLayersMetadataLayer;
        OGRLayer                      *m_poRelationshipsLayer;
        VSILFILE                      *m_fpGML;
        bool                           m_bLayerInitFinished;
        bool                           m_bValidate;
        bool                           m_bFirstPassDone;
        /** Map from a SRS name to a boolean indicating if its coordinate
            order is inverted. */
        std::map<CPLString, bool>      m_oMapSRSNameToInvertedAxis;

        /** Map from geometry field definition to its expected SRSName */
        std::map<OGRGeomFieldDefn*, CPLString> m_oMapGeomFieldDefnToSRSName;

        std::vector<PairURIFilename>   m_aoXSDs;

        GMLASConfiguration             m_oConf;

        /** Schema cache */
        GMLASResourceCache             m_oCache;

        GMLASXPathMatcher              m_oIgnoredXPathMatcher;

        GMLASSwapCoordinatesEnum       m_eSwapCoordinates;

        /** Base unique identifier */
        CPLString                      m_osHash;

        void TranslateClasses( OGRGMLASLayer* poParentLayer,
                               const GMLASFeatureClass& oFC );

    public:
        OGRGMLASDataSource();
        virtual ~OGRGMLASDataSource();

        virtual int         GetLayerCount();
        virtual OGRLayer    *GetLayer(int);
        virtual OGRLayer    *GetLayerByName(const char* pszName);

        bool Open(GDALOpenInfo* poOpenInfo);

        std::vector<OGRGMLASLayer*>*          GetLayers()
                                            { return &m_apoLayers; }
        const std::map<CPLString, CPLString>& GetMapURIToPrefix() const
                                            { return m_oMapURIToPrefix; }
        const CPLString&                      GetGMLFilename() const
                                            { return m_osGMLFilename; }
        OGRLayer*                             GetFieldsMetadataLayer()
                                            { return m_poFieldsMetadataLayer; }
        OGRLayer*                             GetLayersMetadataLayer()
                                            { return m_poLayersMetadataLayer; }
        OGRLayer*                             GetRelationshipsLayer()
                                            { return m_poRelationshipsLayer; }
        OGRGMLASLayer*          GetLayerByXPath( const CPLString& osXPath );

        GMLASResourceCache& GetCache() { return m_oCache; }

        void        RunFirstPassIfNeeded( GMLASReader* poReader );

        void        PushUnusedGMLFilePointer( VSILFILE* fpGML );
        VSILFILE   *PopUnusedGMLFilePointer();
        bool        IsLayerInitFinished() const { return m_bLayerInitFinished; }
        GMLASSwapCoordinatesEnum GetSwapCoordinates() const
                                                { return m_eSwapCoordinates; }

        const std::map<CPLString,bool>& GetMapIgnoredXPathToWarn() const {
                                return m_oConf.m_oMapIgnoredXPathToWarn; }
        const GMLASXPathMatcher& GetIgnoredXPathMatcher() const
                                { return  m_oIgnoredXPathMatcher; }
        const CPLString& GetHash() const { return m_osHash; }
};

/************************************************************************/
/*                             OGRGMLASLayer                            */
/************************************************************************/

class OGRGMLASLayer: public OGRLayer
{
        friend class OGRGMLASDataSource;

        OGRGMLASDataSource            *m_poDS;
        GMLASFeatureClass              m_oFC;
        bool                           m_bLayerDefnFinalized;
        OGRFeatureDefn                *m_poFeatureDefn;

        /** Map from XPath to corresponding field index in OGR layer
            definition */
        std::map<CPLString, int>       m_oMapFieldXPathToOGRFieldIdx;

        /** Map from XPath to corresponding geometry field index in OGR layer
            definition */
        std::map<CPLString, int>       m_oMapFieldXPathToOGRGeomFieldIdx;

        /** Map from a OGR field index to the corresponding field index in
            m_oFC.GetFields() */
        std::map<int, int>             m_oMapOGRFieldIdxtoFCFieldIdx;
        std::map<int, int>             m_oMapOGRGeomFieldIdxtoFCFieldIdx;

        bool                           m_bEOF;
        GMLASReader                   *m_poReader;
        VSILFILE                      *m_fpGML;
        /** OGR field index of the ID field */
        int                            m_nIDFieldIdx;
        /** Whether the ID field is generated, or comes from the XML content */
        bool                           m_bIDFieldIsGenerated;
        /** Pointer to parent layer */
        OGRGMLASLayer                 *m_poParentLayer;
        /** OGR field index of the field that points to the parent ID */
        int                            m_nParentIDFieldIdx;

        OGRFeature*                    GetNextRawFeature();

        bool                           InitReader();

        void                           SetLayerDefnFinalized(bool bVal)
                                            { m_bLayerDefnFinalized = bVal; }

    public:
        OGRGMLASLayer(OGRGMLASDataSource* poDS,
                      const GMLASFeatureClass& oFC,
                      OGRGMLASLayer* poParentLayer,
                      bool bAlwaysGenerateOGRPKId);
        virtual ~OGRGMLASLayer();

        virtual const char* GetName() { return GetDescription(); }
        virtual OGRFeatureDefn* GetLayerDefn();
        virtual void ResetReading();
        virtual OGRFeature* GetNextFeature();
        virtual int TestCapability( const char* ) { return FALSE; }

        void PostInit(bool bIncludeGeometryXML);

        const GMLASFeatureClass& GetFeatureClass() const { return m_oFC; }
        int GetOGRFieldIndexFromXPath(const CPLString& osXPath) const;
        int GetOGRGeomFieldIndexFromXPath(const CPLString& osXPath) const;
        int GetIDFieldIdx() const { return m_nIDFieldIdx; }
        bool IsGeneratedIDField() const { return m_bIDFieldIsGenerated; }
        OGRGMLASLayer* GetParent() { return m_poParentLayer; }
        int GetParentIDFieldIdx() const { return m_nParentIDFieldIdx; }
        int GetFCFieldIndexFromOGRFieldIdx(int iOGRFieldIdx) const;
        int GetFCFieldIndexFromOGRGeomFieldIdx(int iOGRGeomFieldIdx) const;
};

/************************************************************************/
/*                              GMLASReader                             */
/************************************************************************/

class GMLASReader : public DefaultHandler
{
        /** Schema cache */
        GMLASResourceCache&        m_oCache;

        /** Object to tell if a XPath must be ignored */
        const GMLASXPathMatcher& m_oIgnoredXPathMatcher;

        /** Whether we should stop parsing */
        bool              m_bParsingError;

        /** Xerces reader object */
        SAX2XMLReader    *m_poSAXReader;

        /** Token for Xerces */
        XMLPScanToken     m_oToFill;

        /** File descriptor (not owned by this object) */
        VSILFILE         *m_fp;

        /** Input source */
        GMLASInputSource *m_GMLInputSource;

        /** Whether we are at the first iteration */
        bool              m_bFirstIteration;

        /** Whether we have reached end of file (or an error) */
        bool              m_bEOF;

        /** Error handler (for Xerces reader) */
        GMLASErrorHandler m_oErrorHandler;

        /** Map URI namespaces to their prefix */
        std::map<CPLString, CPLString> m_oMapURIToPrefix;

        /** List of OGR layers */
        std::vector<OGRGMLASLayer*>* m_papoLayers;

        /** Vector of features ready for consumption */
        std::vector< std::pair<OGRFeature*, OGRGMLASLayer*> > m_aoFeaturesReady;

        /** OGR field index of the current field */
        int               m_nCurFieldIdx;

        /** OGR geometry field index of the current field */
        int               m_nCurGeomFieldIdx;

        /** XML nested level of current field */
        int               m_nCurFieldLevel;

        /** Whether we should store all content of the current field as XML */
        bool              m_bIsXMLBlob;
        bool              m_bIsXMLBlobIncludeUpper;

        /** Content of the current field */
        CPLString         m_osTextContent;

        /** For list field types, list of content */
        CPLStringList     m_osTextContentList;
        /** Estimated memory footprint of m_osTextContentList */
        size_t            m_nTextContentListEstimatedSize;

        /** Which layer is of interest for the reader, or NULL for all */
        OGRGMLASLayer    *m_poLayerOfInterest;

        /** Stack of length of split XPath components */
        std::vector<size_t> m_anStackXPathLength;

        /** Current absolute XPath */
        CPLString           m_osCurXPath;

        /** Current XPath, relative to top-level feature */
        CPLString           m_osCurSubXPath;

        /** Current XML nesting level */
        int                 m_nLevel;

        /** Map layer to global FID */
        std::map<OGRLayer*, int> m_oMapGlobalCounter;

        /** Parsing context */
        struct Context
        {
            /** XML nesting level */
            int             m_nLevel;

            /** Current feature */
            OGRFeature     *m_poFeature;

            /** Layer of m_poFeature */
            OGRGMLASLayer  *m_poLayer;

            /** Current layer in a repeated group */
            OGRGMLASLayer  *m_poGroupLayer;

            /** Nesting level of m_poCurGroupLayer */
            int             m_nGroupLayerLevel;

            /** Index of the last processed OGR field in m_poCurGroupLayer */
            int             m_nLastFieldIdxGroupLayer;

            /** Map layer to local FID */
            std::map<OGRLayer*, int> m_oMapCounter;

            /** Current XPath, relative to (current) top-level feature */
            CPLString       m_osCurSubXPath;

            void Dump();
        };

        /** Current context */
        Context              m_oCurCtxt;

        /** Stack of saved contexts */
        std::vector<Context> m_aoStackContext;

        /** Context used in m_apsXMLNodeStack */
        typedef struct
        {
            /** Current node */
            CPLXMLNode* psNode;

            /** Last child of psNode (for fast append operations) */
            CPLXMLNode* psLastChild;
        } NodeLastChild;

        /** Stack of contexts to build XML tree of GML Geometry */
        std::vector<NodeLastChild> m_apsXMLNodeStack;

        /** Maximum allowed number of XML nesting level */
        int                  m_nMaxLevel;

        /** Maximum allowed size of XML content in byte */
        size_t               m_nMaxContentSize; 

        /** Map from a SRS name to a boolean indicating if its coordinate
            order is inverted. */
        std::map<CPLString, bool>      m_oMapSRSNameToInvertedAxis;

        /** Set of geometry fields with unknown SRS */
        std::set<OGRGeomFieldDefn*>    m_oSetGeomFieldsWithUnknownSRS;

        /** Map from geometry field definition to its expected SRSName.
            This is used to know if reprojection must be done */
        std::map<OGRGeomFieldDefn*, CPLString> m_oMapGeomFieldDefnToSRSName;

        /** Whether this parsing involves schema validation */
        bool                       m_bValidate;

        /** Entity resolver used during schema validation */
        GMLASBaseEntityResolver* m_poEntityResolver;

        /** First level from which warnings about ignored XPath should be
            silent. */
        int                        m_nLevelSilentIgnoredXPath;

        /** Whether a warning should be emitted when an element or attribute is
            found in the document parsed, but ignored because of the ignored
            XPath defined.  */
        std::map<CPLString, bool> m_oMapIgnoredXPathToWarn;

        /** Policy to decide when to invert coordinates */
        GMLASSwapCoordinatesEnum       m_eSwapCoordinates;

        /** Initial pass to guess SRS, etc... */
        bool                        m_bInitialPass;

        /** Base unique identifier */
        CPLString                      m_osHash;

        static void SetField( OGRFeature* poFeature,
                              OGRGMLASLayer* poLayer,
                              int nAttrIdx,
                              const CPLString& osAttrValue );

        void        CreateNewFeature(const CPLString& osLocalname);

        void        PushFeatureReady( OGRFeature* poFeature,
                                      OGRGMLASLayer* poLayer );

        void        BuildXMLBlobStartElement(const CPLString& osNSPrefix,
                                             const CPLString& osLocalname,
                                             const  Attributes& attrs);

        OGRGMLASLayer* GetLayerByXPath( const CPLString& osXPath );

        void        AttachAsLastChild(CPLXMLNode* psNode);

        void        ProcessGeometry();

        void        ProcessAttributes(const Attributes& attrs);

    public:
                        GMLASReader(GMLASResourceCache& oCache,
                                    const GMLASXPathMatcher& oIgnoredXPathMatcher);
                        ~GMLASReader();

        bool Init(const char* pszFilename,
                  VSILFILE* fp,
                  const std::map<CPLString, CPLString>& oMapURIToPrefix,
                  std::vector<OGRGMLASLayer*>* papoLayers,
                  bool bValidate,
                  const std::vector<PairURIFilename>& aoXSDs = std::vector<PairURIFilename>() );

        void SetLayerOfInterest( OGRGMLASLayer* poLayer );

        void SetMapIgnoredXPathToWarn(const std::map<CPLString,bool>& oMap)
                    { m_oMapIgnoredXPathToWarn = oMap; }

        void SetSwapCoordinates(GMLASSwapCoordinatesEnum eVal)
                                            { m_eSwapCoordinates = eVal; }

        VSILFILE* GetFP() const { return m_fp; }

        const std::map<CPLString, bool>& GetMapSRSNameToInvertedAxis() const
                                    { return m_oMapSRSNameToInvertedAxis; }
        void SetMapSRSNameToInvertedAxis( const std::map<CPLString, bool>& oMap )
                                    { m_oMapSRSNameToInvertedAxis = oMap; }

        const std::map<OGRGeomFieldDefn*, CPLString>& GetMapGeomFieldDefnToSRSName() const
                                    { return m_oMapGeomFieldDefnToSRSName; }
        void SetMapGeomFieldDefnToSRSName(const std::map<OGRGeomFieldDefn*, CPLString>& oMap )
                                    { m_oMapGeomFieldDefnToSRSName = oMap; }

        void SetHash(const CPLString& osHash) { m_osHash = osHash; }

        OGRFeature* GetNextFeature( OGRLayer** ppoBelongingLayer = NULL );

        virtual void startElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const    qname,
            const   Attributes& attrs
        );
        virtual  void endElement(
            const   XMLCh* const    uri,
            const   XMLCh* const    localname,
            const   XMLCh* const    qname
        );

        virtual  void characters( const XMLCh *const chars,
                        const XMLSize_t length );

        void RunFirstPass();

        static bool LoadXSDInParser( SAX2XMLReader* poParser,
                                     GMLASResourceCache& oCache,
                                     GMLASBaseEntityResolver& oXSDEntityResolver,
                                     const CPLString& osBaseDirname,
                                     const CPLString& osXSDFilename,
                                     Grammar** ppoGrammar = NULL );
};

#endif // OGR_GMLAS_INCLUDED
