/******************************************************************************
 * $Id$
 *
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

CPL_CVSID("$Id$");

/************************************************************************/
/*                            OGRGMLASLayer()                           */
/************************************************************************/

OGRGMLASLayer::OGRGMLASLayer( OGRGMLASDataSource* poDS,
                              const GMLASFeatureClass& oFC,
                              OGRGMLASLayer* poParentLayer,
                              bool /*bHasChildClasses*/ )
{
    m_poDS = poDS;
    m_oFC = oFC;
    m_poFeatureDefn = new OGRFeatureDefn( oFC.GetName() );
    m_poFeatureDefn->SetGeomType(wkbNone);
    m_poFeatureDefn->Reference();
    m_nIDFieldIdx = -1;
    m_bIDFieldIsGenerated = false;
    m_poParentLayer = poParentLayer;
    m_nParentIDFieldIdx = -1;
    m_poReader = NULL;
    m_fpGML = NULL;

    SetDescription( m_poFeatureDefn->GetName() );

    OGRLayer* poLayersMetadataLayer = m_poDS->GetLayersMetadataLayer();
    OGRFeature* poLayerDescFeature =
                        new OGRFeature(poLayersMetadataLayer->GetLayerDefn());
    poLayerDescFeature->SetField( "layer_name", GetName() );
    if( !m_oFC.GetParentXPath().empty() )
    {
        poLayerDescFeature->SetField( "layer_category", "JUNCTION_TABLE" );
    }
    else
    {
        poLayerDescFeature->SetField( "layer_xpath", m_oFC.GetXPath() );

        poLayerDescFeature->SetField( "layer_category",
                                m_oFC.IsTopLevelElt() ? "TOP_LEVEL_ELEMENT" :
                                                        "NESTED_ELEMENT" );
    }
    CPL_IGNORE_RET_VAL(
            poLayersMetadataLayer->CreateFeature(poLayerDescFeature));
    delete poLayerDescFeature;

    // Are we a regular table ?
    if( m_oFC.GetParentXPath().empty() )
    {
        // Determine if we have an xs:ID attribute/elt, and if it is compulsory,
        // If so, place it as first field (not strictly required, but more readable)
        const std::vector<GMLASField>& oFields = m_oFC.GetFields();
        for(int i=0; i< static_cast<int>(oFields.size()); i++ )
        {
            if( oFields[i].GetType() == GMLAS_FT_ID &&
                oFields[i].IsNotNullable() )
            {
                OGRFieldDefn oFieldDefn( oFields[i].GetName(), OFTString );
                oFieldDefn.SetNullable( false );
                m_nIDFieldIdx = m_poFeatureDefn->GetFieldCount();
                m_oMapFieldXPathToOGRFieldIdx[ oFields[i].GetXPath() ] =
                                            m_nIDFieldIdx;
                m_oMapOGRFieldIdxtoFCFieldIdx[ m_nIDFieldIdx ] = i;
                m_poFeatureDefn->AddFieldDefn( &oFieldDefn );
                break;
            }
        }

        // If we don't have an explicit ID, then we need
        // to generate one, so that potentially related classes can reference it
        // (We could perhaps try to be clever to determine if we really need it)
        if( m_nIDFieldIdx < 0 )
        {
            OGRFieldDefn oFieldDefn( "ogr_pkid", OFTString );
            oFieldDefn.SetNullable( false );
            m_nIDFieldIdx = m_poFeatureDefn->GetFieldCount();
            m_bIDFieldIsGenerated = true;
            m_poFeatureDefn->AddFieldDefn( &oFieldDefn );
        }
    }
}

/************************************************************************/
/*                             PostInit()                               */
/************************************************************************/

void OGRGMLASLayer::PostInit()
{
    const std::vector<GMLASField>& oFields = m_oFC.GetFields();

    OGRLayer* poFieldsMetadataLayer = m_poDS->GetFieldsMetadataLayer();
    OGRLayer* poRelationshipsLayer = m_poDS->GetRelationshipsLayer();

    // Is it a junction table ?
    if( !m_oFC.GetParentXPath().empty() )
    {
        {
            OGRFieldDefn oFieldDefn( "occurence", OFTInteger );
            oFieldDefn.SetNullable( false );
            m_poFeatureDefn->AddFieldDefn( &oFieldDefn );

            OGRFeature* poFieldDescFeature =
                        new OGRFeature(poFieldsMetadataLayer->GetLayerDefn());
            poFieldDescFeature->SetField( "layer_name", GetName() );
            poFieldDescFeature->SetField( "field_name", oFieldDefn.GetNameRef() );
            CPL_IGNORE_RET_VAL(
                    poFieldsMetadataLayer->CreateFeature(poFieldDescFeature));
            delete poFieldDescFeature;
        }

        {
            OGRFieldDefn oFieldDefn( "parent_pkid", OFTString );
            oFieldDefn.SetNullable( false );
            m_poFeatureDefn->AddFieldDefn( &oFieldDefn );

            OGRFeature* poFieldDescFeature =
                                new OGRFeature(poFieldsMetadataLayer->GetLayerDefn());
            poFieldDescFeature->SetField( "layer_name", GetName() );
            poFieldDescFeature->SetField( "field_name", oFieldDefn.GetNameRef() );
            CPL_IGNORE_RET_VAL(
                    poFieldsMetadataLayer->CreateFeature(poFieldDescFeature));
            delete poFieldDescFeature;
        }
        {
            OGRFieldDefn oFieldDefn( "child_pkid", OFTString );
            oFieldDefn.SetNullable( false );
            m_poFeatureDefn->AddFieldDefn( &oFieldDefn );

            OGRFeature* poFieldDescFeature =
                                new OGRFeature(poFieldsMetadataLayer->GetLayerDefn());
            poFieldDescFeature->SetField( "layer_name", GetName() );
            poFieldDescFeature->SetField( "field_name", oFieldDefn.GetNameRef() );
            CPL_IGNORE_RET_VAL(
                    poFieldsMetadataLayer->CreateFeature(poFieldDescFeature));
            delete poFieldDescFeature;
        }

        OGRGMLASLayer* poParentLeftLayer =
                    m_poDS->GetLayerByXPath(m_oFC.GetParentXPath());
        OGRGMLASLayer* poChildLayer =
                    m_poDS->GetLayerByXPath(m_oFC.GetChildXPath());
        if( poParentLeftLayer != NULL && poChildLayer != NULL )
        {
            OGRFeature* poRelationshipsFeature =
                            new OGRFeature(poRelationshipsLayer->GetLayerDefn());
            poRelationshipsFeature->SetField( "relation_name",
                                              m_oFC.GetName().c_str());
            poRelationshipsFeature->SetField( "parent_layer",
                                              poParentLeftLayer->GetName() );
            poRelationshipsFeature->SetField( "parent_field",
                    poParentLeftLayer->GetLayerDefn()->
                        GetFieldDefn(poParentLeftLayer->GetIDFieldIdx())->GetNameRef() );
            poRelationshipsFeature->SetField( "child_layer",
                                              poChildLayer->GetName() );
            poRelationshipsFeature->SetField( "child_field", 
                    poChildLayer->GetLayerDefn()->
                        GetFieldDefn(poChildLayer->GetIDFieldIdx())->GetNameRef() );
            CPL_IGNORE_RET_VAL(
                    poRelationshipsLayer->CreateFeature(poRelationshipsFeature));
            delete poRelationshipsFeature;
        }

        return;
    }

    // If we are a child class, then add a field to reference the parent.
    if( m_poParentLayer != NULL )
    {
        CPLString osFieldName("parent_");
        osFieldName += m_poParentLayer->GetLayerDefn()->GetFieldDefn(
                                m_poParentLayer->GetIDFieldIdx())->GetNameRef();
        OGRFieldDefn oFieldDefn( osFieldName, OFTString );
        oFieldDefn.SetNullable( false );
        m_nParentIDFieldIdx = m_poFeatureDefn->GetFieldCount();
        m_poFeatureDefn->AddFieldDefn( &oFieldDefn );

        OGRFeature* poRelationshipsFeature =
                        new OGRFeature(poRelationshipsLayer->GetLayerDefn());
        poRelationshipsFeature->SetField( "parent_layer",
                                          m_poParentLayer->GetName() );
        poRelationshipsFeature->SetField( "parent_field",
            m_poParentLayer->GetLayerDefn()->GetFieldDefn(
                            m_poParentLayer->GetIDFieldIdx())->GetNameRef() );
        poRelationshipsFeature->SetField( "child_layer", GetName() );
        poRelationshipsFeature->SetField( "child_field", osFieldName.c_str() );
        CPL_IGNORE_RET_VAL(
                poRelationshipsLayer->CreateFeature(poRelationshipsFeature));
        delete poRelationshipsFeature;
    }

    for(int i=0; i< static_cast<int>(oFields.size()); i++ )
    {
        OGRGMLASLayer* poRelatedLayer = NULL;
        if( !oFields[i].GetRelatedClassXPath().empty() )
        {
            poRelatedLayer =
                    m_poDS->GetLayerByXPath(oFields[i].GetRelatedClassXPath());
            if( poRelatedLayer != NULL )
            {
                OGRFeature* poRelationshipsFeature =
                        new OGRFeature(poRelationshipsLayer->GetLayerDefn());
                poRelationshipsFeature->SetField( "parent_layer", GetName() );
                poRelationshipsFeature->SetField( "parent_field",
                                                    oFields[i].GetName().c_str() );
                poRelationshipsFeature->SetField( "child_layer",
                                                  poRelatedLayer->GetName() );
                poRelationshipsFeature->SetField( "child_field", 
                    poRelatedLayer->GetLayerDefn()->
                        GetFieldDefn(poRelatedLayer->GetIDFieldIdx())->GetNameRef() );
                CPL_IGNORE_RET_VAL(
                        poRelationshipsLayer->CreateFeature(poRelationshipsFeature));
                delete poRelationshipsFeature;
            }
            else
            {
                CPLDebug("GMLAS", "Cannot find class matching %s",
                         oFields[i].GetRelatedClassXPath().c_str());
            }
        }

        OGRFeature* poFieldDescFeature =
                            new OGRFeature(poFieldsMetadataLayer->GetLayerDefn());
        poFieldDescFeature->SetField( "layer_name", GetName() );
        poFieldDescFeature->SetField( "field_name",
                                     oFields[i].GetName().c_str() );
        if( !oFields[i].GetXPath().empty() )
        {
            poFieldDescFeature->SetField( "field_xpath",
                                        oFields[i].GetXPath().c_str() );
        }
        else if( !oFields[i].GetAlternateXPaths().empty() )
        {
            CPLString osXPath;
            const std::vector<CPLString>& aoXPaths =
                                            oFields[i].GetAlternateXPaths();
            for( size_t j=0; j<aoXPaths.size(); j++ )
            {
                if( j != 0 ) osXPath += ",";
                osXPath += aoXPaths[j];
            }
            poFieldDescFeature->SetField( "field_xpath", osXPath.c_str() );
        }
        poFieldDescFeature->SetField( "field_type",
                                     oFields[i].GetTypeName().c_str() );
        if( oFields[i].GetMinOccurs() != -1 )
        {
            poFieldDescFeature->SetField( "field_min_occurs",
                                        oFields[i].GetMinOccurs() );
        }
        if( oFields[i].GetMaxOccurs() == MAXOCCURS_UNLIMITED )
        {
            poFieldDescFeature->SetField( "field_max_occurs", INT_MAX );
        }
        else if( oFields[i].GetMaxOccurs() != -1 )
        {
            poFieldDescFeature->SetField( "field_max_occurs",
                                        oFields[i].GetMaxOccurs() );
        }
        if( !oFields[i].GetFixedValue().empty() )
        {
            poFieldDescFeature->SetField( "field_fixed_value",
                                         oFields[i].GetFixedValue() );
        }
        if( !oFields[i].GetDefaultValue().empty() )
        {
            poFieldDescFeature->SetField( "field_default_value",
                                         oFields[i].GetDefaultValue() );
        }
        switch( oFields[i].GetCategory() )
        {
            case GMLASField::REGULAR:
                poFieldDescFeature->SetField( "field_category",
                                             "REGULAR");
                break;
            case GMLASField::PATH_TO_CHILD_ELEMENT_NO_LINK:
                poFieldDescFeature->SetField( "field_category",
                                             "PATH_TO_CHILD_ELEMENT_NO_LINK");
                break;
            case GMLASField::PATH_TO_CHILD_ELEMENT_WITH_LINK:
                poFieldDescFeature->SetField( "field_category",
                                             "PATH_TO_CHILD_ELEMENT_WITH_LINK");
                break;
            case GMLASField::PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE:
                poFieldDescFeature->SetField( "field_category",
                                "PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE");
                break;
            default:
                CPLAssert(FALSE);
                break;
        }
        if( poRelatedLayer != NULL )
        {
            poFieldDescFeature->SetField( "field_related_layer",
                                         poRelatedLayer->GetName() );
        }
        // TODO: set field_documentation
        CPL_IGNORE_RET_VAL(poFieldsMetadataLayer->CreateFeature(poFieldDescFeature));
        delete poFieldDescFeature;

        // Check whether the field is OGR instanciable
        if( oFields[i].GetCategory() ==
                                GMLASField::PATH_TO_CHILD_ELEMENT_NO_LINK ||
            oFields[i].GetCategory() ==
                        GMLASField::PATH_TO_CHILD_ELEMENT_WITH_JUNCTION_TABLE )
            continue;

        OGRFieldType eType = OFTString;
        OGRFieldSubType eSubType = OFSTNone;
        CPLString osOGRFieldName( oFields[i].GetName() );
        switch( oFields[i].GetType() )
        {
            case GMLAS_FT_STRING:
                eType = OFTString;
                break;
            case GMLAS_FT_ID:
            {
                eType = OFTString;
                if( oFields[i].IsNotNullable() )
                {
                    continue;
                }
                break;
            }
            case GMLAS_FT_BOOLEAN:
                eType = OFTInteger;
                eSubType = OFSTBoolean;
                break;
            case GMLAS_FT_SHORT:
                eType = OFTInteger;
                eSubType = OFSTInt16;
                break;
            case GMLAS_FT_INT32:
                eType = OFTInteger;
                break;
            case GMLAS_FT_INT64:
                eType = OFTInteger64;
                break;
            case GMLAS_FT_FLOAT:
                eType = OFTReal;
                eSubType = OFSTFloat32;
                break;
            case GMLAS_FT_DOUBLE:
                eType = OFTReal;
                break;
            case GMLAS_FT_DECIMAL:
                eType = OFTReal;
                break;
            case GMLAS_FT_DATE:
                eType = OFTDate;
                break;
            case GMLAS_FT_TIME:
                eType = OFTTime;
                break;
            case GMLAS_FT_DATETIME:
                eType = OFTDateTime;
                break;
            case GMLAS_FT_BASE64BINARY:
            case GMLAS_FT_HEXBINARY:
                eType = OFTBinary;
                break;
            case GMLAS_FT_ANYURI:
                eType = OFTString;
                break;
            case GMLAS_FT_ANYTYPE:
                eType = OFTString;
                break;
            case GMLAS_FT_ANYSIMPLETYPE:
                eType = OFTString;
                break;
            case GMLAS_FT_GEOMETRY:
            {
                // Create a geometry field
                OGRGeomFieldDefn oGeomFieldDefn( osOGRFieldName, 
                                                 oFields[i].GetGeomType() );
                m_poFeatureDefn->AddGeomFieldDefn( &oGeomFieldDefn );

                const int iOGRGeomIdx =
                                m_poFeatureDefn->GetGeomFieldCount() - 1;
                if( !oFields[i].GetXPath().empty() )
                {
                    m_oMapFieldXPathToOGRGeomFieldIdx[ oFields[i].GetXPath() ]
                            = iOGRGeomIdx ;
                }
                else
                {
                    const std::vector<CPLString>& aoXPaths =
                                        oFields[i].GetAlternateXPaths();
                    for( size_t j=0; j<aoXPaths.size(); j++ )
                    {
                        m_oMapFieldXPathToOGRGeomFieldIdx[ aoXPaths[j] ]
                                = iOGRGeomIdx ;
                    }
                }

                m_oMapOGRGeomFieldIdxtoFCFieldIdx[ iOGRGeomIdx ] = i;

                // Suffix the regular non-geometry field
                osOGRFieldName += "_xml";
                eType = OFTString;
                break;
            }
            default:
                CPLError(CE_Warning, CPLE_AppDefined,
                         "Unhandled type in enum: %d",
                         oFields[i].GetType() );
                break;
        }

/*
        if( oFields[i].GetType() == GMLAS_FT_GEOMETRY &&
            !poDS->GetCreateGeometryXMLField() )
        {
            continue;
        }
*/

        if( oFields[i].IsArray() )
        {
            switch( eType )
            {
                case OFTString: eType = OFTStringList; break;
                case OFTInteger: eType = OFTIntegerList; break;
                case OFTInteger64: eType = OFTInteger64List; break;
                case OFTReal: eType = OFTRealList; break;
                default:
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "Unhandled type in enum: %d", eType );
                    break;
            }
        }
        OGRFieldDefn oFieldDefn( osOGRFieldName, eType );
        oFieldDefn.SetSubType(eSubType);
        if( oFields[i].IsNotNullable() )
            oFieldDefn.SetNullable( false );
        CPLString osDefaultOrFixed = oFields[i].GetDefaultValue();
        if( osDefaultOrFixed.empty() )
            osDefaultOrFixed = oFields[i].GetFixedValue();
        if( !osDefaultOrFixed.empty() )
        {
            char* pszEscaped = CPLEscapeString(
                                        osDefaultOrFixed, -1, CPLES_SQL );
            oFieldDefn.SetDefault( (CPLString("'") +
                                        pszEscaped + CPLString("'")).c_str() );
            CPLFree(pszEscaped);
        }
        oFieldDefn.SetWidth( oFields[i].GetWidth() );
        m_poFeatureDefn->AddFieldDefn( &oFieldDefn );

        const int iOGRIdx = m_poFeatureDefn->GetFieldCount() - 1;
        if( !oFields[i].GetXPath().empty() )
        {
            m_oMapFieldXPathToOGRFieldIdx[ oFields[i].GetXPath() ] = iOGRIdx ;
        }
        else
        {
            const std::vector<CPLString>& aoXPaths = oFields[i].GetAlternateXPaths();
            for( size_t j=0; j<aoXPaths.size(); j++ )
            {
                m_oMapFieldXPathToOGRFieldIdx[ aoXPaths[j] ] = iOGRIdx ;
            }
        }

        m_oMapOGRFieldIdxtoFCFieldIdx[iOGRIdx] = i;
    }

    // In the case we have nested elements but we managed to fold into top
    // level class, then register intermediate paths so they are not reported
    // as unexpected in debug traces
    for(size_t i=0; i<oFields.size(); i++ )
    {
        std::vector<CPLString> aoXPaths = oFields[i].GetAlternateXPaths();
        if( aoXPaths.empty() )
            aoXPaths.push_back(oFields[i].GetXPath());
        for( size_t j=0; j<aoXPaths.size(); j++ )
        {
            if( aoXPaths[j].size() > m_oFC.GetXPath().size() )
            {
                // Split on both '/' and '@'
                char** papszTokens = CSLTokenizeString2(
                    aoXPaths[j].c_str() + m_oFC.GetXPath().size() + 1,
                    "/@", 0 );
                CPLString osSubXPath = m_oFC.GetXPath();
                for(int k=0; papszTokens[k] != NULL &&
                            papszTokens[k+1] != NULL; k++)
                {
                    osSubXPath += "/";
                    osSubXPath += papszTokens[k];
                    if( m_oMapFieldXPathToOGRFieldIdx.find( osSubXPath ) ==
                                                m_oMapFieldXPathToOGRFieldIdx.end() )
                    {
                        m_oMapFieldXPathToOGRFieldIdx[ osSubXPath ] =
                                                            IDX_COMPOUND_FOLDED;
                    }
                }
                CSLDestroy(papszTokens);
            }
        }
    }
}

/************************************************************************/
/*                           ~OGRGMLASLayer()                           */
/************************************************************************/

OGRGMLASLayer::~OGRGMLASLayer()
{
    m_poFeatureDefn->Release();
    delete m_poReader;
    if( m_fpGML != NULL )
        VSIFCloseL(m_fpGML);
}

/************************************************************************/
/*                       GetOGRFieldIndexFromXPath()                    */
/************************************************************************/

int OGRGMLASLayer::GetOGRFieldIndexFromXPath(const CPLString& osXPath) const
{
    std::map<CPLString, int>::const_iterator oIter =
        m_oMapFieldXPathToOGRFieldIdx.find(osXPath);
    if( oIter == m_oMapFieldXPathToOGRFieldIdx.end() )
        return -1;
    return oIter->second;
}

/************************************************************************/
/*                      GetOGRGeomFieldIndexFromXPath()                 */
/************************************************************************/

int OGRGMLASLayer::GetOGRGeomFieldIndexFromXPath(const CPLString& osXPath) const
{
    std::map<CPLString, int>::const_iterator oIter =
        m_oMapFieldXPathToOGRGeomFieldIdx.find(osXPath);
    if( oIter == m_oMapFieldXPathToOGRGeomFieldIdx.end() )
        return -1;
    return oIter->second;
}

/************************************************************************/
/*                     GetFCFieldIndexFromOGRFieldIdx()                 */
/************************************************************************/

int OGRGMLASLayer::GetFCFieldIndexFromOGRFieldIdx(int iOGRFieldIdx) const
{
    std::map<int, int>::const_iterator oIter =
        m_oMapOGRFieldIdxtoFCFieldIdx.find(iOGRFieldIdx);
    if( oIter == m_oMapOGRFieldIdxtoFCFieldIdx.end() )
        return -1;
    return oIter->second;
}

/************************************************************************/
/*                  GetFCFieldIndexFromOGRGeomFieldIdx()                */
/************************************************************************/

int OGRGMLASLayer::GetFCFieldIndexFromOGRGeomFieldIdx(int iOGRGeomFieldIdx) const
{
    std::map<int, int>::const_iterator oIter =
        m_oMapOGRGeomFieldIdxtoFCFieldIdx.find(iOGRGeomFieldIdx);
    if( oIter == m_oMapOGRGeomFieldIdxtoFCFieldIdx.end() )
        return -1;
    return oIter->second;
}

/************************************************************************/
/*                              ResetReading()                          */
/************************************************************************/

void OGRGMLASLayer::ResetReading()
{
    delete m_poReader;
    m_poReader = NULL;
}

/************************************************************************/
/*                          GetNextRawFeature()                         */
/************************************************************************/

OGRFeature* OGRGMLASLayer::GetNextRawFeature()
{
    if( m_poReader == NULL )
    {
        if( m_fpGML == NULL )
        {
            m_fpGML = VSIFOpenL(m_poDS->GetGMLFilename(), "rb");
            if( m_fpGML == NULL )
                return NULL;
        }

        m_poReader = new GMLASReader();
        m_poReader->Init( m_poDS->GetGMLFilename(),
                          m_fpGML,
                          m_poDS->GetMapURIToPrefix(),
                          m_poDS->GetLayers() );
        m_poReader->SetLayerOfInterest( this );
    }
    return m_poReader->GetNextFeature();
}

/************************************************************************/
/*                            GetNextFeature()                          */
/************************************************************************/

OGRFeature* OGRGMLASLayer::GetNextFeature()
{
    while( true )
    {
        OGRFeature *poFeature = GetNextRawFeature();
        if( poFeature == NULL )
            return NULL;

        if( (m_poFilterGeom == NULL
             || FilterGeometry( poFeature->GetGeometryRef() ) )
            && (m_poAttrQuery == NULL
                || m_poAttrQuery->Evaluate( poFeature )) )
        {
            return poFeature;
        }

        delete poFeature;
    }
}
