/******************************************************************************
 * $Id$
 *
 * Project:  OpenGIS Simple Features Reference Implementation
 * Purpose:  The OGRTriangulatedSurface geometry class.
 * Author:   Avyav Kumar Singh <avyavkumar at gmail dot com>
 *
 ******************************************************************************
 * Copyright (c) 2016, Avyav Kumar Singh <avyavkumar at gmail dot com>
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

#include "ogr_geometry.h"
#include "ogr_p.h"
#include "ogr_sfcgal.h"
#include "ogr_geos.h"
#include "ogr_api.h"
#include "ogr_libs.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                        OGRTriangulatedSurface()                      */
/************************************************************************/

/**
 * \brief Constructor.
 *
 */

OGRTriangulatedSurface::OGRTriangulatedSurface()

{ }

/************************************************************************/
/*        OGRTriangulatedSurface( const OGRTriangulatedSurface& )       */
/************************************************************************/

/**
 * \brief Copy constructor.
 *
 */

OGRTriangulatedSurface::OGRTriangulatedSurface( const OGRTriangulatedSurface& other ) :
    OGRPolyhedralSurface(other)
{ }

/************************************************************************/
/*                        ~OGRTriangulatedSurface()                     */
/************************************************************************/

/**
 * \brief Destructor
 *
 */

OGRTriangulatedSurface::~OGRTriangulatedSurface()

{ }

/************************************************************************/
/*                 operator=( const OGRTriangulatedSurface&)            */
/************************************************************************/

/**
 * \brief Assignment operator.
 *
 */

OGRTriangulatedSurface& OGRTriangulatedSurface::operator=( const OGRTriangulatedSurface& other )
{
    if( this != &other)
    {
        OGRSurface::operator=( other );
        oMP = other.oMP;
    }
    return *this;
}

/************************************************************************/
/*                          getGeometryName()                           */
/************************************************************************/

/**
 * \brief Returns the geometry name of the TriangulatedSurface
 *
 * @return "TIN"
 *
 */

const char* OGRTriangulatedSurface::getGeometryName() const
{
    return "TIN" ;
}

/************************************************************************/
/*                          getGeometryType()                           */
/************************************************************************/

/**
 * \brief Returns the WKB Type of TriangulatedSurface
 *
 */

OGRwkbGeometryType OGRTriangulatedSurface::getGeometryType() const
{
    if( (flags & OGR_G_3D) && (flags & OGR_G_MEASURED) )
        return wkbTINZM;
    else if( flags & OGR_G_MEASURED  )
        return wkbTINM;
    else if( flags & OGR_G_3D )
        return wkbTINZ;
    else
        return wkbTIN;
}

/************************************************************************/
/*                               clone()                                */
/************************************************************************/

/**
 * \brief Make a copy of this object.
 *
 * This method relates to the SFCOM IGeometry::clone() method.
 *
 * This method is the same as the C function OGR_G_Clone().
 *
 * @return a new object instance with the same geometry, and spatial
 * reference system as the original.
 */

OGRGeometry* OGRTriangulatedSurface::clone() const
{
    OGRTriangulatedSurface *poNewTIN;
    poNewTIN = (OGRTriangulatedSurface*) OGRGeometryFactory::createGeometry(getGeometryType());
    if( poNewTIN == NULL )
        return NULL;

    poNewTIN->assignSpatialReference(getSpatialReference());
    poNewTIN->flags = flags;

    for( int i = 0; i < oMP.nGeomCount; i++ )
    {
        if( (poNewTIN->oMP)._addGeometry( oMP.papoGeoms[i] ) != OGRERR_NONE )
        {
            delete poNewTIN;
            return NULL;
        }
    }
    return poNewTIN;
}

/************************************************************************/
/*                         isCompatibleSubType()                        */
/************************************************************************/

//! @cond Doxygen_Suppress
OGRBoolean OGRTriangulatedSurface::isCompatibleSubType( OGRwkbGeometryType eSubType ) const
{
    return wkbFlatten( eSubType ) == wkbTriangle;
}
//! @endcond

/************************************************************************/
/*                           importFromWkt()                            */
/*              Instantiate from well known text format.                */
/************************************************************************/

/**
 * \brief Assign geometry from well known text data.
 *
 * The object must have already been instantiated as the correct derived
 * type of geometry object to match the text type.  This method is used
 * by the OGRGeometryFactory class, but not normally called by application
 * code.
 *
 * This method relates to the SFCOM IWks::ImportFromWKT() method.
 *
 * This method is the same as the C function OGR_G_ImportFromWkt().
 *
 * @param ppszInput pointer to a pointer to the source text.  The pointer is
 *                    updated to pointer after the consumed text.
 *
 * @return OGRERR_NONE if all goes well, otherwise any of
 * OGRERR_NOT_ENOUGH_DATA, OGRERR_UNSUPPORTED_GEOMETRY_TYPE, or
 * OGRERR_CORRUPT_DATA may be returned.
 */

OGRErr OGRTriangulatedSurface::importFromWkt( char ** ppszInput )

{
    int bHasZ = FALSE, bHasM = FALSE;
    bool bIsEmpty = false;
    OGRErr      eErr = importPreambuleFromWkt(ppszInput, &bHasZ, &bHasM, &bIsEmpty);
    flags = 0;
    if( eErr != OGRERR_NONE )
        return eErr;
    if( bHasZ ) flags |= OGR_G_3D;
    if( bHasM ) flags |= OGR_G_MEASURED;
    if( bIsEmpty )
        return OGRERR_NONE;

    char        szToken[OGR_WKT_TOKEN_MAX];
    const char  *pszInput = *ppszInput;
    eErr = OGRERR_NONE;

    /* Skip first '(' */
    pszInput = OGRWktReadToken( pszInput, szToken );

    // Read each surface
    OGRRawPoint *paoPoints = NULL;
    int          nMaxPoints = 0;
    double      *padfZ = NULL;

    do
    {

        // Get the first token
        const char* pszInputBefore = pszInput;
        pszInput = OGRWktReadToken( pszInput, szToken );

        OGRSurface* poSurface;

        // Start importing
        if (EQUAL(szToken,"("))
        {
            OGRTriangle      *poTriangle = new OGRTriangle();
            poSurface = poTriangle;
            pszInput = pszInputBefore;
            eErr = poTriangle->importFromWKTListOnly( (char**)&pszInput, bHasZ, bHasM,
                                                     paoPoints, nMaxPoints, padfZ );
        }
        else if (EQUAL(szToken, "EMPTY") )
        {
            poSurface = new OGRTriangle();
        }

        /* We accept TRIANGLE() but this is an extension to the BNF, also */
        /* accepted by PostGIS */
        else if (EQUAL(szToken,"TRIANGLE"))
        {
            OGRGeometry* poGeom = NULL;
            pszInput = pszInputBefore;
            eErr = OGRGeometryFactory::createFromWkt( (char **) &pszInput,
                                                       NULL, &poGeom );
            poSurface = (OGRSurface*) poGeom;
        }
        else
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Unexpected token : %s", szToken);
            eErr = OGRERR_CORRUPT_DATA;
            break;
        }

        if( eErr == OGRERR_NONE )
            eErr = addGeometryDirectly( poSurface );
        if( eErr != OGRERR_NONE )
        {
            delete poSurface;
            break;
        }

        // Read the delimiter following the surface.
        pszInput = OGRWktReadToken( pszInput, szToken );

    } while( szToken[0] == ',' && eErr == OGRERR_NONE );

    CPLFree( paoPoints );
    CPLFree( padfZ );

    // Check for a closing bracket
    if( eErr != OGRERR_NONE )
        return eErr;

    if( szToken[0] != ')' )
        return OGRERR_CORRUPT_DATA;

    *ppszInput = (char *) pszInput;
    return OGRERR_NONE;
}

/************************************************************************/
/*                            exportToWkt()                             */
/************************************************************************/

/**
 * \brief Convert a geometry into well known text format.
 *
 * This method relates to the SFCOM IWks::ExportToWKT() method.
 *
 * This method is the same as the C function OGR_G_ExportToWkt().
 *
 * @param ppszDstText a text buffer is allocated by the program, and assigned
 *                    to the passed pointer. After use, *ppszDstText should be
 *                    freed with OGRFree().
 * @param eWkbVariant the specification that must be conformed too :
 *                    - wbkVariantOgc for old-style 99-402 extended dimension (Z) WKB types
 *                    - wbkVariantIso for SFSQL 1.2 and ISO SQL/MM Part 3
 *
 * @return Currently OGRERR_NONE is always returned.
 */

OGRErr OGRTriangulatedSurface::exportToWkt ( char ** ppszDstText,
                                           CPL_UNUSED OGRwkbVariant eWkbVariant ) const
{
    return exportToWktInternal(ppszDstText, wkbVariantIso, "TRIANGLE");
}

/************************************************************************/
/*                            addGeometry()                             */
/************************************************************************/

/**
 * \brief Add a new geometry to the TriangulatedSurface.
 *
 * Only a TRIANGLE can be added to a TRIANGULATEDSURFACE.
 *
 * If a polygon is passed as parameter, OGR tries to cast it as a triangle and then add it.
 *
 * @return OGRErr OGRERR_NONE if the polygon is successfully added
 */

OGRErr OGRTriangulatedSurface::addGeometry (const OGRGeometry *poNewGeom)
{
    if (!(EQUAL(poNewGeom->getGeometryName(),"POLYGON") || EQUAL(poNewGeom->getGeometryName(),"TRIANGLE")))
        return OGRERR_UNSUPPORTED_GEOMETRY_TYPE;

    // If it is a triangle, we can add it to the TIN without any hassle
    else if (EQUAL(poNewGeom->getGeometryName(), "TRIANGLE"))
    {
        OGRGeometry *poClone = poNewGeom->clone();
        OGRErr      eErr;

        if (poClone == NULL)
            return OGRERR_FAILURE;

        eErr = addGeometryDirectly(poClone);

        if( eErr != OGRERR_NONE )
            delete poClone;

        return eErr;
    }

    // We can only add polygon as a triangle
    else
    {
        OGRErr eErr;
        OGRTriangle *poTriangle = new OGRTriangle(*((OGRPolygon *)poNewGeom), eErr);
        if (poTriangle != NULL && eErr == OGRERR_NONE)
        {
            eErr = addGeometryDirectly(poTriangle);

            if( eErr != OGRERR_NONE )
                delete poTriangle;

            return eErr;
        }
        else
        {
            delete poTriangle;
            return OGRERR_UNSUPPORTED_GEOMETRY_TYPE;
        }
    }
}

/************************************************************************/
/*                        addGeometryDirectly()                         */
/************************************************************************/

/**
 * \brief Add a geometry directly to the container.
 *
 * This method is the same as the C function OGR_G_AddGeometryDirectly().
 *
 * There is no SFCOM analog to this method.
 *
 * @param poNewGeom geometry to add to the container.
 *
 * @return OGRERR_NONE if successful, or OGRERR_UNSUPPORTED_GEOMETRY_TYPE if
 * the geometry type is illegal for the type of geometry container.
 */

OGRErr OGRTriangulatedSurface::addGeometryDirectly (OGRGeometry *poNewGeom)
{
    if (!EQUAL(poNewGeom->getGeometryName(), "TRIANGLE"))
    {
        return OGRERR_UNSUPPORTED_GEOMETRY_TYPE;
    }

    if( poNewGeom->Is3D() && !Is3D() )
        set3D(TRUE);

    if( poNewGeom->IsMeasured() && !IsMeasured() )
        setMeasured(TRUE);

    if( !poNewGeom->Is3D() && Is3D() )
        poNewGeom->set3D(TRUE);

    if( !poNewGeom->IsMeasured() && IsMeasured() )
        poNewGeom->setMeasured(TRUE);

    OGRGeometry** papoNewGeoms = (OGRGeometry **) VSI_REALLOC_VERBOSE( oMP.papoGeoms,
                                             sizeof(void*) * (oMP.nGeomCount+1) );
    if( papoNewGeoms == NULL )
        return OGRERR_FAILURE;

    oMP.papoGeoms = papoNewGeoms;
    oMP.papoGeoms[oMP.nGeomCount] = poNewGeom;
    oMP.nGeomCount++;

    return OGRERR_NONE;
}

/************************************************************************/
/*                         CastToMultiPolygon()                         */
/************************************************************************/

/**
 * \brief Casts the OGRTriangulatedSurface to an OGRMultiPolygon
 *
 * The passed in geometry is consumed and a new one returned (or NULL in case
 * of failure)
 *
 * @param poTS the input geometry - ownership is passed to the method.
 * @return new geometry.
 */

OGRMultiPolygon* OGRTriangulatedSurface::CastToMultiPolygon(OGRTriangulatedSurface* poTS)
{
    OGRMultiPolygon *poMultiPolygon = new OGRMultiPolygon();

    for (int i = 0; i < poTS->oMP.nGeomCount; i++)
    {
        OGRTriangle *geom = reinterpret_cast<OGRTriangle *>(poTS->oMP.papoGeoms[i]);
        poTS->oMP.papoGeoms[i] = NULL;
        OGRPolygon *poPolygon = OGRSurface::CastToPolygon(geom);
        poMultiPolygon->addGeometryDirectly(poPolygon);
    }
    delete poTS;

    return poMultiPolygon;
}
