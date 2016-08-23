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
#include "ogr_api.h"

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
        OGRPolyhedralSurface::operator=( other );
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
/*                         isCompatibleSubType()                        */
/************************************************************************/

//! @cond Doxygen_Suppress
OGRBoolean OGRTriangulatedSurface::isCompatibleSubType( OGRwkbGeometryType eSubType ) const
{
    return wkbFlatten( eSubType ) == wkbTriangle;
}
//! @endcond

/************************************************************************/
/*                         getSubGeometryName()                         */
/************************************************************************/

//! @cond Doxygen_Suppress
const char* OGRTriangulatedSurface::getSubGeometryName() const
{
    return "TRIANGLE";
}
//! @endcond

/************************************************************************/
/*                         getSubGeometryType()                         */
/************************************************************************/

//! @cond Doxygen_Suppress
OGRwkbGeometryType OGRTriangulatedSurface::getSubGeometryType() const
{
    return wkbTriangle;
}
//! @endcond

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
    // If the geometry is a polygon, check if it can be cast as a tirangle
    if( EQUAL(poNewGeom->getGeometryName(),"POLYGON") )
    {
        OGRErr eErr = OGRERR_FAILURE;
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
    
    return OGRPolyhedralSurface::addGeometry(poNewGeom);
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
