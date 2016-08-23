/******************************************************************************
 * $Id$
 *
 * Project:  OpenGIS Simple Features Reference Implementation
 * Purpose:  The OGRTriangle geometry class.
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
#include "ogr_api.h"
#include "cpl_error.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                             OGRTriangle()                            */
/************************************************************************/

/**
 * \brief Constructor.
 *
 */

OGRTriangle::OGRTriangle()
{ }

/************************************************************************/
/*                             OGRTriangle()                            */
/************************************************************************/

/**
 * \brief Copy constructor.
 *
 */

OGRTriangle::OGRTriangle(const OGRTriangle& other) :
    OGRPolygon(other)
{ }

/************************************************************************/
/*                             OGRTriangle()                            */
/************************************************************************/

/**
 * \brief Constructs an OGRTriangle from a valid OGRPolygon. In case of error, NULL is returned.
 *
 * @param other the Polygon we wish to construct a triangle from
 * @param eErr encapsulates an error code; contains OGRERR_NONE if the triangle is constructed successfully
 */

OGRTriangle::OGRTriangle(const OGRPolygon& other, OGRErr &eErr)
{
    // In case of Polygon, we have to check that it is a valid triangle -
    // closed and contains one external ring of four points
    // If not, then eErr will contain the error description
    const OGRCurve *poCurve = other.getExteriorRingCurve();
    if (other.getNumInteriorRings() == 0 &&
        poCurve != NULL && poCurve->get_IsClosed() &&
        poCurve->getNumPoints() == 4)
    {
        // everything is fine
        eErr = this->addRing( const_cast<OGRCurve*>(poCurve) );
        if (eErr != OGRERR_NONE)
            CPLError( CE_Failure, CPLE_NotSupported, "Invalid Triangle");
    }
}

/************************************************************************/
/*                             OGRTriangle()                            */
/************************************************************************/

/**
 * \brief Construct a triangle from points
 *
 * @param p Point 1
 * @param q Point 2
 * @param r Point 3
 */

OGRTriangle::OGRTriangle(const OGRPoint &p, const OGRPoint &q, const OGRPoint &r)
{
    OGRLinearRing *poCurve = new OGRLinearRing();
    poCurve->addPoint(&p);
    poCurve->addPoint(&q);
    poCurve->addPoint(&r);
    poCurve->addPoint(&p);

    oCC.addCurveDirectly(this, poCurve, TRUE);
}

/************************************************************************/
/*                             ~OGRTriangle()                            */
/************************************************************************/

/**
 * \brief Destructor
 *
 */

OGRTriangle::~OGRTriangle()
{
    if (!oCC.IsEmpty())
    {
        oCC.empty(this);
    }
}

/************************************************************************/
/*                    operator=( const OGRGeometry&)                    */
/************************************************************************/

/**
 * \brief Assignment operator
 *
 * @param other A triangle passed as a parameter
 *
 * @return OGRTriangle A copy of other
 *
 */

OGRTriangle& OGRTriangle::operator=( const OGRTriangle& other )
{
    if( this != &other)
    {
        OGRPolygon::operator=( other );
        oCC = other.oCC;
    }
    return *this;
}

/************************************************************************/
/*                          getGeometryName()                           */
/************************************************************************/

/**
 * \brief Returns the geometry name of the triangle
 *
 * @return "TRIANGLE"
 *
 */

const char* OGRTriangle::getGeometryName() const
{
    return "TRIANGLE";
}

/************************************************************************/
/*                          getGeometryType()                           */
/************************************************************************/

/**
 * \brief Returns the WKB Type of Triangle
 *
 */

OGRwkbGeometryType OGRTriangle::getGeometryType() const
{
    if( (flags & OGR_G_3D) && (flags & OGR_G_MEASURED) )
        return wkbTriangleZM;
    else if( flags & OGR_G_MEASURED  )
        return wkbTriangleM;
    else if( flags & OGR_G_3D )
        return wkbTriangleZ;
    else
        return wkbTriangle;
}

/************************************************************************/
/*                        quickValidityCheck()                          */
/************************************************************************/

bool OGRTriangle::quickValidityCheck() const
{
    return oCC.nCurveCount == 0 ||
           (oCC.nCurveCount == 1 &&
            oCC.papoCurves[0]->getNumPoints() == 4 &&
            oCC.papoCurves[0]->get_IsClosed());
}

/************************************************************************/
/*                           importFromWkb()                            */
/************************************************************************/

/**
 * \brief Assign geometry from well known binary data.
 *
 * The object must have already been instantiated as the correct derived
 * type of geometry object to match the binaries type.  This method is used
 * by the OGRGeometryFactory class, but not normally called by application
 * code.
 *
 * This method relates to the SFCOM IWks::ImportFromWKB() method.
 *
 * This method is the same as the C function OGR_G_ImportFromWkb().
 *
 * @param pabyData the binary input data.
 * @param nSize the size of pabyData in bytes, or zero if not known.
 * @param eWkbVariant if wkbVariantPostGIS1, special interpretation is done for curve geometries code
 *
 * @return OGRERR_NONE if all goes well, otherwise any of
 * OGRERR_NOT_ENOUGH_DATA, OGRERR_UNSUPPORTED_GEOMETRY_TYPE, or
 * OGRERR_CORRUPT_DATA may be returned.
 */

OGRErr OGRTriangle::importFromWkb( unsigned char *pabyData,
                                  int nSize,
                                  OGRwkbVariant eWkbVariant )
{
    OGRErr eErr = OGRPolygon::importFromWkb( pabyData, nSize, eWkbVariant );
    if( eErr != OGRERR_NONE )
        return eErr;

    if ( !quickValidityCheck() )
    {
        empty();
        return OGRERR_CORRUPT_DATA;
    }

    return OGRERR_NONE;
}

/************************************************************************/
/*                           importFromWkt()                            */
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

OGRErr OGRTriangle::importFromWkt( char ** ppszInput )

{
    OGRErr eErr = OGRPolygon::importFromWkt( ppszInput );
    if( eErr != OGRERR_NONE )
        return eErr;

    if ( !quickValidityCheck() )
    {
        empty();
        return OGRERR_CORRUPT_DATA;
    }

    return OGRERR_NONE;
}

/************************************************************************/
/*                           addRingDirectly()                          */
/************************************************************************/

OGRErr OGRTriangle::addRingDirectly( OGRCurve * poNewRing )
{
    if (oCC.nCurveCount == 0)
        return addRingDirectlyInternal( poNewRing, TRUE );
    else
        return OGRERR_FAILURE;
}

