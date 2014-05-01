/******************************************************************************
 * $Id$
 *
 * Project:  GeoPackage Translator
 * Purpose:  Implements GeoPackageDriver.
 * Author:   Paul Ramsey <pramsey@boundlessgeo.com>
 *
 ******************************************************************************
 * Copyright (c) 2013, Paul Ramsey <pramsey@boundlessgeo.com>
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

#include "ogr_geopackage.h"

// g++ -g -Wall -fPIC -shared -o ogr_geopackage.so -Iport -Igcore -Iogr -Iogr/ogrsf_frmts -Iogr/ogrsf_frmts/gpkg ogr/ogrsf_frmts/gpkg/*.c* -L. -lgdal 


/* "GP10" in ASCII bytes */
static const char aGpkgId[4] = {0x47, 0x50, 0x31, 0x30};
static const size_t szGpkgIdPos = 68;

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

static GDALDataset *OGRGeoPackageDriverOpen( GDALOpenInfo* poOpenInfo )
{
    /* Requirement 3: File name has to end in "gpkg" */
    /* http://opengis.github.io/geopackage/#_file_extension_name */
    if( !EQUAL(CPLGetExtension(poOpenInfo->pszFilename), "GPKG") )
        return NULL;

    /* Check that the filename exists and is a file */
    if( poOpenInfo->fpL == NULL)
        return NULL;

    /* Requirement 2: A GeoPackage SHALL contain 0x47503130 ("GP10" in ASCII) */
    /* in the application id */
    /* http://opengis.github.io/geopackage/#_file_format */
    if( poOpenInfo->nHeaderBytes < 68 + 4 ||
        memcmp(poOpenInfo->pabyHeader + 68, aGpkgId, 4) != 0 )
    {
        CPLError( CE_Failure, CPLE_AppDefined, "bad application_id on '%s'",
                  poOpenInfo->pszFilename);
        return NULL;
    }

    OGRGeoPackageDataSource   *poDS = new OGRGeoPackageDataSource();

    if( !poDS->Open( poOpenInfo->pszFilename, poOpenInfo->eAccess == GA_Update ) )
    {
        delete poDS;
        poDS = NULL;
    }

    return poDS;
}


/************************************************************************/
/*                               Create()                               */
/************************************************************************/

static GDALDataset *OGRGeoPackageDriverCreate( const char * pszFilename,
                                    int nBands, int nXSize, int nYSize, GDALDataType eDT,
                                    char **papszOptions )
{
	/* First, ensure there isn't any such file yet. */
    VSIStatBufL sStatBuf;

    if( VSIStatL( pszFilename, &sStatBuf ) == 0 )
    {
        CPLError( CE_Failure, CPLE_AppDefined, 
                  "A file system object called '%s' already exists.",
                  pszFilename );

        return NULL;
    }
	
    OGRGeoPackageDataSource   *poDS = new OGRGeoPackageDataSource();

    if( !poDS->Create( pszFilename, papszOptions ) )
    {
        delete poDS;
        poDS = NULL;
    }

    return poDS;
}

/************************************************************************/
/*                               Delete()                               */
/************************************************************************/

static CPLErr OGRGeoPackageDriverDelete( const char *pszFilename )

{
    if( VSIUnlink( pszFilename ) == 0 )
        return CE_None;
    else
        return CE_Failure;
}

/************************************************************************/
/*                         RegisterOGRGeoPackage()                       */
/************************************************************************/

void RegisterOGRGeoPackage()
{
    GDALDriver  *poDriver;

    if( GDALGetDriverByName( "GPKG" ) == NULL )
    {
        poDriver = new GDALDriver();

        poDriver->SetDescription( "GPKG" );
        poDriver->SetMetadataItem( GDAL_DCAP_VECTOR, "YES" );
        poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                                   "GeoPackage" );
        poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                                   "drv_geopackage.html" );

        poDriver->pfnOpen = OGRGeoPackageDriverOpen;
        poDriver->pfnCreate = OGRGeoPackageDriverCreate;
        poDriver->pfnDelete = OGRGeoPackageDriverDelete;

        GetGDALDriverManager()->RegisterDriver( poDriver );
    }
}


