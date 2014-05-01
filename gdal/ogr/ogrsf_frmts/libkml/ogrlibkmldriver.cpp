/******************************************************************************
 *
 * Project:  KML Translator
 * Purpose:  Implements OGRLIBKMLDriver
 * Author:   Brian Case, rush at winkey dot org
 *
 ******************************************************************************
 * Copyright (c) 2010, Brian Case
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
 *****************************************************************************/

#include "ogr_libkml.h"
#include "cpl_conv.h"
#include "cpl_error.h"
#include "cpl_multiproc.h"

#include <kml/dom.h>

using kmldom::KmlFactory;

static void *hMutex = NULL;
static KmlFactory* m_poKmlFactory = NULL;

/******************************************************************************
 OGRLIBKMLDriverUnload()
******************************************************************************/

static void OGRLIBKMLDriverUnload ( GDALDriver* poDriver )
{
    if( hMutex != NULL )
        CPLDestroyMutex(hMutex);
    hMutex = NULL;
    delete m_poKmlFactory;
    m_poKmlFactory = NULL;
}

/******************************************************************************
 Open()
******************************************************************************/

static GDALDataset *OGRLIBKMLDriverOpen ( GDALOpenInfo* poOpenInfo )
{
    if( !poOpenInfo->bStatOK )
        return NULL;
    if( !poOpenInfo->bIsDirectory )
    {
        if( !EQUAL(CPLGetExtension(poOpenInfo->pszFilename), "kml") &&
            !EQUAL(CPLGetExtension(poOpenInfo->pszFilename), "kmz") )
            return NULL;
    }

    {
        CPLMutexHolderD(&hMutex);
        if( m_poKmlFactory == NULL )
            m_poKmlFactory = KmlFactory::GetFactory (  );
    }

    OGRLIBKMLDataSource *poDS = new OGRLIBKMLDataSource ( m_poKmlFactory );

    if ( !poDS->Open ( poOpenInfo->pszFilename, poOpenInfo->eAccess == GA_Update ) ) {
        delete poDS;

        poDS = NULL;
    }

    return poDS;
}


/************************************************************************/
/*                               Create()                               */
/************************************************************************/

static GDALDataset *OGRLIBKMLDriverCreate( const char * pszName,
                                    int nBands, int nXSize, int nYSize, GDALDataType eDT,
                                    char **papszOptions )
{
    CPLAssert ( NULL != pszName );
    CPLDebug ( "LIBKML", "Attempt to create: %s", pszName );

    {
        CPLMutexHolderD(&hMutex);
        if( m_poKmlFactory == NULL )
            m_poKmlFactory = KmlFactory::GetFactory (  );
    }

    OGRLIBKMLDataSource *poDS = new OGRLIBKMLDataSource ( m_poKmlFactory );

    if ( !poDS->Create ( pszName, papszOptions ) ) {
        delete poDS;

        poDS = NULL;
    }

    return poDS;
}

/******************************************************************************
 DeleteDataSource()

 note: this method recursivly deletes an entire dir if the datasource is a dir
       and all the files are kml or kmz
 
******************************************************************************/

static CPLErr OGRLIBKMLDriverDelete( const char *pszName )
{

    /***** dir *****/

    VSIStatBufL sStatBuf = { };
    if ( !VSIStatL ( pszName, &sStatBuf ) && VSI_ISDIR ( sStatBuf.st_mode ) ) {

        char **papszDirList = NULL;

        if ( !( papszDirList = VSIReadDir ( pszName ) ) ) {
            if ( VSIRmdir ( pszName ) < 0 )
                return CE_Failure;
        }

        int nFiles = CSLCount ( papszDirList );
        int iFile;

        for ( iFile = 0; iFile < nFiles; iFile++ ) {
            if ( CE_Failure ==
                 OGRLIBKMLDriverDelete ( papszDirList[iFile] ) ) {
                CSLDestroy ( papszDirList );
                return CE_Failure;
            }
        }

        if ( VSIRmdir ( pszName ) < 0 ) {
            CSLDestroy ( papszDirList );
            return CE_Failure;
        }

        CSLDestroy ( papszDirList );
    }

   /***** kml *****/

    else if ( EQUAL ( CPLGetExtension ( pszName ), "kml" ) ) {
        if ( VSIUnlink ( pszName ) < 0 )
            return CE_Failure;
    }

    /***** kmz *****/

    else if ( EQUAL ( CPLGetExtension ( pszName ), "kmz" ) ) {
        if ( VSIUnlink ( pszName ) < 0 )
            return CE_Failure;
    }

    /***** do not delete other types of files *****/

    else
        return CE_Failure;

    return CE_None;
}

/******************************************************************************
 RegisterOGRLIBKML()
******************************************************************************/

void RegisterOGRLIBKML (
     )
{
    GDALDriver  *poDriver;

    if( GDALGetDriverByName( "LIBKML" ) == NULL )
    {
        poDriver = new GDALDriver();

        poDriver->SetDescription( "LIBKML" );
        poDriver->SetMetadataItem( GDAL_DCAP_VECTOR, "YES" );
        poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                                   "LIBKML" );
        poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                                   "drv_libkml.html" );

        poDriver->SetMetadataItem( GDAL_DCAP_VIRTUALIO, "YES" );

        poDriver->pfnOpen = OGRLIBKMLDriverOpen;
        poDriver->pfnCreate = OGRLIBKMLDriverCreate;
        poDriver->pfnDelete = OGRLIBKMLDriverDelete;
        poDriver->pfnUnloadDriver = OGRLIBKMLDriverUnload;

        GetGDALDriverManager()->RegisterDriver( poDriver );
    }
}
