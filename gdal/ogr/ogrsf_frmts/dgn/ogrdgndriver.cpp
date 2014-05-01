/******************************************************************************
 * $Id$
 *
 * Project:  OpenGIS Simple Features Reference Implementation
 * Purpose:  Implements OGRDGNDriver class.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam (warmerdam@pobox.com)
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

#include "ogr_dgn.h"
#include "cpl_conv.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

static int OGRDGNDriverIdentify( GDALOpenInfo* poOpenInfo )

{
    return poOpenInfo->fpL != NULL &&
           DGNTestOpen(poOpenInfo->pabyHeader, poOpenInfo->nHeaderBytes);
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

static GDALDataset *OGRDGNDriverOpen( GDALOpenInfo* poOpenInfo )

{
    OGRDGNDataSource    *poDS;
    
    if( !OGRDGNDriverIdentify(poOpenInfo) )
        return NULL;

    poDS = new OGRDGNDataSource();

    if( !poDS->Open( poOpenInfo->pszFilename, TRUE, (poOpenInfo->eAccess == GA_Update) )
        || poDS->GetLayerCount() == 0 )
    {
        delete poDS;
        return NULL;
    }
    else
        return poDS;
}

/************************************************************************/
/*                              Create()                                */
/************************************************************************/

static GDALDataset *OGRDGNDriverCreate( const char * pszName,
                                int nBands, int nXSize, int nYSize, GDALDataType eDT,
                                char **papszOptions )

{
/* -------------------------------------------------------------------- */
/*      Return a new OGRDataSource()                                    */
/* -------------------------------------------------------------------- */
    OGRDGNDataSource    *poDS = NULL;

    poDS = new OGRDGNDataSource();
    
    if( !poDS->PreCreate( pszName, papszOptions ) )
    {
        delete poDS;
        return NULL;
    }
    else
        return poDS;
}

/************************************************************************/
/*                          RegisterOGRDGN()                            */
/************************************************************************/

void RegisterOGRDGN()

{
    GDALDriver  *poDriver;

    if( GDALGetDriverByName( "DGN" ) == NULL )
    {
        poDriver = new GDALDriver();

        poDriver->SetDescription( "DGN" );
        poDriver->SetMetadataItem( GDAL_DCAP_VECTOR, "YES" );
        poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                                   "DGN" );
        poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                                   "drv_dgn.html" );

        poDriver->pfnOpen = OGRDGNDriverOpen;
        poDriver->pfnIdentify = OGRDGNDriverIdentify;
        poDriver->pfnCreate = OGRDGNDriverCreate;

        GetGDALDriverManager()->RegisterDriver( poDriver );
    }
}

