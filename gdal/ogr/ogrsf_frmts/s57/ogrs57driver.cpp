/******************************************************************************
 * $Id$
 *
 * Project:  S-57 Translator
 * Purpose:  Implements OGRS57Driver
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 1999, Frank Warmerdam
 * Copyright (c) 2013, Even Rouault <even dot rouault at mines-paris dot org>
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

#include "ogr_s57.h"
#include "cpl_conv.h"
#include "cpl_multiproc.h"

CPL_CVSID("$Id$");

S57ClassRegistrar *OGRS57Driver::poRegistrar = NULL;
static void* hS57RegistrarMutex = NULL;

/************************************************************************/
/*                            OGRS57Driver()                            */
/************************************************************************/

OGRS57Driver::OGRS57Driver()

{
}

/************************************************************************/
/*                           ~OGRS57Driver()                            */
/************************************************************************/

OGRS57Driver::~OGRS57Driver()

{
    if( poRegistrar != NULL )
    {
        delete poRegistrar;
        poRegistrar = NULL;
    }
    
    if( hS57RegistrarMutex != NULL )
    {
        CPLDestroyMutex(hS57RegistrarMutex);
        hS57RegistrarMutex = NULL;
    }
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

GDALDataset *OGRS57Driver::Open( GDALOpenInfo* poOpenInfo )

{
    OGRS57DataSource    *poDS;

    if( poOpenInfo->nHeaderBytes < 10 )
        return NULL;
    const char* pachLeader = (const char* )poOpenInfo->pabyHeader;
    if( (pachLeader[5] != '1' && pachLeader[5] != '2'
                && pachLeader[5] != '3' )
            || pachLeader[6] != 'L'
            || (pachLeader[8] != '1' && pachLeader[8] != ' ') )
    {
        return NULL;
    }

    poDS = new OGRS57DataSource;
    if( !poDS->Open( poOpenInfo->pszFilename, TRUE ) )
    {
        delete poDS;
        poDS = NULL;
    }

    if( poDS && poOpenInfo->eAccess == GA_Update )
    {
        delete poDS;
        CPLError( CE_Failure, CPLE_OpenFailed,
                  "S57 Driver doesn't support update." );
        return NULL;
    }

    return poDS;
}

/************************************************************************/
/*                              Create()                                */
/************************************************************************/

GDALDataset *OGRS57Driver::Create( const char * pszName,
                                int nBands, int nXSize, int nYSize, GDALDataType eDT,
                                char **papszOptions )
{
    OGRS57DataSource *poDS = new OGRS57DataSource();

    if( poDS->Create( pszName, papszOptions ) )
        return poDS;
    else
    {
        delete poDS;
        return NULL;
    }
}

/************************************************************************/
/*                          GetS57Registrar()                           */
/************************************************************************/

S57ClassRegistrar *OGRS57Driver::GetS57Registrar()

{
/* -------------------------------------------------------------------- */
/*      Instantiate the class registrar if possible.                    */
/* -------------------------------------------------------------------- */
    CPLMutexHolderD(&hS57RegistrarMutex);

    if( poRegistrar == NULL )
    {
        poRegistrar = new S57ClassRegistrar();

        if( !poRegistrar->LoadInfo( NULL, NULL, FALSE ) )
        {
            delete poRegistrar;
            poRegistrar = NULL;
        }
    }

    return poRegistrar;
}

/************************************************************************/
/*                           RegisterOGRS57()                           */
/************************************************************************/

void RegisterOGRS57()

{
    GDALDriver  *poDriver;

    if( GDALGetDriverByName( "S57" ) == NULL )
    {
        poDriver = new OGRS57Driver();

        poDriver->SetDescription( "S57" );
        poDriver->SetMetadataItem( GDAL_DCAP_VECTOR, "YES" );
        poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                                   "SDTS" );
        poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                                   "drv_s57.html" );

        poDriver->SetMetadataItem( GDAL_DCAP_VIRTUALIO, "YES" );

        poDriver->pfnOpen = OGRS57Driver::Open;
        poDriver->pfnCreate = OGRS57Driver::Create;

        GetGDALDriverManager()->RegisterDriver( poDriver );
    }
}
