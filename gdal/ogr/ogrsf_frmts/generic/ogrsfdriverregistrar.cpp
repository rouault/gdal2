/******************************************************************************
 * $Id$
 *
 * Project:  OpenGIS Simple Features Reference Implementation
 * Purpose:  The OGRSFDriverRegistrar class implementation.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 1999,  Les Technologies SoftMap Inc.
 * Copyright (c) 2008-2012, Even Rouault <even dot rouault at mines-paris dot org>
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

#include "ogrsf_frmts.h"
#include "ogr_api.h"

CPL_CVSID("$Id$");

/************************************************************************/
/*                         OGRSFDriverRegistrar                         */
/************************************************************************/

/**
 * \brief Constructor
 *
 * Normally the driver registrar is constucted by the 
 * OGRSFDriverRegistrar::GetRegistrar() accessor which ensures singleton
 * status.  
 */

OGRSFDriverRegistrar::OGRSFDriverRegistrar()

{
}

/************************************************************************/
/*                       ~OGRSFDriverRegistrar()                        */
/************************************************************************/

OGRSFDriverRegistrar::~OGRSFDriverRegistrar()

{
}

/************************************************************************/
/*                           GetRegistrar()                             */
/************************************************************************/


OGRSFDriverRegistrar *OGRSFDriverRegistrar::GetRegistrar()
{
    static      OGRSFDriverRegistrar oSingleton;
    return &oSingleton;
}

/************************************************************************/
/*                           OGRCleanupAll()                            */
/************************************************************************/

/**
 * \brief Cleanup all OGR related resources. 
 *
 * FIXME
 */
void OGRCleanupAll()

{
    GDALDestroyDriverManager();
}


/************************************************************************/
/*                                Open()                                */
/************************************************************************/

OGRDataSource *OGRSFDriverRegistrar::Open( const char * pszName,
                                           int bUpdate,
                                           OGRSFDriver ** ppoDriver )

{
    GDALOpenInfo oOpenInfo( pszName, (bUpdate) ? GA_Update : GA_ReadOnly );
    GDALDataset* poDS = (GDALDataset*) GDALOpenInternal(oOpenInfo, NULL, FALSE, TRUE );
    if( poDS == NULL || poDS->GetDriver() == NULL ||
        poDS->GetDriver()->GetMetadataItem("OGR_DRIVER") == NULL )
    {
        delete poDS;
        return NULL;
    }
    if( ppoDriver != NULL )
        *ppoDriver = (OGRSFDriver*) poDS->GetDriver();
    return (OGRDataSource*) poDS;
}

/************************************************************************/
/*                              OGROpen()                               */
/************************************************************************/

OGRDataSourceH OGROpen( const char *pszName, int bUpdate,
                        OGRSFDriverH *pahDriverList )

{
    VALIDATE_POINTER1( pszName, "OGROpen", NULL );

    return (OGRDataSourceH) OGRSFDriverRegistrar::GetRegistrar()->Open( pszName, bUpdate, 
                               (OGRSFDriver **) pahDriverList );
}

/************************************************************************/
/*                             OpenShared()                             */
/************************************************************************/

OGRDataSource *
OGRSFDriverRegistrar::OpenShared( const char * pszName, int bUpdate,
                                  OGRSFDriver ** ppoDriver )

{
    GDALOpenInfo oOpenInfo( pszName, (bUpdate) ? GA_Update : GA_ReadOnly );
    GDALDataset* poDS = (GDALDataset*) GDALOpenSharedInternal(oOpenInfo, NULL, FALSE, TRUE );
    if( poDS == NULL || poDS->GetDriver() == NULL ||
        poDS->GetDriver()->GetMetadataItem("OGR_DRIVER") == NULL )
    {
        delete poDS;
        return NULL;
    }
    if( ppoDriver != NULL )
        *ppoDriver = (OGRSFDriver*) poDS->GetDriver();
    return (OGRDataSource*) poDS;
}

/************************************************************************/
/*                           OGROpenShared()                            */
/************************************************************************/

OGRDataSourceH OGROpenShared( const char *pszName, int bUpdate,
                              OGRSFDriverH *pahDriverList )

{
    VALIDATE_POINTER1( pszName, "OGROpenShared", NULL );

    return (OGRDataSourceH) OGRSFDriverRegistrar::GetRegistrar()->OpenShared( pszName, bUpdate, 
                               (OGRSFDriver **) pahDriverList );
}

/************************************************************************/
/*                         ReleaseDataSource()                          */
/************************************************************************/

OGRErr OGRSFDriverRegistrar::ReleaseDataSource( GDALDataset * poDS )

{
    GDALClose( (GDALDatasetH) poDS );
    return OGRERR_NONE;
}

/************************************************************************/
/*                        OGRReleaseDataSource()                        */
/************************************************************************/

OGRErr OGRReleaseDataSource( OGRDataSourceH hDS )

{
    VALIDATE_POINTER1( hDS, "OGRReleaseDataSource", OGRERR_INVALID_HANDLE );

    return OGRSFDriverRegistrar::GetRegistrar()->ReleaseDataSource((OGRDataSource *) hDS);
}

/************************************************************************/
/*                           GetOpenDSCount()                           */
/************************************************************************/

int OGRSFDriverRegistrar::GetOpenDSCount()
{
    CPLError(CE_Failure, CPLE_AppDefined, "Stub implementation in GDAL 2.0");
    return 0;
}


/************************************************************************/
/*                         OGRGetOpenDSCount()                          */
/************************************************************************/

int OGRGetOpenDSCount()

{
    return OGRSFDriverRegistrar::GetRegistrar()->GetOpenDSCount();
}

/************************************************************************/
/*                             GetOpenDS()                              */
/************************************************************************/

OGRDataSource *OGRSFDriverRegistrar::GetOpenDS( int iDS )

{
    CPLError(CE_Failure, CPLE_AppDefined, "Stub implementation in GDAL 2.0");
    return NULL;
}

/************************************************************************/
/*                            OGRGetOpenDS()                            */
/************************************************************************/

OGRDataSourceH OGRGetOpenDS( int iDS )

{
    return (OGRDataSourceH) OGRSFDriverRegistrar::GetRegistrar()->GetOpenDS( iDS );
}

/************************************************************************/
/*                          OpenWithDriverArg()                         */
/************************************************************************/

GDALDataset* OGRSFDriverRegistrar::OpenWithDriverArg(GDALDriver* poDriver,
                                                 GDALOpenInfo* poOpenInfo)
{
    OGRDataSource* poDS = (OGRDataSource*)
                ((OGRSFDriver*)poDriver)->Open(poOpenInfo->pszFilename,
                                        poOpenInfo->eAccess == GA_Update);
    if( poDS != NULL && poDS->GetOGRDriver() == NULL )
        poDS->SetOGRDriver( (OGRSFDriver*) poDriver );
    return poDS;
}
/************************************************************************/
/*                          CreateVectorOnly()                          */
/************************************************************************/

GDALDataset* OGRSFDriverRegistrar::CreateVectorOnly( GDALDriver* poDriver,
                                                     const char * pszName,
                                                     char ** papszOptions )
{
    OGRDataSource* poDS = (OGRDataSource*)
        ((OGRSFDriver*)poDriver)->CreateDataSource(pszName, papszOptions);
    if( poDS != NULL && poDS->GetOGRDriver() == NULL )
        poDS->SetOGRDriver( (OGRSFDriver*) poDriver );
    return poDS;
}

/************************************************************************/
/*                           RegisterDriver()                           */
/************************************************************************/

void OGRSFDriverRegistrar::RegisterDriver( OGRSFDriver * poDriver )

{
    GDALDriver* poGDALDriver = (GDALDriver*) GDALGetDriverByName( poDriver->GetName() ) ;
    if( poGDALDriver == NULL)
    {
        poDriver->SetDescription( poDriver->GetName() );
        poDriver->SetMetadataItem("OGR_DRIVER", "YES");

        poDriver->pfnOpenWithDriverArg = OpenWithDriverArg;
        poDriver->pfnCreateVectorOnly = CreateVectorOnly;

        GetGDALDriverManager()->RegisterDriver( poDriver );
    }
    else
    {
        if( poGDALDriver->GetMetadataItem("OGR_DRIVER") == NULL)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                    "A non OGR driver is registered with the same name: %s", poDriver->GetName());
        }
        delete poDriver;
    }
}

/************************************************************************/
/*                         OGRRegisterDriver()                          */
/************************************************************************/

void OGRRegisterDriver( OGRSFDriverH hDriver )

{
    VALIDATE_POINTER0( hDriver, "OGRRegisterDriver" );

    OGRSFDriverRegistrar::GetRegistrar()->RegisterDriver( 
        (OGRSFDriver *) hDriver );
}

/************************************************************************/
/*                          DeregisterDriver()                          */
/************************************************************************/

void OGRSFDriverRegistrar::DeregisterDriver( OGRSFDriver * poDriver )

{
    GetGDALDriverManager()->DeregisterDriver( poDriver );
}

/************************************************************************/
/*                        OGRDeregisterDriver()                         */
/************************************************************************/

void OGRDeregisterDriver( OGRSFDriverH hDriver )

{
    VALIDATE_POINTER0( hDriver, "OGRDeregisterDriver" );

    OGRSFDriverRegistrar::GetRegistrar()->DeregisterDriver( 
        (OGRSFDriver *) hDriver );
}

/************************************************************************/
/*                           GetDriverCount()                           */
/************************************************************************/

int OGRSFDriverRegistrar::GetDriverCount()

{
    /* We must be careful only to return drivers that are actual OGRSFDriver* */
    GDALDriverManager* poDriverManager = GetGDALDriverManager();
    int nTotal = poDriverManager->GetDriverCount();
    int nOGRDriverCount = 0;
    for(int i=0;i<nTotal;i++)
    {
        GDALDriver* poDriver = poDriverManager->GetDriver(i);
        if( poDriver->GetMetadataItem("OGR_DRIVER") != NULL )
            nOGRDriverCount ++;
    }
    return nOGRDriverCount;
}

/************************************************************************/
/*                         OGRGetDriverCount()                          */
/************************************************************************/

int OGRGetDriverCount()

{
    return OGRSFDriverRegistrar::GetRegistrar()->GetDriverCount();
}

/************************************************************************/
/*                             GetDriver()                              */
/************************************************************************/

OGRSFDriver *OGRSFDriverRegistrar::GetDriver( int iDriver )

{
    /* We must be careful only to return drivers that are actual OGRSFDriver* */
    GDALDriverManager* poDriverManager = GetGDALDriverManager();
    int nTotal = poDriverManager->GetDriverCount();
    int nOGRDriverCount = 0;
    for(int i=0;i<nTotal;i++)
    {
        GDALDriver* poDriver = poDriverManager->GetDriver(i);
        if( poDriver->GetMetadataItem("OGR_DRIVER") != NULL )
        {
            if( nOGRDriverCount == iDriver )
                return (OGRSFDriver*) poDriver;
            nOGRDriverCount ++;
        }
    }
    return NULL;
}

/************************************************************************/
/*                            OGRGetDriver()                            */
/************************************************************************/

OGRSFDriverH OGRGetDriver( int iDriver )

{
    return (OGRSFDriverH) OGRSFDriverRegistrar::GetRegistrar()->GetDriver( iDriver );
}

/************************************************************************/
/*                          GetDriverByName()                           */
/************************************************************************/

OGRSFDriver *OGRSFDriverRegistrar::GetDriverByName( const char * pszName )

{
    GDALDriverManager* poDriverManager = GetGDALDriverManager();
    GDALDriver* poGDALDriver =
        poDriverManager->GetDriverByName(CPLSPrintf("OGR_%s", pszName));
    if( poGDALDriver == NULL )
        poGDALDriver = poDriverManager->GetDriverByName(pszName);
    if( poGDALDriver == NULL ||
        poGDALDriver->GetMetadataItem("OGR_DRIVER") == NULL )
        return NULL;
    return (OGRSFDriver*) poGDALDriver;
}

/************************************************************************/
/*                         OGRGetDriverByName()                         */
/************************************************************************/

OGRSFDriverH OGRGetDriverByName( const char *pszName )

{
    VALIDATE_POINTER1( pszName, "OGRGetDriverByName", NULL );

    return (OGRSFDriverH) 
        OGRSFDriverRegistrar::GetRegistrar()->GetDriverByName( pszName );
}
