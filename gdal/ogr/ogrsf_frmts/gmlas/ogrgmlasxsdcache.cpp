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
/*                               Open()                                 */
/************************************************************************/

VSILFILE* GMLASResourceCache::Open( const CPLString& osResource,
                                    const CPLString& osBasePath,
                                    CPLString& osOutFilename )
{
    osOutFilename = osResource;
    if( osResource.find("http://") == 0 ||
        osResource.find("https://") == 0 )
    {
        osOutFilename = "/vsicurl_streaming/" + osResource;
    }
    else if( CPLIsFilenameRelative( osResource ) && !osResource.empty() )
    {
        /* Transform a/b + ../c --> a/c */
        CPLString osResourceModified(osResource);
        CPLString osBasePathModified(osBasePath);
        while( (osResourceModified.find("../") == 0 ||
                osResourceModified.find("..\\") == 0) &&
               !osBasePathModified.empty() )
        {
            osBasePathModified = CPLGetDirname(osBasePathModified);
            osResourceModified = osResourceModified.substr(3);
        }

        osOutFilename = CPLFormFilename(osBasePathModified,
                                        osResourceModified, NULL);
    }

    CPLDebug("GMLAS", "Resolving %s (%s) to %s",
                osResource.c_str(),
                osBasePath.c_str(),
                osOutFilename.c_str());

    VSILFILE* fp = NULL;
    if( osOutFilename.find("/vsicurl_streaming/") == 0 )
    {
        CPLString osLaunderedName(osOutFilename);
        if( osOutFilename.find("/vsicurl_streaming/http://") == 0 )
            osLaunderedName = osLaunderedName.substr(
                                    strlen("/vsicurl_streaming/http://") );
        else if( osOutFilename.find("/vsicurl_streaming/https://") == 0 )
            osLaunderedName = osLaunderedName.substr(
                                    strlen("/vsicurl_streaming/https://") );
        for(size_t i=0; i<osLaunderedName.size(); i++)
        {
            if( !isalnum(osLaunderedName[i]) && osLaunderedName[i] != '.' )
                osLaunderedName[i] = '_';
        }

        CPLString osCachedFileName("cache_xsd/" + osLaunderedName);
        fp = VSIFOpenL( osCachedFileName, "rb");
        if( fp != NULL )
        {
            CPLDebug("GMLAS", "Use cached %s", osCachedFileName.c_str());
        }
        else
        {
            VSIStatBufL sStat;
            if( VSIStatL("cache_xsd", &sStat) == 0 )
            {
                CPLString osTmpfilename( osCachedFileName + ".tmp" );
                CPLCopyFile( osTmpfilename, osOutFilename);
                VSIRename( osTmpfilename, osCachedFileName );
                fp = VSIFOpenL(osCachedFileName, "rb");
            }
            else
            {
                fp = VSIFOpenL(osOutFilename, "rb");
            }
        }
    }
    else
    {
        fp = VSIFOpenL(osOutFilename, "rb");
    }

    if( fp == NULL )
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Cannot resolve %s", osResource.c_str());
    }

    return fp;
}
