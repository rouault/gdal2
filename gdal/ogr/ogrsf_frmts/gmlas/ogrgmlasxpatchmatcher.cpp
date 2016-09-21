/******************************************************************************
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
/*                          GMLASXPathMatcher()                         */
/************************************************************************/

GMLASXPathMatcher::GMLASXPathMatcher()
{
}

/************************************************************************/
/*                         ~GMLASXPathMatcher()                         */
/************************************************************************/

GMLASXPathMatcher::~GMLASXPathMatcher()
{
}

/************************************************************************/
/*                           SetRefXPaths()                             */
/************************************************************************/

void    GMLASXPathMatcher::SetRefXPaths(const std::map<CPLString, CPLString>&
                                                oMapPrefixToURIReferenceXPaths,
                                         const std::vector<CPLString>& 
                                                aosReferenceXPaths)
{
    m_oMapPrefixToURIReferenceXPaths = oMapPrefixToURIReferenceXPaths;
    m_aosReferenceXPaths = aosReferenceXPaths;
}

/************************************************************************/
/*                         MatchesRefXPath()                            */
/************************************************************************/

bool GMLASXPathMatcher::MatchesRefXPath(
                        const CPLString& osXPath,
                        const CPLString& osRefXPath,
                        const std::map<CPLString,CPLString>& oMapURIToPrefix)
                                                                          const
{
    size_t iRefPos = 0;
    size_t iPos = 0;
    bool bAbsoluteRef = false;
    if( osRefXPath.size() >= 2 &&
        osRefXPath[0] == '/' && osRefXPath[1] == '/' )
    {
        iRefPos += 2;
    }
    else if( osRefXPath.size() >= 1 && osRefXPath[0] == '/' )
    {
        iRefPos += 1;
        bAbsoluteRef = true;
    }

    while( iPos < osXPath.size() && iRefPos < osRefXPath.size() )
    {
        size_t iPosNextSlash = osXPath.find('/', iPos);
        size_t iRefPosNextSlash = osRefXPath.find('/', iRefPos);

        if( iRefPos == iRefPosNextSlash )
        {
            bAbsoluteRef = false;
            iRefPos ++;
            continue;
        }

        CPLString osCurNode;
        if( iPosNextSlash == std::string::npos )
            osCurNode = osXPath.substr(iPos);
        else
            osCurNode = osXPath.substr(iPos, iPosNextSlash - iPos);
        CPLString osRefCurNode;
        if( iRefPosNextSlash == std::string::npos )
            osRefCurNode = osRefXPath.substr(iRefPos);
        else
            osRefCurNode = osRefXPath.substr(iRefPos,
                                    iRefPosNextSlash - iRefPos);

        // Translate the configuration prefix to the equivalent in
        // this current schema
        size_t iRefPosColumn = osRefCurNode.find(':');
        if( iRefPosColumn != std::string::npos )
        {
            bool bIsAttr = ( osRefCurNode[0] == '@' );
            CPLString osPrefix(
                        osRefCurNode.substr(bIsAttr ? 1 : 0,
                                    iRefPosColumn - (bIsAttr ? 1 : 0)) );
            CPLString osLocalname(
                        osRefCurNode.substr(iRefPosColumn+1) );

            std::map<CPLString, CPLString>::const_iterator oIter =
                m_oMapPrefixToURIReferenceXPaths.find(osPrefix);
            if( oIter != m_oMapPrefixToURIReferenceXPaths.end() )
            {
                CPLString osURI( oIter->second );
                oIter = oMapURIToPrefix.find( osURI );
                if( oIter == oMapURIToPrefix.end() )
                    return false;
                osPrefix = oIter->second;
            }
            osRefCurNode = ((bIsAttr) ? "@": "") +
                                 osPrefix + ":" + osLocalname;
        }

        if( osCurNode != osRefCurNode )
        {
            if( bAbsoluteRef )
                return false;

            if( iPosNextSlash == std::string::npos )
                return false;
            iPos = iPosNextSlash + 1;
            continue;
        }

        if( iPosNextSlash == std::string::npos )
            iPos = osXPath.size();
        else
            iPos = iPosNextSlash + 1;

        if( iRefPosNextSlash == std::string::npos )
            iRefPos = osRefXPath.size();
        else
            iRefPos = iRefPosNextSlash + 1;

        bAbsoluteRef = true;
    }

    return (!bAbsoluteRef || iPos == osXPath.size()) &&
            iRefPos == osRefXPath.size();
}

/************************************************************************/
/*                         MatchesRefXPath()                            */
/************************************************************************/

bool GMLASXPathMatcher::MatchesRefXPath(
                            const CPLString& osXPath,
                            const std::map<CPLString,CPLString>& oMapURIToPrefix,
                            CPLString& osOutMatchedXPath) const
{
    for(size_t i = 0; i < m_aosReferenceXPaths.size(); ++i )
    {
        const CPLString& osRefXPath( m_aosReferenceXPaths[i] );
        if( MatchesRefXPath(osXPath, osRefXPath, oMapURIToPrefix) )
        {
            osOutMatchedXPath = osRefXPath;
            return true;
        }
    }
    return false;
}
