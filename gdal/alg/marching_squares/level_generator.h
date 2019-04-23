/******************************************************************************
 *
 * Project:  Marching square algorithm
 * Purpose:  Core algorithm implementation for contour line generation.
 * Author:   Oslandia <infos at oslandia dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Oslandia <infos at oslandia dot com>
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
#ifndef MARCHING_SQUARE_LEVEL_GENERATOR_H
#define MARCHING_SQUARE_LEVEL_GENERATOR_H

#include <vector>
#include <limits>
#include <cmath>
#include <cstdlib>
#include "utility.h"

#include "cpl_port.h"

namespace marching_squares {

struct ILevelGenerator
{
    virtual ~ILevelGenerator() {}
    virtual double level( int idx ) const = 0;
};

class LevelIterator
{
public:
    LevelIterator( const ILevelGenerator& parent, int idx ) : parent_( parent ), idx_( idx ) {}
    // Warning: this is a "pseudo" iterator, since operator* returns a value, not
    // a reference. This means we cannot have operator->
    std::pair<int, double> operator*() const
    {
        return std::make_pair( idx_, parent_.level( idx_ ) );
    }
    bool operator!=( const LevelIterator& other ) const
    {
        return idx_ != other.idx_;
    }
    const LevelIterator& operator++()
    {
        idx_++;
        return *this;
    }
private:
    const ILevelGenerator& parent_;
    int idx_;
};

class Range
{
public:
    Range( LevelIterator b, LevelIterator e ) : begin_( b ), end_( e ) {}
    LevelIterator begin() const { return begin_; }
    LevelIterator end() const { return end_; }
private:
    LevelIterator begin_;
    LevelIterator end_;
};

struct ILevelRangeGenerator: public ILevelGenerator
{
    virtual Range range( double min, double max ) const = 0;
};

struct FixedLevelRangeIterator: public ILevelRangeGenerator
{
public:
    FixedLevelRangeIterator( const double* levels, size_t count, double maxLevel = Inf ) : levels_( levels ), count_( count ), maxLevel_( maxLevel )
    {
    }

    Range range( double min, double max ) const override
    {
        if ( min > max )
            std::swap( min, max );
        size_t b = 0;
        for (; b != count_ && levels_[b] < fudge(levels_[b], min); b++);
        if ( min == max )
            return Range( LevelIterator( *this, int(b) ), LevelIterator( *this, int(b) ) );
        size_t e = b;
        for (; e != count_ && levels_[e] <= fudge(levels_[e], max); e++);
        return Range( LevelIterator( *this, int(b) ), LevelIterator( *this, int(e) ) );
    }

    double level( int idx ) const override
    {
        if ( idx >= int(count_) )
            return maxLevel_;
        return levels_[size_t(idx)];
    }

private:
    const double* levels_;
    size_t count_;
    double maxLevel_;
    CPL_DISALLOW_COPY_ASSIGN(FixedLevelRangeIterator)
};

struct IntervalLevelRangeIterator: public ILevelRangeGenerator
{
    // Construction by a offset and an interval
    IntervalLevelRangeIterator( double offset, double interval ): offset_( offset ), interval_( interval ) {}

    Range range( double min, double max ) const override
    {
        if ( min > max )
            std::swap(min, max);

        // compute the min index, adjusted to the fudged value if needed
        int i1 = static_cast<int>(ceil((min - offset_) / interval_));
        double l1 = fudge( level( i1 ), min );
        if ( l1 > min )
            i1 = static_cast<int>(ceil((l1 - offset_) / interval_));
        LevelIterator b( *this, i1 );

        if ( min == max )
            return Range( b, b );

        // compute the max index, adjusted to the fudged value if needed
        int i2 = static_cast<int>(floor((max - offset_) / interval_)+1);
        double l2 = fudge( level( i2 ), max );
        if ( l2 > max )
            i2 = static_cast<int>(floor((l2 - offset_) / interval_)+1);
        LevelIterator e( *this, i2 );
        
        return Range( b, e );
    }

    double level( int idx ) const override
    {
        return idx * interval_ + offset_;
    }

private:
    const double offset_;
    const double interval_;
};

class ExponentialLevelRangeIterator: public ILevelRangeGenerator
{
public:
    ExponentialLevelRangeIterator( double base ) : base_( base ), base_ln_( std::log( base_ ) ) {}

    double level( int idx ) const override
    {
        if ( idx <= 0 )
            return 0.0;
        return std::pow( base_, idx - 1);
    }

    Range range( double min, double max ) const override
    {
        if ( min > max )
            std::swap(min, max);

        int i1 = index1( min );
        double l1 = fudge( level( i1 ), min );
        if ( l1 > min )
            i1 = index1(l1 );
        LevelIterator b( *this, i1 );

        if ( min == max )
            return Range( b, b );

        int i2 = index2( max );
        double l2 = fudge( level( i2 ), max );
        if ( l2 > max )
            i2 = index2( l2 );
        LevelIterator e( *this, i2 );

        return Range( b, e );
    }

private:
    int index1( double plevel ) const
    {
        if ( plevel < 1.0 )
            return 1;
        return static_cast<int>(ceil(std::log( plevel ) / base_ln_))+1;
    }
    int index2( double plevel ) const
    {
        if ( plevel < 1.0 )
            return 0;
        return static_cast<int>(floor(std::log( plevel ) / base_ln_)+1)+1;
    }

    // exponentation base
    const double base_;
    const double base_ln_;
};


}
#endif
