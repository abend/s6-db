/*
 ***************************************************************************
 *
 * Copyright (c) 2008 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;



public enum SqlRelationType
{
    EQUAL( "=" ),
    NOT_EQUAL( "!="),
    IS( "IS" ),
    LIKE( "LIKE" ),
    GREATER_THAN( ">" ),
    LESS_THAN( "<" ),
    GREATER_THAN_OR_EQUAL( ">=" ),
    LESS_THAN_OR_EQUAL( "<=" ),
    OVERLAPS( "&&" ),
    IN( "IN" ),
    NOT_IN( "NOT IN" );

    private final String    _type;

    SqlRelationType( final String    type )
    {
        _type = type;
    }


    @Override
    public String toString()
    {
        return _type;
    }
}
