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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;


public enum SqlColumnType
{
    OTHER ( "USER-DEFINED" ),
    GEOMETRY( "geometry" ),

    BOOLEAN ( "boolean" ),

    TEXT ( "text" ),
    VARCHAR ( "character varying" ),
    CHAR ( "character" ),

    DATE( "date" ),
    TIMESTAMP ( "timestamp with time zone" ),

    SMALLINT ( "smallint" ),
    INT ( "integer" ),
    LONG ( "bigint" ),
    REAL ( "real" ),
    DOUBLE ( "double precision" ),
    NUMERIC ( "numeric" );


    //
    //    WARNING:    The values stored in here are the values found in
    //    the information_schema data_type column.
    //
    //    select distinct data_type from information_schema.columns order by data_type;
    //
    private    String    _dbValue;


    private SqlColumnType( final String    dbValue )
    {
        _dbValue = dbValue;
    }


    public static SqlColumnType fromString( final String    value )
    {
        for( SqlColumnType    type : values() )
        {
            if( type._dbValue.equals( value ) )
            {
                return type;
            }
        }

        return OTHER;
    }


    public boolean isText()
    {
        switch ( this )
        {
        case TEXT:
        case CHAR:
        case VARCHAR:
            return true;
        default:
            return false;
        }
    }


    public boolean isNumeric()
    {
        switch ( this )
        {
        case DOUBLE:
        case INT:
        case LONG:
        case NUMERIC:
        case REAL:
        case SMALLINT:
            return true;
        default:
            return false;
        }
    }


    public boolean isDate()
    {
        return ( this == DATE || this == TIMESTAMP );
    }


    public String getDbValue()
    {
        return _dbValue;
    }


    public Date coerceDate( final Date    date )
    {
        if( date == null )
        {
            return null;
        }

        switch( this )
        {
        case DATE:
            return ( date instanceof java.sql.Date ) ? date : new java.sql.Date( date.getTime() );

        case TIMESTAMP:
            return ( date instanceof java.sql.Timestamp ) ? date : new java.sql.Timestamp( date.getTime() );

        default:
            throw new IllegalArgumentException( "Can't coerce a date to " + this );
        }
    }


    /**
     * Note: This may not do what you were hoping! Date/timestamp must be in the right format for a java.sql.(Date|Timestamp),
     * which is quite restrictive.
     * @param value
     * @return
     */
    public Date coerceDate( final String    value )
    {
        if( StringUtils.isBlank( value ) )
        {
            return null;
        }

        switch( this )
        {
        case DATE:
            return java.sql.Date.valueOf( value );

        case TIMESTAMP:
            return java.sql.Timestamp.valueOf( value );

        default:
            throw new IllegalArgumentException( "Can't coerce a date to " + this );
        }
    }


    /**
     * Coerces a number or string type into the proper Number type
     *
     * @param value
     * @return
     */
    public Number coerceNumber( final Number    value )
    {
        if( value == null )
        {
            return null;
        }

        switch( this )
        {
        case SMALLINT:
            return ( value instanceof Short ) ? value : value.shortValue();
        case INT:
            return ( value instanceof Integer ) ? value : value.intValue();
        case LONG:
            return ( value instanceof Long ) ? value : value.longValue();
        case REAL:
            return ( value instanceof Float ) ? value : value.floatValue();
        case DOUBLE:
            return ( value instanceof Double ) ? value : value.doubleValue();
        case NUMERIC:
            if( value instanceof BigDecimal || value instanceof BigInteger )
            {
                return value;
            }

            if( value instanceof Integer || value instanceof Long )
            {
                return BigInteger.valueOf( value.longValue() );
            }

            if( value instanceof Double || value instanceof Float )
            {
                return BigDecimal.valueOf( value.doubleValue() );
            }
            break;
        }

        throw new IllegalArgumentException( "Can't convert " + value.getClass().getName() + " to " + this );
    }

    public Number coerceNumber( final String    value )
    {
        if( value == null )
        {
            return null;
        }

        switch( this )
        {
        case SMALLINT:
            return Short.valueOf( value );
        case INT:
            return Integer.valueOf( value );
        case LONG:
            return Long.valueOf( value );
        case REAL:
            return Float.valueOf( value );
        case DOUBLE:
            return Double.valueOf( value );
        case NUMERIC:
            return ( value.indexOf( '.' ) < 0 ) ? new BigInteger( value ) : new BigDecimal( value );
        }

        throw new IllegalArgumentException( "Can't numerically coerce String to " + this );
    }
}
