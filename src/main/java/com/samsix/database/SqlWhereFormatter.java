/**
 ***************************************************************************
 *
 * Copyright (c) 2001-2012 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;


import org.apache.commons.lang3.builder.ToStringBuilder;

import com.samsix.util.string.StringUtilities;


public class SqlWhereFormatter
    extends
        AbstractSqlFormatter
{
    private final StringBuffer    _buffer;
    private boolean         _firstAnd    = true;


    public SqlWhereFormatter()
    {
        _buffer = new StringBuffer();
    }



    @Override
    public SqlFormatter append( final String    column,
                                final String    value,
                                final boolean   wrapQuotes )
    {
        String      newValue    = value;
        String      operation   = " = ";

        if ( value == null )
        {
            operation   = " IS ";
            newValue    = "NULL";
        }
        else if ( wrapQuotes )
        {
            newValue = StringUtilities.wrapQuotes( newValue );
        }

        _buffer.append( getAnd() );
        _buffer.append( column );
        _buffer.append( operation );
        _buffer.append( newValue );

        return this;
    }



    @Override
    public SqlFormatter append( final String    column,
                                final long      value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Long      value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, value.toString(), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final int       value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String     column,
                                final Integer    value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, value.toString(), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final double    value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Double    value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, value.toString(), false );
    }



    @Override
    public SqlFormatter append( final String     column,
                                final boolean    value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String     column,
                                final Boolean    value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, value.toString(), false );
    }



    private SqlFormatter append( final String    whereClause )
    {
        _buffer.append( getAnd() );
        _buffer.append( whereClause );

        return this;
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId )
    {
        return append( objectId.getWhereClause() );
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix )
    {
        return append( objectId.getWhereClause( prefix ) );
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix,
                                final String      suffix )
    {
        return append( objectId.getWhereClause( prefix, suffix ) );
    }



    public String getWhereClause()
    {
        return _buffer.toString();
    }



    private String getAnd()
    {
        if ( _firstAnd )
        {
            _firstAnd = false;

            return "\n\t     ";
        }

        return "\n\t AND ";
    }


    @Override
    public String   toString()
    {
        return new ToStringBuilder( this )
            .append( "where", _buffer )
            .toString();
    }
}
