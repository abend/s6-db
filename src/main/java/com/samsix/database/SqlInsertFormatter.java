/*
 ***************************************************************************
 *
 * Copyright (c) 2001-2008 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;


import com.samsix.util.string.StringUtilities;


public final class SqlInsertFormatter
    extends
        AbstractSqlFormatter
{
    private final StringBuffer    _columnBuffer;
    private final StringBuffer    _valueBuffer;


    public SqlInsertFormatter()
    {
        _columnBuffer   = new StringBuffer();
        _valueBuffer    = new StringBuffer();
    }


    @Override
    public SqlFormatter append( final String    column,
                                final String    value,
                                final boolean   wrapQuotes )
    {
        String    newValue = value;

        if ( value == null )
        {
            newValue = "NULL";
        }
        else if ( wrapQuotes )
        {
            newValue = StringUtilities.wrapQuotes( newValue );
        }

        StringUtilities.appendToBuffer( _columnBuffer,
                                        column,
                                        StringUtilities.COMMA_SPACE_DELIMITER
                                        + "\n\t" );

        StringUtilities.appendToBuffer( _valueBuffer,
                                        newValue,
                                        StringUtilities.COMMA_SPACE_DELIMITER
                                        + "\n\t" );

        return this;
    }


    @Override
    public SqlFormatter append( final DBRepresentable    objectId )
    {
        return append( objectId.getColumnClause(),
                       objectId.getValuesClause(),
                       false );
    }


    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix )
    {
        return append( objectId.getColumnClause( prefix ),
                       objectId.getValuesClause(),
                       false );
    }


    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix,
                                final String      suffix )
    {
        return append( objectId.getColumnClause( prefix, suffix ),
                       objectId.getValuesClause(),
                       false );
    }


    public String getColumnClause()
    {
        return _columnBuffer.toString();
    }


    public String getValueClause()
    {
        return _valueBuffer.toString();
    }
}
