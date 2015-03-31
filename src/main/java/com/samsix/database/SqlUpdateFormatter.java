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


import java.util.Map;
import java.util.Map.Entry;

import com.samsix.util.string.StringUtilities;



public final class SqlUpdateFormatter
    extends
        AbstractSqlFormatter
{
    private final StringBuffer    _buffer;


    public SqlUpdateFormatter()
    {
        _buffer = new StringBuffer();
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

        StringUtilities.appendToBuffer( _buffer,
                                        column
                                        + " = "
                                        + newValue,
                                        StringUtilities.COMMA_SPACE_DELIMITER
                                        + "\n\t" );

        return this;
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId )
    {
        return append( objectId, null );
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix )
    {
        return append( objectId, prefix, null );
    }



    @Override
    public SqlFormatter append( final DBRepresentable    objectId,
                                final String      prefix,
                                final String      suffix )
    {
        //
        // k-n May 23, 2005:
        //    OK, this is REALLY bad but I don't have time to make it right
        //    or even test if this code works right now.  It should work
        //    but its still really bad.  I just want the i/f
        //    to have this method so that I don't have to distinguish btwn
        //    the type of formatter I am using.  I know the version of this
        //    method in SqlInsertFormatter works just not this one.
        //
        String    whereClause = objectId.getWhereClause( prefix, suffix );

        //
        //    Now the bad part.  Search for the string " AND " and replace
        //    it with ", ".  Really should add a getUpdateClause to ObjectId
        //    but that's pretty bad too.
        //
        whereClause = StringUtilities.replace( whereClause,
                                               " AND ",
                                               StringUtilities.COMMA_SPACE_DELIMITER );
        StringUtilities.appendToBuffer( _buffer,
                                        whereClause,
                                        StringUtilities.COMMA_SPACE_DELIMITER
                                        + "\n\t" );

        return this;
    }



    public int append( final Map<?,?>    oldValues,
                       final Map<?,?>    newValues )
    {
        Object    newValue;
        Object    oldValue;
        int       nChangedCols = 0;

        for ( Entry<?,?> colEntry : newValues.entrySet() )
        {
            newValue = newValues.get( colEntry.getKey() );
            oldValue = oldValues.get( colEntry.getKey() );

            if ( newValue == null && oldValue != null )
            {
                append( colEntry.getKey().toString(), "NULL", false );
                nChangedCols++;
            }
            else if ( newValue != null && ! newValue.equals( oldValue ) )
            {
                append( colEntry.getKey().toString(), newValue );
                nChangedCols++;
            }
        }

        return nChangedCols;
    }



    public String getUpdateClause()
    {
        return _buffer.toString();
    }
}
