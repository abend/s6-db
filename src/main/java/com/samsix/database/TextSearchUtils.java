/*
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
import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.samsix.util.string.StringUtilities;


public class TextSearchUtils
{
    private static final Logger logger = Logger.getLogger( TextSearchUtils.class );


    private static final Map<ConnectionInfo,Boolean>    _hasFullSearchMap =
        new WeakHashMap<ConnectionInfo,Boolean>();


    private static boolean getHasFullSearch( final ConnectionInfo    connectionInfo )
    {
        Boolean    hasFullSearch = _hasFullSearchMap.get( connectionInfo );
        if ( hasFullSearch != null )
        {
            return hasFullSearch.booleanValue();
        }

        //
        //    This is a separate module and thus might not be installed so we have to check.
        //
        String    sql;
        sql = "select exists (select * from information_schema.routines where " +
              "routine_name like 'to_tsvector') as hasfullsearch";

        Database db = new Database( connectionInfo );

        try {
            RecordSet rs = db.getRecordSet( sql );
            if(rs.next()) {
                hasFullSearch = Boolean.valueOf( rs.getBoolean( "hasfullsearch", true ) );
            }
            else {
                hasFullSearch = false;
            }
            rs.close();
        }
        catch ( DatabaseException ex ) {
            //
            //    I dunno, default to true?
            //
            hasFullSearch = true;
            logger.error( "Failed to determine full text search capabilities", ex );
        }
        finally {
            db.release();
        }

        _hasFullSearchMap.put( connectionInfo, hasFullSearch );

        return hasFullSearch.booleanValue();
    }


    public static String getTextSearchClause( final ConnectionInfo    connectionInfo,
                                              final String            columnName,
                                              final String            value )
    {
        if ( getHasFullSearch( connectionInfo ) )
        {
            return "to_tsvector('english', " + columnName + ") @@ plainto_tsquery('english', "
                   + StringUtilities.wrapQuotes( value ) + ")";
        }
        else
        {
            //
            //    Do really bad slow query instead
            //
            return "lower( " + columnName + " ) like lower( "
                         + StringUtilities.wrapQuotes( "%"
                         + value + "%" ) + " )";
        }
    }
}
