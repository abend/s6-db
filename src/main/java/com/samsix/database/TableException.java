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

import java.sql.Statement;



public final class TableException
    extends
        DatabaseException
{
    /**
     * 
     */
    private static final long serialVersionUID = 4632168783510664160L;


    public TableException()
    {
        //    Do nothing
    }

    public DatabaseException cantGetAggregate( final String       tableName,
                                               final String       columnName,
                                               final String       aggregate,
                                               final Throwable    ex )
    {
        init( "Unable to get the "
              + aggregate
              + " value for column ["
              + columnName
              + "] in table ["
              + tableName
              + "].",
              ex );

        return this;
    }


    public DatabaseException cantExecuteSql( final ConnectionInfo    info,
                                             final String            sql,
                                             final Throwable         ex )
    {
        init( "Unable to execute sql ["
              + sql
              + "] on db ["
              + info.getDbServerName()
              + ":"
              + info.getDatabase()
              + "]"
              , ex );
        return this;
    }


    public DatabaseException cantPrepareStatement( final String       sql,
                                                   final Throwable    ex )
    {
        init( "Can't prepare statement for sql [" + sql + "]", ex );

        return this;
    }


    public DatabaseException cantGetStatementResultSet( final Throwable    ex,
                                                        final Statement    statement )
    {
        init( "Unable to retrieve results from statement ["
              + statement
              + "]",
              ex );

        return this;
    }


    public DatabaseException cantExecuteStatement( final Throwable    ex,
                                                   final Statement    statement )
    {
        init( "Unable to retrieve execute statement ["
              + statement
              + "]",
              ex );

        return this;
    }


    public DatabaseException cantCreateStatement( final Throwable    ex )
    {
        init( "Unable to create statement.", ex );

        return this;
    }


    public DatabaseException cantCloseStatement( final Throwable    ex )
    {
        init( "Unable to close statement", ex );

        return this;
    }
}
