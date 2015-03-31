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


@SuppressWarnings("serial")
public final class ConnectionPoolException
    extends
        DatabaseException
{
    public ConnectionPoolException()
    {
        //    Do nothing.
    }


    public DatabaseException
    cantCreateConnectionPool( Throwable          ex )
    {
        init( "Unable to create connection pool.", ex );

        return this;
    }



    public DatabaseException
    cantGetConnectionPack( Throwable    ex )
    {
        init( "Can't get connection pack.", ex );
        return this;
    }
}
