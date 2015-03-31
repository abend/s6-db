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
public final class ConnectionException
    extends
        DatabaseException
{
    public ConnectionException()
    {
        //    Do nothing.
    }


    public DatabaseException cantGetConnection( ConnectionInfo    info )
    {
        init( "Unable to obtain database connection for info [" + info + "[." );

        return this;
    }
}
