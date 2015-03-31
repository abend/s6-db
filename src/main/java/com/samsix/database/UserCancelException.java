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


public class UserCancelException
    extends
        DatabaseException
{
    /**
     * 
     */
    private static final long serialVersionUID = 3785038478067694131L;

    public UserCancelException()
    {
        init( "Cancel requested" );
        setUserError( true );
    }
}
