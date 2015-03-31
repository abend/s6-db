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

public class AdministrativeCancelException
    extends
        DatabaseException
{
    /**
     * 
     */
    private static final long serialVersionUID = -6960741843791340784L;

    public AdministrativeCancelException()
    {
        super( "The database system terminated your connection administratively" );
    }
}
