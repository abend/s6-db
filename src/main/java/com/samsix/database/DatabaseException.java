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



import com.samsix.util.SamSixException;


/**
 *    Package-level exception.
 *    <p>
 *    This exists so we are able to have every exception in this
 *    package be a subclass of this exception so that we can
 *    distinquish between exceptions from this package and other
 *    packages, and so we can say "throws DatabaseException".
 *    <p>
 *    Used in throw clauses to make client code more amenable to
 *    change.
 */
public class DatabaseException
    extends
        SamSixException
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;


    public DatabaseException()
    {
        //    Do nothing.
    }


    public DatabaseException( final String       msg,
                              final Throwable    ex )
    {
        init( msg, ex );
    }


    public DatabaseException( final String    msg )
    {
        init( msg );
    }
}
