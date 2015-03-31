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


/**
 *    Class of table utilities.
 *    <p>
 *    Methods to manipulate rows of data in tables.
 */
public interface RecordSetHandler
{
    public void initRecordSetHandling();


    public void handleRow( final RecordSet    recordSet )
        throws
            DatabaseException;


    public void finishRecordSetHandling( int    rowCount );
}
