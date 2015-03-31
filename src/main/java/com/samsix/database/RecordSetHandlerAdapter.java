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

public abstract class RecordSetHandlerAdapter
    implements
        RecordSetHandler
{
    @Override
    public void initRecordSetHandling()
    {
        // do nothing
    }


    @Override
    public void finishRecordSetHandling( final int rowCount )
    {
        // do nothing
    }
}
