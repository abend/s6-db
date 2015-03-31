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



public final class RecordSetException
    extends
        DatabaseException
{
    /**
     * 
     */
    private static final long serialVersionUID = 4112097657981779624L;


    public RecordSetException()
    {
        //    Do nothing.
    }


    public DatabaseException cantMoveCursor( Throwable    ex )
    {
        init( "Can't move cursor to next row.", ex );

        return this;
    }


    public DatabaseException cantGetRowNum( Throwable    ex )
    {
        init( "Unable to get current row num.", ex );

        return this;
    }



    public DatabaseException cantReadColumnValue( String       columnName,
                                                  Throwable    ex )
    {
        init( "Can't retrieve data for column [" + columnName + "].", ex );
        return this;
    }



    public DatabaseException cantReadColumnValue( int          columnNumber,
                                                  Throwable    ex )
    {
        init( "Can't retrieve data for column number [" + columnNumber + "].",
              ex );
        return this;
    }



    public DatabaseException cantGetColumnInfo( Throwable    ex )
    {
        init( "Can't get column information.", ex );
        return this;
    }



    public DatabaseException cantGetVectorData( Throwable    ex )
    {
        init( "Can't get vector data.", ex );
        return this;
    }



    public DatabaseException cantParseStringIntoDate( String       value,
                                                      Throwable    ex )
    {
        init( "Trouble parsing string ["
              + value
              + "] into a date.", ex );

        return this;
    }
    
    
    public DatabaseException cantClose( Throwable    ex )
    {
        init( "Trouble closing.", ex );
        
        return this;
    }
}
