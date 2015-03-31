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



public final class TransactionException
    extends
        DatabaseException
{
    /**
     * 
     */
    private static final long serialVersionUID = 8764712223812743543L;


    public TransactionException()
    {
        //    Do nothing.
    }


    public DatabaseException cantBegin( ConnectionInfo    info,
                                        Throwable         ex )
    {
        init( "Unable to start transaction on the database of type [" 
              + info 
              + "].",
              ex );

        return this;
    }



    public DatabaseException cantCommit( ConnectionInfo    info,
                                         Throwable         ex )
    {
        init( "Unable to commit transaction on database of type ["
              + info
              + "].",
              ex );

        return this;
    }



    public DatabaseException cantRollback( ConnectionInfo    info,
                                           Throwable         ex )
    {
        init( "Unable to rollback transaction on database of type ["
              + info
              + "].",
              ex );

        return this;
    }
    
    
    public DatabaseException transactionFailure( Throwable    ex )
    {
        if( ex instanceof DatabaseException )
        {
            return (DatabaseException) ex;
        }
        
        init( "Failure while performing transaction.", ex );
        
        return this;
    }
}
