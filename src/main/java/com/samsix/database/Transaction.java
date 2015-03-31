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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.log4j.Logger;

/**
 *    Helper class for doing long, drawn out transactions.
 *    Handles the logic of maintaining a database connection,
 *    removes likelihood for error.
 */

public class Transaction
{
    private static Logger    logger = Logger.getLogger( Transaction.class );

    protected final ConnectionInfo    _connectionInfo;
    private Database                  _db;

    private Queue<CommitTask>         _commitTasks;
    private Map<String,Object>        _properties;


    public Transaction( final ConnectionInfo    connectionInfo )
    {
        _connectionInfo = connectionInfo;
    }


    public Database begin()
        throws
            DatabaseException
    {
        if( _db == null )
        {
            _db = new Database( _connectionInfo );
        }

        _db.beginTransaction();

        return _db;
    }


    public Database getDatabase()
        throws
            DatabaseException
    {
        if( _db == null )
        {
            throw new DatabaseException( "Cannot continue nonexistent transaction" );
        }

        return _db;
    }


    public int updateRow( final String    schema,
                          final String    tableName,
                          final String    updateClause,
                          final String    whereClause )
        throws
            DatabaseException
    {
        return getDatabase().getTable( schema, tableName ).updateRow( updateClause, whereClause );
    }


    public void rollback()
    {
        if( _db == null )
        {
            return;
        }

        try
        {
            _db.rollbackTransaction();
        }
        catch ( DatabaseException    ex )
        {
            logger.error( "Can't rollback transaction.", ex );
        }
        finally
        {
            //
            //    Rollback any changes in code
            //
            if( _commitTasks != null )
            {
                CommitTask    task;
                while( ( task = _commitTasks.poll() ) != null )
                {
                    task.rollback();
                }
            }

            _db.release();
            _db = null;
        }
    }


    public void commit()
        throws
            DatabaseException
    {
        if( _db == null )
        {
            throw new DatabaseException( "Cannot commit nonexistent transaction" );
        }

        _db.commitTransaction();

        //
        //    If we're not in a transaction, we actually just committed
        //    That means our DB has been released for us
        //
        if ( ! _db.inTransaction() )
        {
            _db.release();
            _db = null;

            if( _commitTasks != null )
            {
                CommitTask    task;
                while( ( task = _commitTasks.poll() ) != null )
                {
                    task.commit();
                }
            }
        }
    }
    
    
    /**
    * If the transaction has been committed or rolled back, does nothing.
    * If it hasn't, the transaction is rolled back.
    */
    public void release()
    {
        if( _db == null )
        {
            return;
        }

        rollback();
    }


    public void addCommitTask( final CommitTask    task )
    {
        //
        //    Lazily create commit task queue that orders by priority
        //
        if( _commitTasks == null )
        {
            synchronized( this )
            {
                if( _commitTasks == null )
                {
                    Comparator<CommitTask>    comparator;
                    comparator = new Comparator<CommitTask>()
                    {
                        @Override
                        public int compare( final CommitTask    o1,
                                            final CommitTask    o2 )
                        {
                            return o1.getTaskPriority() - o2.getTaskPriority();
                        }
                    };

                    _commitTasks = new PriorityQueue<CommitTask>( 4, comparator );
                }
            }
        }

        _commitTasks.add( task );
    }


    public void putProperty( final String    name,
                             final Object    value )
    {
        if( _properties == null )
        {
            synchronized( this )
            {
                if( _properties == null )
                {
                    _properties = Collections.synchronizedMap( new HashMap<String,Object>() );
                }
            }
        }

        _properties.put( name, value );
    }


    public Object getProperty( final String    name )
    {
        return _properties == null ? null : _properties.get( name );
    }


    public boolean hasProperty( final String    name )
    {
        return _properties == null ? false : _properties.containsKey( name );
    }


    public Map<String,?> getProperties()
    {
        return _properties;
    }


    //////////////////////////////////////////////
    //
    //    CommitTask Interface
    //
    //////////////////////////////////////////////

    /**
     *    A task to complete upon a successful commit of the entire transaction.
     *    Tasks are executed in order of their priority.
     */
    public static interface CommitTask
    {
        public final static int DEFAULT_TASK_PRIORITY = 100;

        /**
         *    Commit any changes to memory after a successful commit
         *    to the database. Not allowed to throw any exceptions.
         *    Not guaranteed to be called in the Swing thread.
         */
        public void commit();


        public void rollback();


        /**
         *    The priority that determines what order this commit task
         *    is executed in. Lower numbers are executed first.
         */
        public int getTaskPriority();
    }


    /**
     *    A basic commit task, meant to be used as an adapter class
     */
    public static class CommitTaskAdapter
        implements
            CommitTask
    {
        @Override
        public void commit()
        {
            //    Do nothing
        }


        @Override
        public void rollback()
        {
            //    Do nothing
        }


        @Override
        public int getTaskPriority()
        {
            return DEFAULT_TASK_PRIORITY;
        }
    }
}
