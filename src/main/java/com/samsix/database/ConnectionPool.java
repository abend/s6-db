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


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;


/**
 *    Creates new connections on demand, up to a max number if specified.
 *    <p>
 *    It also makes sure a connection is still open before it is
 *    returned to a client.
 */
public class ConnectionPool
{
    protected final static Logger logger = Logger.getLogger( ConnectionPool.class );

    private final static int    ONE_SECOND   = 1000;            // in milliseconds
    private final static int    ONE_MINUTE   = 60 * ONE_SECOND; // in seconds

    //
    //    Locking mechanism for notifying of new pack availability
    //
    private final ReentrantLock    _lock = new ReentrantLock();
    private final Condition        _availablePack = _lock.newCondition();

    //
    //    The number of connections currently checked out.
    //
    private final LinkedList<ConnectionPack>      _freeConnections;
    private final List<ConnectionPack>            _leasedConnections;

    /**
     *    Timer to decide whether Connections should be released
     *    from the _pool?
     */
    private final Timer             _reapTimer;

    private final ConnectionInfo    _connectionInfo;


    public ConnectionPool( final ConnectionInfo    connectionInfo )
    {
        _connectionInfo = connectionInfo;

        _freeConnections   = new LinkedList<ConnectionPack>();
        _leasedConnections = new ArrayList<ConnectionPack>();

        //
        //    Set up the reaper task.
        //
        int    delay = 30 * ONE_SECOND;

        _reapTimer = new Timer( "DatabaseConnectionReaper", true );
        _reapTimer.schedule( new ConnectionReaper( this ),
                             delay,
                             delay );
    }


    /**
     *    Checks in a connection to the _pool.
     *    <p>
     *    Notify other Threads that may be waiting for a connection.
     */
    public void releaseConnectionPack( final ConnectionPack    pack )
    {
        if ( logger.isInfoEnabled() )
        {
            logger.info( "\n\treleasing connection:"
                         + "\n\t- connection: [" + pack + "]" );
        }

        _lock.lock();

        try
        {
            //
            //    Put the connection at the end of the List
            //
            _freeConnections.addLast( pack );
            _leasedConnections.remove( pack );
            pack.expireLease();

            if ( logger.isInfoEnabled() )
            {
                logger.info( "\n\t- num free connections: ["
                             + _freeConnections.size()
                             + "]"
                             + "\n\t- free connections: [" + _freeConnections + "]"
                             + "\n\t- num checked out: ["
                                 + _leasedConnections.size() + "]"
                             + "\n\t- checkedOut: ["
                                 + _leasedConnections + "]" );
            }

            //
            //    Because we are only releasing one connection pack here,
            //    notify only one thread.
            //
            _availablePack.signal();
        }
        finally
        {
            _lock.unlock();
        }
    }


    public Collection<String> getActiveSql()
    {
        Collection<String>    sql = new HashSet<String>();

        _lock.lock();

        try
        {
            for ( ConnectionPack    pack : _leasedConnections )
            {
                pack.collectActiveSql( sql );
            }
        }
        finally
        {
            _lock.unlock();
        }

        return sql;
    }


    public void cancelAllStatements()
        throws
            SQLException
    {
        _lock.lock();

        try
        {
            for ( ConnectionPack    pack : _leasedConnections )
            {
                pack.cancelAllStatements();
            }
        }
        finally
        {
            _lock.unlock();
        }
    }


    public ConnectionPack getConnectionPack( final long    timeout )
        throws
            DatabaseException
    {
        final long         startTime = System.currentTimeMillis();

        ConnectionPack     pack;
        while ( ( pack = getConnectionPack() ) == null )
        {
            if ( ( System.currentTimeMillis() - startTime ) >= timeout )
            {
                //
                //    Timeout has expired
                //
                return null;
            }

            //
            //    Wait for a notification from the connection pool
            //    that releaseConnectionPack() has been called
            //
            _lock.lock();
            try
            {
                _availablePack.await( timeout, TimeUnit.MILLISECONDS );
            }
            catch( InterruptedException    ex )
            {
                //
                //    Re-signal that the current thread has been interrupted
                //
                Thread.currentThread().interrupt();
                return null;
            }
            finally
            {
                _lock.unlock();
            }
        }

        return pack;

    }


    /**
     *    Checks out a connection from the _pool.
     *    <p>
     *    If no free connection is available, a new connection is
     *    created unless the max number of connections has been
     *    reached.
     *    <p>
     *    If a free connection has been closed by the database, it's
     *    removed from the _pool and this method is called again
     *    recursively.
     */
    public ConnectionPack getConnectionPack()
        throws
            DatabaseException
    {
        ConnectionPack      pack = null;

        _lock.lock();

        try
        {
            if ( logger.isInfoEnabled() )
            {
                logger.info( "\n\tRequest for connection..."
                             + "\n\t- num free connections: ["
                             + _freeConnections.size()
                             + "]"
                             + "\n\t- free connections: [" + _freeConnections + "]"
                             + "\n\t- num checked out: ["
                                 + _leasedConnections.size() + "]"
                             + "\n\t- checkedOut: ["
                                 + _leasedConnections + "]" );
            }

            if ( ! _freeConnections.isEmpty() )
            {
    //             //
    //             //    Pick the first Connection in the List to get
    //             //    round-robin usage.  This is important so that
    //             //    there's less chance of a connection being timed out
    //             //    for inactivity by the server.
    //             //
    //             pack = (ConnectionPack) _freeConnections.getFirst();
    //             _freeConnections.removeFirst();
                //
                //    The above causes the problem that once a bunch of connections
                //    are created, for whatever reason, they aren't let go of
                //    until the user completely stops using the application for
                //    the duration of the reaper delay time.  This is because each
                //    connection keeps get used over and over again and never
                //    becomes stale as long as the user is doing something.
                //    So taking the most recently used connection allows the
                //    the older connections to be reaped even if the user is
                //    using the application.
                //
                pack = _freeConnections.removeLast();

                pack.lease();

                boolean    isClosed;

                try
                {
                    isClosed = pack.isClosed();
                }
                catch ( Throwable    ex )
                {
                    isClosed = true;
                }

                if ( isClosed )
                {
                    pack = null;

                    if ( logger.isInfoEnabled() )
                    {
                        logger.info( "Removed bad connection from ["
                                     + _connectionInfo + "]" );
                    }

                    try
                    {
                        pack = getConnectionPack();
                    }
                    catch ( Throwable    ex )
                    {
                        throw new ConnectionPoolException().cantGetConnectionPack( ex );
                    }
                }
            }
            else if ( ( _connectionInfo.getMaxConnections() == 0 )
                      || ( _leasedConnections.size() < _connectionInfo.getMaxConnections() ) )
            {
                if ( logger.isInfoEnabled() )
                {
                    logger.info( "Getting new connection ..." );
                }

                pack = new ConnectionPack( _connectionInfo );
            }

            if ( pack != null )
            {
                _leasedConnections.add( pack );
            }

            if ( logger.isInfoEnabled() )
            {
                logger.info( "\n\t- num free connections: ["
                             + _freeConnections.size()
                             + "]"
                             + "\n\t- free connections: [" + _freeConnections + "]"
                             + "\n\t- num checked out: ["
                                 + _leasedConnections.size() + "]"
                             + "\n\t- checkedOut: ["
                                 + _leasedConnections + "]" );
            }
        }
        finally
        {
            _lock.unlock();
        }

        return pack;
    }


    /**
     *    Closes all available connections.
     */
    public void releaseAllConnections()
    {
        _lock.lock();

        try
        {
            _freeConnections.addAll( _leasedConnections );
            _leasedConnections.clear();

            Iterator<ConnectionPack>    iterator = _freeConnections.iterator();

            while ( iterator.hasNext() )
            {
                ConnectionPack       pack;
                pack = iterator.next();

                try
                {
                    pack.close();

                    if ( logger.isInfoEnabled() )
                    {
                        logger.info( "Closed connection for _pool" );
                    }

                }
                catch ( Throwable    ex )
                {
                    //
                    //    Just log it and move on.
                    //
                    logger.error( "Can't close connection for _pool", ex );
                }
            }

            _freeConnections.clear();
        }
        finally
        {
            _lock.unlock();
        }
    }


    void reapConnections()
    {
        _lock.lock();

        try
        {
            if ( logger.isInfoEnabled() )
            {
                logger.info( "\n\treaping connections."
                             + "\n\t- current: [" + System.currentTimeMillis() + "]"
                             + "\n\t- num free connections: ["
                             + _freeConnections.size()
                             + "]"
                             + "\n\t- free connections: [" + _freeConnections + "]"
                             + "\n\t- num checked out: ["
                                 + _leasedConnections.size() + "]"
                             + "\n\t- checkedOut: ["
                                 + _leasedConnections + "]" );
            }

            long                     timeout = ONE_MINUTE;
            long                     stale   = System.currentTimeMillis() - timeout;
            Iterator<ConnectionPack> iter    = _freeConnections.iterator();

            while ( iter.hasNext() )
            {
                ConnectionPack    pack = iter.next();

                if ( logger.isInfoEnabled() )
                {
                    logger.info( "\n\t- stale: [" + stale + "]"
                                 + "\n\t- candidate: [" + pack + "]" );
                }


                //
                //    If it's still in use (not likely), or it hasn't gone
                //    stale yet, skip reaping it.
                //
                if ( pack.isInUse() || stale < pack.getLastUse() )
                {
                    pack.removeOldStatements();
                    continue;
                }

                try
                {
                    pack.close();
                    iter.remove();

                    if ( logger.isInfoEnabled() )
                    {
                        logger.info( "\n\treaping:"
                                     + "\n\t- pack: [" + pack + "]" );
                    }

                    pack = null;
                }
                catch ( Throwable    ex )
                {
                    logger.error( "can't close connection:"
                                  + pack,
                                  ex );
                }
            }
        }
        finally
        {
            _lock.unlock();
        }
    }


    public void shutdown()
    {
        _reapTimer.cancel();
        releaseAllConnections();
    }


    //========================================
    //    class ConnectionReaper
    //========================================

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
            .append( "freeConnections", _freeConnections )
            .append( "leasedConnections", _leasedConnections )
            .append( "reapTimer", _reapTimer )
            .toString();
    }


    //========================================
    //    class ConnectionReaper
    //========================================

    static class ConnectionReaper
        extends
            TimerTask
    {
        private final ConnectionPool     _pool;

        public ConnectionReaper( final ConnectionPool    dirtyPool )
        {
            _pool = dirtyPool;
        }


        @Override
        public void run()
        {
            try
            {
                //    Should never happen, but what the hey.
                if ( _pool == null )
                {
                    return;
                }

                _pool.reapConnections();
            }
            catch ( Throwable    ex )
            {
                logger.error( "Can't reap connections", ex );
            }
        }
    }
}
