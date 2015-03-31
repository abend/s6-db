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


import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;



/**
 *    Simple class to hold the connection information to the database.
 */
class ConnectionPack
{
    protected final static Logger    logger = Logger.getLogger( ConnectionPack.class );

    private final static AtomicInteger    _sequenceGenerator = new AtomicInteger();

    private final ConnectionInfo    _info;
    private Connection              _connection;
    private boolean                 _inUse;
    private long                    _lastUse;
    private final int               _sequence = _sequenceGenerator.incrementAndGet();

    private final Collection<SqlStatement>    _statements;


    public ConnectionPack( final ConnectionInfo     info )
    {
        _info = info;
        _statements = new HashSet<SqlStatement>();

        lease();
    }


    public ConnectionInfo getConnectionInfo()
    {
        return _info;
    }


    public void lease()
    {
        _inUse = true;
    }


    public void expireLease()
    {
        _inUse   = false;
        _lastUse = System.currentTimeMillis();
        synchronized ( _statements )
        {
            _statements.clear();
        }
    }


    public void close()
        throws
            SQLException
    {
        if ( _connection != null )
        {
            _connection.close();
            _connection = null;
        }

        _statements.clear();
    }


    public int getId()
    {
        return _sequence;
    }


    public void removeOldStatements()
    {
        synchronized ( _statements )
        {
            Iterator<SqlStatement>    iter = _statements.iterator();

            while ( iter.hasNext() )
            {
                SqlStatement sqlStatement = iter.next();
                Statement statement = sqlStatement.statement.get();

                // remove expired statements
                if ( statement == null )
                {
                    iter.remove();
                    continue;
                }

                // remove closed statements
                try
                {
                    if( statement.isClosed() || statement.getConnection().isClosed() )
                    {
                        iter.remove();
                    }
                }
                catch( SQLException ex )
                {
                    logger.warn( "Failed to check if statement is closed", ex );
                }
            }
        }
    }


    public boolean isClosed()
        throws
            SQLException
    {
        if ( _connection == null )
        {
            //    Not sure, seems right.  Not to worry?
            return true;
        }

        return _connection.isClosed();
    }


    public void cancelAllStatements()
        throws
            SQLException
    {
        synchronized ( _statements )
        {
            for ( SqlStatement    sqlStatement : _statements )
            {
                Statement statement = sqlStatement.statement.get();
                if ( statement != null && ! statement.isClosed() )
                {
                    statement.cancel();
                }
            }

            _statements.clear();
        }
    }


    public Statement createStatement( final boolean    scrollable,
                                      final String     sql )
        throws
            SQLException
    {
        //
        // k-n Feb  6, 2004:
        //    The default should be for forward_only because that's what
        //    the java API spec says but I think
        //    the postgres jdbc driver defaults to ResultSet.TYPE_SCROLL_INSENSITIVE
        //    because they didn't used to have forward_only cursors maybe and they
        //    didn't want to break existing code.  But it doesn't seem to create
        //    a forward_only cursor even if I specify.  Weird.  Why not?  Not supported yet?
        //
        int resultSetType;
        if ( scrollable )
        {
            resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
        }
        else
        {
            resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        }

        Statement    statement;
        statement = getConnection().createStatement( resultSetType,
                                                     ResultSet.CONCUR_READ_ONLY );
        synchronized ( _statements )
        {
            _statements.add( new SqlStatement( statement, sql ) );
        }

        return statement;
    }


    public PreparedStatement createPreparedResultStatement( final boolean    scrollable,
                                                            final String     sql )
        throws
            SQLException
    {
        //
        // k-n Feb  6, 2004:
        //    The default should be for forward_only because that's what
        //    the java API spec says but I think
        //    the postgres jdbc driver defaults to ResultSet.TYPE_SCROLL_INSENSITIVE
        //    because they didn't used to have forward_only cursors maybe and they
        //    didn't want to break existing code.  But it doesn't seem to create
        //    a forward_only cursor even if I specify.  Weird.  Why not?  Not supported yet?
        //
        int resultSetType = scrollable ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY;

        PreparedStatement    statement;
        statement = getConnection().prepareStatement( sql, resultSetType, ResultSet.CONCUR_READ_ONLY );
        synchronized ( _statements )
        {
            _statements.add( new SqlStatement( statement, sql ) );
        }

        return statement;
    }


    public PreparedStatement createPreparedExecuteStatement( final boolean    wantAutogeneratedKeys,
                                                             final String     sql )
        throws
            SQLException
    {
        int autoGeneratedKeys = wantAutogeneratedKeys ? PreparedStatement.RETURN_GENERATED_KEYS
                                                      : PreparedStatement.NO_GENERATED_KEYS;

        PreparedStatement    statement;
        statement = getConnection().prepareStatement( sql, autoGeneratedKeys );
        synchronized ( _statements )
        {
            _statements.add( new SqlStatement( statement, sql ) );
        }

        return statement;
    }


    public void collectActiveSql( final Collection<String>    sql )
    {
        synchronized ( _statements )
        {
            for ( SqlStatement sqlStatement : _statements )
            {
                Statement statement = sqlStatement.statement.get();
                if ( statement != null )
                {
                    try
                    {
                        if( ! statement.isClosed() )
                        {
                            if( statement instanceof PreparedStatement )
                            {
                                sql.add( statement.toString() );
                            }
                            else
                            {
                                sql.add( sqlStatement.sql );
                            }
                        }
                    }
                    catch( SQLException ex )
                    {
                        logger.warn( "Couldn't collect active sql", ex );
                    }
                }
            }
        }
    }


    public Connection getConnection()
        throws
            SQLException
    {
        if ( _connection == null )
        {
            if( logger.isDebugEnabled() )
            {
                if( ( ! java.awt.GraphicsEnvironment.isHeadless() )
                    && java.awt.EventQueue.isDispatchThread() )
                {
                    logger.debug( "getConnection() on swing thread",
                                  new Exception() );
                }
            }

            if ( logger.isInfoEnabled() )
            {
                logger.info( "Establishing connection with...\n"
                             + "\n\turl: [" + _info.getUrl() + "]"
                             + "\n\tuser: [" + _info.getUserName() + "]",
                             new Exception( "StackTrace:" ) );
            }

            _connection = _info.getConnection();

//            if ( _connection instanceof PGConnection )
//            {
//                //
//                //    This is what the postgres jdbc layer must be doing
//                //    automatically.  So we don't need to do this if using
//                //    the postgis.jar but I wanted to show it because I'm
//                //    overriding it to create our own handler and I wanted
//                //    to show what we are overriding.
//                //
//                //   ((PGConnection)_connection).addDataType( "geometry",
//                //                                             org.postgis.PGgeometry.class );
//                //
//                ((PGConnection)_connection).addDataType( "geometry",
//                                                         com.samsix.database.PostgisGeom.class );
//            }
        }

        return _connection;
    }


    public long getLastUse()
    {
        return _lastUse;
    }


    public boolean isInUse()
    {
        return _inUse;
    }


    // ================================
    //
    //    Object interface
    //
    // ================================

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
            .append( "info", _info )
            .append( "connection", _connection )
            .append( "inUse", isInUse() )
            .append( "lastUse", getLastUse() )
            .toString();
    }


    // ================================
    //
    //    SqlStatement class
    //
    // ================================

    private class SqlStatement
    {
        public final WeakReference<Statement> statement;
        public final String sql;


        SqlStatement( final Statement    statement,
                      final String       sql )
        {
            this.statement = new WeakReference<Statement>( statement );
            this.sql = sql;
        }
    }
}
