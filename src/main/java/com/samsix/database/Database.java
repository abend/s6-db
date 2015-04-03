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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.postgresql.util.PSQLException;

import com.samsix.util.log.LoggerControl;


/**
 *    Class of database utilities.
 *    <p>
 *    Methods to get connections, tables, and execute queries, etc.
 */
public class Database
{
    private static Logger logger = Logger.getLogger( Database.class );

    //
    //    Log the SQL commands to their own log file.
    //
    public static Logger    sqlLog = Logger.getLogger( LoggerControl.SQL_LOG );


    //
    //    Store the databases platform (i.e. postgres, sybase, db2, etc.)
    //
    //    Use static constants.
    //
    public  final static String      PLATFORM_POSTGRES      = "POSTGRES";
    public  final static String      PLATFORM_DB2           = "DB2";
    public  final static String      PLATFORM_SYBASE        = "SYBASE";
    public  final static String      PLATFORM_ORACLE        = "ORACLE";
    public  final static String      PLATFORM_SQLSERVER     = "SQLSERVER";

    public  final static String      DRIVER_POSTGRES        = "org.postgresql.Driver";
    public  final static String      DRIVER_SQLSERVER       = "net.sourceforge.jtds.jdbc.Driver";

    private ConnectionPack           _connectionPack;

    private int                      _transactionCount      = 0;
    private boolean                  _cacheConnection       = false;

    private static boolean           _enableLogging         = true;

    private final ConnectionInfo     _info;

    private volatile Statement       _currentSelectStatement;


    public Database( final ConnectionInfo    info )
    {
        _info = info;
    }


    public static boolean getEnableLogging()
    {
        return _enableLogging;
    }


    public static void setEnableLogging( final boolean    enableLogging )
    {
        _enableLogging = enableLogging;
    }
    
    
    public String formatDbElement(final String    name)
    {
        //
        // If the tablename is not all lowercase AND the database is
        // postgres then we need to wrap the tablename in double-quotes.
        //
        if (PLATFORM_POSTGRES.equals(_info.getPlatform()) && ! name.toLowerCase().equals(name)) {
            return "\"" + name + "\"";
        }
        return name;
    }


    public Table getTable( final String    tablename )
    {
        return getTable( null, tablename );
    }


    public Table getTable( final String    schema,
                           final String    tablename )
    {
        String    table;
        if ( schema == null )
        {
            table = formatDbElement(tablename);
        }
        else
        {
            table = formatDbElement(schema) + "." + formatDbElement(tablename);
        }

        return new Table( this, table );
    }


    /**
     *    Execute arbitrary sql...  Nothing should be returned
     *    by the sql.
     */
    public boolean execute( final String    sql )
        throws
            DatabaseException
    {
        try
        {
            final Statement statement = createStatement( sql );
            boolean execute = statement.execute( sql );
            statement.close();

            return execute;
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, sql, ex );
        }
        finally
        {
            release();
        }
    }


    public int getCurrentSequenceNumber( final String    sequenceName )
        throws
            DatabaseException
    {
        return getSequenceNumber( "currval", sequenceName );
    }


    public int getNextSequenceNumber( final String    sequenceName )
        throws
            DatabaseException
    {
        return getSequenceNumber( "nextval", sequenceName );
    }


    private int getSequenceNumber( final String    functionName,
                                   final String    sequenceName )
        throws
            DatabaseException
    {
        String    sql;
        sql = "select " + functionName + "('" + sequenceName + "') as id";
        RecordSet recordSet = getRecordSet( sql );
        recordSet.next();
        int id = recordSet.getInt( "id" );
        recordSet.close();

        return id;
    }


    private Statement createStatement( final String    sql )
        throws
            DatabaseException
    {
        return createStatement( getConnectionPack(), sql, false );
    }


    private Statement createStatement( final ConnectionPack    connectionPack,
                                       final String            sql,
                                       final boolean           scrollable )
        throws
            DatabaseException
    {
        try
        {
            //
            //    Always log if we are in debug mode
            //
            if ( sqlLog.isDebugEnabled()
                 || ( _enableLogging && sqlLog.isInfoEnabled() ) )
            {
                //
                //    Add a semi-colon so that it can be copied and
                //    pasted into PSQL for examination
                //
                sqlLog.info( "[" + connectionPack.getId() + "]\n"
                             + sql + ";" );

                //    For when regular debugging just isn't enough
//                Exception    exception = new Exception( "trace" );
//                StringWriter    log = new StringWriter();
//                exception.printStackTrace( new PrintWriter( log ) );
//                sqlLog.info( "---- TRACE -----\n" + log + "----------------" );
            }

            // For debugging swing thread no-nos
//            if( java.awt.EventQueue.isDispatchThread() )
//            {
//                logger.error( "DB ACCESS FROM SWING THREAD: " + sql, new Exception( "swing violation" ) );
//            }

            return connectionPack.createStatement( scrollable, sql );
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantCreateStatement( ex );
        }
    }


    /**
     *    Given an SQL string, run it against the database
     */
    public int executeUpdate( final String    sql )
        throws
            DatabaseException
    {
        try
        {
            final Statement statement = createStatement( sql );
            int executeUpdate = statement.executeUpdate( sql );
            statement.close();

            return executeUpdate;
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, sql, ex );
        }
        finally
        {
            release();
        }
    }
    
    
    public RecordSet executeUpdateGetKeys( final String    sql )
        throws
            DatabaseException
    {
        try
        {
            final Statement statement = createStatement( sql );
            statement.executeUpdate( sql, Statement.RETURN_GENERATED_KEYS );
            return new RecordSet(statement.getGeneratedKeys(), statement);
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, sql, ex );
        }
        finally
        {
            release();
        }
    }


    /**
     *    Given an SQL string, run it against the database.
     *    Returns a map of column names to values.
     */
    public Map<String,?> executeReturning( final String      sql,
                                           final String[]    returnColumnNames )
        throws
            DatabaseException
    {
        try
        {
            Statement    statement = createStatement( sql );

            statement.executeUpdate( sql, returnColumnNames );

            ResultSet    keys = statement.getGeneratedKeys();
            if( keys != null && keys.next() )
            {
                //
                //    Optimize for the general use case where we just return a sequence number
                //
                if( returnColumnNames.length == 1 )
                {
                    return Collections.singletonMap( returnColumnNames[ 0 ], keys.getObject( returnColumnNames[ 0 ] ) );
                }

                Map<String,Object>    values = new LinkedHashMap<String,Object>( returnColumnNames.length );

                for ( String columnName : returnColumnNames )
                {
                    values.put( columnName, keys.getObject( columnName ) );
                }

                keys.close();
                statement.close();

                return values;
            }
            else
            {
                if( keys != null )
                {
                    keys.close();
                }
                statement.close();

                throw new IllegalStateException( "No keys returned" );
            }
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, sql, ex );
        }
    }


    /**
     *    Given an SQL string, run it against the database.
     *    Returns the sequence number created from the insert.
     */
    public int executeSequencedInsert( final String    sql,
                                       final String    sequenceColumnName )
        throws
            DatabaseException
    {
        Map<String,?>    results = executeReturning( sql, new String[] { sequenceColumnName } );

        return ( (Number) results.get( sequenceColumnName ) ).intValue();
    }


    /**
     *    Executes arbitrary SQL, ignoring any result sets.
     */
    public void perform( final String    sql )
        throws
            DatabaseException
    {
        Statement    statement = createStatement( sql );

        try
        {
            boolean    isResultSet = statement.execute( sql );

            while( true )
            {
                if( isResultSet )
                {
                    statement.getResultSet().close();
                }
                else
                {
                    if( statement.getUpdateCount() == -1 )
                    {
                        break;
                    }
                }

                isResultSet = statement.getMoreResults();
            }

            statement.close();
        }
        catch( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, sql, ex );
        }
    }


    public RecordSet getRecordSet( final String    sql )
        throws
            DatabaseException
    {
        return getRecordSet( sql, false );
    }


    /**
     *    Return a RecordSet object based on the provided SQL.
     *    <p>
     *    NOTE:  All calls to this have to take care of releasing the database
     *    after they are finished with the returned RecordSet.
     *    The reason we can't call release inside of here is because I
     *    wasn't able to test something accurately.  We are not getting
     *    forward-only cursors even if that's what we ask for.  Rather we are
     *    always getting scrollable cursors.  Its possible that there is just
     *    something weird about pg 7.3 and when we switch to 7.4 we'll get the
     *    forward-only cursors we ask for.  What I wanted to test, before I
     *    added release to this procedure, is whether or not I could release
     *    the connection before we were done reading the forward-only cursor.
     *    Just not sure and since we aren't currently getting forward-only
     *    I simply can't test that.
     */
    public RecordSet getRecordSet( final String     sql,
                                   final boolean    scrollable )
        throws
            DatabaseException
    {
        final Resultant resultant = getResultSet( sql, scrollable );

        return new RecordSet( resultant.resultSet, resultant.statement );
    }


    public static RecordSet getRecordSet( final PreparedStatement    statement )
        throws
            DatabaseException
    {
        if ( sqlLog.isDebugEnabled() || ( _enableLogging && sqlLog.isInfoEnabled() ) )
        {
            sqlLog.info( "[STATEMENT]\n" + statement + ";" );
        }

        try
        {
            // don't embed a statement in the recordset here as the user is
            // passing in a statement and we expect them to handle closing it
            return new RecordSet( statement.executeQuery(), null );
        }
        catch ( SQLException    ex )
        {
            throw new TableException().cantGetStatementResultSet( ex, statement );
        }
    }


    public static int executeUpdate( final PreparedStatement    statement )
        throws
            DatabaseException
    {
        if ( sqlLog.isDebugEnabled() || ( _enableLogging && sqlLog.isInfoEnabled() ) )
        {
            sqlLog.info( "[STATEMENT]\n" + statement + ";" );
        }

        try
        {
            return statement.executeUpdate();
        }
        catch ( SQLException    ex )
        {
            throw new TableException().cantExecuteStatement( ex, statement );
        }
    }


    public static void closeStatement( final Statement    statement )
        throws
            DatabaseException
    {
        try
        {
            statement.close();
        }
        catch ( SQLException ex )
        {
            throw new TableException().cantCloseStatement( ex );
        }
    }


    /**
     * Try to avoid using this; relies on garbage collector to close statements
     * @param sql
     * @return
     * @throws DatabaseException
     */
    public ResultSet getResultSet( final String    sql )
        throws
            DatabaseException
    {
        return getResultSet( sql, false ).resultSet;
    }


    static class Resultant
    {
        final ResultSet resultSet;
        final Statement statement;

        public Resultant( final ResultSet resultSet,
                          final Statement statement )
        {
            this.resultSet = resultSet;
            this.statement = statement;
        }
    }


    private Resultant getResultSet( final String     sql,
                                    final boolean    scrollable )
        throws
            DatabaseException
    {
        ConnectionPack    connectionPack = getConnectionPack();

        try
        {
            Statement    statement = createStatement( connectionPack, sql, scrollable );

            _currentSelectStatement = statement;

            statement.execute( sql );

            //
            //    We need to strip off any queries that prefix our
            //    actual result set query that returns our data.
            //
            //    This might be a command such as "set enable_seqscan=false;"
            //
            //    We do this by checking update count.
            //
            //    If not a result  set returning query then this will be
            //    zero or positive int.  Otherwise, it is -1.
            //
            while ( statement.getUpdateCount() >= 0 )
            {
                statement.getMoreResults();
            }

            //
            //    If we have gotten this far, we have either a result set
            //    or no more results
            //
            ResultSet    resultSet = statement.getResultSet();

            if ( resultSet == null )
            {
                throw new DatabaseException( "No result set found in query." );
            }

            return new Resultant( resultSet, statement );
        }
        catch ( PSQLException    psex )
        {
            String    errorCode = psex.getSQLState();

            //
            //    Postgresql error codes:
            //    http://www.postgresql.org/docs/8.4/static/errcodes-appendix.html
            //
            if( errorCode != null && errorCode.startsWith( "57" ) )
            {
                if( "57014".equals( errorCode ) )
                {
                    throw new UserCancelException();
                }

                throw new AdministrativeCancelException();
            }
            else
            {
                throw new TableException().cantExecuteSql( connectionPack.getConnectionInfo(),
                                                           sql,
                                                           psex );
            }
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( connectionPack.getConnectionInfo(),
                                                       sql,
                                                       ex );
        }
        finally
        {
            _currentSelectStatement = null;
        }
    }


    Object getAggregate( final String    sql )
        throws
            DatabaseException
    {
        try
        {
            //
            //    Run the query and get the results.
            //
            RecordSet    recordSet = getRecordSet( sql );

            //
            //    Just return the first column of the first row
            //    since that's all the data we should be getting back.
            //
            Object result = null;

            if ( recordSet.next() )
            {
                result = recordSet.getObject( 1 );
            }

            recordSet.close();

            return result;
        }
        finally
        {
            release();
        }
    }


    /**
     *    Explicitly tell the database to cache its connection.
     *    This is to be used in case you know you are going to
     *    be doing a number of consecutive queries on the database.
     *    Saves having to rely on the ConnectionPool for caching.
     *    Use <code>freeConnection</code> to force a release of
     *    the connection.
     */
    public void cacheConnection()
    {
        _cacheConnection = true;
    }


    /**
     *    Used to force a release of a connection if cacheConnection
     *    has been called.  Rapidly repeated queries can be bracketed
     *    in a cacheConnection()/freeConnection() pair for faster
     *    execution.
     */
    public void freeConnection()
        throws
            DatabaseException
    {
        _cacheConnection = false;
        release();
    }


    /**
     *    Try and get a connection to the database.
     */
    private ConnectionPack getConnectionPack()
        throws
            DatabaseException
    {
        if ( _connectionPack != null )
        {
            return _connectionPack;
        }


        if ( logger.isInfoEnabled() )
        {
            logger.info( "Retrieving connection from the connection pool." );
        }

        _connectionPack = _info.getConnectionPack( 10000 );

        if ( _connectionPack == null )
        {
            throw new ConnectionException().cantGetConnection( _info );
        }

        return _connectionPack;
    }


    public ConnectionInfo getConnectionInfo()
        throws
            DatabaseException
    {
        return _info;
    }


    /**
     *    Release the database connection.
     *    <p>
     *    This procedure should be called as soon as you are done
     *    with the connection.
     *    <p>
     *    Waiting until the database object is garbage collected may
     *    result in some performance issues.
     *
     */
    public void release()
    {
        if ( _connectionPack == null || inTransaction() || _cacheConnection )
        {
            return;
        }

        if ( logger.isInfoEnabled() )
        {
            logger.info( "Releasing connection to database." );
        }

        _info.releaseConnectionPack( _connectionPack );
        _connectionPack = null;
        _transactionCount = 0;
    }


    /**
     *    @return true if this database represents an actual active database connection, false otherwise
     */
    public boolean isActive()
    {
        return _connectionPack != null;
    }


    //
    //    Mostly here to keep old code looking familiar
    //    Also, lets us know if we're done transacting
    //
    public boolean inTransaction()
    {
        return _transactionCount > 0;
    }


    /**
     *    Release the connection from the specified database object.
     *    Silently fail by just logging the exception if it occurs.
     */
    public static void release( final Database    db )
    {
        if ( db == null )
        {
            return;
        }

        try
        {
            db.release();
        }
        catch ( Throwable    ex )
        {
            logger.error( "Can't release connection.", ex );
        }
    }


    /**
     *    Begin a database transaction
     */
    public void beginTransaction()
        throws
            DatabaseException
    {
        try
        {
            if( _transactionCount > 0 )     // Already in a transaction
            {
                _transactionCount++;

                if( sqlLog.isDebugEnabled() )
                {
                    sqlLog.debug( "[" + getConnectionPack().getId()
                                  + " INC LEVEL TO " +
                                  _transactionCount + "]" );
                }

                return;
            }

            if ( logger.isInfoEnabled() )
            {
                logger.info( "Starting a transaction on [" + _info + "]" );
            }

            if( sqlLog.isInfoEnabled() )
            {
                sqlLog.info( "[" + getConnectionPack().getId() + " BEGIN]" );
            }

            getConnectionPack().getConnection().setAutoCommit( false );

            // Only increment the transactionCount the first time once we've actually
            // acquired the connection pack successfully. Prevents us from trying to
            // rollback when we failed to acquire a database connection in the first place.
            _transactionCount++;
        }
        catch ( Throwable    ex )
        {
            throw new TransactionException().cantBegin( _info, ex );
        }
    }


    /**
     *    Commit a database transaction
     */
    public void commitTransaction()
        throws
            DatabaseException
    {
        if ( ! inTransaction() )
        {
            return;
        }

        _transactionCount--;

        if( _transactionCount > 0 )     // Still in a more outer transaction
        {
            if( sqlLog.isDebugEnabled() )
            {
                sqlLog.debug( "[" + getConnectionPack().getId()
                              + " DEC LEVEL TO " +
                              _transactionCount + "]" );
            }

            return;
        }

        try
        {
            if ( logger.isInfoEnabled() )
            {
                logger.info( "Commiting transaction on [" + _info + "]" );
            }

            if( sqlLog.isInfoEnabled() )
            {
                sqlLog.info( "[" + getConnectionPack().getId() + " COMMIT]" );
            }

            ConnectionPack     connectionPack = getConnectionPack();
            connectionPack.getConnection().commit();

            //
            //    Turn auto-committing back on as that was the default
            //    before the transaction was begun.
            //
            connectionPack.getConnection().setAutoCommit( true );
        }
        catch ( Throwable    ex )
        {
            throw new TransactionException().cantCommit( _info, ex );
        }
        finally
        {
            release();
        }
    }


    public static void rollbackTransaction( final Database    db )
    {
        if ( db == null )
        {
            return;
        }

        try
        {
            db.rollbackTransaction();
        }
        catch ( Throwable    ex )
        {
            //
            //    What can we do?  Should we go ahead and let it go
            //    up the stack?  How serious is this?
            //    CE: I think this is potentially serious, db could be corrupted.
            //
            logger.error( "Unable to rollback a transaction", ex );
        }
    }


    /**
     *    Roll back a database transaction
     */
    public void rollbackTransaction()
        throws
            DatabaseException
    {
        if ( ! inTransaction() )
        {
            return;
        }

        //
        //    Why do we set this to 0?  Because it doesn't really matter if
        //    we do the rollback on an inner or an outer transaction,
        //    assuming the methods are throwing and catching exceptions in the
        //    proper manner...  perhaps that's a bold assumption.
        //
        _transactionCount = 0;

        try
        {
            if ( logger.isInfoEnabled() )
            {
                logger.info( "Rolling back the transaction on [" + _info + "]" );
            }

            if( sqlLog.isInfoEnabled() )
            {
                sqlLog.info( "[" + getConnectionPack().getId() + " ROLLBACK]" );
            }

            ConnectionPack     connectionPack = getConnectionPack();
            connectionPack.getConnection().rollback();

            //
            //    Turn auto-committing back on as that was the default
            //    before the transaction was begun
            //
            connectionPack.getConnection().setAutoCommit( true );
        }
        catch ( Throwable    ex )
        {
            throw new TransactionException().cantRollback( _info, ex );
        }
        finally
        {
            release();
        }
    }


    public PreparedStatement prepareStatement( final String    sql )
        throws
            DatabaseException
    {
        final ConnectionPack    connectionPack = getConnectionPack();

        try
        {
            //
            //    Always log if we are in debug mode
            //
            if ( sqlLog.isDebugEnabled()
                 || ( _enableLogging && sqlLog.isInfoEnabled() ) )
            {
                sqlLog.info( "[" + connectionPack.getId() + "] PREPARE: \n"
                             + sql );
            }

            return connectionPack.getConnection().prepareStatement( sql );
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantPrepareStatement( sql, ex );
        }
    }


    public static void execute( final ConnectionInfo      connectionInfo,
                                final String              sql,
                                final RecordSetHandler    handler )
        throws
            DatabaseException
    {
        Database    db = new Database( connectionInfo );
        try
        {
            db.execute( sql, handler );
        }
        finally
        {
            db.release();
        }
    }


    public void execute( final String              sql,
                         final RecordSetHandler    handler )
        throws
            DatabaseException
    {
        handler.initRecordSetHandling();

        int    count = 0;

        try
        {
            //
            //    Debbugging code for determing query speeds.
            //
            //    org.apache.commons.lang3.time.StopWatch
            //
//            StopWatch    sw = new StopWatch();
//            sw.start();

            RecordSet    recordSet = getRecordSet( sql );

//            sw.split();
//            long    split = sw.getSplitTime();

            //
            //    Process each of the rows in our result set
            //
            while ( recordSet.next() )
            {
                handler.handleRow( recordSet );
                count++;
            }

//            sw.stop();
//            sqlLog.info( "query: " + split + ", features: " + ( sw.getTime() - split ) + ", total: " + sw.getTime() );

            recordSet.close();
        }
        catch( DatabaseException    ex )
        {
            //
            //    Some exception got thrown along the way, and we might need to
            //    do cleanup in the calling object so call this with a count of zero.
            //
            handler.finishRecordSetHandling( 0 );

            throw ex;
        }

        handler.finishRecordSetHandling( count );
    }


    //
    //    Log how many rows were returned for a particular piece of SQL
    //
//    private void logRowsReturned( final int       numRows,
//                                  final String    sql )
//    {
//        if ( logger.isInfoEnabled() )
//        {
//            logger.info( "\n\tFinished query. ["
//                         + numRows
//                         + " rows affected from SQL ["
//                         + sql
//                         + "]" );
//        }
//    }


    public void cancelCurrentStatement()
    {
        final Statement    currentStatement = _currentSelectStatement;

        if( currentStatement != null )
        {
            try
            {
                if( ! currentStatement.isClosed() )
                {
                    currentStatement.cancel();
                }
            }
            catch( Throwable    ex )
            {
                logger.warn( "Failed to cancel statement", ex );
            }
        }
    }


    /**
     * Gets an actual, raw java.sql.Connection instance for the current connection.
     * @return
     * @throws DatabaseException
     */
    public Connection getRawConnection()
        throws
            DatabaseException
    {
        ConnectionPack connectionPack = getConnectionPack();

        try
        {
            return connectionPack.getConnection();
        }
        catch( Exception ex )
        {
            throw new DatabaseException( "Unable to acquire connection", ex );
        }
    }

    //
    // AUTOMATIC PARAMETERIZED QUERIES
    //

    public RecordSet getParamRecordSet( final String sql, final Object ... parameters )
        throws
            DatabaseException
    {
        List<Object> listParameters = parameters == null ? Collections.emptyList() : Arrays.asList( parameters );
        return getRecordSet( sql, false, listParameters );
    }


    public RecordSet getScrollableParamRecordSet( final String sql, final boolean scrollable, final Object ... parameters )
        throws
            DatabaseException
    {
        List<Object> listParameters = parameters == null ? Collections.emptyList() : Arrays.asList( parameters );
        return getRecordSet( sql, scrollable, listParameters );
    }


    public RecordSet getRecordSet( final String sql, final Collection<Object> parameters )
        throws DatabaseException
    {
        return getRecordSet( sql, false, parameters );
    }


    public RecordSet getRecordSet( final String sql, final boolean scrollable, final Collection<Object> parameters )
        throws DatabaseException
    {
        final Resultant resultant = getParamResultSet( sql, scrollable, parameters );

        return new RecordSet( resultant.resultSet, resultant.statement );
    }


    public static void populateStatement( final PreparedStatement statement,
                                          final Collection<Object> parameters )
        throws SQLException
    {
        int idx = 1;
        for ( Object value : parameters )
        {
            if( value instanceof SqlNull )
            {
                statement.setNull( idx++, ( (SqlNull) value ).getSqlType() );
                continue;
            }

            statement.setObject( idx++, value );
        }
    }


    private Resultant getParamResultSet( final String     sql,
                                         final boolean    scrollable,
                                         final Collection<Object> parameters )
        throws
            DatabaseException
    {
        ConnectionPack    connectionPack = getConnectionPack();

        String displaySql = sql;

        try
        {
            PreparedStatement statement = connectionPack.createPreparedResultStatement( scrollable, sql );
            populateStatement( statement, parameters );

            // toString on a preparedStatement unofficially gives the raw sql
            displaySql = statement.toString();

            _currentSelectStatement = statement;

            if ( sqlLog.isDebugEnabled() || ( _enableLogging && sqlLog.isInfoEnabled() ) )
            {
               // Add a semi-colon so that it can be copied and pasted into PSQL for examination
               sqlLog.info( "[" + connectionPack.getId() + "]\n" + displaySql + ";" );
            }


            ResultSet resultSet = statement.executeQuery();
            if ( resultSet == null )
            {
                throw new DatabaseException( "No result set found in query." );
            }

            return new Resultant( resultSet, statement );
        }
        catch ( PSQLException    psex )
        {
            String    errorCode = psex.getSQLState();

            //
            //    Postgresql error codes:
            //    http://www.postgresql.org/docs/8.4/static/errcodes-appendix.html
            //
            if( errorCode != null && errorCode.startsWith( "57" ) )
            {
                if( "57014".equals( errorCode ) )
                {
                    throw new UserCancelException();
                }

                throw new AdministrativeCancelException();
            }
            else
            {
                throw new TableException().cantExecuteSql( connectionPack.getConnectionInfo(),
                                                           displaySql,
                                                           psex );
            }
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( connectionPack.getConnectionInfo(),
                                                       displaySql,
                                                       ex );
        }
        finally
        {
            _currentSelectStatement = null;
        }
    }


    /**
     * Executes a parameterized SQL query
     * @param sql
     * @param parameters
     * @return
     * @throws DatabaseException
     */
    public int executeUpdate( final String sql, final Object ... parameters )
        throws DatabaseException
    {
        List<Object> listParameters = parameters == null ? Collections.emptyList() : Arrays.asList( parameters );
        return executeUpdate( sql, listParameters );
    }


    /**
     * Executes a parameterized SQL query
     * @param sql
     * @param parameters
     * @return
     * @throws DatabaseException
     */
    public int executeUpdate( final String sql,
                              final Collection<Object> parameters )
        throws DatabaseException
    {
        String displaySql = sql;

        try
        {
            ConnectionPack connectionPack = getConnectionPack();

            final PreparedStatement statement = connectionPack.createPreparedExecuteStatement( false, sql );
            populateStatement( statement, parameters );

            // toString on a preparedStatement unofficially gives the raw sql
            displaySql = statement.toString();

            if ( sqlLog.isDebugEnabled() || ( _enableLogging && sqlLog.isInfoEnabled() ) )
            {
               // Add a semi-colon so that it can be copied and pasted into PSQL for examination
               sqlLog.info( "[" + connectionPack.getId() + "]\n" + displaySql + ";" );
            }

            int executeUpdate = statement.executeUpdate();

            statement.close();

            return executeUpdate;
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantExecuteSql( _info, displaySql, ex );
        }
        finally
        {
            release();
        }
    }


    /**
     * Executes a parameterized SQL query using a RecordSetHandler
     * @param sql
     * @param parameters
     * @param handler
     * @throws DatabaseException
     */
    public void execute( final String sql,
                         final Collection<Object> parameters,
                         final RecordSetHandler handler )
        throws
            DatabaseException
    {
        handler.initRecordSetHandling();

        int    count = 0;

        try
        {
            RecordSet recordSet = getRecordSet( sql, parameters );

            while ( recordSet.next() )
            {
                handler.handleRow( recordSet );
                count++;
            }

            recordSet.close();
        }
        catch( DatabaseException    ex )
        {
            handler.finishRecordSetHandling( 0 );

            throw ex;
        }

        handler.finishRecordSetHandling( count );
    }

    /**
     *    Return the database and user logging in
     */
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
               .append( "info", _info )
               .append( "connectionPack", _connectionPack )
               .toString();
    }
}
