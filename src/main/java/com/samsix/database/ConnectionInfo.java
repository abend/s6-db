/*
 ***************************************************************************
 *
 * Copyright (c) 2001-2010 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;


import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.samsix.util.io.ResourceReader;
import com.samsix.util.net.NoProxyPortSelector;
import com.samsix.util.weak.WeakHashSet;


/**
 *    Simple class to hold the connection information to the database.
 */
public class ConnectionInfo
    implements
        Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = -1173114288261496068L;

    public static String     DBTYPE_PRIMARY = "Primary";
    public static String PROP_SUFFIX_SSL = ".UseSSL";

    private static Collection<ConnectionInfo>       _connectionInfos =
        Collections.synchronizedSet( new WeakHashSet<ConnectionInfo>() );

    private transient ConnectionPool                _connectionPool;

    private final int         _maxConnections;
    private final String      _password;
    private final String      _url;
    private final String      _userName;
    private final String      _driver;
    private final String      _platform;
    private       String      _dbServer;
    private       String      _database;
    private       boolean     _useSSL = false;


    public ConnectionInfo( final String    driver,
                           final String    url,
                           final String    userName,
                           final String    password,
                           final String    platform,
                           final int       maxConnections,
                           final String    dbServer,
                           final String    database )
    {
        _driver         = driver;
        _url            = url;
        _userName       = userName;
        _password       = password;

        _platform       = platform;
        _maxConnections = maxConnections;
        _dbServer       = dbServer;
        _database       = database;

        //
        //    Keep a weak, static reference around so we can kill it later
        //    during a tomcat redeploy, for instance.
        //
        _connectionInfos.add( this );
    }


    public static String getResourceBaseKey( final String    dbType )
    {
        if ( StringUtils.isBlank( dbType ) )
        {
            return "Database." + DBTYPE_PRIMARY;
        }
        else
        {
            return "Database." + dbType;
        }
    }


    public static ConnectionInfo valueOf( final ResourceReader    reader,
                                          final String            dbType )
    {

        return pValueOf( reader, dbType, null );
    }


    public static ConnectionInfo valueOf( final ResourceReader    reader,
                                          final String            dbServer,
                                          final String            dbName )
    {
        ConnectionInfo    info = pValueOf( reader,
                                           DBTYPE_PRIMARY,
                                           getPostgresUrl( dbServer, dbName, null ) );
        info._dbServer = dbServer;
        info._database = dbName;

        return info;
    }


    /**
     *     Use the active info to assume some things about the connection you want
     *     if it is not provided.  Such as platform, dbServer, etc.
     *
     * @param reader
     * @param dbType
     * @param activeInfo
     * @return
     */
    public static ConnectionInfo valueOf( final ResourceReader    reader,
                                          final String            dbType,
                                          final ConnectionInfo    activeInfo )
    {
        String    keyBase = getResourceBaseKey( dbType );

        if ( Database.PLATFORM_POSTGRES.equals( getPlatform( reader, keyBase ) )
             && Database.PLATFORM_POSTGRES.equals( activeInfo.getPlatform() ) )
        {
            String    dbServer = reader.getString( keyBase + ".Server", activeInfo.getDbServerName() );
            String    dbName = reader.getString( keyBase + ".Database", dbType );

            return postgresVersionOf( dbServer,
                                      dbName,
                                      getUserName( reader, keyBase, activeInfo.getUserName() ),
                                      getPassword( reader, keyBase, activeInfo.getPassword() ),
                                      reader.getInteger( keyBase + ".Port", null ),
                                      getMaxConnections( reader, keyBase ) );
        }

        return pValueOf( reader, dbType, null );
    }


    private static ConnectionInfo pValueOf( final ResourceReader    reader,
                                            final String            dbType,
                                            final String            defaultUrl )
    {
        String    keyBase = getResourceBaseKey( dbType );

        String    url = getUrl( reader, keyBase, defaultUrl );

        if ( StringUtils.isEmpty( url ) )
        {
            return null;
        }

        ConnectionInfo connectionInfo;
        connectionInfo = new ConnectionInfo( reader.getString( keyBase + ".Driver",
                                                               Database.DRIVER_POSTGRES ),
                                             url,
                                             getUserName( reader, keyBase, null ),
                                             getPassword( reader, keyBase, null ),
                                             getPlatform( reader, keyBase ),
                                             getMaxConnections( reader, keyBase ),
                                             null,
                                             null );
        connectionInfo._useSSL = reader.getBoolean( keyBase + PROP_SUFFIX_SSL, false );
        
        return connectionInfo;
    }


    private static String getUserName( final ResourceReader    reader,
                                       final String            keyBase,
                                       final String            defaultUserName )
    {
        return reader.getString( keyBase + ".UserName", defaultUserName );
    }


    private static String getPassword( final ResourceReader    reader,
                                       final String            keyBase,
                                       final String            defaultPassword )
    {
        return reader.getString( keyBase + ".Password", defaultPassword );
    }


    private static String getUrl( final ResourceReader    reader,
                                  final String            keyBase,
                                  final String            defaultUrl )
    {
        return reader.getString( keyBase + ".Url", defaultUrl );
    }


    private static String getPlatform( final ResourceReader    reader,
                                       final String            keyBase )
    {
        return reader.getString( keyBase + ".Platform", Database.PLATFORM_POSTGRES );
    }


    private static int getMaxConnections( final ResourceReader    reader,
                                          final String            keyBase )
    {
        return reader.getInt( keyBase + ".MaxConnections", 12 );
    }


    public static ConnectionInfo postgresVersionOf( final String    dbServer,
                                                    final String    dbName,
                                                    final String    userName,
                                                    final String    password )
    {
        return postgresVersionOf( dbServer, dbName, userName, password, null, 12 );
    }


    private static String getPostgresUrl( final String    server,
                                          final String    name,
                                          final Integer   port )
    {
        final String    host;

        if( port != null )
        {
            host = server + ":" + port;
        }
        else
        {
            host = server;
        }

        return "jdbc:postgresql://" + host + "/" + name;
    }


    public static ConnectionInfo postgresVersionOf( final String    dbName )
    {
        return postgresVersionOf( "localhost", dbName );
    }


    public static ConnectionInfo postgresVersionOf( final String    dbHost,
                                                    final String    dbName )
    {
        if ( dbHost == null ) {
            return postgresVersionOf( dbName );
        }
        
        return postgresVersionOf( dbHost, dbName, null, null, 5432, 12 );
    }
    

    public static ConnectionInfo postgresVersionOf( final String    dbServer,
                                                    final String    dbName,
                                                    final String    userName,
                                                    final String    password,
                                                    final Integer   port,
                                                    final int       maxConnections )
    {
        return new ConnectionInfo( Database.DRIVER_POSTGRES,
                                   getPostgresUrl( dbServer, dbName, port ),
                                   userName,
                                   password,
                                   Database.PLATFORM_POSTGRES,
                                   maxConnections,
                                   dbServer,
                                   dbName );
    }


    public static ConnectionInfo sqlServerVersionOf( final String    dbServerName,
                                                     final String    database,
                                                     final String    userName,
                                                     final String    password )
    {
        //
        // format:
        // jdbc:jtds:sqlserver://${host}:1433/${database}
        //
        return new ConnectionInfo( Database.DRIVER_SQLSERVER,
                                   "jdbc:jtds:sqlserver://" + dbServerName + "/" + database,
                                   userName,
                                   password,
                                   Database.PLATFORM_SQLSERVER,
                                   12,
                                   dbServerName,
                                   database );
    }


    public String getDbServerName()
    {
        return _dbServer;
    }


    public String getDatabase()
    {
        return _database;
    }


    private void init()
        throws
            DatabaseException
    {
        //
        //    If this is a jdbc URL (what else would it be?), make sure
        //    we don't ever try to proxy it.
        //
        if( _url != null && _url.startsWith( "jdbc:" ) )
        {
            try
            {
                URI    uri  = new URI( _url.substring( 5 ) );
                int    port = uri.getPort();

                if( port >= 0 )
                {
                    NoProxyPortSelector.getInstance().addNoProxyPort( port );
                }

            }
            catch ( Throwable    ex )
            {
                //
                //    Do nothing
                //
            }
        }

        if ( Database.sqlLog.isInfoEnabled() )
        {
            Database.sqlLog.info( "connection info [" + this + "]" );
        }

        //
        //    Load driver if it is provided.  If not, then we
        //    hope they have loaded it in some other way.
        //
        //    e.g. by stating it on the command line like
        //    -Djdbc.drivers=org.postgresql.Driver
        //
        if ( _driver != null )
        {
            try
            {
                Class.forName( _driver );
            }
            catch ( Throwable    ex )
            {
                throw new DatabaseException( "Unable to create connection pool.", ex );
            }
        }

        _connectionPool = new ConnectionPool( this );
    }


    public void shutdown()
    {
        if( _connectionPool != null )
        {
            _connectionPool.shutdown();
        }
    }


    public static void shutdownAll()
    {
        synchronized( _connectionInfos )
        {
            for( ConnectionInfo    connectionInfo : _connectionInfos )
            {
                //
                //    Can be null if it's already been gc'd
                //
                if( connectionInfo != null )
                {
                    connectionInfo.shutdown();
                }
            }
        }
    }


    public String getUrl()
    {
        return _url;
    }


    public String getUserName()
    {
        return _userName;
    }


    public int getMaxConnections()
    {
        return _maxConnections;
    }


    public String getPassword()
    {
        return _password;
    }


    public String getDriver()
    {
        return _driver;
    }


    /**
     *    Get the database platform we are using.
     */
    public String getPlatform()
    {
        return _platform;
    }


    // ===========================================
    //
    //    Stuff to do with the ConnectionPool
    //
    // ===========================================

    private ConnectionPool getConnectionPool()
        throws
            DatabaseException
    {
        if ( _connectionPool == null )
        {
            try
            {
                synchronized( this )
                {
                    if( _connectionPool == null )
                    {
                        init();
                    }
                }
            }
            catch ( Throwable    ex )
            {
                Database.sqlLog.error( "Unable to reinitialize connectionPool",
                                       ex );


                if( ex instanceof DatabaseException )
                {
                    throw (DatabaseException) ex;
                }
                else
                {
                    throw new DatabaseException( "Unable to create connection pool.", ex );
                }
            }
        }

        return _connectionPool;
    }


    /**
     *    Checks out a connection from the _pool. If no free connection
     *    is available, a new connection is created unless the max
     *    number of connections has been reached. If a free connection
     *    has been closed by the database, it's removed from the _pool
     *    and this method is called again recursively.
     *    <P>
     *    If no connection is available and the max number has been
     *    reached, this method waits the specified time for one to be
     *    checked in.
     *
     * @param timeout The timeout value in milliseconds
     */
    public ConnectionPack getConnectionPack( final long    timeout )
        throws
            DatabaseException
    {
        return getConnectionPool().getConnectionPack( timeout );
    }


    /**
     *    Checks in a connection to the _pool.
     *    <p>
     *    Notify other Threads that may be waiting for a connection.
     */
    public void releaseConnectionPack( final ConnectionPack    pack )
    {
        try
        {
            getConnectionPool().releaseConnectionPack( pack );
        }
        catch( DatabaseException    ex )
        {
            //    Ignore, it's already logged
        }
    }


    public void cancelAllStatements()
        throws
            SQLException
    {
        try
        {
            getConnectionPool().cancelAllStatements();
        }
        catch ( DatabaseException    ex )
        {
            //    Ignore, it's already logged
        }
    }


    public Collection<String> getActiveSql()
    {
        try
        {
            return getConnectionPool().getActiveSql();
        }
        catch ( DatabaseException ex )
        {
            //    Ignore, it's already logged
            return Collections.emptyList();
        }
    }


    public final Driver getJDBCDriver()
        throws
            SQLException
    {
        return DriverManager.getDriver( _url );
    }


    public final Connection getConnection()
        throws
            SQLException
    {
        Properties    connectionProps = new Properties();

        if( _userName != null )
        {
            connectionProps.put( "user", _userName );
        }

        if ( _password != null )
        {
            connectionProps.put( "password", _password );
        }
        
        if ( _useSSL ) {
            connectionProps.put( "ssl", "true" );
            
            //
            // Not sure yet what to do for other platforms.  Will have to figure
            // that out when needed.
            //
            if ( Database.PLATFORM_POSTGRES.equals( _platform ) ) {
                connectionProps.put( "sslfactory", "org.postgresql.ssl.NonValidatingFactory" );
            }
        }
        
        return getJDBCDriver().connect( _url, connectionProps );
    }


    public String toShortString()
    {
        return _userName + "@" + _url;
    }


    // ================================
    //
    //    Object interface
    //
    // ================================

    @Override
    public String toString()
    {
        try
        {
            return new ToStringBuilder( this )
                .append( "URL",             getUrl() )
                .append( "User name",       getUserName() )
                .append( "Driver",          getDriver() )
                .append( "Max connections", getMaxConnections() )
                .append( "Platform",        getPlatform() )
                    .toString();
        }
        catch ( Throwable    ex )
        {
            return "can't get that info for you: " + ex;
        }
    }
}
