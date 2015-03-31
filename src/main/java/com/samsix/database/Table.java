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


import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.samsix.util.string.StringUtilities;


/**
 *    Represents a table in the database, providing query, update,
 *    insert and delete access.  Query is the best supported.
 *    <p>
 *    This is a very simple way of accessing the database.  Generally
 *    a factory class would use the this class to construct java
 *    objects which represent the data in the table.
 *    <p>
 *    Use of this class tends to constrain how both the database and
 *    java code are structured.
 *    <p>
 *    A heavy use of views results from this constraint.
 *    <p>
 *    Also includes methods to manipulate rows of data in tables.
 *    <code>
 *      Database    db = null;
 *      try
 *      {
 *           //
 *           //    Get a database connection.
 *           //
 *           Database   db         = new Database();
 *
 *
 *           //
 *           //    Get a table object representing the DocLink table.
 *           //
 *           Table      table      = db.getTable( Names.DocumentLink );
 *
 *           //
 *           //    Get the database records.
 *           //
 *           RecordSet  recordSet;
 *           recordSet = table.getRecordSet( " fname = 33 and lname = 4508" );
 *
 *           //
 *           //    Loop round all the database records returned.
 *           //
 *           while ( recordSet.next() )
 *           {
 *               docLink = recordSet.getString( Names.docname );
 *               isSheet = recordSet.getBoolean( Names.isSheet );
 *               url     = recordSet.getString( Names.url );
 *           }
 *      }
 *      finally
 *      {
 *           Database.release( db );
 *      }
 *
 *    </code>
 */
public final class Table
{
//    private static Logger
//          logger = Logger.getLogger( Table.class );



    /**
     *    Name of the database object holding our table.
     */
    private final Database    _database;


    /**
     *    The table we are trying to access.
     */
    private final String      _tableName;



    /**
     *    Standard constructor for a Table.
     */
    Table( final Database    database,
           final String      tableName )
    {
        _database  = database;
        _tableName = tableName;
    }


    /**
     *    Return our tablename.
     */
    public String getName()
    {
        return _tableName;
    }


    public void release()
    {
        Database.release( _database );
    }


    /**
     *    Add on an optional 'where' clause to the query to restrict
     *    the rows returned.
     */
    private String getWhereClause( final String    criteria )
    {
        if ( StringUtils.isBlank( criteria ) )
        {
            return "";
        }

        return " WHERE " + criteria;
    }


    private String buildSql( final String    criteria,
                             final String    orderBy,
                             final int       rowLimit )
    {
        String sql = "SELECT * FROM " + _tableName + getWhereClause( criteria );

        if ( orderBy != null )
        {
            sql += " ORDER BY " + orderBy;
        }

        //
        //    Add on any limit that may have been defined in the resources.
        //
        if ( rowLimit > 0 )
        {
            sql += " LIMIT " + rowLimit;
        }

        return sql;
    }


    /**
     *    With the criteria being null, we'll get all the rows.
     */
    public RecordSet getRecordSet()
        throws
            DatabaseException
    {
        return getRecordSet( null );
    }


    /**
     *    Restrict the number of rows returned, with an
     *    SQL string representing a WHERE clause.
     *    Hmmm.... seems to do just the opposite.
     */
    public RecordSet getRecordSet( final String    criteria )
        throws
            DatabaseException
    {
        return getRecordSet( criteria, null, -1 );
    }


    public RecordSet getRecordSet( final String    criteria,
                                   final String    orderBy )
        throws
            DatabaseException
    {
        return getRecordSet( criteria, orderBy, -1 );
    }


    /**
     *    Restrict the number of rows returned, with an
     *    SQL string representing a WHERE clause.
     */
    public RecordSet getRecordSet( final String    criteria,
                                   final int       rowLimit )
        throws
            DatabaseException
    {
        return getRecordSet( criteria, null, rowLimit );
    }


    private RecordSet getRecordSet( final String    criteria,
                                    final String    orderBy,
                                    final int       rowLimit )
        throws
            DatabaseException
    {
        return _database.getRecordSet( buildSql( criteria, orderBy, rowLimit ) );
    }


    /**
     *    Find the max value in the given column.
     */
    public Object getMax( final String    columnName,
                          final String    criteria )
        throws
            DatabaseException
    {
        return getAggregate( columnName, criteria, "MAX" );
    }


    public boolean exists( final String    condition )
        throws
            DatabaseException
    {
        String    sql;
        sql = "SELECT EXISTS( SELECT * FROM "
              + _tableName
              + " WHERE "
              + condition + ")";

        RecordSet    recordSet = _database.getRecordSet( sql );

        while ( recordSet.next() )
        {
            return recordSet.getBoolean( 1 );
        }

        return false;
    }


    /**
     *    Find the count of columns matching the given criteria
     */
    public long getCount( final String    criteria )
        throws
            DatabaseException
    {
        Object    count = getAggregate( "*", criteria, "COUNT" );

        if ( count instanceof Number )
        {
            return ( (Number) count ).longValue();
        }
        else
        {
            throw new DatabaseException( "Incorrect object type for count [" + count + "]" );
        }
    }


    /**
     *    Get the stipulated aggregate for the given column.
     */
    private Object getAggregate( final String    columnName,
                                 final String    criteria,
                                 final String    aggregate )
        throws
            DatabaseException
    {
        try
        {
            String     sql = "SELECT " + aggregate
                           + "(" + columnName + ") FROM " + _tableName
                           + getWhereClause( criteria );

            return _database.getAggregate( sql );
        }
        catch ( Throwable    ex )
        {
            throw new TableException().cantGetAggregate( _tableName,
                                                         columnName,
                                                         aggregate,
                                                         ex);
        }
    }


    public int insertSequencedRow( final SqlInsertFormatter    formatter,
                                   final String                sequenceColumnName )
        throws
            DatabaseException
    {
        return _database.executeSequencedInsert( getInsertSql( formatter ), sequenceColumnName );
    }


    public int insertRow( final SqlInsertFormatter    formatter )
        throws
            DatabaseException
    {
        return insertRow( formatter.getColumnClause(), formatter.getValueClause() );
    }


    public int insertRow( final String    columnList,
                          final String    valueList )
        throws
            DatabaseException
    {
        return _database.executeUpdate( getInsertSql( columnList, valueList ) );
    }


    public RecordSet insertRowGetKeys( final SqlInsertFormatter    formatter )
        throws
            DatabaseException
    {
        return _database.executeUpdateGetKeys(getInsertSql(formatter.getColumnClause(),
                                                           formatter.getValueClause()));
    }


    public String getInsertSql( final SqlInsertFormatter    formatter )
    {
        return getInsertSql( formatter.getColumnClause(), formatter.getValueClause() );
    }


    public String getInsertSql( final String    columnList,
                                final String    valueList )
    {
        StringBuilder    buffer = new StringBuilder();

        buffer.append( "INSERT INTO " ).append( _tableName );

        if ( columnList != null )
        {
            buffer.append( " ( " ).append( columnList ).append( " )" );
        }

        buffer.append( " VALUES ( " ).append( valueList ).append( " )" );

        return buffer.toString();
    }


    /**
     *     Insert the specified values into the database, returning the specified columns.
     */
    public Map<String,?> insertReturning( final String      columnList,
                                          final String      valueList,
                                          final String[]    returnColumnNames )
        throws
            DatabaseException
    {
        return _database.executeReturning( getInsertSql( columnList, valueList ), returnColumnNames );
    }


    /**
     *     Insert the specified values into the database, returning the specified columns.
     */
    public Map<String,?> insertReturning( final SqlInsertFormatter    formatter,
                                          final String[]              returnColumnNames )
        throws
            DatabaseException
    {
        return _database.executeReturning( getInsertSql( formatter ), returnColumnNames );
    }


    public String getUpdateSql( final String    updateClause,
                                final String    whereClause )
    {
        if( StringUtils.isBlank( updateClause ) )
        {
            return null;
        }

        StringBuilder    buffer = new StringBuilder();

        buffer.append( "UPDATE " ).append( _tableName ).append( " SET " ) .append( updateClause );

        if( ! StringUtils.isBlank( whereClause ) )
        {
            buffer.append( " WHERE " ).append( whereClause );
        }

        return buffer.toString();
    }


    /**
     *     Insert the specified values into the database.
     */
    public int updateRow( final String    updateClause,
                          final String    whereClause )
        throws
            DatabaseException
    {
        final String    updateSql = getUpdateSql( updateClause, whereClause );

        //
        //    Just return if we have no column list (names) supplied
        //
        if ( updateSql == null )
        {
            return 0;
        }

        //
        //    Perform the update
        //
        return _database.executeUpdate( updateSql );
    }


    /**
     *     Deletes rows from the table based on the specified criteria.
     *     <p>
     *     If no criteria is specified, the whole table is deleted.
     *     <p>
     *     Returns the number of rows affected.
     */
    public int deleteRows( final String    criteria )
        throws
            DatabaseException
    {
        return _database.executeUpdate( "DELETE FROM " + _tableName + getWhereClause( criteria ) );
    }


    /**
     *     Truncates a table.
     *     <p>
     *     Returns the number of rows affected.
     */
    public int truncate()
        throws
            DatabaseException
    {
        //
        //    At least as late as version 6.1, DB2 does not accept the
        //    TRUNCATE command and so you have to do a delete which
        //    unfortunately has logging concerns.
        //
        if ( _database.getConnectionInfo()
                      .getPlatform().equals( Database.PLATFORM_DB2 ) )
        {
            return _database.executeUpdate( "DELETE FROM " + _tableName );
        }

        return _database.executeUpdate( "TRUNCATE TABLE " + _tableName );
    }


    /**
     *    Query the database to get the rowcount for the table.
     */
    public long getRowCount()
        throws
            DatabaseException
    {
        return getCount( null );
    }


    /**
     *    Given a field (column) name and the value for that field,
     *    build a 'LIKE' clause for it.
     *    <p>
     *    This has a fuzzy search on both sides of the search.
     *    <p>
     *    Will only work with Strings (at the moment)
     */
    public String addLikeClause( final String    fieldName,
                                 final String    field )
    {
        boolean initialFuzzy = true;

        return addLikeClause( fieldName, field, initialFuzzy );
    }


    /**
     *    Given a field (column) name and the value for that field,
     *    build a 'LIKE' clause for it.
     *    <p>
     *    This has a fuzzy search at the end of the value, but
     *    the beginning is optional.
     *    <p>
     *    Will only work with Strings (at the moment)
     */
    public String addLikeClause( final String     fieldName,
                                 final String     field,
                                 final boolean    initialFuzzy )
    {
        String fuzzy = "%";

        if ( ! initialFuzzy )
        {
            fuzzy = "";
        }

        String likeClause = "";


        //
        //    only deal with non-empty field values.
        //
        if ( !StringUtils.isBlank( field ) )
        {
            likeClause = " UPPER( " + fieldName + " ) LIKE "
                         + StringUtilities.wrapQuotes( fuzzy
                                                       + field.toUpperCase()
                                                       + "%" );
        }

        return likeClause;
    }


    /**
     *    Return the tablename and database info
     */
    @Override
    public String toString()
    {
        return getName() + " - " + _database;
    }
}
