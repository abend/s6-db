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
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;


/**
 *    Implements RecordSet, providing a tool to process
 *    the results of an sql query.
 */
public class RecordSet
{
    private static SimpleDateFormat     _dateFormatter = new SimpleDateFormat( "yyyy-MM-dd" );

    private final ResultSet _resultSet;

    private final WeakReference<Statement> _statement;


    /**
     *    Create a new RecordSet object backed by the provided ResultSet.
     */
    public RecordSet( final ResultSet resultSet, final Statement statement )
    {
        _resultSet = resultSet;
        _statement = ( statement == null ) ? null : new WeakReference<Statement>( statement );
    }


    // backwards compatibility
    public RecordSet( final ResultSet resultSet )
    {
        this( resultSet, null );
    }


    public boolean next()
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.next();
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantMoveCursor( ex );
        }
    }


    public void beforeFirst()
        throws
            DatabaseException
    {
        try
        {
            _resultSet.beforeFirst();
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantMoveCursor( ex );
        }
    }



    public int getRow()
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getRow();
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetRowNum( ex );
        }
    }


    public int getColumnCount()
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnCount();
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public String getColumnName( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnName( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }



    public String getColumnLabel( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnLabel( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }



    public int getColumnType( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnType( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public String getColumnTypeName( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnTypeName( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public int getColumnDisplaySize( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getColumnDisplaySize( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public byte[] getBytes( final String    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getBytes( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public int getScale( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getScale( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public int getPrecision( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getPrecision( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public String getSchemaName( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getSchemaName( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public String getTableName( final int    column )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getMetaData().getTableName( column );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetColumnInfo( ex );
        }
    }


    public boolean hasColumn( final String    colName )
    {
        try
        {
            //
            //    This throws an exception if the column
            //    doesn't exist.  Otherwise, it returns
            //    the column index.
            //
            _resultSet.findColumn( colName );

            return true;
        }
        catch ( Throwable    ex )
        {
            //
            //    OK, there error could be a bad resultSet
            //    or some other such but I'm going to assume
            //    here that the error results from the column
            //    not existed.
            //
            return false;
        }
    }


    public void close()
        throws
            DatabaseException
    {
        try
        {
            _resultSet.close();

            if( _statement != null )
            {
                Statement statement = _statement.get();

                if( statement != null )
                {
                    statement.close();
                }
            }
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantClose( ex );
        }
    }


    public void closeSilently()
    {
        try
        {
            close();
        }
        catch ( Throwable    ex )
        {
            Logger.getLogger( getClass() ).error( "Failed to close resultSet", ex );
        }
    }


    //===================================
    //
    //    Data extracting methods.
    //
    //===================================

    /**
     *    Cast to array of the type you expect.  For a example, if you expect
     *    a two-dimensional String array, write this...
     *
     *    String[][]    array = (String[][]) recordSet.getArray( colName );
     */
    public Object getArray( final String    colName )
        throws
            DatabaseException
    {
        try
        {
            Array    array = _resultSet.getArray( colName );

            if ( array == null )
            {
                return null;
            }

            return array.getArray();
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    //
    //    If null, this defaults to zero.
    //
    public long getLong( final String    colName )
        throws
            DatabaseException
    {
        return getLong( colName, -1, false );
    }


    public long getLong( final String    colName,
                         final long      defaultValue )
        throws
            DatabaseException
    {
        return getLong( colName, defaultValue, true );
    }


    private long getLong( final String     colName,
                          final long       defaultValue,
                          final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            long value = _resultSet.getLong( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Long getLongObj( final String    colName )
        throws
            DatabaseException
    {
        try
        {
            Long value = _resultSet.getLong( colName );

            return _resultSet.wasNull() ? null : value;
        }
        catch( SQLException ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Long getLongObj( final int    colNumber )
        throws
            DatabaseException
    {
        try
        {
            Long value = _resultSet.getLong( colNumber );

            return _resultSet.wasNull() ? null : value;
        }
        catch( SQLException ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }


    public int getInt( final int    colNumber )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getInt( colNumber );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }


    //
    //    If null, this defaults to zero
    //
    public int getInt( final String    colName )
        throws
            DatabaseException
    {
        return getInt( colName, -1, false );
    }


    public int getInt( final String    colName,
                       final int       defaultValue )
        throws
            DatabaseException
    {
        return getInt( colName, defaultValue, true );
    }


    private int getInt( final String     colName,
                        final int        defaultValue,
                        final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            int value = _resultSet.getInt( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Integer getInteger( final String    colName )
        throws
            DatabaseException
    {
        try
        {
            Integer value = _resultSet.getInt( colName );

            return _resultSet.wasNull() ? null : value;
        }
        catch( SQLException ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Integer getInteger( final int    colNumber )
        throws
            DatabaseException
    {
        try
        {
            Integer value = _resultSet.getInt( colNumber );

            return _resultSet.wasNull() ? null : value;
        }
        catch( SQLException ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }


    //
    //    If null, this defaults to minus 1.
    //
    public double getDouble( final String    colName )
        throws
            DatabaseException
    {
        return getDouble( colName, -1, false );
    }


    public double getDouble( final String    colName,
                             final double    defaultValue )
        throws
            DatabaseException
    {
        return getDouble( colName, defaultValue, true );
    }


    private double getDouble( final String     colName,
                              final double     defaultValue,
                              final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            double value = _resultSet.getDouble( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Double getDoubleObj( final String     colName )
        throws
            DatabaseException
    {
        try
        {
            double value = _resultSet.getDouble( colName );

            return _resultSet.wasNull() ? null : Double.valueOf( value );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    //
    //    If null, this defaults to minus 1.
    //
    public float getFloat( final String    colName )
        throws
            DatabaseException
    {
        return getFloat( colName, -1f, false );
    }


    public float getFloat( final String    colName,
                           final float     defaultValue )
        throws
            DatabaseException
    {
        return getFloat( colName, defaultValue, true );
    }


    private float getFloat( final String     colName,
                            final float      defaultValue,
                            final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            float    value = _resultSet.getFloat( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Float getFloatObj( final String     colName )
        throws
            DatabaseException
    {
        try
        {
            float    value = _resultSet.getFloat( colName );

            return _resultSet.wasNull() ? null : Float.valueOf( value );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    /**
     *    Returns 0 if string in colName is null, first char otherwise
     */
    public char getChar( final String    colName )
        throws
            DatabaseException
    {
        String string = getString( colName, null, false );

        return string == null ? 0 : string.charAt( 0 );
    }


    //
    //    If null, this defaults to zero.
    //
    public String getString( final String    colName )
        throws
            DatabaseException
    {
        return getString( colName, null, false );
    }


    public String getString( final String    colName,
                             final String    defaultValue )
        throws
            DatabaseException
    {
        return getString( colName, defaultValue, true );
    }


    public String getString( final int    colNumber )
        throws
            DatabaseException
    {
        return getString( colNumber, null, false );
    }


    public String getString( final int       colNumber,
                             final String    defaultValue )
        throws
            DatabaseException
    {
        return getString( colNumber, defaultValue, true );
    }


    private String getString( final int        colNumber,
                              final String     defaultValue,
                              final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            String    value = _resultSet.getString( colNumber );

            //
            //    Should trim Strings in case they came from a char column.
            //    Because if so they will be padded with spaces that we don't
            //    want.
            //
            //    I use stripEnd here which is like an rtrim()
            //
            if ( value != null )
            {
                value = StringUtils.stripEnd( value, null );
            }

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }


    private String getString( final String     colName,
                              final String     defaultValue,
                              final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            String    value = _resultSet.getString( colName );

            //
            //    Should trim Strings in case they came from a char column.
            //    Because if so they will be padded with spaces that we don't
            //    want.
            //
            //    I use stripEnd here which is like an rtrim()
            //
            if ( value != null )
            {
                value = StringUtils.stripEnd( value, null );
            }

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public boolean getBoolean( final int    colNumber )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getBoolean( colNumber );
        }
        catch( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }


    //
    //    If null, this defaults to zero
    //
    public boolean getBoolean( final String    colName )
        throws
            DatabaseException
    {
        return getBoolean( colName, false, false );
    }


    public boolean getBoolean( final String     colName,
                               final boolean    defaultValue )
        throws
            DatabaseException
    {
        return getBoolean( colName, defaultValue, true );
    }


    private boolean getBoolean( final String      colName,
                                final boolean     defaultValue,
                                final boolean     defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            boolean value = _resultSet.getBoolean( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Date getDate( final String    colName )
        throws
            DatabaseException
    {
        try
        {
            return _resultSet.getTimestamp( colName );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Date getDate( final String     colName,
                         final Date       defaultDate )
        throws
            DatabaseException
    {
        try
        {
            Date    tmp = _resultSet.getTimestamp( colName );

            if ( tmp == null )
            {
                return defaultDate;
            }

            return tmp;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public DateTime getDateTime( final String    colName )
        throws
            DatabaseException
    {
        try
        {
            Timestamp datetime = _resultSet.getTimestamp(colName); 
            if (datetime == null) {
                return null;
            }
            return new DateTime(datetime);
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    /**
     *    Fetches an sql ARRAY of integers from the database. Note that this
     *    will return null if the array is null.
     *
     *    @param colName
     *    @return a list of integers, or null
     *    @throws DatabaseException
     */
    public List<Integer> getIntArray( final String     colName )
        throws
            DatabaseException
    {
        try
        {
            Array    array =  _resultSet.getArray( colName );

            if( array == null )
            {
                return null;
            }

            ResultSet    resultSet = array.getResultSet();

            ArrayList<Integer>    vals = new ArrayList<Integer>();

            while( resultSet.next() )
            {
                vals.add( resultSet.getInt( 2 ) );
            }

            return vals;
        }

        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }



    //
    //    If null, this defaults to zero
    //
    public Object getObject( final String    colName )
        throws
            DatabaseException
    {
        return getObject( colName, null, false );
    }


    public Object getObject( final String    colName,
                             final Object    defaultValue )
        throws
            DatabaseException
    {
        return getObject( colName, defaultValue, true );
    }


    private Object getObject( final String     colName,
                              final Object     defaultValue,
                              final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            Object value = _resultSet.getObject( colName );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colName, ex );
        }
    }


    public Object getObject( final int    colNumber )
        throws
            DatabaseException
    {
        return getObject( colNumber, null, false );
    }


    public Object getObject( final int       colNumber,
                             final Object    defaultValue )
        throws
            DatabaseException
    {
        return getObject( colNumber, defaultValue, true );
    }


    private Object getObject( final int        colNumber,
                              final Object     defaultValue,
                              final boolean    defaultSpecified )
        throws
            DatabaseException
    {
        try
        {
            Object    value = _resultSet.getObject( colNumber );

            //
            //    If the default was specified then we will
            //    use that if the last value was null.  Otherwise,
            //    we will use the default value inherent in the
            //    ResultSet call.
            //
            if ( defaultSpecified && _resultSet.wasNull() )
            {
                return defaultValue;
            }

            return value;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantReadColumnValue( colNumber, ex );
        }
    }

    
    public Vector<Vector<Object>> getDataVector()
        throws
            DatabaseException
    {
        try
        {
            Vector<Vector<Object>>    vector = new Vector<Vector<Object>>();
            Vector<Object>    rowVector;
            int       colCount = getColumnCount();
            while ( next() )
            {
                rowVector = new Vector<Object>( colCount );
                for ( int ii=0; ii < colCount; ii++ )
                {
                    //
                    //    resultset is 1 based instead of zero for
                    //    some reason.
                    //
                    rowVector.add( _resultSet.getObject( ii + 1 ) );
                }

                vector.add( rowVector );
            }

            return vector;
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException().cantGetVectorData( ex );
        }
    }


    /**
     *    This was originally written to extract calendar
     *    objects from data stored as Strings with the format
     *    YYYY-MM-DD.  May need to be modified for other uses.
     */
    public Calendar getCalendar( final String    colName )
        throws
            DatabaseException
    {
        String    value = getString( colName );

        if ( value == null )
        {
            return null;
        }

        Calendar    calendar = Calendar.getInstance();

        try
        {
            calendar.setTime( _dateFormatter.parse( value ) );
        }
        catch ( Throwable    ex )
        {
            throw new RecordSetException()
                .cantParseStringIntoDate( value, ex );
        }

        return calendar;
    }


    /**
     *    @returns the current row as a map keyed on the name
     *    of the columns.
     */
    public Map<String, Object> toMap()
        throws
            DatabaseException
    {
        return toMap( null );
    }


    public Map<String, Object> toMap( final MapColumnTranslator    translator )
        throws
            DatabaseException

    {
        final int    columnCount = getColumnCount();
        Map<String, Object>    map = new HashMap<String, Object>( columnCount );

        for ( int ii = 1; ii <= columnCount; ii++ )
        {
            String    columnName = getColumnName( ii );
            Object    columnValue = getObject( ii );

            if( translator != null )
            {
                columnValue = translator.transformValue( columnName, columnValue );

                if( columnValue == MapColumnTranslator.DO_NOT_ADD )
                {
                    continue;
                }
            }

            map.put( columnName, columnValue );
        }

        return map;
    }
}
