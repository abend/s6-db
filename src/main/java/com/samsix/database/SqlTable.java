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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.samsix.util.string.StringUtilities;



public class SqlTable
    implements
        SqlComponent
{
    private final String      _schema;
    private final String      _name;
    private final String      _alias;

    //
    //    Need a list separate from the map because we want to preserve the order
    //    of the columns for table creation and things like that.
    //
    private final List<SqlColumn>    _sqlColumns;
    private final List<SqlColumn>    _primaryKey;
    private final Map<String, SqlColumn>    _colMap;


    public SqlTable( final String      name )
    {
        this( name, null );
    }


    public SqlTable( final String      name,
                     final String      alias )
    {
        this( (String) null, name, alias );
    }


    public SqlTable( final String    schema,
                     final String    name,
                     final String    alias )
    {
//        return new SqlTable( schema + "." + name, alias );
        _schema = schema;
        _alias  = alias;
        _name   = name;

        _sqlColumns = new ArrayList<SqlColumn>();
        _primaryKey = new ArrayList<SqlColumn>();
        _colMap = new HashMap<String, SqlColumn>();
    }


    public SqlTable( final SqlTable    table,
                     final String      schema,
                     final String      alias )
    {
        _schema = schema;
        _alias  = alias;

        _name       = table._name;
        _sqlColumns = table._sqlColumns;
        _primaryKey = table._primaryKey;
        _colMap     = table._colMap;
    }


//
//
//    public static SqlTable schemaValueOf( final String      schema,
//                                          final SqlTable    table )
//    {
//        SqlTable    newTable = new SqlTable( schema + "." + table._name,
//                                             table._alias );
//
//        newTable._sqlColumns.addAll( table._sqlColumns );
//        newTable._primaryKey.addAll( table._primaryKey );
//
//        return newTable;
//    }


    public String getSchema()
    {
        return _schema;
    }


    public String getName()
    {
        return _name;
    }


    /**
     *     This is only returning name right now but in anticipation of
     *     some day having the name be split into it's schema and tablename
     *     separately this function will still be here so that it can
     *     be changed to schema.tablename and know that we aren't breaking
     *     code that was expecting only the name.
     */
    public String getFullName()
    {
        if ( _schema == null )
        {
            return _name;
        }

        return _schema + "." + _name;
    }


    public String getAlias()
    {
        return _alias;
    }


    public String getSqlReference()
    {
        if ( ! StringUtilities.isNullOrEmptyOrBlank( _alias ) )
        {
            return _alias;
        }

        return getFullName();
    }


    public SqlColumn getColumn( final String    colName )
    {
        return _colMap.get( colName.toLowerCase() );
    }


    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        buffer.append( getFullName() );

        if ( ! StringUtilities.isNullOrEmpty( _alias ) )
        {
            buffer.append( " " )
                .append( _alias );
        }
    }


    public void addColumn( final SqlColumn    column )
    {
        column.setTable( this );

        _sqlColumns.add( column );
        _colMap.put( column.getName().toLowerCase(), column );
    }


    public void addPrimaryKeyColumn( final SqlColumn    column )
    {
        _primaryKey.add( column );
        _colMap.put( column.getName().toLowerCase(), column );
    }


    public List<SqlColumn> getPrimaryKey()
    {
        return _primaryKey;
    }


    public boolean isPartOfPrimaryKey( final String    colName )
    {
        for ( SqlColumn    column : _primaryKey )
        {
            if ( column.getName().equalsIgnoreCase( colName ) )
            {
                return true;
            }
        }

        return false;
    }


    public List<SqlColumn> getColumns()
    {
        return _sqlColumns;
    }


//     public SqlColumn getColumn( int    colNum )
//     {
//         if ( colNum < 0 || colNum >= getColumnCount() )
//         {
//             return null;
//         }

//         return (SqlColumn) _sqlColumns.get( colNum );
//     }



//     public int getColumnCount()
//     {
//         return _sqlColumns.size();
//     }


    //========================================
    //
    //    Object i/f
    //
    //========================================

    @Override
    public boolean equals( final Object    obj )
    {
        if ( ! ( obj instanceof SqlTable ) )
        {
            return false;
        }

        SqlTable    rhs = (SqlTable) obj;
        return new EqualsBuilder()
            .append( _schema, rhs._schema )
            .append( _name, rhs._name )
            .append( _alias, rhs._alias )
            .isEquals();
    }


    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
            .append( _schema )
            .append( _name )
            .append( _alias )
            .toHashCode();
    }


    @Override
    public String toString()
    {
        return "schema: " + _schema + "; name: " + _name + "; alias: " + _alias;
    }
}
