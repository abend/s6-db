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


import java.awt.geom.Rectangle2D;
import java.util.Collection;

import com.samsix.util.string.StringUtilities;
import com.samsix.util.string.ToStringWrapper;


public abstract class SqlConditionContainer
{
    public abstract void addCondition( SqlCondition    condition );


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlColumnType      columnType,
                              final SqlRelationType    relation,
                              final String             value )
    {
        addCondition( new SqlColumn( table, column, columnType ),
                      relation,
                      value );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlRelationType    relation,
                              final String             value )
    {
        addCondition( table, column, SqlColumnType.TEXT, relation, value );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlRelationType    relation,
                              final int                value )
    {
        addCondition( new SqlColumn( table, column, SqlColumnType.INT ),
                      relation,
                      String.valueOf( value ) );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlRelationType    relation,
                              final long               value )
    {
        addCondition( new SqlColumn( table, column, SqlColumnType.INT ),
                      relation,
                      String.valueOf( value ) );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlRelationType    relation,
                              final double             value )
    {
        addCondition( new SqlColumn( table, column, SqlColumnType.DOUBLE ),
                      relation,
                      String.valueOf( value ) );
    }


    public void addWildcardCondition( final SqlTable    table,
                                      final String      column,
                                      final String      value )
    {
        String    newValue;
        SqlRelationType    relation = SqlRelationType.EQUAL;
        if ( value.indexOf( "*" ) >= 0 )
        {
            newValue = value.replace( '*', '%' );
            relation = SqlRelationType.LIKE;
        }
        else
        {
            newValue = value;
        }

        addCondition( table, column, relation, newValue );
    }


    public void addBoxCondition( final SqlTable           table,
                                 final String             column,
                                 final SqlRelationType    relation,
                                 final Rectangle2D        value )
    {
        StringBuffer valueBuffer = new StringBuffer( 128 );

        //
        //    Using box data type inherent to postgres
        //
        valueBuffer.append( "'((" )
                   .append( value.getX() + value.getWidth() )
                   .append( ", " )
                   .append( value.getY() + value.getHeight() )
                   .append( "), (" )
                   .append( value.getX() )
                   .append( ", " )
                   .append( value.getY() )
                   .append( "))'" );

        addCondition( new SqlColumn( table, column, SqlColumnType.OTHER ),
                      relation,
                      valueBuffer.toString() );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final SqlRelationType    relation,
                              final double             value,
                              final String             cast )
    {
        addCondition( new SqlColumn( table, column, SqlColumnType.DOUBLE ),
                      relation,
                      String.valueOf( value ) + cast );
    }


    public void addCondition( final SqlTable           table,
                              final String             column,
                              final boolean            value )
    {
        addCondition( new SqlColumn( table, column, SqlColumnType.BOOLEAN ),
                      SqlRelationType.EQUAL,
                      String.valueOf( value ) );
    }


    public void addCondition( final SqlColumn          column,
                              final SqlRelationType    relation,
                              final String             value )
    {
        addCondition( column, relation, value, null );
    }


    public void addCondition( final SqlColumn          column,
                              final SqlRelationType    relation,
                              final String             value,
                              final String             function )
    {
        StandardSqlCondition    condition;

        condition = new StandardSqlCondition( column, relation, value );
        condition.setFunction( function );

        addCondition( condition );
    }


    public <T> void addInCondition( final SqlTable         table,
                                    final String           column,
                                    final Collection<T>    values,
                                    final SqlColumnType    type )
    {
        ToStringWrapper<T>    wrapper = null;

        if( type == SqlColumnType.OTHER || type == SqlColumnType.TEXT ||
            type == SqlColumnType.TIMESTAMP )
        {
            wrapper = new ToStringWrapper<T>()
            {
                @Override
                public String toString( final T    source )
                {
                    if( source == null )
                    {
                        return "null";
                    }
                    else
                    {
                        return StringUtilities.wrapQuotes( source.toString() );
                    }
                }
            };
        }

        addCondition( new SqlColumn( table, column, SqlColumnType.OTHER ),
                      SqlRelationType.IN,
                      "("
                      + StringUtilities.collectionToString( values, ", ", wrapper )
                      + ")" );
    }
}
