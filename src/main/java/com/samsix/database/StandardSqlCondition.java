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



public class StandardSqlCondition
    implements
        SqlCondition
{
    private final String             _value;
    private final SqlRelationType    _relation;
    private final SqlColumn          _column;
    private String                   _function;


    public StandardSqlCondition( final SqlColumn    column,
                                 final String       value )
    {
        this( column, SqlRelationType.EQUAL, value );
    }



    public StandardSqlCondition( final SqlColumn          column,
                                 final SqlRelationType    relation,
                                 final String             value )
    {
        _column    = column;
        _relation  = relation;
        _value     = value;
    }



    /**
     *    Set any function that you want the column to be passed into before
     *    making the comparison.  e.g. "lower" if you want to do a case-insensitive
     *    search.
     */
    public void setFunction( final String    function )
    {
        _function = function;
    }



    //========================================
    //
    //    SqlCondition interface
    //
    //========================================

    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        if ( _function != null )
        {
            buffer.append( _function )
                  .append( "( " );
        }

        buffer.append( _column.getSqlReference() )
              .append( " " );

        if ( _function != null )
        {
            buffer.append( ") " );
        }

        //
        //    If our relation is IS or IS_NOT then we just ignore the value
        //    because it has to be NULL.
        //
        if ( _relation.equals( SqlRelationType.IS ) )
        {
            buffer.append( _relation ).append( " " ).append( _value );
        }
        else if ( _value == null )
        {
            addStandardNullValue( buffer, _relation );
        }
        else
        {
            buffer.append( _relation )
                  .append( " " )
                  .append( _column.formatValue( _value ) );
        }
    }


    public static void addStandardNullValue( final StringBuffer buffer, final SqlRelationType relation )
    {
        //
        //    If value is null then we convert our relationship type to an
        //    appropriate one and search on null.
        //    Chris and Ken wrote this but then couldn't decide whether to
        //    leave it in.  Going to comment it out until we decide if we need it.
        //    Lucas decided we need it. Or at least, it makes his life easier.
        //
        buffer.append( SqlRelationType.IS );

        switch( relation ) {
        case EQUAL:
        case IS:
            buffer.append( " NULL" );
            break;

        case NOT_EQUAL:
            buffer.append( " NOT NULL" );
            break;

        default:
            throw new IllegalArgumentException( "Illegal null value used with " + relation );
        }
    }


    //========================================
    //
    //    Object interface
    //
    //========================================

    @Override
    public String toString()
    {
        return "column: [" + _column + "; relation: " + _relation + "]; " +
        	   "value: " + _value;
    }
}
