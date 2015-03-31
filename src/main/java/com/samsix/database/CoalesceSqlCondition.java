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


/**
 * An SQL condition that takes any number of columns, generating something like
 * "COALESCE(a.col1,b.col2) = 'value'"
 */
public class CoalesceSqlCondition
    implements
        SqlCondition
{
    private final String             _value;
    private final SqlRelationType    _relation;
    private final SqlColumn[]        _columns;


    public CoalesceSqlCondition( final SqlRelationType    relation,
                                 final String             value,
                                 final SqlColumn      ... columns )
    {
        _columns   = columns;
        _relation  = relation;
        _value     = value;

        // sanity check column types, they must be the same
        SqlColumnType type = null;
        for( SqlColumn column : columns ) {
            if( type == null ) {
                type = column.getType();
            }
            else if( type != column.getType() ) {
                throw new IllegalArgumentException( "Types in coalesce condition must be equal" );
            }
        }
    }


    //========================================
    //
    //    SqlCondition interface
    //
    //========================================

    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        // build COALESCE(a,b..) statement
        buffer.append( "COALESCE(" );
        for(int idx = 0; idx < _columns.length; idx++) {
            if(idx > 0) {
                buffer.append( ", " );
            }

            buffer.append( _columns[idx].getSqlReference() );
        }
        buffer.append( ") " );

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
            StandardSqlCondition.addStandardNullValue( buffer, _relation );
        }
        else
        {
            buffer.append( _relation )
                  .append( " " )
                  .append( _columns[0].formatValue( _value ) );
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
        return "columns: [" + _columns + "; relation: " + _relation + "]; " +
               "value: " + _value;
    }
}
