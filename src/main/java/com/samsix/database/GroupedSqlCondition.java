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


import java.util.LinkedList;
import java.util.List;



public class GroupedSqlCondition
    extends
        SqlConditionContainer
    implements
        SqlCondition
{
    public static enum Delimiter
    {
        AND, OR;
    }


    private final    Delimiter             _delimiter;
    private          boolean               _isNotCondition;
    private final    List<SqlCondition>    _listConditions;


    /**
     *    Sql Conditions grouped by AND's.
     */
    public GroupedSqlCondition()
    {
        this( Delimiter.AND );
    }



    /**
     *    Sql Conditions grouped by AND's or OR's
     *    depending on the value of the parameter isAndGrouped.
     */
    public GroupedSqlCondition( final Delimiter    delimiter )
    {
        _delimiter = delimiter;

        _listConditions = new LinkedList<SqlCondition>();
    }



    @Override
    public void addCondition( final SqlCondition    condition )
    {
        _listConditions.add( condition );
    }



    public boolean isEmpty()
    {
        return ( _listConditions.size() == 0 );
    }



    public void setIsNotCondition( final boolean    isNotCondition )
    {
        _isNotCondition = isNotCondition;
    }


    public GroupedSqlCondition copy()
    {
        GroupedSqlCondition    condition = new GroupedSqlCondition( _delimiter );

        condition._listConditions.addAll( _listConditions );
        condition._isNotCondition = _isNotCondition;

        return condition;
    }


    //========================================
    //
    //    SqlCondition i/f
    //
    //========================================

    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        int    size = _listConditions.size();
        if ( size == 0 )
        {
            return;
        }

        boolean  isFirst = true;

        if ( _isNotCondition )
        {
            buffer.append( "NOT " );
        }

        if ( size > 1 )
        {
            buffer.append( "( " );
        }

        for( SqlCondition condition : _listConditions )
        {
            if ( ! isFirst )
            {
                switch( _delimiter )
                {
                case AND:
                    buffer.append( SqlStatement.DELIMITER_AND );
                    break;

                case OR:
                    buffer.append( SqlStatement.DELIMITER_OR );
                    break;
                }
            }

            condition.appendToSqlBuffer( buffer );

            isFirst = false;
        }

        if ( size > 1 )
        {
            buffer.append( " )" );
        }
    }
}
