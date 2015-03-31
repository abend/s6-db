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


/**
 *    An object oriented way of building sql statements.  The api
 *    is oriented around a main table, which is the primary focus
 *    of the query, though joins to other tables may be used.
 *    Complex where clauses may be built using conditions such as
 *    <code>IS</code>, <code>LIKE</code>, <code>less than</code>,
 *    etc.
 */
public class SqlStatement
    extends
        SqlConditionContainer
{
    private static final String  DELIMITER_COMMA = ",\n\t";
    public  static final String  DELIMITER_AND   = "\n\tAND ";
    public  static final String  DELIMITER_JOIN  = "\n\t";
    public  static final String  DELIMITER_OR    = "\n\tOR ";


    private final List<SqlColumn>        _listSelect = new LinkedList<SqlColumn>();
    private String                       _selectString = null;
    private       GroupedSqlCondition    _conditionGroup = new GroupedSqlCondition();
    private final SqlTable               _mainTable;
    private final List<SqlJoin>          _innerJoins = new LinkedList<SqlJoin>();
    private final List<SqlJoin>          _leftOuterJoins = new LinkedList<SqlJoin>();
    private boolean                      _selectDistinct = false;

    //    
    //    For now I'm just implementing this as a string.
    //    Could get fancy later and make it a list of
    //    special order by types.
    //    
    private String                 _orderBy = null;
    
    private Integer                _limit = null;


    /**
     *    Sometimes its just easier to list the items you are selecting
     *    by writing out the string.  You can still add them using addSelect
     *    and, in fact, you can mix and match, but this just gives you the
     *    ability to specify it more simply since the select string is usually
     *    constant.  Usually just the conditions of the query change.
     */
    public SqlStatement( final SqlTable    mainTable,
                         final String      selectString )
    {
        _mainTable    = mainTable;
        _selectString = selectString;
    }


    public SqlStatement( final SqlTable    mainTable )
    {
        _mainTable = mainTable;
    }
    
    
    public SqlStatement( final String    tableName,
                         final String    tableAlias,
                         final String    selectString )
    {
        _mainTable    = new SqlTable( tableName, tableAlias );
        _selectString = selectString;
    }
    
    
    public SqlTable getMainTable()
    {
        return _mainTable;
    }


    public void setSelectString( final String    selectString )
    {
        _selectString = selectString;
    }


    public void appendSelectString( final String    selectStringChunk )
    {
        if ( _selectString == null )
        {
            _selectString = selectStringChunk;
        }
        else
        {
            _selectString += ", " + selectStringChunk;
        }
    }


    public void addSelect( final SqlColumn    column )
    {
        _listSelect.add( column );
    }


    public SqlJoin addLeftOuterJoin( final SqlTable    table1,
                                     final SqlTable    table2,
                                     final String      columnName )
    {
        return addLeftOuterJoin( table1, table2, columnName, columnName );
    }


    public SqlJoin addLeftOuterJoin( final SqlTable    table1,
                                     final SqlTable    table2,
                                     final String      column1Name,
                                     final String      column2Name )
    {
        SqlJoin    join;
        join = new SqlJoin( table1, table2, column1Name, column2Name, true );

        _leftOuterJoins.add( join );

        return join;
    }


    public SqlJoin addInnerJoin( final SqlTable    table1,
                                 final SqlTable    table2,
                                 final String      columnName )
    {
        return addInnerJoin( table1, table2, columnName, columnName );
    }


    /**
     *    Specify primaryColumnName if different.
     */
    public SqlJoin addInnerJoin( final SqlTable    table1,
                                 final SqlTable    table2,
                                 final String      column1Name,
                                 final String      column2Name )
    {
        SqlJoin    join = new SqlJoin( table1, table2, column1Name, column2Name );

        _innerJoins.add( join );

        return join;
    }

    
    public SqlTable findTable( final String    tableName )
    {
        if ( getMainTable().getName().equals( tableName ) )
        {
            return getMainTable();
        }

        //    
        //    Now look for table in INNER JOINS.  Don't think
        //    we need to look in OUTER JOINS but if someone finds
        //    this necessary in the future just add it.
        //  
        for( SqlJoin join : _innerJoins )
        {
            if ( join.getTable2().getName().equals( tableName ) )
            {
                return join.getTable2();
            }
        }

        return null;
    }


    public void setSelectDistinct( final boolean    selectDistinct )
    {
        _selectDistinct = selectDistinct;
    }


    @Override
    public void addCondition( final SqlCondition    condition )
    {
        _conditionGroup.addCondition( condition );
    }


    public void setOrderBy( final String     orderBy )
    {
        _orderBy = orderBy;
    }


    public void setLimit( final int    limit )
    {
        _limit = limit;
    }

    
    public String getSql()
    {
        StringBuffer    buffer = new StringBuffer( 128 );

        //
        //    Add the SELECT clause
        //
        buffer.append( "SELECT " );

        if ( _selectDistinct )
        {
            buffer.append( "DISTINCT " );
        }

        if ( _listSelect.size() == 0 && _selectString == null )
        {
            buffer.append( "*" );
        }
        else
        {
            if ( _selectString != null )
            {
                buffer.append( _selectString );

                //    
                //    If we are going to add more selects then
                //    we need the comma.
                //    
                if ( _listSelect.size() > 0 )
                {
                    buffer.append( ", " );
                }
            }
            
            if ( _listSelect.size() > 0 )
            {
                appendClause( buffer, _listSelect, DELIMITER_COMMA );
            }
        }


        //
        //    Add the FROM clause
        //
        buffer.append( "\n\tFROM " );
        getMainTable().appendToSqlBuffer( buffer );

        //    
        //    Add Inner Joins
        //    
        appendClause( buffer, _innerJoins, DELIMITER_JOIN, true );

        //    
        //    Now add the outer joins after ALL of the inner joins
        //    because the optimizer likes that better it seems.  The circuit
        //    queries that rely on outer joins don't work unless the outer
        //    joins come last.
        //    
        appendClause( buffer, _leftOuterJoins, DELIMITER_JOIN, true );

        //
        //    Add Where Clause
        //
        if ( ! _conditionGroup.isEmpty() )
        {
            buffer.append( "\n\tWHERE " );

            _conditionGroup.appendToSqlBuffer( buffer );
        }

        if ( _orderBy != null )
        {
            buffer.append( "\n\tORDER BY " ).append( _orderBy );
        }

        if ( _limit != null )
        {
            buffer.append( "\n\tLIMIT " ).append( _limit );
        }

        return buffer.toString();
    }


    private void appendClause( final StringBuffer                    buffer,
                               final List<? extends SqlComponent>    components,
                               final String                          delimiter )
    {
        appendClause( buffer, components, delimiter, false );
    }


    private void appendClause( final StringBuffer                    buffer,
                               final List<? extends SqlComponent>    components,
                               final String                          delimiter,
                               final boolean                         delimitFirst )
    {
        boolean    isFirst = true;
        
        for ( SqlComponent    component : components )
        {
            if ( ! isFirst || delimitFirst )
            {
                buffer.append( delimiter );
            }
            
            component.appendToSqlBuffer( buffer );
            
            isFirst = false;
        }
    }
    
    
    public SqlStatement copy()
    {
        SqlStatement    sql = new SqlStatement( _mainTable, _selectString );
        
        sql._listSelect.addAll( sql._listSelect );
        sql._conditionGroup = _conditionGroup.copy();
        sql._innerJoins.addAll( _innerJoins );
        sql._leftOuterJoins.addAll( _leftOuterJoins );
        sql._selectDistinct = _selectDistinct;
        sql._orderBy = _orderBy;
        sql._limit = _limit;
        
        return sql;
    }

    
    @Override
    public String toString()
    {
        return getSql();
    }
}
