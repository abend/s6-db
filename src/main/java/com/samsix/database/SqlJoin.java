/*
 ***************************************************************************
 *
 * Copyright (c) 2001-2008 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;


import java.util.LinkedList;
import java.util.List;

import com.samsix.util.string.StringPair;


public class SqlJoin
    implements
        SqlComponent
{
    private  static final String  DELIMITER_AND   = " AND ";


    private final SqlTable            _table1;
    private final SqlTable            _table2;
    private final List<StringPair>    _conditions;
    private boolean                   _isLeftOuter = false;



    public SqlJoin( final SqlTable    table1,
                    final SqlTable    table2,
                    final String      columnName )
    {
        this( table1, table2, columnName, false );
    }



    public SqlJoin( final SqlTable    table1,
                    final SqlTable    table2,
                    final String      column1Name,
                    final String      column2Name )
    {
        this( table1, table2, column1Name, column2Name, false );
    }



    public SqlJoin( final SqlTable    table1,
                    final SqlTable    table2,
                    final String      columnName,
                    final boolean     isLeftOuter )
    {
        this( table1, table2, columnName, columnName, isLeftOuter );
    }



    public SqlJoin( final SqlTable    table1,
                    final SqlTable    table2,
                    final String      column1Name,
                    final String      column2Name,
                    final boolean     isLeftOuter )
    {
        _table1      = table1;
        _table2      = table2;
        _isLeftOuter = isLeftOuter;

        _conditions = new LinkedList<StringPair>();

        addCondition( column1Name, column2Name );
    }



    public void addCondition( final String    columnName )
    {
        addCondition( columnName, columnName );
    }



    public void addCondition( final String    column1Name,
                              final String    column2Name )
    {
        _conditions.add( new StringPair( column1Name, column2Name ) );
    }



    public SqlTable getTable2()
    {
        return _table2;
    }



    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        if ( _isLeftOuter )
        {
            buffer.append( "LEFT OUTER JOIN " );
        }
        else
        {
            buffer.append( "INNER JOIN " );
        }

        _table2.appendToSqlBuffer( buffer );

        buffer.append( " ON ( " );

        SqlColumn     column1;
        SqlColumn     column2;
        boolean       isFirst;

        isFirst   = true;

        for ( StringPair condition : _conditions )
        {
            if ( ! isFirst )
            {
                buffer.append( DELIMITER_AND );
            }

            column1 = new SqlColumn( _table1, condition.getKey() );
            column2 = new SqlColumn( _table2, condition.getValue() );

            buffer.append( column2.getSqlReference() )
                .append( " = " )
                .append( column1.getSqlReference() );

            isFirst = false;
        }

        buffer.append( " )" );
    }
}
