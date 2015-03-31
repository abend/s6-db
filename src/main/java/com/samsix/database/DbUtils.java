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



public class DbUtils
{
    private DbUtils()
    {
        //    do not instantiate.
    }


    public static SqlTable getTable( final Database    db,
                                     final String      schema,
                                     final String      tableName )
        throws
            DatabaseException
    {
        String    sql;
        sql = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_schema = '"
              + schema.toLowerCase()
              + "' AND table_name = '"
              + tableName.toLowerCase()
              + "' ORDER BY ordinal_position";

        RecordSet    recordSet = db.getRecordSet( sql );

        SqlTable    table = new SqlTable( schema + "." + tableName );

        boolean   noRows = true;

        while ( recordSet.next() )
        {
            noRows = false;

            table.addColumn( getColumn( recordSet, table ) );
        }

        //
        //    If we had no rows the table must not exist.  Return null.
        //
        if ( noRows )
        {
            return null;
        }

        return table;
    }


    public static SqlColumn getColumn( final RecordSet    recordSet,
    			                       final SqlTable     table )
        throws
        	DatabaseException
    {
        SqlColumnType    type;
        type = SqlColumnType.fromString( recordSet.getString( "data_type" ) );

        SqlColumn    column;
        column = new SqlColumn( table,
                                recordSet.getString( "column_name" ),
                                type );

        column.setCharMaxLength( recordSet.getInteger( "character_maximum_length" ) );

        return column;
    }


    public static void createTable( final Database    db,
                                    final SqlTable    table )
        throws
            DatabaseException
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( "CREATE TABLE " )
              .append( table.getFullName() )
              .append( "\n(" );

        String    pKeySql = null;
        boolean    isFirst = true;

        for ( SqlColumn    pKey : table.getPrimaryKey() )
        {
            //
            //    Append the primary key columns first
            //
            appendColToCreateTable( buffer, pKey, isFirst );

            if ( isFirst )
            {
                isFirst = false;
                pKeySql = "ALTER TABLE " + table.getSqlReference() + " ADD PRIMARY KEY (";
            }
            else
            {
                pKeySql += ", ";
            }

            pKeySql += pKey.getName();
        }

        if ( pKeySql != null )
        {
            pKeySql += ")";
        }


        for ( SqlColumn    column : table.getColumns() )
        {
            appendColToCreateTable( buffer, column, isFirst );

           isFirst = false;
        }

        buffer.append( "\n)" );

        db.execute( buffer.toString() );

        //
        //   Now run the primary key sql
        //
        if ( pKeySql != null )
        {
            db.execute( pKeySql );
        }
    }


    private static void appendColToCreateTable( final StringBuffer    buffer,
                                                final SqlColumn       column,
                                                final boolean         isFirst )
    {
        if ( ! isFirst )
        {
            buffer.append( "," );
        }

        buffer.append( "\n\t" )
              .append( column.getName() )
              .append( "\t" )
              .append( column.getType().getDbValue() );

       if ( column.getCharMaxLength() != null )
       {
           buffer.append( "(" ).append( column.getCharMaxLength() ).append( ")" );
       }

       if ( column.getConstraints() != null )
       {
           buffer.append( "\t" )
                 .append( column.getConstraints() );
       }
    }
}
