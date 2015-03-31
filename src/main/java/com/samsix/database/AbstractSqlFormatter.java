/*
 ***************************************************************************
 *
 * Copyright (c) 2001-2012 Sam Six.  All rights reserved.
 *
 * Company:      http://www.samsix.com
 *
 ***************************************************************************
 */
package com.samsix.database;


import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import com.samsix.util.string.StringUtilities;
import com.samsix.util.string.ToStringWrapper;


public abstract class AbstractSqlFormatter
    implements
        SqlFormatter
{
    /**
     *    This constant, when used as a Date parameter in any subclass,
     *    will tell the server to use its own current date (the now() function)
     */
    public final static Date DATE_NOW = new Date(-1)
    {
        private static final long serialVersionUID = 2191831066696256354L;

        @Override
        public boolean equals( final Object    obj )
        {
            return obj == this;
        }
    };


    @Override
    public SqlFormatter append( final String    column,
                                final String    value )
    {
        return append( column, value, true );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final int       value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Integer   value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final long      value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Long      value )
    {
        if ( value == null )
        {
            return append( column, null, false );
        }

        return append( column, value.toString(), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Date      when )
    {
        if ( when == null )
        {
            return append( column, null, true );
        }

        //
        //    If it's the special value by identity, use now() instead
        //
        if( when == DATE_NOW )
        {
            return append( column, "now()", false );
        }

        //
        //    Date toString() method on some computers formats the
        //    date as ...
        //    'Wed Mar 08 16:41:20 GMT-08:00 2006'
        //    ... which doesn't seem to be liked by postgres.
        //    So we have to use the java.text.DateFormat class.
        //
        return append( column,
                       StringUtilities.getDbDateFormatter().format( when ),
                       true );
    }



    @Override
    public SqlFormatter append( final String      column,
                                final Calendar    when )
    {
        if ( when == null )
        {
            return append( column, null, true );
        }

        return append( column, when.getTime() );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final double    value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String    column,
                                final Double    value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String     column,
                                final boolean    value )
    {
        return append( column, String.valueOf( value ), false );
    }



    @Override
    public SqlFormatter append( final String     column,
                                final Boolean    value )
    {
        return append( column, String.valueOf( value ), false );
    }


    @Override
    public SqlFormatter append( final String    column,
                                final char      value )
    {
        return append( column, String.valueOf( value ), true );
    }


    @Override
    public SqlFormatter append( final String       column,
                                final Character    value )
    {
        return append( column, String.valueOf( value ), true );
    }


    @Override
    public SqlFormatter appendParameter( final String    column )
    {
        return append( column, "?", false );
    }


    /**
     *    This method is just for use with the append( Map )
     *    method since it always uses the append( colName, Object )
     *    type method and here the prefix acts like the colName.
     *    MUST KEEP THIS METHOD
     */
    private SqlFormatter append( final String      prefix,
                                 final DBRepresentable    objectId )
    {
        return append( objectId, prefix );
    }


    public SqlFormatter append( final String    column,
                                final String    values[] )
    {
        String    value = null;

        if( values != null )
        {
            ToStringWrapper<String>    wrapper = new ToStringWrapper<String>()
            {

                @Override
                public String toString( final String    source )
                {
                    if( source == null )
                    {
                        return "NULL";
                    }
                    else
                    {
                        return StringUtilities.wrapQuotes( source );
                    }
                }

            };

            value = "ARRAY["
                    + StringUtilities.collectionToString( Arrays.asList( values ),
                                                          wrapper )
                    + "]";
        }

        return append( column, value, false );
    }


    public void append( final String    colName,
                        final Object    value )
    {
        if ( value instanceof String )
        {
            append( colName, (String) value );
        }
        else if ( value instanceof Integer )
        {
            append( colName, (Integer) value );
        }
        else if ( value instanceof Boolean )
        {
            append( colName, (Boolean) value );
        }
        else if ( value instanceof Double )
        {
            append( colName, (Double) value );
        }
        else if ( value instanceof Long )
        {
            append( colName, (Long) value );
        }
        else if ( value instanceof DBRepresentable )
        {
            append( colName, (DBRepresentable) value );
        }
        else if ( value instanceof Date )
        {
            append( colName, (Date) value );
        }
        else if ( value instanceof Calendar )
        {
            append( colName, (Calendar) value );
        }
        else if( value instanceof Character )
        {
            append( colName, (Character) value );
        }
        else
        {
            //
            //    Assume it's a string if it doesn't match any of the above types.
            //
            append( colName, String.valueOf( value ) );
        }
    }


    @Override
    public void append( final String                tableAlias,
                        final Map<String,Object>    map )
    {
        for ( Entry<?,?> colEntry : map.entrySet() )
        {
            Object    value;
            value = map.get( colEntry.getKey() );

            String    colName;
            if ( tableAlias == null )
            {
                colName = colEntry.getKey().toString();
            }
            else
            {
                colName = tableAlias + "." + colEntry.getKey().toString();
            }

            if ( value == null )
            {
                append( colName, "NULL", false );
            }
            else
            {
                append( colName, value );
            }
        }
    }


    @Override
    public void append( final Map<String,Object>    map )
    {
        append( null, map );
    }
}
