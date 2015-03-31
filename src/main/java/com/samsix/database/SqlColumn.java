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


import org.apache.commons.lang3.builder.ToStringBuilder;

import com.samsix.util.string.StringUtilities;


public class SqlColumn
    implements
        SqlComponent
{
    private final String           _name;
    private final SqlColumnType    _type;
    private SqlTable               _table;
    private final String           _alias;
    private String                 _properName;
    private String                 _constraints;

    //
    //    Used for char and varchars
    //
    private Integer               _charMaxLength;

    private String           _prefix = null;
    private String           _suffix = null;


    public SqlColumn( final SqlTable    table,
                      final String      name )
    {
        this( table, name, SqlColumnType.OTHER, null );
    }


    public SqlColumn( final SqlTable    table,
                      final String      name,
                      final String      alias )
    {
        this( table, name, SqlColumnType.OTHER, alias );
    }


    public SqlColumn( final String           name,
                      final SqlColumnType    type )
    {
        this( null, name, type, null );
    }


    public SqlColumn( final SqlTable         table,
                      final String           name,
                      final SqlColumnType    type )
    {
        this( table, name, type, null );
    }


    public SqlColumn( final SqlTable         table,
                      final String           name,
                      final String           properName,
                      final SqlColumnType    type )
    {
        this( table, name, type, null );

        _properName = properName;
    }


    public SqlColumn( final SqlTable         table,
                      final String           name,
                      final SqlColumnType    type,
                      final String           alias )
    {
        _table = table;
        _name  = name;
        _type  = type;
        _alias = alias;
    }


    public void setTable( final SqlTable    table )
    {
        _table = table;
    }


    public String getName()
    {
        return _name;
    }


    public SqlColumnType getType()
    {
        return _type;
    }


    public String getProperName()
    {
        if ( _properName == null )
        {
            return _name;
        }

        return _properName;
    }


    public void setCharMaxLength( final Integer    charMaxLength )
    {
        _charMaxLength = charMaxLength;
    }


    public Integer getCharMaxLength()
    {
        return _charMaxLength;
    }


    public void setConstraints( final String    constraints )
    {
        _constraints = constraints;
    }


    public String getConstraints()
    {
        return _constraints;
    }


    /**
     *    The prefix and suffix are simply mechanisms to cheat
     *    in case you have an usual column (like a function
     *    call that you need to shoehorn into this structure.
     *    When the column is formatted for output the prefix
     *    and suffix are tacked on.
     */
    public void setPrefix( final String    prefix )
    {
        _prefix = prefix;
    }


    public String getPrefix()
    {
        return _prefix;
    }


    public void setSuffix( final String    suffix )
    {
        _suffix = suffix;
    }


    public String formatValue( final String    value )
    {
        if ( _type == SqlColumnType.TEXT )
        {
            return StringUtilities.wrapQuotes( value );
        }

        return value;
    }


    public String getSqlReference()
    {
        String    value;

        if ( ! StringUtilities.isNullOrEmptyOrBlank( _alias ) )
        {
            value = _alias;
        }
        else
        {
            value = getStandardSqlReference();
        }

        if ( _prefix != null )
        {
            value = _prefix + value;
        }

        if ( _suffix != null )
        {
            value += _suffix;
        }

        return value;
    }


    private String getStandardSqlReference()
    {
        String    ref;
        if ( _table == null )
        {
            return _name;
        }

        ref = _table.getSqlReference();
        return  ref + "." + _name;
    }


    @Override
    public void appendToSqlBuffer( final StringBuffer    buffer )
    {
        buffer.append( getStandardSqlReference() );

        if ( ! StringUtilities.isNullOrEmptyOrBlank( _alias ) )
        {
            buffer.append( " AS " )
                  .append( _alias );
        }
    }


    //========================================
    //
    //    Standard common methods
    //
    //========================================


    @Override
    public String toString()
    {
        return new ToStringBuilder( this ).append( "table", _table )
                                          .append( "name", _name )
                                          .append( "alias", _alias )
                                          .append( "type", _type )
                                          .append( "charMaxLength", _charMaxLength )
                                          .toString();
    }
}
