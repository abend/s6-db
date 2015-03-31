package com.samsix.database;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Represents a null type passed to database varadic functions
 */
public final class SqlNull
{
    private final Class<?> _type;
    private final int _sqlType;

    public SqlNull( final Class<?> type )
    {
        _type = type;
        _sqlType = getSqlType( type );
    }


    public Class<?> getType()
    {
        return _type;
    }


    public int getSqlType()
    {
        return _sqlType;
    }


    private int getSqlType( final Class<?> type )
    {
        if( type == Boolean.class )
        {
            return java.sql.Types.BOOLEAN;
        }
        else if( type == String.class )
        {
            return java.sql.Types.VARCHAR;
        }
        else if( type == Integer.class )
        {
            return java.sql.Types.INTEGER;
        }
        else if( type == Byte.class )
        {
            return java.sql.Types.TINYINT;
        }
        else if( type == Short.class )
        {
            return java.sql.Types.SMALLINT;
        }
        else if( type == Long.class )
        {
            return java.sql.Types.BIGINT;
        }
        else if( type == Float.class )
        {
            return java.sql.Types.FLOAT;
        }
        else if( type == Double.class )
        {
            return java.sql.Types.DOUBLE;
        }
        else if( type == BigInteger.class )
        {
            return java.sql.Types.NUMERIC;
        }
        else if( type == BigDecimal.class )
        {
            return java.sql.Types.DECIMAL;
        }
        else if( type == java.util.Date.class || type == java.sql.Timestamp.class )
        {
            return java.sql.Types.TIMESTAMP;
        }
        else if( type == java.sql.Date.class )
        {
            return java.sql.Types.DATE;
        }
        else if( type == java.sql.Time.class )
        {
            return java.sql.Types.TIME;
        }
        else
        {
            return java.sql.Types.NULL;
        }
    }
}
