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
 *    Used in RecordSet.toMap() to transform a column value
 */
public interface MapColumnTranslator
{
    /**    An object that prevents adding this column to the map */
    public static Object DO_NOT_ADD = new Object();
    
    /**
     *    @param columnName
     *    @param columnValue
     *    @return a new object to represent the value in the map,
     *    or MapColumnTranslator.DO_NOT_ADD to skip it entirely.
     */
    public Object transformValue( final String    columnName,
                                  final Object    columnValue );
}
