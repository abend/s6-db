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


import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.joda.time.DateTime;



public interface SqlFormatter
{
    public SqlFormatter append( String    column,
                                String    value,
                                boolean   wrapQuotes );



    public SqlFormatter append( String    column,
                                String    value );



    public SqlFormatter append( String    column,
                                int       value );



    public SqlFormatter append( String    column,
                                Integer   value );



    public SqlFormatter append( String    column,
                                long      value );



    public SqlFormatter append( String    column,
                                Long      value );



    public SqlFormatter append( String    column,
                                Date      when );



    public SqlFormatter append( String    column,
                                Calendar  when );
    
    
    
    public SqlFormatter append( final String    column,
                                final DateTime  when );

    

    public SqlFormatter append( String    column,
                                double    value );



    public SqlFormatter append( String    column,
                                Double    value );



    public SqlFormatter append( String     column,
                                boolean    value );



    public SqlFormatter append( String     column,
                                Boolean    value );


    public SqlFormatter append( String    column,
                                char      value );


    public SqlFormatter append( String         column,
                                Character      value );


    public SqlFormatter append( DBRepresentable    objid );



    public SqlFormatter append( DBRepresentable    objid,
                                String      prefix );



    public SqlFormatter append( DBRepresentable    objid,
                                String      prefix,
                                String      suffix );


    /**
     *    Appends a "?" for a prepared statement.
     *
     *    @param column
     *    @return
     */
    public SqlFormatter appendParameter( String    column );


    public void append( Map<String,Object>    map );


    public void append( final String                tableAlias,
                        final Map<String,Object>    map );
}
