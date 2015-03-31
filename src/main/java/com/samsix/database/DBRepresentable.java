package com.samsix.database;




public interface DBRepresentable
{

    public abstract String getWhereClause();


    public abstract String getWhereClause( String prefix );


    public abstract String getWhereClause( String prefix,
                                           String suffix );


    public abstract String getColumnClause();


    public abstract String getSetClause( String prefix );


    //
    //    Get the column portion of the clause for example
    //    in an insert statement.  The prefix is used to
    //    identify different objects that might be stored
    //    in the same row in a table.  For instance, in the
    //    PolyFeatureContents table we store the polygon entity
    //    and the entities contained within its boundaries.
    //
    public abstract String getColumnClause( String prefix );


    public abstract String getColumnClause( String prefix,
                                            String suffix );


    public abstract String getValuesClause();


    public abstract SqlCondition getSqlCondition( SqlTable table );


    public abstract SqlCondition getSqlCondition( SqlTable table,
                                                  String prefix,
                                                  String suffix );

}
