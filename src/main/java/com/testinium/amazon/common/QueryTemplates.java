package com.testinium.amazon.common;

public class QueryTemplates {
    public static String ATHENA_DELETE_QUERY = "DROP TABLE merge ";
    public static String ATHENA_CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS merge WITH (format='{format}', external_location='{external_location}') AS SELECT * FROM \"{table}\"";
}