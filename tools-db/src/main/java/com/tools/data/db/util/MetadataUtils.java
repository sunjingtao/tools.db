package com.tools.data.db.util;

import com.tools.data.db.metadata.Column;
import com.tools.data.db.metadata.Table;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MetadataUtils {

    public static <V> V find(String key, Map<String, ? extends V> map) {
        V value = map.get(key);
        if (value != null) {
            return value;
        }
        for (Entry<String, ? extends V> entry : map.entrySet()) {
            if (equals(key, entry.getKey(), true)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static boolean equals(String str1, String str2) {
        return equals(str1, str2, false);
    }

    private static boolean equals(String str1, String str2, boolean ignoreCase) {
        if (str1 == null) {
            return str2 == null;
        } else {
            return ignoreCase ? str1.equalsIgnoreCase(str2) : str1.equals(str2);
        }
    }
    
    public static String trimmed(String input) {
        if(input == null) {
            return input;
        } else {
            return input.trim();
        }
    }

    /**
     * Call {@link DatabaseMetaData#getColumns(String, String, String, String)},
     * wrapping any internal runtime exception into an {@link SQLException}.
     */
    public static ResultSet getColumns(DatabaseMetaData dmd, String catalog,
            String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            return dmd.getColumns(catalog, schemaPattern, tableNamePattern,
                    columnNamePattern);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call {@link DatabaseMetaData#getIndexInfo(String, String, String,
     * boolean, boolean)}, wrapping any internal runtime exception into an
     * {@link SQLException}.
     */
    public static ResultSet getIndexInfo(DatabaseMetaData dmd,
            String catalog, String schema, String table,
            boolean unique, boolean approximate) throws SQLException {
        try {
            return dmd.getIndexInfo(catalog, schema, table, unique,
                    approximate);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call {@link DatabaseMetaData#getImportedKeys(String, String, String)},
     * wrapping any internal runtime exception into an {@link SQLException}.
     */
    public static ResultSet getImportedKeys(DatabaseMetaData dmd,
            String catalog, String schema, String table) throws SQLException {
        try {
            return dmd.getImportedKeys(catalog, schema, table);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call {@link DatabaseMetaData#getPrimaryKeys(String, String, String)},
     * wrapping any internal runtime exeption into an {@link SQLException}.
     */
    public static ResultSet getPrimaryKeys(DatabaseMetaData dmd,
            String catalog, String schema, String table) throws SQLException {
        try {
            return dmd.getPrimaryKeys(catalog, schema, table);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call {@link DatabaseMetaData#getTables(String, String, String,
     * String[])}, wrapping any internal runtime exception into an
     * {@link SQLException}.
     */
    public static ResultSet getTables(DatabaseMetaData dmd,
            String catalog, String schemaPattern, String tableNamePattern,
            String[] types) throws SQLException {
        try {
            return dmd.getTables(catalog, schemaPattern, tableNamePattern,
                    types);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call {@link DatabaseMetaData#getProcedures(String, String, String)},
     * wrapping any internal runtime exception into an {@link SQLException}.
     */
    public static ResultSet getProcedures(DatabaseMetaData dmd,
            String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        try {
            return dmd.getProcedures(catalog, schemaPattern,
                    procedureNamePattern);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /**
     * Call { DatabaseMetaData#geFunctions(String, String, String)},
     * wrapping any internal runtime exception into an {@link SQLException}.
     */
    public static ResultSet getFunctions(DatabaseMetaData dmd,
            String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        try {
            return dmd.getFunctions(catalog, schemaPattern,
                    functionNamePattern);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    public static Set<String> getColumnNameSet(Table table, boolean camelCase){
        Set<String> columnSet = new HashSet<>();
        Collection<Column> columns = table.getColumns();
        for(Column column : columns){
            columnSet.add(camelCase ? StringUtils.toCamelCase(column.getName()) : column.getName());
        }
        return  columnSet;
    }
}
