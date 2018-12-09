package com.tools.data.db.util;

import com.tools.data.db.core.DBType;
import com.tools.data.db.core.Nullable;
import com.tools.data.db.core.Ordering;
import com.tools.data.db.core.SQLType;
import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.metadata.Index;
import com.tools.data.db.metadata.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public final class JDBCUtils {
    private static final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

    public static final String DEFAULT_FOMAT_PATTERN = "yyyy-MM-dd";

    private static EnumSet<SQLType> charTypes = EnumSet.of(SQLType.CHAR, SQLType.VARCHAR, SQLType.LONGVARCHAR);
    private static EnumSet<SQLType> dateTypes = EnumSet.of(SQLType.DATE, SQLType.TIME, SQLType.TIMESTAMP);
    private static EnumSet<SQLType> numericTypes = EnumSet.of(SQLType.TINYINT, SQLType.INTEGER, SQLType.BIGINT, SQLType.SMALLINT,
            SQLType.FLOAT, SQLType.DOUBLE, SQLType.REAL, SQLType.NUMERIC, SQLType.DECIMAL);

    private static final DateFormat[] DATE_PARSING_FORMATS = new DateFormat[]{
            new SimpleDateFormat(DEFAULT_FOMAT_PATTERN),
            DateFormat.getDateInstance(),
            DateFormat.getTimeInstance(DateFormat.SHORT),
            DateFormat.getTimeInstance(DateFormat.SHORT, TimestampType.LOCALE),
            new SimpleDateFormat("yyyy-MM-dd"), // NOI18N
            new SimpleDateFormat("MM-dd-yyyy"), // NOI18N
    };

    {
        for (int i = 0; i < DATE_PARSING_FORMATS.length; i++) {
            DATE_PARSING_FORMATS[i].setLenient(false);
        }
    }


    public static java.sql.Date convert(Object value) throws DatabaseException {
        Calendar cal = Calendar.getInstance();

        if (null == value) {
            return null;
        } else if (value instanceof Timestamp) {
            cal.setTimeInMillis(((Timestamp) value).getTime());
        } else if (value instanceof java.util.Date) {
            cal.setTimeInMillis(((java.util.Date) value).getTime());
        }else if (value instanceof String) {
            java.util.Date dVal = doParse ((String) value);
            if (dVal == null) {
                throw new DatabaseException(MessageFormat.format("Can't convert {0} {1} due {2}.", value.getClass().getName(), value.toString())); // NOI18N
            }
            cal.setTimeInMillis(dVal.getTime());
        } else {
            throw new DatabaseException(MessageFormat.format("Can't convert {0} {1} due {2}.", value.getClass().getName(), value.toString())); // NOI18N
        }

        // Normalize to 0 hour in default time zone.
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return new java.sql.Date(cal.getTimeInMillis());
    }

    private static java.util.Date doParse (String sVal) {
        java.util.Date dVal = null;
        for (DateFormat format : DATE_PARSING_FORMATS) {
            try {
                dVal = format.parse (sVal);
                break;
            } catch (ParseException ex) {
                logger.info(ex.getLocalizedMessage() , ex);
            }
        }
        return dVal;
    }


    public static boolean isNumeric(int jdbcType) {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                return true;

            default:
                return false;
        }
    }

    public static boolean isPrecisionRequired(int jdbcType, boolean isdb2) {
        if (isdb2 && jdbcType == Types.BLOB || jdbcType == Types.CLOB) {
            return true;
        } else {
            return isPrecisionRequired(jdbcType);
        }
    }

    public static boolean isPrecisionRequired(int jdbcType) {
        switch (jdbcType) {
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.JAVA_OBJECT:
            case Types.LONGVARCHAR:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.ARRAY:
            case Types.STRUCT:
            case Types.DISTINCT:
            case Types.REF:
            case Types.DATALINK:
                return false;

            default:
                return true;
        }
    }

    public static boolean isScaleRequired(int type) {
        switch (type) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                return true;
            default:
                return false;
        }
    }

    public static boolean isBinary(int jdbcType) {
        switch (jdbcType) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static boolean isString(int jdbcType) {
        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case -9:  //NVARCHAR
            case -8:  //ROWID
            case -15: //NCHAR
            case -16: //NLONGVARCHAR
                return true;
            default:
                return false;
        }
    }

    public static boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    public static String replaceInString(String originalString, String[] victims, String[] replacements) {
        StringBuilder resultBuffer = new StringBuilder();
        boolean bReplaced = false;

        for (int charPosition = 0; charPosition < originalString.length(); charPosition++) {
            for (int nSelected = 0; !bReplaced && (nSelected < victims.length); nSelected++) {
                if (originalString.startsWith(victims[nSelected], charPosition)) {
                    resultBuffer.append(replacements[nSelected]);
                    bReplaced = true;
                    charPosition += victims[nSelected].length() - 1;
                }
            }

            if (!bReplaced) {
                resultBuffer.append(originalString.charAt(charPosition));
            } else {
                bReplaced = false;
            }
        }
        return resultBuffer.toString();
    }
    /**
     * Get the SQLType for the given java.sql.Type type.
     *
     * @param type the java.sql.Type type specifier
     * @return SQLType.the SQLType for this java.sql.Type, or null if it's not recognized
     */
    public static SQLType getSQLType(int type) {
        switch (type) {
            case Types.BIT: return SQLType.BIT;
            case Types.TINYINT: return SQLType.TINYINT;
            case Types.SMALLINT: return SQLType.SMALLINT;
            case Types.INTEGER: return SQLType.INTEGER;
            case Types.BIGINT: return SQLType.BIGINT;
            case Types.FLOAT: return SQLType.FLOAT;
            case Types.REAL: return SQLType.REAL;
            case Types.DOUBLE: return SQLType.DOUBLE;
            case Types.NUMERIC: return SQLType.NUMERIC;
            case Types.DECIMAL: return SQLType.DECIMAL;
            case Types.CHAR: return SQLType.CHAR;
            case Types.VARCHAR: return SQLType.VARCHAR;
            case Types.LONGVARCHAR: return SQLType.LONGVARCHAR;
            case Types.DATE: return SQLType.DATE;
            case Types.TIME: return SQLType.TIME;
            case Types.TIMESTAMP: return SQLType.TIMESTAMP;
            case Types.BINARY: return SQLType.BINARY;
            case Types.VARBINARY: return SQLType.VARBINARY;
            case Types.LONGVARBINARY: return SQLType.LONGVARBINARY;
            case Types.NULL: return SQLType.NULL;
            case Types.OTHER: return SQLType.OTHER;
            case Types.JAVA_OBJECT: return SQLType.JAVA_OBJECT;
            case Types.DISTINCT: return SQLType.DISTINCT;
            case Types.STRUCT: return SQLType.STRUCT;
            case Types.ARRAY: return SQLType.ARRAY;
            case Types.BLOB: return SQLType.BLOB;
            case Types.CLOB: return SQLType.CLOB;
            case Types.REF: return SQLType.REF;
            case Types.DATALINK: return SQLType.DATALINK;
            case Types.BOOLEAN: return SQLType.BOOLEAN;
            case Types.LONGNVARCHAR: return SQLType.LONGVARCHAR;
            case Types.NCHAR: return SQLType.CHAR;
            case Types.NCLOB: return SQLType.CLOB;
            case Types.NVARCHAR: return SQLType.VARCHAR;
            case Types.SQLXML: return SQLType.SQLXML;
            case Types.ROWID: return SQLType.ROWID;
            default:
                logger.info("Unknown JDBC column type: " + type + ". Returns null.");
                return null;
        }
    }

    public static boolean isCharType(SQLType type) {
        return charTypes.contains(type);
    }

    public static boolean isDateType(SQLType type) {
        return dateTypes.contains(type);
    }

    public static boolean isNumericType(SQLType type) {
        return numericTypes.contains(type);
    }

    public static Nullable getColumnNullable(int dbmdColumnNullable) {
        switch (dbmdColumnNullable) {
            case DatabaseMetaData.columnNoNulls:
                return Nullable.NOT_NULLABLE;
            case DatabaseMetaData.columnNullable:
                return Nullable.NULLABLE;
            case DatabaseMetaData.columnNullableUnknown:
            default:
                return Nullable.UNKNOWN;
        }
    }

    public static Nullable getProcedureNullable(int dbmdProcedureNullable) {
        switch (dbmdProcedureNullable) {
            case DatabaseMetaData.procedureNoNulls:
                return Nullable.NOT_NULLABLE;
            case DatabaseMetaData.procedureNullable:
                return Nullable.NULLABLE;
            case DatabaseMetaData.procedureNullableUnknown:
            default:
                return Nullable.UNKNOWN;
        }
    }
    
    public static Parameter.Direction getProcedureDirection(short sqlDirection) {
        switch (sqlDirection) {
            case DatabaseMetaData.procedureColumnOut:
                return Parameter.Direction.OUT;
            case DatabaseMetaData.procedureColumnInOut:
                return Parameter.Direction.INOUT;
            case DatabaseMetaData.procedureColumnIn:
                return Parameter.Direction.IN;
            default:
                logger.info( "Unknown direction value from DatabaseMetadata.getProcedureColumns(): " + sqlDirection);
                return Parameter.Direction.IN;
        }
    }

    public static Parameter.Direction getFunctionDirection(short sqlDirection) {
        switch (sqlDirection) {
            case DatabaseMetaData.functionColumnOut:
                return Parameter.Direction.OUT;
            case DatabaseMetaData.functionColumnInOut:
                return Parameter.Direction.INOUT;
            case DatabaseMetaData.functionColumnIn:
                return Parameter.Direction.IN;
            default:
                logger.info( "Unknown direction value from DatabaseMetadata.getFunctionColumns(): " + sqlDirection);
                return Parameter.Direction.IN;
        }
    }

    public static Ordering getOrdering(String ascOrDesc) {
        if (ascOrDesc == null || ascOrDesc.length() == 0) {
            return Ordering.NOT_SUPPORTED;
        } else if (ascOrDesc.equals("A")) {
            return Ordering.ASCENDING;
        } else if (ascOrDesc.equals("D")) {
            return Ordering.DESCENDING;
        } else {
            logger.info( "Unexpected ordering code from database: " + ascOrDesc);
            return Ordering.NOT_SUPPORTED;
        }

    }

    public static Index.IndexType getIndexType(short sqlIndexType) {
        switch (sqlIndexType) {
            case DatabaseMetaData.tableIndexHashed:
                return Index.IndexType.HASHED;
            case DatabaseMetaData.tableIndexClustered:
                return Index.IndexType.CLUSTERED;
            case DatabaseMetaData.tableIndexOther:
                return Index.IndexType.OTHER;
            case DatabaseMetaData.tableIndexStatistic:
                logger.info( "Got unexpected index type of tableIndexStatistic, marking as 'other'");
                return Index.IndexType.OTHER;
            default:
                logger.info( "Unexpected index type code from database metadata: " + sqlIndexType);
                return Index.IndexType.OTHER;
        }
    }

    public static int getDBTypeFromURL(String url) {
        int dbtype;
        // get the database type based on the product name converted to lowercase
        url = url.toLowerCase();
        if (url.contains("sybase")) { // NOI18N
            dbtype = DBType.SYBASE;
        } else if (url.contains("sqlserver")) { // NOI18N
            dbtype = DBType.SQLSERVER;
        } else if (url.contains("db2")) { // NOI18N
            dbtype = DBType.DB2;
        } else if (url.contains("orac")) { // NOI18N
            dbtype = DBType.ORACLE;
        } else if (url.contains("axion")) { // NOI18N
            dbtype = DBType.AXION;
        } else if (url.contains("derby")) { // NOI18N
            dbtype = DBType.DERBY;
        } else if (url.contains("postgre")) { // NOI18N
            dbtype = DBType.PostgreSQL;
        } else if (url.contains("mysql")) { // NOI18N
            dbtype = DBType.MYSQL;
        } else if (url.contains("pointbase")) { // NOI18N
            dbtype = DBType.POINTBASE;
        } else {
            dbtype = DBType.JDBC;
        }
        return dbtype;
    }

    public static String appendLimitCondition(String sql,int dbType,int startOffset,int pagesize){
        if(dbType == DBType.MYSQL){
            return MessageFormat.format("{0} LIMIT {1},{2}",sql,startOffset,pagesize);
        }
        if(dbType == DBType.SQLSERVER){
            return MessageFormat.format(
                    "SELECT TOP {0} T2.* FROM (SELECT ROW_NUMBER() OVER (ORDER BY ID) AS ROWNUM,T1.* FROM ({1}) T1 ) T2 WHERE ROWNUM > {2}",
                    pagesize,sql,startOffset);
        }
        if(dbType == DBType.ORACLE){
            return MessageFormat.format(
                    "SELECT T2.* FROM (SELECT T1.*,ROWNUM FROM ({0}) T1 WHERE ROWNUM >= {1}) T2 WHERE ROWNUM <= {2}",
                    sql,startOffset,pagesize
            );
        }
        logger.error("unsupported db type, cann't construct limit info to sql !");
        return null;
    }

    public static Set<String> getColumnSetFromResultSetMetadata(ResultSetMetaData metaData) throws SQLException{
        int columnCount = metaData.getColumnCount();
        Set<String> columnSet = new HashSet<>(columnCount);
        for(int i = 1; i <= columnCount; i++){
            columnSet.add(metaData.getColumnName(i));
        }
        return columnSet;
    }
}
