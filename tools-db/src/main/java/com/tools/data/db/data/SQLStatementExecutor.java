package com.tools.data.db.data;

import com.tools.data.db.api.DatabaseConnection;
import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.util.BeanUtils;
import com.tools.data.db.util.JDBCUtils;
import com.tools.data.db.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class SQLStatementExecutor{
    protected Logger logger = LoggerFactory.getLogger(SQLStatementExecutor.class);

    protected boolean lastCommitState;
    private boolean runInTransaction = false;
    private DatabaseConnection dbConnection;
    private String sql;

    public SQLStatementExecutor(DatabaseConnection dbConnection) {
        if (dbConnection == null) {
            throw new IllegalArgumentException("parameter connection in SQLStatementExecutor cann't be null !");
        }
        this.dbConnection = dbConnection;
    }

    public <T> T selectOne(Class clazz, String sql){
        try{
            Statement stmt = dbConnection.openConnection().createStatement();
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            if(rs== null || !rs.next())return null;
            if(rs.getFetchSize() > 1)
                throw new DatabaseException("respect to find one row ,but result count is larger than one !");
            //select sql may be select info from multi tables ,so must parse sql and get columns
            Set<String> columns = JDBCUtils.getColumnSetFromResultSetMetadata(rs.getMetaData());
            Map record = new HashMap();
            for(String column : columns){
                record.put(StringUtils.toCamelCase(column),rs.getString(column));
            }
            return (T) BeanUtils.convertToObject(clazz,record);
        }catch (Exception ex){
            logger.error(ex.getMessage());
        }
        return null;
    }

    public <T> List<T> selectAll(Class clazz, String sql){
        try{
            Statement stmt = dbConnection.openConnection().createStatement();
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            List<T> resultList = new ArrayList<>(rs.getFetchSize());
            Map record = null;
            Set<String> columns = JDBCUtils.getColumnSetFromResultSetMetadata(rs.getMetaData());
            while (rs.next()){
                //select sql may be select info from multi tables ,so must parse sql and get columns
                record = new HashMap();
                for(String column : columns) {
                    record.put(StringUtils.toCamelCase(column),rs.getString(column));
                }
                resultList.add((T) BeanUtils.convertToObject(clazz, record));
            }
            return resultList;
        }catch (Exception ex){
            logger.error(ex.getMessage());
        }
        return null;
    }

    public <T> List<T> selectPage(Class clazz, String sql,int startOffset,int pagesize){
        int dbType = JDBCUtils.getDBTypeFromURL(dbConnection.getConnectionUrl().getUrlTemplate());
        return selectAll(clazz,JDBCUtils.appendLimitCondition(sql,dbType,startOffset,pagesize));
    }

    private boolean isSelectStatement(String queryString) {
        String sqlUpperTrimmed = queryString.trim().toUpperCase();
        return sqlUpperTrimmed.startsWith("SELECT") && (! sqlUpperTrimmed.contains("INTO"));
    }

    private boolean executeSQLStatementForExtraction(Statement stmt, String sql) throws SQLException {
        logger.info("executeSQLStatementForExtraction: {0}", sql); // NOI18N

        long startTime = System.currentTimeMillis();
        boolean isResultSet = false;

        try {
            if (stmt instanceof PreparedStatement) {
                isResultSet = ((PreparedStatement) stmt).execute();
            } else {
                isResultSet = stmt.execute(sql);
            }
        } catch (NullPointerException ex) {
            logger.error("Failed to execute SQL Statement [{0}], cause: {1}", new Object[]{sql, ex});
            throw new SQLException(ex);
        } catch (SQLException sqlExc) {
            logger.error("Failed to execute SQL Statement [{0}], cause: {1}", new Object[]{sql, sqlExc});
            throw sqlExc;
        } finally {
            logWarnings(stmt);
        }

        long executionTime = System.currentTimeMillis() - startTime;
        logger.info("executeSQLStatementForExtraction time : {}",executionTime);
        return isResultSet;
    }

    private void logWarnings(Statement stmt) {
        try {
            for (SQLWarning warning = stmt.getConnection().getWarnings(); warning != null; warning = warning.getNextWarning()) {
                logger.warn(warning.getMessage());
            }
            stmt.getConnection().clearWarnings();
        } catch (Throwable ex) {
            logger.error("Failed to retrieve warnings", ex);
        }
        try {
            for(SQLWarning warning = stmt.getWarnings(); warning != null; warning = warning.getNextWarning()) {
                logger.warn(warning.getMessage());
            }
            stmt.clearWarnings();
        } catch (Throwable ex) {
            logger.error("Failed to retrieve warnings", ex);
        }
    }

    private int executePreparedStatement(PreparedStatement stmt) throws SQLException {
        long startTime = System.currentTimeMillis();
        stmt.execute();
        long executionTime = System.currentTimeMillis() - startTime;
        logger.info("executePreparedStatement time : {}",executionTime);
        return stmt.getUpdateCount();
    }

    private boolean setAutocommit(boolean newState) {
        Connection connection = dbConnection.openConnection();
        try {
            if (connection != null) {
                boolean lastState = connection.getAutoCommit();
                connection.setAutoCommit(newState);
                return lastState;
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return newState;
    }

    private boolean commit() {
        Connection connection = dbConnection.openConnection();
        if(! runInTransaction) {
            return true;
        }
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException sqlEx) {
            logger.error(sqlEx.getMessage());
            return false;
        }
        return true;
    }

    private void rollback() {
        Connection connection = dbConnection.openConnection();
        if(! runInTransaction) {
            logger.error("failed to rollback ,because it is not need to rollback !");
        }
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }
}
