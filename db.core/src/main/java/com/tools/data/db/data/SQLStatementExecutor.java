package com.tools.data.db.data;

import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.metadata.Metadata;
import com.tools.data.db.util.DBReadWriteUtils;
import com.tools.data.db.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.MessageFormat;
import java.util.List;

public abstract class SQLStatementExecutor{
    protected Logger logger = LoggerFactory.getLogger(SQLStatementExecutor.class);

    protected boolean lastCommitState;
    private boolean runInTransaction = false;
    private Connection connection;
    private Metadata metadata;
    private String sql;

    public SQLStatementExecutor(Connection connection, Metadata metadata) {
        if (connection == null || metadata == null) {
            throw new IllegalArgumentException("parameter connection or metadata in SQLStatementExecutor cann't be null !");
        }
        this.connection = connection;
        this.metadata = metadata;
    }

    public <T> T selectOne(String sql){
        try{
            Statement stmt = connection.createStatement();
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            if(rs != null && rs.getFetchSize() > 1)
                throw new DatabaseException("respect to find one row ,but result count is larger than one !");
//            Table table = metadata.
        }catch (Exception ex){
            logger.error(ex.getMessage());
        }
        return null;
    }

    public <T extends Object> T executeSql(){
//        try {
//            if(runInTransaction) {
//                lastCommitState = setAutocommit(false);
//            } else {
//                lastCommitState = setAutocommit(true);
//            }
//            Statement stmt = connection.createStatement();
//            boolean isResultSet = stmt.execute(sql);
//            return isResultSet ? (T)stmt.getResultSet() : (T)stmt.getUpdateCount();
//        } catch (Exception ex){
//            logger.error(ex.getMessage());
//        }finally {
//            logger.info(MessageFormat.format("sql execute finished : {0}",sql));
//            setAutocommit(lastCommitState);
//        }
        return null;
    }

    private boolean isSelectStatement(String queryString) {
        String sqlUpperTrimmed = queryString.trim().toUpperCase();
        return sqlUpperTrimmed.startsWith("SELECT") && (! sqlUpperTrimmed.contains("INTO"));
    }

    private int executeInsertRow(final DBTable table,final String[] insertSQLs,final Object[][] insertedRows) throws SQLException, DatabaseException {
        int done = 0;
        List<DBColumn> columns = table.getColumnList();
        for (int j = 0; j < insertSQLs.length; j++) {
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQLs[j])) {
                int pos = 1;
                for (int i = 0; i < insertedRows[j].length; i++) {
                    Object val = insertedRows[j][i];
                    // Check for Constant e.g <NULL>, <DEFAULT>, <CURRENT_TIMESTAMP> etc
                    if (JDBCUtils.isSQLConstantString(val, columns.get(i))) {
                        continue;
                    }
                    // literals
                    int colType = columns.get(i).getJdbcType();
                    DBReadWriteUtils.setAttributeValue(pstmt, pos++, colType, val);
                }
                int rows = pstmt.executeUpdate();
                if (rows != 1) {
                    throw new SQLException("executeInsertRow respect to insert one ,but result is " + rows);
                }
                done++;
            }
        }
        return done;
    }

//    private void executeDeleteRow(final DBTable table, final DataViewTableUI rsTable) {
//        dataView.setEditable(false);
//
//        SQLStatementGenerator generator = dataView.getSQLStatementGenerator();
//        String title = NbBundle.getMessage(SQLExecutorManager.class, "LBL_sql_delete");
//
//        class DeleteElement {
//            public List<Object> values = new ArrayList<>();
//            public List<Integer> types = new ArrayList<>();
//            public String sql;
//        }
//
//        final List<DeleteElement> rows = new ArrayList<>();
//        for(int viewRow: rsTable.getSelectedRows()) {
//            int modelRow = rsTable.convertRowIndexToModel(viewRow);
//            DeleteElement de = new DeleteElement();
//            de.sql = generator.generateDeleteStatement(table, de.types, de.values, modelRow, rsTable.getModel());
//            rows.add(de);
//        }
//
//        SQLStatementExecutor executor = new SQLStatementExecutor(dataView, title, "", true) {
//            @Override
//            public void execute() throws SQLException, DBException {
//                dataView.setEditable(false);
//                for (DeleteElement de: rows) {
//                    if (Thread.currentThread().isInterrupted() || error) {
//                        break;
//                    }
//                    deleteARow(de);
//                }
//            }
//
//            private void deleteARow(DeleteElement deleteRow) throws SQLException, DBException {
//                PreparedStatement pstmt = conn.prepareStatement(deleteRow.sql);
//                try {
//                    int pos = 1;
//                    for (Object val : deleteRow.values) {
//                        DBReadWriteHelper.setAttributeValue(pstmt, pos, deleteRow.types.get(pos - 1), val);
//                        pos++;
//                    }
//
//                    int rows = executePreparedStatement(pstmt);
//                    if (rows == 0) {
//                        error = true;
//                        errorMsg += NbBundle.getMessage(SQLExecutorManager.class, "MSG_no_match_to_delete");
//                    } else if (rows > 1) {
//                        error = true;
//                        errorMsg += NbBundle.getMessage(SQLExecutorManager.class, "MSG_no_unique_row_for_match");
//                    }
//                } finally {
//                    DataViewUtils.closeResources(pstmt);
//                }
//            }
//
//            @Override
//            public void finished() {
//            }
//
//            @Override
//            protected void executeOnSucess() {
//                SQLExecutorManager.this.executeQuery();
//            }
//        };
//
//    }

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
    protected abstract void execute() throws SQLException;

    private boolean commit() {
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
