///*
// * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
// *
// * Copyright 1997-2010 Oracle and/or its affiliates. All rights reserved.
// *
// * Oracle and Java are registered trademarks of Oracle and/or its affiliates.
// * Other names may be trademarks of their respective owners.
// *
// * The contents of this file are subject to the terms of either the GNU
// * General Public License Version 2 only ("GPL") or the Common
// * Development and Distribution License("CDDL") (collectively, the
// * "License"). You may not use this file except in compliance with the
// * License. You can obtain a copy of the License at
// * http://www.netbeans.org/cddl-gplv2.html
// * or nbbuild/licenses/CDDL-GPL-2-CP. See the License for the
// * specific language governing permissions and limitations under the
// * License.  When distributing the software, include this License Header
// * Notice in each file and include the License file at
// * nbbuild/licenses/CDDL-GPL-2-CP.  Oracle designates this
// * particular file as subject to the "Classpath" exception as provided
// * by Oracle in the GPL Version 2 section of the License file that
// * accompanied this code. If applicable, add the following below the
// * License Header, with the fields enclosed by brackets [] replaced by
// * your own identifying information:
// * "Portions Copyrighted [year] [name of copyright owner]"
// *
// * Contributor(s):
// *
// * The Original Software is NetBeans. The Initial Developer of the Original
// * Software is Sun Microsystems, Inc. Portions Copyright 1997-2007 Sun
// * Microsystems, Inc. All Rights Reserved.
// *
// * If you wish your version of this file to be governed by only the CDDL
// * or only the GPL Version 2, indicate your decision by adding
// * "[Contributor] elects to include this software in this distribution
// * under the [CDDL or GPL Version 2] license." If you do not indicate a
// * single choice of license, a recipient has the option to distribute
// * your version of this file under either the CDDL, the GPL Version 2 or
// * to extend the choice of license to its licensees as provided above.
// * However, if you add GPL Version 2 code and therefore, elected the GPL
// * Version 2 license, then the option applies only if the new code is
// * made subject to such option by the copyright holder.
// */
//package com.tools.data.db.data;
//
//import com.tools.data.db.exception.DatabaseException;
//import com.tools.data.db.lexer.SQLConstant;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.Blob;
//import java.sql.Clob;
//import java.sql.SQLException;
//import java.sql.Types;
//import java.text.MessageFormat;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class SQLStatementGenerator {
//    private static final Logger logger = LoggerFactory.getLogger(SQLStatementGenerator.class);
//
//    public String generateInsertStatement(DBTable table, Object[] insertedRow) throws DatabaseException {
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder insertSql = new StringBuilder();
//        insertSql.append("INSERT INTO "); // NOI18N
//
//        StringBuilder colNames = new StringBuilder(" ("); // NOI18N
//        StringBuilder values = new StringBuilder();
//        String commaStr = ", "; // NOI18N
//        boolean comma = false;
//        for (int i = 0; i < insertedRow.length; i++) {
//            DBColumn dbcol = columns.get(i);
//            Object val = insertedRow[i];
//
//            if (dbcol.isGenerated()) { // NOI18N
//                continue;
//            }
//
//            if ((val == null || val.equals("<NULL>")) && !dbcol.isNullable()) { // NOI18N
//                throw new DatabaseException(MessageFormat.format("column {} cann't be null !",dbcol.getName()));
//            }
//
//            if (comma) {
//                values.append(commaStr);
//                colNames.append(commaStr);
//            } else {
//                comma = true;
//            }
//
//            // Check for Constant e.g <NULL>, <DEFAULT>, <CURRENT_TIMESTAMP> etc
//            if (val instanceof SQLConstant) {
//                values.append(((SQLConstant) val).name());
//            } else { // ELSE literals
//                values.append(val == null ? " NULL " : "?"); // NOI18N
//            }
//            colNames.append(dbcol.getQualifiedName(true));
//        }
//
//        colNames.append(")"); // NOI18N
//        insertSql.append(table.getFullyQualifiedName(true));
//        insertSql.append(colNames.toString());
//        insertSql.append(" Values("); // NOI18N
//        insertSql.append(values.toString());
//        insertSql.append(")"); // NOI18N
//
//        return insertSql.toString();
//    }
//
//    public String generateRawInsertStatement(DBTable table, Object[] insertedRow) throws DatabaseException {
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder rawInsertSql = new StringBuilder();
//        rawInsertSql.append("INSERT INTO "); // NOI18N
//
//        String rawcolNames = " ("; // NOI18N
//        String rawvalues = "";  // NOI18N
//        String commaStr = ", "; // NOI18N
//        boolean comma = false;
//        for (int i = 0; i < insertedRow.length; i++) {
//            DBColumn dbcol = columns.get(i);
//            Object val = insertedRow[i];
//
//            if (dbcol.isGenerated()) { // NOI18N
//                continue;
//            }
//
//            if ((val == null || val.equals("<NULL>")) && !dbcol.isNullable()) { // NOI18N
//                throw new DatabaseException(NbBundle.getMessage(SQLStatementGenerator.class, "MSG_nullable_check"));
//            }
//
//            if (comma) {
//                rawvalues += commaStr;
//                rawcolNames += commaStr;
//            } else {
//                comma = true;
//            }
//
//            // Check for Constant e.g <NULL>, <DEFAULT>, <CURRENT_TIMESTAMP> etc
//            if (val instanceof SQLConstant) {
//                rawvalues += (((SQLConstant) val).name());
//            } else { // ELSE literals
//                rawvalues += getQualifiedValue(dbcol.getJdbcType(), insertedRow[i]);
//            }
//            rawcolNames += dbcol.getQualifiedName(false);
//        }
//
//        rawcolNames += ")"; // NOI18N
//        rawInsertSql.append(table.getFullyQualifiedName(false));
//        rawInsertSql.append(rawcolNames);
//        rawInsertSql.append(" \n\tVALUES (");  // NOI18N
//        rawInsertSql.append(rawvalues);
//        rawInsertSql.append(")"); // NOI18N
//
//        return rawInsertSql.toString();
//    }
//
//    public String generateUpdateStatement(DBTable table, int row, Map<Integer, Object> changedRow, List<Object> values, List<Integer> types) throws DatabaseException {
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder updateStmt = new StringBuilder();
//        updateStmt.append("UPDATE ").append(table.getFullyQualifiedName(true)).append(" SET "); // NOI18N
//        String commaStr = ", "; // NOI18N
//        boolean comma = false;
//        for (Integer col : changedRow.keySet()) {
//            DBColumn dbcol = columns.get(col);
//            Object value = changedRow.get(col);
//            int type = dbcol.getJdbcType();
//
//            if ((value == null || value.equals("<NULL>")) && !dbcol.isNullable()) { // NOI18N
//                throw new DatabaseException(NbBundle.getMessage(SQLStatementGenerator.class, "MSG_nullable_check"));
//            }
//
//            if (comma) {
//                updateStmt.append(commaStr);
//            } else {
//                comma = true;
//            }
//
//            updateStmt.append(dbcol.getQualifiedName(true));
//            // Check for Constant e.g <NULL>, <DEFAULT>, <CURRENT_TIMESTAMP> etc
//            if (value instanceof SQLConstant) {
//                updateStmt.append(" = ").append(((SQLConstant) value).name());
//            // NULL ist reported as an SQL constant, so treat it as such
//            } else if ( value == null ) {
//                updateStmt.append(" = NULL"); // NOI18N
//            } else { // ELSE literals
//                updateStmt.append(" = ?"); // NOI18N
//                values.add(value);
//                types.add(type);
//            }
//        }
//
//        updateStmt.append(" WHERE "); // NOI18N
//        generateWhereCondition(table, updateStmt, types, values, row, tblModel);
//        return updateStmt.toString();
//    }
//
//    public String generateUpdateStatement(DBTable table, int row, Map<Integer, Object> changedRow, DataViewTableUIModel tblModel) throws DatabaseException {
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder rawUpdateStmt = new StringBuilder();
//        rawUpdateStmt.append("UPDATE ").append(table.getFullyQualifiedName(false)).append(" SET "); // NOI18N
//
//        String commaStr = ", "; // NOI18N
//        boolean comma = false;
//        for (Integer col : changedRow.keySet()) {
//            DBColumn dbcol = columns.get(col);
//            Object value = changedRow.get(col);
//            int type = dbcol.getJdbcType();
//
//            if ((value == null || value.equals("<NULL>")) && !dbcol.isNullable()) { // NOI18N
//                throw new DatabaseException(NbBundle.getMessage(SQLStatementGenerator.class, "MSG_nullable_check"));
//            }
//
//            if (comma) {
//                rawUpdateStmt.append(commaStr);
//            } else {
//                comma = true;
//            }
//
//            rawUpdateStmt.append(dbcol.getQualifiedName(true));
//            // Check for Constant e.g <NULL>, <DEFAULT>, <CURRENT_TIMESTAMP> etc
//            if (value instanceof SQLConstant) {
//                rawUpdateStmt.append(" = ").append(((SQLConstant) value).name());
//            } else { // ELSE literals
//                rawUpdateStmt.append(" = ").append(getQualifiedValue(type, value).toString());
//            }
//        }
//
//        rawUpdateStmt.append(" WHERE "); // NOI18N
//        generateWhereCondition(table, rawUpdateStmt, row, tblModel);
//        return rawUpdateStmt.toString();
//    }
//
//    public String generateDeleteStatement(DBTable table, List<Integer> types, List<Object> values, int rowNum, DataViewTableUIModel tblModel) {
//        StringBuilder deleteStmt = new StringBuilder();
//        deleteStmt.append("DELETE FROM ").append(table.getFullyQualifiedName(true)).append(" WHERE "); // NOI18N
//
//        generateWhereCondition(table, deleteStmt, types, values, rowNum, tblModel);
//        return deleteStmt.toString();
//    }
//
//    public String generateDeleteStatement(DBTable table, int rowNum, DataViewTableUIModel tblModel) {
//        StringBuilder rawDeleteStmt = new StringBuilder();
//        rawDeleteStmt.append("DELETE FROM ").append(table.getFullyQualifiedName(false)).append(" WHERE "); // NOI18N
//
//        generateWhereCondition(table, rawDeleteStmt, rowNum, tblModel);
//        return rawDeleteStmt.toString();
//    }
//
//    // TODO: Support for FK, and other constraint and Index recreation.
//    public String generateCreateStatement(DBTable table) throws DatabaseException, Exception {
//        boolean isdb2 = table.getParentObject().getDBType() == DBMetaDataFactory.DB2;
//
//        StringBuilder sql = new StringBuilder();
//        List<DBColumn> columns = table.getColumnList();
//        sql.append("CREATE TABLE ").append(table.getQualifiedName(false)).append(" ("); // NOI18N
//        int count = 0;
//        for (DBColumn col : columns) {
//            if (count++ > 0) {
//                sql.append(", "); // NOI18N
//            }
//
//            String typeName = col.getTypeName();
//            sql.append(col.getQualifiedName(false)).append(" ");
//
//            int scale = col.getScale();
//            int precision = col.getPrecision();
//            if (precision > 0 && DataViewUtils.isPrecisionRequired(col.getJdbcType(), isdb2)) {
//                if (typeName.contains("(")) { // Handle MySQL Binary Type // NOI18N
//                    sql.append(typeName.replace("(", "(" + precision)); // NOI18N
//                } else {
//                    sql.append(typeName).append("(").append(precision); // NOI18N
//                    if (scale > 0 && DataViewUtils.isScaleRequired(col.getJdbcType())) {
//                        sql.append(", ").append(scale).append(")"); // NOI18N
//                    } else {
//                        sql.append(")"); // NOI18N
//                    }
//                }
//            } else {
//                sql.append(typeName);
//            }
//
//            if (DataViewUtils.isBinary(col.getJdbcType()) && isdb2) {
//                sql.append("  FOR BIT DATA "); // NOI18N
//            }
//
//            if (col.hasDefault()) {
//                sql.append(" DEFAULT ").append(col.getDefaultValue()).append(" "); // NOI18N
//            }
//
//            if (!col.isNullable()) {
//                sql.append(" NOT NULL"); // NOI18N
//            }
//
//            if (col.isGenerated()) {
//                sql.append(" ").append(getAutoIncrementText(table.getParentObject().getDBType()));
//            }
//        }
//
//        DBPrimaryKey pk = table.getPrimaryKey();
//        if (pk != null) {
//            count = 0;
//            sql.append(", PRIMARY KEY ("); // NOI18N
//            for (String col : pk.getColumnNames()) {
//                if (count++ > 0) {
//                    sql.append(", "); // NOI18N
//                }
//                sql.append(table.getQuoter().quoteIfNeeded(col));
//            }
//            sql.append(")"); // NOI18N
//        }
//        sql.append(")"); // NOI18N
//
//        return sql.toString();
//    }
//
//    private boolean addSeparator(boolean and, StringBuilder sql, String sep) {
//        if (and) {
//            sql.append(sep);
//            return true;
//        } else {
//            return true;
//        }
//    }
//
//    private void generateNameValue(DBColumn column, StringBuilder sql, Object value, List<Object> values, List<Integer> types) {
//        sql.append(column.getQualifiedName(true));
//        if (value != null) {
//            values.add(value);
//            types.add(column.getJdbcType());
//            sql.append(" = ? "); // NOI18N
//        } else { // Handle NULL value in where condition
//            sql.append(" IS NULL "); // NOI18N
//        }
//    }
//
//    private void generateNameValue(DBColumn column, StringBuilder sql, Object value) {
//        String columnName = column.getQualifiedName(false);
//        int type = column.getJdbcType();
//
//        sql.append(columnName);
//        if (value != null) {
//            sql.append(" = ").append(getQualifiedValue(type, value)); // NOI18N
//        } else { // Handle NULL value in where condition
//            sql.append(" IS NULL"); // NOI18N
//        }
//    }
//
//    void generateWhereCondition(DBTable table, StringBuilder result, List<Integer> types, List<Object> values, int rowNum, DataViewTableUIModel model) {
//
//        DBPrimaryKey key = table.getPrimaryKey();
//        Set<String> columnsSelected = new HashSet<>();
//        boolean and = false;
//
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder pkSelect = new StringBuilder();
//        List<Integer> pkTypes = new ArrayList<>();
//        List<Object> pkObject = new ArrayList<>();
//
//        if (key != null) {
//            for (String keyName : key.getColumnNames()) {
//                for (int i = 0; i < model.getColumnCount(); i++) {
//                    DBColumn dbcol = columns.get(i);
//                    String columnName = dbcol.getName();
//                    if (columnName.equals(keyName)) {
//                        Object val = model.getOriginalValueAt(rowNum, i);
//                        if (val != null) {
//                            columnsSelected.add(columnName);
//                            and = addSeparator(and, pkSelect, " AND "); // NOI18N
//                            generateNameValue(dbcol, pkSelect, val, pkObject, pkTypes);
//                            break;
//                        }
//                    }
//                }
//            }
//        }
//
//        if (key != null && columnsSelected.equals(new HashSet<>(key.getColumnNames()))) {
//            result.append(pkSelect);
//            types.addAll(pkTypes);
//            values.addAll(pkObject);
//        } else {
//            and = false;
//            for (int i = 0; i < model.getColumnCount(); i++) {
//                DBColumn dbcol = columns.get(i);
//                Object val = model.getOriginalValueAt(rowNum, i);
//                and = addSeparator(and, result, " AND "); // NOI18N
//                generateNameValue(dbcol, result, val, values, types);
//            }
//        }
//    }
//
//    void generateWhereCondition(DBTable table, StringBuilder sql, int rowNum, DataViewTableUIModel model) {
//        DBPrimaryKey key = table.getPrimaryKey();
//        Set<String> columnsSelected = new HashSet<>();
//        boolean and = false;
//
//        List<DBColumn> columns = table.getColumnList();
//
//        StringBuilder pkSelect = new StringBuilder();
//
//        if (key != null) {
//            for (String keyName : key.getColumnNames()) {
//                for (int i = 0; i < model.getColumnCount(); i++) {
//                    DBColumn dbcol = columns.get(i);
//                    String columnName = dbcol.getName();
//                    if (columnName.equals(keyName)) {
//                        Object val = model.getOriginalValueAt(rowNum, i);
//                        if (val != null) {
//                            columnsSelected.add(columnName);
//                            and = addSeparator(and, pkSelect, " AND "); // NOI18N
//                            generateNameValue(dbcol, pkSelect, val);
//                            break;
//                        }
//                    }
//                }
//            }
//        }
//
//        if (key != null && columnsSelected.equals(new HashSet<>(key.getColumnNames()))) {
//            sql.append(pkSelect);
//        } else {
//            and = false;
//            for (int i = 0; i < model.getColumnCount(); i++) {
//                DBColumn dbcol = columns.get(i);
//                Object val = model.getOriginalValueAt(rowNum, i);
//                and = addSeparator(and, sql, " AND "); // NOI18N
//                generateNameValue(dbcol, sql, val);
//            }
//        }
//    }
//
//    private Object getQualifiedValue(int type, Object val) {
//        if (val == null) {
//            return "NULL"; // NOI18N
//        }
//        if (type == Types.BIT && !(val instanceof Boolean)) {
//            return "b'" + val + "'"; // NOI18N
//        } else if (DataViewUtils.isNumeric(type)) {
//            return val;
//        } else if (val instanceof Clob) {
//            try {
//                Clob lob = (Clob) val;
//                String result = lob.getSubString(1, (int) lob.length());
//                return "'" + result.replace("'", "''") + "'"; //NOI18N
//            } catch (SQLException ex) {
//                LOG.log(Level.INFO, "Failed to read CLOB", ex); //NOI18N
//            }
//        } else if (val instanceof Blob) {
//            try {
//                Blob lob = (Blob) val;
//                byte[] result = lob.getBytes(1, (int) lob.length());
//                return "x'" + BinaryToStringConverter.convertToString(
//                        result, 16, false) + "'"; // NOI18N
//            } catch (SQLException ex) {
//                LOG.log(Level.INFO, "Failed to read BLOB", ex); //NOI18N
//            }
//        }
//        // Fallback if previous converts fail
//        return "'" + val.toString().replace("'", "''") + "'"; //NOI18N
//    }
//
//    private String getAutoIncrementText(int dbType) throws Exception {
//        switch (dbType) {
//            case DBMetaDataFactory.MYSQL:
//                return "AUTO_INCREMENT"; // NOI18N
//
//            case DBMetaDataFactory.PostgreSQL:
//                return "SERIAL"; // NOI18N
//
//            case DBMetaDataFactory.SQLSERVER:
//                return "IDENTITY"; // NOI18N
//            default:
//                return "GENERATED ALWAYS AS IDENTITY"; // NOI18N
//        }
//    }
//}