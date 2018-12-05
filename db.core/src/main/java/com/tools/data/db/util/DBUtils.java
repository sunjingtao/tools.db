/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 1997-2010 Oracle and/or its affiliates. All rights reserved.
 *
 * Oracle and Java are registered trademarks of Oracle and/or its affiliates.
 * Other names may be trademarks of their respective owners.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common
 * Development and Distribution License("CDDL") (collectively, the
 * "License"). You may not use this file except in compliance with the
 * License. You can obtain a copy of the License at
 * http://www.netbeans.org/cddl-gplv2.html
 * or nbbuild/licenses/CDDL-GPL-2-CP. See the License for the
 * specific language governing permissions and limitations under the
 * License.  When distributing the software, include this License Header
 * Notice in each file and include the License file at
 * nbbuild/licenses/CDDL-GPL-2-CP.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the GPL Version 2 section of the License file that
 * accompanied this code. If applicable, add the following below the
 * License Header, with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * Contributor(s):
 *
 * The Original Software is NetBeans. The Initial Developer of the Original
 * Software is Sun Microsystems, Inc. Portions Copyright 1997-2009 Sun
 * Microsystems, Inc. All Rights Reserved.
 *
 * If you wish your version of this file to be governed by only the CDDL
 * or only the GPL Version 2, indicate your decision by adding
 * "[Contributor] elects to include this software in this distribution
 * under the [CDDL or GPL Version 2] license." If you do not indicate a
 * single choice of license, a recipient has the option to distribute
 * your version of this file under either the CDDL, the GPL Version 2 or
 * to extend the choice of license to its licensees as provided above.
 * However, if you add GPL Version 2 code and therefore, elected the GPL
 * Version 2 license, then the option applies only if the new code is
 * made subject to such option by the copyright holder.
 */
package com.tools.data.db.util;

import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.meta.DBColumn;
import com.tools.data.db.meta.DBForeignKey;
import com.tools.data.db.meta.DBTable;
import com.tools.data.db.sql.SQLConstant;

import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements a date type which can generate instances of java.sql.Date and other JDBC
 * date-related types.
 *
 * @author Ahimanikya Satapathy
 */
public class DBUtils {

    public static final String DEFAULT_FOMAT_PATTERN = "yyyy-MM-dd"; // NOI18N
    private static final DateFormat[] DATE_PARSING_FORMATS = new DateFormat[]{
        new SimpleDateFormat (DEFAULT_FOMAT_PATTERN),
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

    private synchronized static java.util.Date doParse (String sVal) {
        java.util.Date dVal = null;
        for (DateFormat format : DATE_PARSING_FORMATS) {
            try {
                dVal = format.parse (sVal);
                break;
            } catch (ParseException ex) {
                Logger.getLogger (DBUtils.class.getName ()).log (Level.FINEST, ex.getLocalizedMessage () , ex);
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

    // NULL, DEFAULT, CURRENT_TIMESTAMP etc.
    @Deprecated
    public static boolean isSQLConstantString(Object value) {
        return isSQLConstantString(value, null);
    }

    // NULL, DEFAULT, CURRENT_TIMESTAMP etc.
    public static boolean isSQLConstantString(Object value, DBColumn col) {
        return (value == null || value instanceof SQLConstant);
    }

    public static boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    public static void closeResources(PreparedStatement pstmt) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException ex) {
            //ignore
        }
    }

    public static void closeResources(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException sqex) {
                // ignore
            }
        }
    }

    public static void closeResources(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException sqex) {
                // ignore
            }
        }
    }

    public static String getForeignKeyString(DBColumn column) {
        String refString = column.getName() + " --> "; // NOI18N
        StringBuilder str = new StringBuilder(refString);
        DBTable table = column.getParentObject();
        boolean firstReferencedColumn = false;

        for(DBForeignKey fk: table.getForeignKeys()) {
            if (fk.contains(column)) {
                for(String pkColName: fk.getPKColumnNames()) {
                    str.append(pkColName);
                    if (firstReferencedColumn) {
                        firstReferencedColumn = false;
                    } else {
                        str.append(", "); // NOI18N
                    }
                }
            }
        }

        return str.toString();
    }
    public static final String[] HTML_ALLOWABLES = {"&amp;", "&quot;", "&lt;", "&gt;"}; // NOI18N
    public static final String[] HTML_ILLEGALS = {"&", "\"", "<", ">"}; // NOI18N

    public static String escapeHTML(String string) {
        return replaceInString(string, HTML_ILLEGALS, HTML_ALLOWABLES);
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
}
