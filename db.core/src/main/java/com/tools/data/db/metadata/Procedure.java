/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2010 Oracle and/or its affiliates. All rights reserved.
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
 *
 * Contributor(s):
 *
 * Portions Copyrighted 2008 Sun Microsystems, Inc.
 */

package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.util.JDBCUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author David Van Couvering
 */
public class Procedure extends Element{

    private static final Logger LOGGER = Logger.getLogger(Procedure.class.getName());

    private final Schema jdbcSchema;
    private final String name;

    private Map<String, Column> columns;
    private Map<String, Parameter> parameters;
    private Value returnValue;

    public Procedure(Schema jdbcSchema, String name) {
        this.jdbcSchema = jdbcSchema;
        this.name = name;
    }

    public final Schema getParent() {
        return jdbcSchema;
    }

    public final String getName() {
        return name;
    }

    public final Collection<Column> getColumns() {
        return initColumns().values();
    }

    public final Column getColumn(String name) {
        return MetadataUtilities.find(name, initColumns());
    }

    public final void refresh() {
        columns = null;
        parameters = null;
    }

    public Collection<Parameter> getParameters() {
        return initParameters().values();
    }

    public Parameter getParameter(String name) {
        return initParameters().get(name);
    }

    public Value getReturnValue() {
        return initReturnValue();
    }

    @Override
    public String toString() {
        return "JDBCProcedure[name='" + name + "']"; // NOI18N
    }

    protected Column createJDBCColumn(int position, ResultSet rs) throws SQLException {
        return new Column(this, position, Value.createProcedureValue(rs, this));
    }

    protected Parameter createJDBCParameter(int position, ResultSet rs) throws SQLException {
        Parameter.Direction direction = JDBCUtils.getProcedureDirection(rs.getShort("COLUMN_TYPE")); //NOI18N
        return new Parameter(this, Value.createProcedureValue(rs, this), direction, position);
    }

    protected Value createJDBCValue(ResultSet rs) throws SQLException {
        return Value.createProcedureValue(rs, this);
    }

    protected void createProcedureInfo() {
        LOGGER.log(Level.FINE, "Initializing procedure info in {0}", this);
        
        Map<String, Column> newColumns = new LinkedHashMap<>();
        Map<String, Parameter> newParams = new LinkedHashMap<>();
        int resultCount = 0;
        int paramCount = 0;
        
        DatabaseMetaData dmd = jdbcSchema.getJDBCCatalog().getJDBCMetadata().getDmd();
        String catalogName = jdbcSchema.getJDBCCatalog().getName();
        String schemaName = jdbcSchema.getName();
        
        try (ResultSet rs = dmd.getProcedureColumns(catalogName, schemaName, name, "%");) {  // NOI18N
            while (rs.next()) {
                short columnType = rs.getShort("COLUMN_TYPE");
                switch (columnType) {
                    case DatabaseMetaData.procedureColumnResult:
                        resultCount++;
                        addColumn(resultCount, rs, newColumns);
                        break;
                    case DatabaseMetaData.procedureColumnIn:
                    case DatabaseMetaData.procedureColumnInOut:
                    case DatabaseMetaData.procedureColumnOut:
                    case DatabaseMetaData.procedureColumnUnknown:
                        paramCount++;
                        addParameter(paramCount, rs, newParams);
                        break;
                    case DatabaseMetaData.procedureColumnReturn:
                        setReturnValue(rs);
                        break;
                    default:
                        LOGGER.log(Level.INFO, "Encountered unexpected column type {0} when retrieving metadadta for procedure {1}", new Object[]{columnType, name});
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format(
                    "Failed to retrieve procedure info for catalog: '%s', schema: '%s', procedure: '%s'",
                    catalogName, schemaName, name
            ), e);
        } catch (SQLException e) {
            throw new MetadataException(String.format(
                    "Failed to retrieve procedure info for catalog: '%s', schema: '%s', procedure: '%s'",
                    catalogName, schemaName, name
            ), e);
        }
        columns = Collections.unmodifiableMap(newColumns);
        parameters = Collections.unmodifiableMap(newParams);
    }

    private void addColumn(int position, ResultSet rs, Map<String,Column> newColumns) throws SQLException {
        Column column = createJDBCColumn(position, rs);
        newColumns.put(column.getName(), column);
        LOGGER.log(Level.FINE, "Created column {0}", column);
    }

    private void addParameter(int position, ResultSet rs, Map<String,Parameter> newParams) throws SQLException {
        Parameter  param = createJDBCParameter(position, rs);
        newParams.put(param.getName(), param);
        LOGGER.log(Level.FINE, "Created parameter {0}", param);
    }

    private void setReturnValue(ResultSet rs) throws SQLException {
        returnValue = createJDBCValue(rs);
        LOGGER.log(Level.FINE, "Created return value {0}", returnValue);
    }

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    private Map<String, Column> initColumns() {
        if (columns != null) {
            return columns;
        }
        createProcedureInfo();
        return columns;
    }

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    private Map<String, Parameter> initParameters() {
        if (parameters != null) {
            return parameters;
        }
        createProcedureInfo();
        return parameters;
    }

    private Value initReturnValue() {
        if (returnValue != null) {
            return returnValue;
        }
        createProcedureInfo();
        return returnValue;
    }
}