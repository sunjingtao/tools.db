package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.util.JDBCUtils;
import com.tools.data.db.util.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Function implements Element{

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);
    private final Schema jdbcSchema;
    private final String name;
    private Map<String, Column> columns;
    private Map<String, Parameter> parameters;
    private Value returnValue;

    public Function(Schema jdbcSchema, String name) {
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
        return MetadataUtils.find(name, initColumns());
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

    public String toString() {
        return "JDBCFunction[name='" + name + "']"; // NOI18N
    }

    protected Column createJDBCColumn(int position, ResultSet rs) throws SQLException {
        return new Column(this, position, Value.createFunctionValue(rs, this));
    }

    protected Parameter createJDBCParameter(int position, ResultSet rs) throws SQLException {
        Parameter.Direction direction = JDBCUtils.getFunctionDirection(rs.getShort("COLUMN_TYPE"));
        return new Parameter(this, Value.createFunctionValue(rs, this), direction, position);
    }

    protected Value createJDBCValue(ResultSet rs) throws SQLException {
        return Value.createFunctionValue(rs, this);
    }

    protected void createProcedureInfo() {
        logger.info( "Initializing procedure info in " + this);

        Map<String, Column> newColumns = new LinkedHashMap<String, Column>();
        Map<String, Parameter> newParams = new LinkedHashMap<String, Parameter>();
        int resultCount = 0;
        int paramCount = 0;

        try {
            ResultSet rs = jdbcSchema.getCatalog().getMetadata().getFunctionColumns(jdbcSchema.getCatalog().getName(), jdbcSchema.getName(), name, "%"); // NOI18N
            try {
                while (rs.next()) {
                    short columnType = rs.getShort("COLUMN_TYPE");
                    switch (columnType) {
                        case DatabaseMetaData.functionColumnResult:
                            addColumn(++resultCount, rs, newColumns);
                            break;
                        case DatabaseMetaData.functionColumnIn:
                        case DatabaseMetaData.functionColumnInOut:
                        case DatabaseMetaData.functionColumnOut:
                        case DatabaseMetaData.functionColumnUnknown:
                            addParameter(++paramCount, rs, newParams);
                            break;
                        case DatabaseMetaData.functionReturn:
                            setReturnValue(rs);
                            break;
                        default:
                            logger.info( "Encountered unexpected column type " + columnType + " when retrieving metadadta for procedure " + name);
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        columns = Collections.unmodifiableMap(newColumns);
        parameters = Collections.unmodifiableMap(newParams);
    }

    private void addColumn(int position, ResultSet rs, Map<String, Column> newColumns) throws SQLException {
        Column column = createJDBCColumn(position, rs);
        newColumns.put(column.getName(), column);
        logger.info( "Created column {0}", column);
    }

    private void addParameter(int position, ResultSet rs, Map<String, Parameter> newParams) throws SQLException {
        Parameter param = createJDBCParameter(position, rs);
        newParams.put(param.getName(), param);
        logger.info( "Created parameter {0}", param);
    }

    private void setReturnValue(ResultSet rs) throws SQLException {
        returnValue = createJDBCValue(rs);
        logger.info( "Created return value {0}", returnValue);
    }

    private Map<String, Column> initColumns() {
        if (columns != null) {
            return columns;
        }
        createProcedureInfo();
        return columns;
    }

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
