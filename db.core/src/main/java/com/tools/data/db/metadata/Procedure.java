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

public class Procedure implements Element{

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

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
        logger.info( "Initializing procedure info in {0}", this);
        
        Map<String, Column> newColumns = new LinkedHashMap<>();
        Map<String, Parameter> newParams = new LinkedHashMap<>();
        int resultCount = 0;
        int paramCount = 0;
        
        DatabaseMetaData dmd = jdbcSchema.getCatalog().getMetadata();
        String catalogName = jdbcSchema.getCatalog().getName();
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
                        logger.info( "Encountered unexpected column type {0} when retrieving metadadta for procedure {1}", new Object[]{columnType, name});
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
        logger.info( "Created column {0}", column);
    }

    private void addParameter(int position, ResultSet rs, Map<String,Parameter> newParams) throws SQLException {
        Parameter  param = createJDBCParameter(position, rs);
        newParams.put(param.getName(), param);
        logger.info( "Created parameter {0}", param);
    }

    private void setReturnValue(ResultSet rs) throws SQLException {
        returnValue = createJDBCValue(rs);
        logger.info( "Created return value {0}", returnValue);
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
