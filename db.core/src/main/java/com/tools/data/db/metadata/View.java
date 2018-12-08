package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.util.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class View implements Element{
    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    private final Schema jdbcSchema;
    private final String name;
    private Map<String, Column> columns;

    public View(Schema jdbcSchema, String name) {
        this.jdbcSchema = jdbcSchema;
        this.name = name;
    }

    @Override
    public String toString() {
        return "JDBCView[name='" + getName() + "']"; // NOI18N
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

    protected Column createJDBCColumn(ResultSet rs) throws SQLException {
        int ordinalPosition = rs.getInt("ORDINAL_POSITION");
        return new Column(this, ordinalPosition, Value.createTableColumnValue(rs, this));
    }

    protected void createColumns() {
        Map<String, Column> newColumns = new LinkedHashMap<String, Column>();
        try {
            ResultSet rs = jdbcSchema.getCatalog().getMetadata().getColumns(jdbcSchema.getCatalog().getName(), jdbcSchema.getName(), name, "%"); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        Column column = createJDBCColumn(rs);
                        newColumns.put(column.getName(), column);
                        logger.info( "Created column {0}", column);
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        columns = Collections.unmodifiableMap(newColumns);
    }

    private Map<String, Column> initColumns() {
        if (columns != null) {
            return columns;
        }
        logger.info( "Initializing columns in {0}", this);
        createColumns();
        return columns;
    }

}
