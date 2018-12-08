package com.tools.data.db.metadata;

import com.tools.data.db.metadata.mysql.MySQLSchema;
import com.tools.data.db.metadata.oracle.OracleSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class Catalog extends Element{

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    protected final Metadata jdbcMetadata;
    protected final String name;
    protected Schema schema;

    public Catalog(Metadata jdbcMetadata, String name) {
        this.jdbcMetadata = jdbcMetadata;
        this.name = name;
        logger.info("Create JDBCCatalog(jdbcMetadata={0}, name={1}",jdbcMetadata, name);
    }

    @Override
    public Element getParent() {
        return null;
    }

    public final String getName() {
        return name;
    }

    public final Schema getSchema() {
        if(schema == null){
            schema = new Schema(this, name);
        }
        return schema;
    }

    protected Schema createJDBCSchema(String name, boolean _default, boolean synthetic) {
        try {
            DatabaseMetaData dmd = getMetadata().getDmd();
            if ("Oracle".equals(dmd.getDatabaseProductName())) { // NOI18N
                return new OracleSchema(this, getName());
            }

            if ("mysql".equalsIgnoreCase(dmd.getDatabaseProductName())) { // NOI18N
                return new MySQLSchema(this, getName());
            }

            return new Schema(this, getName());
        } catch (SQLException e) {
            logger.info(e.getMessage());
        }
        return null;
    }

    public final Metadata getMetadata() {
        return jdbcMetadata;
    }
}
