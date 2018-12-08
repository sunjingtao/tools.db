package com.tools.data.db.metadata;

import com.tools.data.db.metadata.mysql.MySQLSchema;
import com.tools.data.db.metadata.oracle.OracleSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class Catalog implements Element{
    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    private String name;
    private Schema schema;
    private DatabaseMetaData dmd;

    public Catalog(Connection connection) {
        try {
            name = connection.getCatalog();
            dmd = connection.getMetaData();
        } catch (SQLException e) {
            logger.info("Could not load catalog from database (getCatalogs failed).");
        }
    }

    @Override
    public Element getParent() {
        return null;
    }

    public final String getName() {
        return name;
    }

    public DatabaseMetaData getMetadata(){
        return dmd;
    }

    public final Schema getSchema() {
        if(schema == null) {
            try {
                if ("Oracle".equals(dmd.getDatabaseProductName())) { // NOI18N
                    schema = new OracleSchema(this, getName());
                }

                if ("mysql".equalsIgnoreCase(dmd.getDatabaseProductName())) { // NOI18N
                    schema = new MySQLSchema(this, getName());
                }

                schema = new Schema(this, getName());
            } catch (SQLException e) {
                logger.info(e.getMessage());
            }
        }
        return schema;
    }

}
