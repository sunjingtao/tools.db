package com.tools.data.db.metadata.mysql;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.metadata.Catalog;
import com.tools.data.db.metadata.Procedure;
import com.tools.data.db.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MySQLSchema extends Schema {

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);
    
    public MySQLSchema(Catalog jdbcCatalog, String name) {
        super(jdbcCatalog, name);
    }

    @Override
    protected void createProcedures() {
        logger.info( "Initializing MySQL procedures in {0}", this);
        Map<String, Procedure> newProcedures = new LinkedHashMap<String, Procedure>();
        // routines
        try {
            DatabaseMetaData dmd = catalog.getMetadata().getDmd();
            Statement stmt = dmd.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery("SELECT NAME, TYPE" // NOI18N
                                            + " FROM mysql.proc WHERE DB='" + catalog.getName() + "'" // NOI18N
                                            + " AND ( TYPE = 'PROCEDURE' OR TYPE = 'FUNCTION' )"); // NOI18N
            try {
                while (rs.next()) {
                    String procedureName = rs.getString("NAME"); // NOI18N
                    Procedure procedure = createJDBCProcedure(procedureName);
                    newProcedures.put(procedureName, procedure);
                    logger.info( "Created MySQL procedure: {0}, type: {1}", new Object[]{procedure, rs.getString("TYPE")});
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                stmt.close();
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        // information_schema.triggers
        try {
            DatabaseMetaData dmd = catalog.getMetadata().getDmd();
            Statement stmt = dmd.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery("SELECT TRIGGER_NAME" // NOI18N
                                            + " FROM information_schema.triggers WHERE TRIGGER_SCHEMA='" + catalog.getName() + "'"); // NOI18N
            try {
                while (rs.next()) {
                    String procedureName = rs.getString("TRIGGER_NAME"); // NOI18N
                    Procedure procedure = createJDBCProcedure(procedureName);
                    newProcedures.put(procedureName, procedure);
                    logger.info( "Created MySQL trigger: {0}", new Object[]{procedure});
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                stmt.close();
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        procedures = Collections.unmodifiableMap(newProcedures);
    }
    
    @Override
    protected Procedure createJDBCProcedure(String procedureName) {
        return new MySQLProcedure(this, procedureName);
    }

    @Override
    public String toString() {
        return "MySQLSchema[jdbcCatalog=" + catalog.getName() + ", name=" + getName() + "]";
    }

}
