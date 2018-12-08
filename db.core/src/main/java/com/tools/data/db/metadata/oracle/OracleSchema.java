package com.tools.data.db.metadata.oracle;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.metadata.*;
import com.tools.data.db.util.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class OracleSchema extends Schema {

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    public OracleSchema(Catalog catalog, String name) {
        super(catalog, name);
    }


    @Override
    protected void createTables() {
        logger.info( "Initializing tables in {0}", this);
        Map<String, Table> newTables = new LinkedHashMap<String, Table>();
        try {
            DatabaseMetaData dmd = getCatalog().getMetadata();
            Set<String> recycleBinTables = getRecycleBinObjects(dmd, "TABLE"); // NOI18N
            ResultSet rs = dmd.getTables(getCatalog().getName(), getName(), "%", new String[]{"TABLE"}); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String type = MetadataUtils.trimmed(rs.getString("TABLE_TYPE")); //NOI18N
                        String tableName = rs.getString("TABLE_NAME"); // NOI18N
                        if (!recycleBinTables.contains(tableName)) {
                            Table table = createJDBCTable(tableName, type.contains("SYSTEM")); //NOI18N
                            newTables.put(tableName, table);
                            logger.info( "Created table {0}", table);
                        } else {
                            logger.info( "Ignoring recycle bin table ''{0}''", tableName);
                        }
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        tables = Collections.unmodifiableMap(newTables);
    }

    private Set<String> getRecycleBinObjects(DatabaseMetaData dmd, String... types) {
        String driverName = null;
        String driverVer = null;
        List<String> emptyList = Collections.emptyList();
        Set<String> result = new HashSet<String>();
        try {
            driverName = dmd.getDriverName();
            driverVer = dmd.getDriverVersion();
            int databaseMajorVersion = 0;
            try {
                databaseMajorVersion = dmd.getDatabaseMajorVersion();
            } catch (UnsupportedOperationException use) {
                logger.info("getDatabaseMajorVersion() on " + dmd, use);
            }
            if (databaseMajorVersion < 10 || types == null) {
                return Collections.emptySet();
            }
            Statement stmt = dmd.getConnection().createStatement();
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery("SELECT OBJECT_NAME, TYPE FROM SYS.DBA_RECYCLEBIN"); // NOI18N
            } catch (SQLException ex) {
                logger.info( ex.getMessage(), ex);
                // try both
                rs = stmt.executeQuery("SELECT OBJECT_NAME, TYPE FROM RECYCLEBIN"); // NOI18N
            }
            if (rs != null) {
                List<String> typesL = types == null ? emptyList : Arrays.asList(types);
                try {
                    while (rs.next()) {
                        String type = rs.getString("TYPE"); // NOI18N
                        if (typesL.isEmpty() || typesL.contains(type)) {
                            result.add(rs.getString("OBJECT_NAME")); // NOI18N
                        }
                    }
                } finally {
                    rs.close();
                }
            }
            stmt.close();
        } catch (Exception e) {
            logger.info( "Error while analyzing the recycle bin. JDBC Driver: " + driverName + "(" + driverVer + ")", e);
        }
        return result;
    }

    @Override
    protected void createProcedures() {
        logger.info( "Initializing Oracle procedures in {0}", this);
        Map<String, Procedure> newProcedures = new LinkedHashMap<String, Procedure>();
        try {
            DatabaseMetaData dmd = getCatalog().getMetadata();
            Statement stmt = dmd.getConnection().createStatement();
            Set<String> recycleBinObjects = getRecycleBinObjects(dmd, "TRIGGER", "FUNCTION", "PROCEDURE"); // NOI18N
            ResultSet rs = stmt.executeQuery("SELECT OBJECT_NAME, OBJECT_TYPE, STATUS FROM SYS.ALL_OBJECTS WHERE OWNER='" + getName() + "'" // NOI18N
                    + " AND ( OBJECT_TYPE = 'PROCEDURE' OR OBJECT_TYPE = 'TRIGGER' OR OBJECT_TYPE = 'FUNCTION' )"); // NOI18N
            try {
                while (rs.next()) {
                    String procedureName = rs.getString("OBJECT_NAME"); // NOI18N
                    Procedure procedure = createJDBCProcedure(procedureName);
                    if (!recycleBinObjects.contains(procedureName)) {
                        newProcedures.put(procedureName, procedure);
                        logger.info( "Created Oracle procedure: {0}, type: {1}, status: {2}", new Object[]{procedure, rs.getString("OBJECT_TYPE"), rs.getString("STATUS")});
                    } else {
                        logger.info("Oracle procedure found id RECYCLEBIN: {0}, type: {1}, status: {2}", new Object[]{procedure, rs.getString("OBJECT_TYPE"), rs.getString("STATUS")});
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
            stmt.close();
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        procedures = Collections.unmodifiableMap(newProcedures);
    }
}
