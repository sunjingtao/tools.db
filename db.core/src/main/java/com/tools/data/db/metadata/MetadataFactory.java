package com.tools.data.db.metadata;

import com.tools.data.db.metadata.mssql.MSSQLMetadata;
import com.tools.data.db.metadata.mysql.MySQLMetadata;
import com.tools.data.db.metadata.oracle.OracleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class MetadataFactory {
    private static Logger logger = LoggerFactory.getLogger(MetadataFactory.class);

    public static Metadata createMetadata(Connection conn, String defaultSchemaName) {
        try {
            DatabaseMetaData dmd = conn.getMetaData();
            if ("Oracle".equals(dmd.getDatabaseProductName())) { // NOI18N
                return new OracleMetadata(conn, defaultSchemaName);
            }

            if ("mysql".equalsIgnoreCase(dmd.getDatabaseProductName())) { // NOI18N
                return new MySQLMetadata(conn, defaultSchemaName);
            }

            String driverName = dmd.getDriverName();
            if (driverName != null) {
                if ((driverName.contains("Microsoft") && driverName.contains("SQL Server")) //NOI18N
                        || driverName.contains("jTDS")) { //NOI18N
                    return new MSSQLMetadata(conn, defaultSchemaName);
                }
            }
        } catch (SQLException e) {
            logger.info(e.getMessage());
        }
        return new Metadata(conn, defaultSchemaName);
    }
}
