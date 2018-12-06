package com.tools.data.db.metadata;

import com.tools.data.db.metadata.mssql.MSSQLMetadata;
import com.tools.data.db.metadata.mysql.MySQLMetadata;
import com.tools.data.db.metadata.oracle.OracleMetadata;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andrei Badea
 */
public class ConnMetadataModel {

    private final static Logger LOGGER = Logger.getLogger(ConnMetadataModel.class.getName());

    private final ReentrantLock lock = new ReentrantLock();
    private final WeakReference<Connection> connRef;
    private final String defaultSchemaName;

    private Metadata jdbcMetadata;

    public ConnMetadataModel(Connection conn, String defaultSchemaName) {
        this.connRef = new WeakReference<Connection>(conn);
        if (defaultSchemaName != null && defaultSchemaName.trim().length() == 0) {
            this.defaultSchemaName = null;
        } else {
            this.defaultSchemaName = defaultSchemaName;
        }
        enterReadAccess(conn);
    }

    public void refresh() {
        LOGGER.fine("Refreshing model");
        lock.lock();
        try {
            jdbcMetadata = null;
        } finally {
            lock.unlock();
        }
    }

    private void enterReadAccess(final Connection conn) {
        if (conn == null) {
            throw new NullPointerException("Connection can not be null");
        }
        try {
            Connection oldConn = (jdbcMetadata != null) ? jdbcMetadata.getConnection() : null;
            if (oldConn != conn) {
                if (conn != null) {
                    jdbcMetadata = createMetadata(conn, defaultSchemaName);
                } else {
                    jdbcMetadata = null;
                }
            }
        } catch (Exception e) {
            //TODO: LOG
        }
    }

    private static Metadata createMetadata(Connection conn, String defaultSchemaName) {
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
            LOGGER.log(Level.INFO, null, e);
        }
        return new Metadata(conn, defaultSchemaName);
    }
}
