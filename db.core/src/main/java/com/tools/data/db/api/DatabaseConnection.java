package com.tools.data.db.api;

import com.tools.data.db.core.ConnectionUrl;
import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.metadata.Metadata;
import com.tools.data.db.metadata.MetadataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;
import java.util.Properties;


public final class DatabaseConnection {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnection.class);

    private String database;
    private String user;
    private String password = ""; //NOI18N
    private Properties connectionProperties = null;
    private ConnectionUrl connectionUrl = null;
    private String connectionUrlString = null;
    private Connection connection = null;
    private Metadata metadata;

    public DatabaseConnection(Properties properties) {
        this.database = properties.getProperty("database");
        this.user = properties.getProperty("user");
        this.password = properties.getProperty("password");
        this.connectionProperties = properties;
        connectionUrl = ConnectionUrlManager.getConnectionUrl(properties.getProperty("dbtype"),properties.getProperty("version"));
        connectionUrlString = ConnectionUrlManager.getValidUrl(connectionUrl,properties);
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public Properties getConnectionProperties() {
        return (Properties) connectionProperties.clone();
    }

    public String getPassword() {
        return password;
    }

    public Connection openConnection(){
        logger.info("begin to create jdbc connection ...");
        if(connection != null) return connection;
        try {
            Class.forName(connectionUrl.getDriver());
            connection = DriverManager.getConnection(connectionUrlString,connectionProperties);
            return connection;
        } catch (Exception e) {
            logger.error(MessageFormat.format("Cannot establish a connection to {0} using {1} ({2})",
                    database, connectionUrl.getDriver(), e.getMessage()));
        }
        return connection;
    }

    public Metadata getMetadata(){
        return MetadataFactory.createMetadata(connection,getDatabase());
    }

    public boolean isConnected() {
        return connection != null;
    }

    public Connection getConnection() {
        return connection;
    }

    public void closeConnection() throws DatabaseException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }
            connection = null;
        }
    }

}
