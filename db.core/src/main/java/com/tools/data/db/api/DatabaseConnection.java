/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 1997-2011 Oracle and/or its affiliates. All rights reserved.
 *
 * Oracle and Java are registered trademarks of Oracle and/or its affiliates.
 * Other names may be trademarks of their respective owners.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common
 * Development and Distribution License("CDDL") (collectively, the
 * "License"). You may not use this file except in compliance with the
 * License. You can obtain a copy of the License at
 * http://www.netbeans.org/cddl-gplv2.html
 * or nbbuild/licenses/CDDL-GPL-2-CP. See the License for the
 * specific language governing permissions and limitations under the
 * License.  When distributing the software, include this License Header
 * Notice in each file and include the License file at
 * nbbuild/licenses/CDDL-GPL-2-CP.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the GPL Version 2 section of the License file that
 * accompanied this code. If applicable, add the following below the
 * License Header, with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * Contributor(s):
 *
 * The Original Software is NetBeans. The Initial Developer of the Original
 * Software is Sun Microsystems, Inc. Portions Copyright 1997-2010 Sun
 * Microsystems, Inc. All Rights Reserved.
 *
 * If you wish your version of this file to be governed by only the CDDL
 * or only the GPL Version 2, indicate your decision by adding
 * "[Contributor] elects to include this software in this distribution
 * under the [CDDL or GPL Version 2] license." If you do not indicate a
 * single choice of license, a recipient has the option to distribute
 * your version of this file under either the CDDL, the GPL Version 2 or
 * to extend the choice of license to its licensees as provided above.
 * However, if you add GPL Version 2 code and therefore, elected the GPL
 * Version 2 license, then the option applies only if the new code is
 * made subject to such option by the copyright holder.
 */

package com.tools.data.db.api;


import com.tools.data.db.exception.DatabaseException;
import com.tools.data.db.lib.ddl.CommandNotSupportedException;
import com.tools.data.db.lib.ddl.DBConnection;
import com.tools.data.db.lib.ddl.DDLException;
import com.tools.data.db.lib.ddl.impl.Specification;
import com.tools.data.db.metadata.api.MetadataModel;

import java.io.ObjectStreamException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Connection information
 * This class encapsulates all information needed for connection to database
 * (database and driver url, login name, password and schema name). It can create JDBC
 * connection and feels to be a bean (has propertychange support and customizer).
 * Instances of this class uses api option to store information about
 * open connection.
 */
public final class DatabaseConnection implements DBConnection {
    public enum State {
        disconnected,
        connecting,
        connected,
        failed
    }

    private static final Logger LOGGER = Logger.getLogger(DatabaseConnection.class.getName());

    static final long serialVersionUID =4554639187416958735L;

    private Connection jdbcConnection;

    /** Driver URL and name */
    private String drv, drvname;

    /** Database URL */
    private String db;

    /** User login name */
    private String usr;

    /** The default catalog */
    private String defaultCatalog = null;

    /** The default schema */
    private String defaultSchema = null;

    private Set<String> importantSchemas = null;

    private Set<String> importantCatalogs = null;

    /** Schema name */
    private String schema;

    /** User password */
    private String pwd = ""; //NOI18N

    /** Remembers password */
    private Boolean rpwd = Boolean.FALSE;

    private String connectionFileName;

    /** Connection name */
    private String name;

    /** The user-specified name that is to be displayed for this connection. */
    private String displayName;

    /** Error code */
    private int errorCode = -1;

    /** this is the connector used for performing connect and disconnect processing */
    private final DatabaseConnector connector = new DatabaseConnector(this);

    /** the DatabaseConnection is essentially used as a container for a metadata model
     * created elsewhere.
     */
    private MetadataModel metadataModel = null;

    /** Properties for connection
     */
    private Properties connectionProperties = new Properties();

    private volatile boolean separateSystemTables = false;

    private Boolean useScrollableCursors = null; // null = driver default

    private State state;

    /**
     * The API DatabaseConnection (delegates to this instance)
     */
    private transient DatabaseConnection dbconn;

    private static final String SUPPORT = "_schema_support"; //NOI18N
    public static final String PROP_DRIVER = "driver"; //NOI18N
    public static final String PROP_DATABASE = "database"; //NOI18N
    public static final String PROP_USER = "user"; //NOI18N
    public static final String PROP_PASSWORD = "password"; //NOI18N
    public static final String PROP_REMEMBER_PASSWORD = "rememberpwd";
    public static final String PROP_SCHEMA = "schema"; //NOI18N
    public static final String PROP_DEFSCHEMA = "defaultSchema"; //NOI18N
    public static final String PROP_DEFCATALOG = "defaultCatalog"; //NOI18N
    public static final String PROP_DRIVERNAME = "drivername"; //NOI18N
    public static final String PROP_NAME = "name"; //NOI18N
    public static final String PROP_DISPLAY_NAME = "displayName"; //NOI18N
    public static final String PROP_CONNECTIONPROPERTIES = "connectionProperties";
    public static final String DRIVER_CLASS_NET = "org.apache.derby.jdbc.ClientDriver"; // NOI18N
    public static final int DERBY_UNICODE_ERROR_CODE = 20000;
    private volatile JDBCDriver jdbcdrv = null;
    private JDBCDriver[] drivers = null;

    /** Default constructor */
    @SuppressWarnings("LeakingThisInConstructor")
    public DatabaseConnection() {
    }

    /** Advanced constructor
     * Allows to specify all needed information.
     * @param driver Driver URL
     * @param database Database URL
     * @param user User login name
     * @param password User password
     */
    public DatabaseConnection(String driver, String database, String user, String password) {
        this(driver, null, database, null, user, password, null, null);
    }

    public DatabaseConnection(String driver, String driverName, String database,
            String theschema, String user, String password) {
        this(driver, driverName, database, theschema, user, password, null, null);
    }

    public DatabaseConnection(String driver, String driverName, String database,
            String theschema, String user) {
        this(driver, driverName, database, theschema, user, null, null, null);
    }

    public DatabaseConnection(String driver, String driverName, String database,
            String theschema, String user, Properties connectionProperties) {
        this(driver, driverName, database, theschema, user, null, null, connectionProperties);
    }

    public DatabaseConnection(String driver, String driverName, String database,
            String theschema, String user, String password,
            Boolean rememberPassword) {
        this(driver, driverName, database, theschema, user, password,
                rememberPassword, null);
    }

    public DatabaseConnection(String driver, String driverName, String database,
            String theschema, String user, String password,
            Boolean rememberPassword, Properties connectionProperties) {
        this();
        drv = driver;
        drvname = driverName;
        db = database;
        usr = user;
        pwd = password;
        rpwd = rememberPassword;
        schema = theschema;
        name = getName();
        setConnectionProperties(connectionProperties);
    }

    /**
     * Find a registered JDBC driver matching this connection. The function
     * makes a best effort search, if at least a driver with a matching classname
     * is present this function will succeed. If a driver with a matching name is
     * present this will be returned.
     *
     * @return matching JDBC driver for connection or NULL if no match is found
     */
    public JDBCDriver findJDBCDriver() {
//        JDBCDriver[] drvs = JDBCDriverManager.getDefault().getDrivers(drv);
        JDBCDriver[] drvs = null;
        if (drivers == null || !Arrays.equals(drvs, drivers)) {
            drivers = drvs;

            JDBCDriver useDriver = null;

            if (drvs.length > 0) {
                // Fallback - potentially false driver (by name), but at least matches requested class
                useDriver = drvs[0];
            for (int i = 0; i < drvs.length; i++) {
                if (drvs[i].getName().equals(getDriverName())) {
                    useDriver = drvs[i];
                    break;
                }
            }
            }

            jdbcdrv = useDriver;
        }
        return jdbcdrv;
    }

    public void setMetadataModel(MetadataModel model) {
        metadataModel = model;
    }

    public MetadataModel getMetadataModel() {
        return metadataModel;
    }

    /** Returns driver class */
    @Override
    public String getDriver() {
        return drv;
    }

    /** Sets driver class
     * Fires propertychange event.
     * @param driver DNew driver URL
     */
    @Override
    public void setDriver(String driver) {
        if (driver == null || driver.equals(drv)) {
            return;
        }

        String olddrv = drv;
        drv = driver;
    }

    @Override
    public String getDriverName() {
        return drvname;
    }

    @Override
    public void setDriverName(String name) {
        if (name == null || name.equals(drvname)) {
            return;
        }

        String olddrv = drvname;
        drvname = name;
    }

    /** Returns database URL */
    @Override
    public String getDatabase() {
        if (db == null) {
            db = "";
        }

        return db;
    }

    /** Sets database URL
     * Fires propertychange event.
     * @param database New database URL
     */
    @Override
    public void setDatabase(String database) {
        if (database == null || database.equals(db)) {
            return;
        }

        String oldDisplayName = getDisplayName();
        String oldName = getName();
        String olddb = db;
        db = database;
        name = null;
        name = getName();
    }

    /** Returns user login name */
    @Override
    public String getUser() {
        if (usr == null) {
            usr = "";
        }

        return usr;
    }

    /** Sets user login name
     * Fires propertychange event.
     * @param user New login name
     */
    @Override
    public void setUser(String user) {
        if (user == null || user.equals(usr)) {
            return;
        }

        String oldDisplayName = getDisplayName();
        String oldName = getName();
        String oldusr = usr;
        usr = user;
        name = null;
        name = getName();
    }

    /** Returns name of the connection */
    @Override
    public String getName() {
        if(name == null) {
            if((getSchema()==null)||(getSchema().length()==0)) {
                name = MessageFormat.format("{0} [{1} on {2}]", getDatabase(), getUser(),
                       "Default schema"); //NOI18N
            } else {
                name = MessageFormat.format("{0} [{1} on {2}]", getDatabase(), getUser(), getSchema()); //NOI18N
            }
        }
        return name;
    }

    /** Sets user name of the connection
     * Fires propertychange event.
     * @param value New connection name
     */
    @Override
    public void setName(String value) {
        if (name == null || getName().equals(value)) {
            return;
        }

        String old = name;
        name = value;
    }

    @Override
    public String getDisplayName() {
        return (displayName != null && displayName.length() > 0) ? displayName : getName();
    }

    @Override
    public void setDisplayName(String value) {
        if ((displayName == null && value == null) || getDisplayName().equals(value)) {
            return;
        }

        String old = displayName;
        displayName = value;
    }

    @Override
    public Properties getConnectionProperties() {
        return (Properties) connectionProperties.clone();
    }

    @Override
    public void setConnectionProperties(Properties connectionProperties) {
        Properties old = this.connectionProperties;
        if (connectionProperties == null) {
            this.connectionProperties = new Properties();
        } else {
            this.connectionProperties = (Properties) connectionProperties.clone();
        }
    }

    /** Returns user schema name */
    @Override
    public String getSchema() {
        if (schema == null) {
            schema = "";
        }

        if (schema.length() == 0) {
            return defaultSchema == null ? "" : defaultSchema;
        }

        return schema;
    }

    /** Sets user schema name
     * Fires propertychange event.
     * @param schema_name New login name
     */
    @Override
    public void setSchema(String schema_name) {
        if (schema_name == null || schema_name.equals(schema)) {
            return;
        }
        String oldDisplayName = getDisplayName();
        String oldName = getName();
        String oldschema = schema;
        name = null;
        schema = schema_name;
        name = getName();
        String newDisplayName = getDisplayName();
    }

    public void setDefaultSchema(String newDefaultSchema) throws Exception {
//        DDLHelper.setDefaultSchema(getConnector().getDatabaseSpecification(), newDefaultSchema);

        String oldName = name;
        name = null;

        String oldDefaultSchema = defaultSchema;
        defaultSchema = newDefaultSchema;

        name = getName();
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setConnectionFileName(String connectionFileName) {
        this.connectionFileName = connectionFileName;
    }

    /** Returns password */
    @Override
    public String getPassword() {
        if (pwd == null) {
        }
        return pwd;
    }

    /** Sets password
     * Fires propertychange event.
     * @param password New password
     */
    @Override
    public void setPassword(String password) {
        if (password == null || password.equals(pwd)) {
            return;
        }
        String oldpwd = pwd;
        if ( password.length() == 0 ) {
            pwd = null;
        } else {
            pwd = password;
        }
    }

    /** Creates JDBC connection
     * Uses DriverManager to create connection to specified database. Throws
     * DDLException if none of driver/database/user/password is set or if
     * driver or database does not exist or is inaccessible.
     */
    @Override
    public Connection createJDBCConnection() throws DDLException {
        LOGGER.log(Level.FINE, "createJDBCConnection()");

        if (drv == null || db == null || usr == null ) {
            throw new DDLException("insufficient information to create a connection"); // NOI18N
        }

        Properties dbprops;
        if (connectionProperties != null) {
            dbprops = getConnectionProperties();
        } else {
            dbprops = new Properties();
        }
        if ((usr != null) && (usr.length() > 0)) {
            dbprops.put("user", usr); //NOI18N
        }
        if ((pwd != null) && (pwd.length() > 0)) {
            dbprops.put("password", pwd); //NOI18N
        }

        try {
            setState(State.connecting);

            JDBCDriver useDriver = findJDBCDriver();
            if (useDriver == null) {
                // will be loaded through DriverManager, make sure it is loaded
                Class.forName(drv);
            }

            Connection connection = DbDriverManager.getDefault().getConnection(db, dbprops, useDriver);
            setJDBCConnection(connection);

            setState(State.connected);

            return connection;
        } catch (SQLException e) {
            String message = MessageFormat.format("Cannot establish a connection to {0} using {1} ({2})", db, drv, e.getMessage()); // NOI18N

            setState(State.failed);

            initSQLException(e);
            throw new DDLException(message, e);
        } catch (ClassNotFoundException | RuntimeException exc) {
            String message = MessageFormat.format("Cannot establish a connection to {0} using {1} ({2})", db, drv, exc.getMessage()); // NOI18N

            setState(State.failed);

            throw new DDLException(message, exc);
        }
    }

        public void connectSync() throws DatabaseException {
        try {
            doConnect();
        } catch (Exception exc) {
            try {
                if (getJDBCConnection() != null) {
                    getJDBCConnection().close();
                }
            } catch (SQLException e) {
                LOGGER.log(Level.FINE, null, e);
            }
            throw new DatabaseException(exc);
        }
    }

        /* return Error code for unit test */
        public int getErrorCode() {
            return errorCode;
        }

    @SuppressWarnings("deprecation")
    private void doConnect() throws DDLException {
        if (drv == null || db == null || usr == null ) {
            throw new DDLException("insufficient information to create a connection");
        }

        Properties dbprops;
        if (connectionProperties != null) {
            dbprops = getConnectionProperties();
        } else {
            dbprops = new Properties();
        }
        if ((usr != null) && (usr.length() > 0)) {
            dbprops.put("user", usr); //NOI18N
        }
        if ((pwd != null) && (pwd.length() > 0)) {
            dbprops.put("password", pwd); //NOI18N
        }

        Connection conn = null;
        try {
            setState(State.connecting);

            JDBCDriver useDriver = findJDBCDriver();
            if (useDriver == null) {
                // will be loaded through DriverManager, make sure it is loaded
                Class.forName(drv);
            }

            conn = DbDriverManager.getDefault().getConnection(db, dbprops, useDriver);
            setJDBCConnection(conn);

            connector.finishConnect(null);

            setState(State.connected);

            if (getConnector().getDatabaseSpecification() != null && getConnector().supportsCommand(Specification.DEFAULT_SCHEMA)) {
                try {
                    setDefaultSchema(getSchema());
                } catch (DDLException | CommandNotSupportedException x) {
                    LOGGER.log(Level.INFO, x.getLocalizedMessage(), x);
                }
            }
        } catch (Exception e) {
            String message = MessageFormat.format("Cannot establish a connection to {0} using {1} ({2})", // NOI18N
                        db, drv, e.getMessage());
            // Issue 69265
            if (drv.equals(DRIVER_CLASS_NET)) {
                if (e instanceof SQLException) {
                    errorCode = ((SQLException) e).getErrorCode();
                    if (errorCode == DERBY_UNICODE_ERROR_CODE) {
                        message = "Unable to create the database because the Derby network driver does not support multibyte characters.";
                    }
                }
            }

            setState(State.failed);

            if (e instanceof SQLException) {
                initSQLException((SQLException)e);
            }

            DDLException ddle = new DDLException(message);
            ddle.initCause(e);

            if (conn != null) {
                setJDBCConnection(null);
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    LOGGER.log(Level.WARNING, null, sqle); // NOI18N
                }
            }

            throw ddle;
        } catch (Throwable t) {
            String message = MessageFormat.format("Cannot establish a connection to {0} using {1} ({2})", // NOI18N
                        db, drv, t.getMessage());
            setState(State.failed);
        } finally {
        }
    }

    public boolean isConnected() {
        return jdbcConnection != null;
    }

    /** Calls the initCause() for SQLException with the value
      * of getNextException() so this exception's stack trace contains
      * the complete data.
      */
    private void initSQLException(SQLException e) {
        SQLException current = e;
        SQLException next = current.getNextException();
        while (next != null) {
            try {
                current.initCause(next);
            } catch (IllegalStateException e2) {
                // do nothing, already initialized
            }
            current = next;
            next = current.getNextException();
        }
    }

    private void setJDBCConnection(Connection c) {
        jdbcConnection = c;
    }

    public Connection getJDBCConnection() {
        return jdbcConnection;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(drv) + Objects.hashCode(db) + Objects.hashCode(usr);
    }

    /** Compares two connections.
     * Returns true if driver, database and login name equals.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DatabaseConnection) {
            DatabaseConnection conn = (DatabaseConnection) obj;
            return Objects.equals(drv, conn.drv)
                    && Objects.equals(drvname, conn.drvname)
                    && Objects.equals(db, conn.db)
                    && Objects.equals(usr, conn.usr)
                    && Objects.equals(getSchema(), conn.getSchema())
                    && Objects.equals(getConnectionProperties(), conn.getConnectionProperties());
        }

        return false;
    }

    /** Reads object from stream */
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        drv = (String) in.readObject();
        db = (String) in.readObject();
        usr = (String) in.readObject();
        schema = (String) in.readObject();
        rpwd = Boolean.FALSE;
        name = (String) in.readObject();

        try {
            drvname = (String) in.readObject();
            displayName = (String) in.readObject();
        } catch (Exception exc) {
            //IGNORE - drvname not stored in 3.6 and earlier
            //IGNORE - displayName not stored in 6.7 and earlier
        }
        try {
            connectionProperties = (Properties) in.readObject();
        } catch (Exception ex) {
            //IGNORE - connectionProperties not stored in 7.3 and earlier
        }

        // boston setting/pilsen setting?
        if ((name != null) && (name.equals(DatabaseConnection.SUPPORT))) {
            // pilsen
        } else {
            // boston
            schema = null;
        }
        name = null;
        name = getName();
    }

    /** Writes object to stream */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        out.writeObject(drv);
        out.writeObject(db);
        out.writeObject(usr);
        out.writeObject(schema);
        out.writeObject(DatabaseConnection.SUPPORT);
        out.writeObject(drvname);
        out.writeObject(displayName);
        out.writeObject(connectionProperties);
    }

    @Override
    public String toString() {
        return "Driver:" + getDriver() + "Database:" + getDatabase().toLowerCase() + "User:" + getUser().toLowerCase() + "Schema:" + getSchema().toLowerCase(); // NOI18N
    }

    /**
     * Gets the API DatabaseConnection which corresponds to this connection.
     */
    public DatabaseConnection getDatabaseConnection() {
        return dbconn;
    }

    public DatabaseConnector getConnector() {
        return connector;
    }

    public void disconnect() throws DatabaseException {
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
            } catch (Exception ex) {
    }

            connector.performDisconnect();
            jdbcConnection = null;
            setState(State.disconnected);
        }
    }

    private Object readResolve() throws ObjectStreamException {
        return this;
    }

    public Set<String> getImportantSchemas() {
        if (importantSchemas == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(importantSchemas);
        }
    }

    public void addImportantSchema(String schema) {
        if (importantSchemas == null) {
            importantSchemas = new HashSet<>();
        }
        List<String> oldList = new ArrayList<>(importantSchemas);
        importantSchemas.add(schema);
    }

    public void removeImportantSchema(String schema) {
        if (importantSchemas != null) {
            List<String> oldList = new ArrayList<>(importantSchemas);
            importantSchemas.remove(schema);
        }
    }

    public boolean isImportantSchema(String schema) {
        return importantSchemas != null && importantSchemas.contains(schema);
    }

    public Set<String> getImportantCatalogs() {
        if (importantCatalogs == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(importantCatalogs);
        }
    }

    public void addImportantCatalog(String database) {
        if (importantCatalogs == null) {
            importantCatalogs = new HashSet<>();
        }
        List<String> oldList = new ArrayList<>(importantCatalogs);
        importantCatalogs.add(database);
    }

    public void removeImportantCatalog(String database) {
        if (importantCatalogs != null) {
            List<String> oldList = new ArrayList<>(importantCatalogs);
            importantCatalogs.remove(database);
        }
    }

    public boolean isImportantCatalog(String database) {
        return importantCatalogs != null && importantCatalogs.contains(database);
    }

    public boolean isSeparateSystemTables() {
        return separateSystemTables;
    }

    public void setSeparateSystemTables(boolean separateSystemTables) {
        boolean oldVal = this.separateSystemTables;
        this.separateSystemTables = separateSystemTables;
    }



    public State getState() {
        return state;
}

    private void setState(State state) {
        State oldState = this.state;
        this.state = state;
    }
}
