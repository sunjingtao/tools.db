/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2010-2012 Oracle and/or its affiliates. All rights reserved.
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
 *
 * Contributor(s):
 *
 * Portions Copyrighted 2010 Sun Microsystems, Inc.
 */
package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.metadata.mssql.MSSQLMetadata;
import com.tools.data.db.metadata.mysql.MySQLMetadata;
import com.tools.data.db.metadata.oracle.OracleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author Andrei Badea
 */
public class Metadata {

    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);
    private Connection conn;
    private String defaultSchemaName;
    private DatabaseMetaData dmd;
    protected Catalog defaultCatalog;
    protected Map<String, Catalog> catalogs;

    public Metadata(Connection conn, String defaultSchemaName) {
        logger.info("Creating metadata for default schema ''{0}''", defaultSchemaName);
        this.conn = conn;
        this.defaultSchemaName = defaultSchemaName;
        try {
            dmd = conn.getMetaData();
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
    }

    public final Catalog getDefaultCatalog() {
        initCatalogs();
        return defaultCatalog;
    }

    public final Collection<Catalog> getCatalogs() {
        return initCatalogs().values();
    }

    public final Catalog getCatalog(String name) {
        Catalog catalog = MetadataUtilities.find(name, initCatalogs());
        if (catalog == null && name == null) {
            return getDefaultCatalog();
        }

        return catalog;
    }

    public Schema getDefaultSchema() {
        Catalog catalog = getDefaultCatalog();
        if (catalog != null) {
            return catalog.getDefaultSchema();
        }
        return null;
    }

    protected Catalog createJDBCCatalog(String name, boolean _default, String defaultSchemaName) {
        return new Catalog(this, name, _default, defaultSchemaName);
    }

    protected void createCatalogs() {
        Map<String, Catalog> newCatalogs = new LinkedHashMap<String, Catalog>();
        try {
            if (!driverReportsBogusCatalogNames()) {
                String defaultCatalogName = conn.getCatalog();
                ResultSet rs = dmd.getCatalogs();
                try {
                    while (rs.next()) {
                        String catalogName = MetadataUtilities.trimmed(rs.getString("TABLE_CAT")); // NOI18N
                        logger.info("Read catalog ''{0}''", catalogName); //NOI18N
                        if (MetadataUtilities.equals(catalogName, defaultCatalogName)) {
                            defaultCatalog = createJDBCCatalog(catalogName, true, defaultSchemaName);
                            newCatalogs.put(defaultCatalog.getName(), defaultCatalog);
                            logger.info("Created default catalog {0}", defaultCatalog); //NOI18N
                        } else {
                            Catalog catalog = createJDBCCatalog(catalogName, false, null);
                            newCatalogs.put(catalogName, catalog);
                            logger.info("Created catalog {0}", catalog); //NOI18N
                        }
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }
        } catch (SQLException e) {
            logger.info("Could not load catalogs list from database (getCatalogs failed).");
        }
        if (defaultCatalog == null) {
            defaultCatalog = createJDBCCatalog(null, true, defaultSchemaName);

            // Issue 154407 - Don't put the default catalog in the list of catalogs if its name is null,
            // unless it's the *only* catalog (e.g. with Derby, where it doesn't have a concept of catalogs)
            if (newCatalogs.isEmpty()) {
                newCatalogs.put(null, defaultCatalog);
            }

            logger.info("Created fallback default catalog {0}", defaultCatalog);
        }
        catalogs = Collections.unmodifiableMap(newCatalogs);
    }

    private Map<String, Catalog> initCatalogs() {
        if (catalogs != null) {
            return catalogs;
        }
        logger.info("Initializing catalogs");
        createCatalogs();
        return catalogs;
    }

    public final Connection getConnection() {
        return conn;
    }

    public final DatabaseMetaData getDmd() {
        return dmd;
    }

    /**
     * Ignore reported catalogs from driver.
     *
     * Seems some drivers (aka the Pointbase jdbc driver) don't support catalogs
     * but against all assumptions report catalog names for getCatalogs(). These
     * names are bogus and need to be ignored.
     *
     * @return
     */
    private boolean driverReportsBogusCatalogNames() throws SQLException {
        String driverName = dmd.getDriverName();
        return "PointBase JDBC Driver".equals(driverName) ||
               "IBM Data Server Driver for JDBC and SQLJ".equals(driverName);
    }

}
