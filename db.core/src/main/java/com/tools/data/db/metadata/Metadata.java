package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class Metadata {
    private static final Logger logger = LoggerFactory.getLogger(Metadata.class);

    private Connection conn;
    private DatabaseMetaData dmd;
    protected Catalog catalog;

    public Metadata(Connection conn) {
        this.conn = conn;
        try {
            dmd = conn.getMetaData();
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
    }

    public final Catalog getDefaultCatalog() {
        if (catalog == null) {
            initCatalog();
        }
        return catalog;
    }

    protected void initCatalog() {
        try {
            String catalogName = conn.getCatalog();
            catalog = new Catalog(this, catalogName);
        } catch (SQLException e) {
            logger.info("Could not load catalog from database (getCatalogs failed).");
        }
    }

    public final Connection getConnection() {
        return conn;
    }

    public final DatabaseMetaData getDmd() {
        return dmd;
    }

}
