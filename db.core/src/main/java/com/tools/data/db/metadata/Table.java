package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.core.Ordering;
import com.tools.data.db.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

public class Table extends Element{

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    private final Schema jdbcSchema;
    private final String name;
    private final boolean system;

    private Map<String, Column> columns;
    private Map<String, Index> indexes;
    private Map<String, ForeignKey> foreignKeys;
    private PrimaryKey primaryKey;

    // Need a marker because there may be *no* primary key, and we don't want
    // to hit the database over and over again when there is no primary key
    private boolean primaryKeyInitialized = false;
    private static final String SQL_EXCEPTION_NOT_YET_IMPLEMENTED = "not yet implemented";

    public Table(Schema jdbcSchema, String name, boolean system) {
        this.jdbcSchema = jdbcSchema;
        this.name = name;
        this.system = system;
    }

    public final Schema getParent() {
        return jdbcSchema;
    }

    public final String getName() {
        return name;
    }

    public final Collection<Column> getColumns() {
        return initColumns().values();
    }

    public final Column getColumn(String name) {
        return MetadataUtilities.find(name, initColumns());
    }

    public PrimaryKey getPrimaryKey() {
        return initPrimaryKey();
    }

    public Index getIndex(String indexName) {
        return MetadataUtilities.find(indexName, initIndexes());
    }

    public Collection<Index> getIndexes() {
        return initIndexes().values();
    }

    public Collection<ForeignKey> getForeignKeys() {
        return initForeignKeys().values();
    }

    public ForeignKey getForeignKeyByInternalName(String name) {
         return MetadataUtilities.find(name, initForeignKeys());
    }

    public boolean isSystem() {
        return system;
    }

    @Override
    public String toString() {
        return "JDBCTable[name='" + name + "']"; // NOI18N
    }

    protected Column createJDBCColumn(ResultSet rs) throws SQLException {
        int position = 0;
        Value jdbcValue;
        if (isOdbc(rs)) {
            jdbcValue = Value.createTableColumnValueODBC(rs, this);
        } else {
            position = rs.getInt("ORDINAL_POSITION");
            jdbcValue = Value.createTableColumnValue(rs, this);
        }
        return new Column(this, position, jdbcValue);
    }

    /** Returns true if this table is under ODBC connection. In such a case
     * some meta data like ORDINAL_POSITION or ASC_OR_DESC are not supported. */
    private boolean isOdbc(ResultSet rs) throws SQLException {
        boolean odbc = jdbcSchema.getCatalog().getMetadata().getDmd().getURL().startsWith("jdbc:odbc");  //NOI18N
        if (odbc) {
            try {
                rs.getInt("PRECISION");
                return true;
            } catch (SQLException e) {
                // ignore and return false at the end; Probably MS Access driver which supports standards
            }
        }
        return false;
    }

    protected PrimaryKey createJDBCPrimaryKey(String pkName, Collection<Column> pkcols) {
        return new PrimaryKey(this, pkName, pkcols);
    }

    protected void createColumns() {
        Map<String, Column> newColumns = new LinkedHashMap<String, Column>();
        try {
            ResultSet rs = MetadataUtilities.getColumns(
                    jdbcSchema.getCatalog().getMetadata().getDmd(),
                    jdbcSchema.getCatalog().getName(), jdbcSchema.getName(),
                    name, "%"); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        Column column = createJDBCColumn(rs);
                        newColumns.put(column.getName(), column);
                        logger.info( "Created column {0}", column); //NOI18N
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            filterSQLException(e);
        }
        columns = Collections.unmodifiableMap(newColumns);
    }

    protected void createIndexes() {
        Map<String, Index> newIndexes = new LinkedHashMap<String, Index>();
        try {
            ResultSet rs = MetadataUtilities.getIndexInfo(
                    jdbcSchema.getCatalog().getMetadata().getDmd(),
                    jdbcSchema.getCatalog().getName(), jdbcSchema.getName(),
                    name, false, true);
            if (rs != null) {
                try {
                    Index index = null;
                    String currentIndexName = null;
                    while (rs.next()) {
                        // Ignore Indices marked statistic
                        // explicit: TYPE == DatabaseMetaData or
                        // implicit: ORDINAL_POSITION == 0
                        // @see java.sql.DatabaseMetaData#getIndexInfo
                        if (rs.getShort("TYPE") //NOI18N
                                == DatabaseMetaData.tableIndexStatistic
                                || rs.getInt("ORDINAL_POSITION") == 0) { //NOI18N
                            continue;
                        }

                        String indexName = MetadataUtilities.trimmed(rs.getString("INDEX_NAME")); //NOI18N
                        if (index == null || !(currentIndexName.equals(indexName))) {
                            index = createJDBCIndex(indexName, rs);
                            logger.info( "Created index {0}", index); //NOI18N

                            newIndexes.put(index.getName(), index);
                            currentIndexName = indexName;
                        }

                        IndexColumn idx = createJDBCIndexColumn(index, rs);
                        if (idx == null) {
                            logger.info("Cannot create index column for {0} from {1}",  //NOI18N
                                    new Object[]{indexName, rs});
                        } else {
                            IndexColumn col = idx;
                            index.addColumn(col);
                            logger.info( "Added column {0} to index {1}",   //NOI18N
                                    new Object[]{col.getName(), indexName});
                        }
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            filterSQLException(e);
        }

        indexes = Collections.unmodifiableMap(newIndexes);
    }

    protected Index createJDBCIndex(String name, ResultSet rs) {
        Index.IndexType type = Index.IndexType.OTHER;
        boolean isUnique = false;
        try {
            type = JDBCUtils.getIndexType(rs.getShort("TYPE"));
            isUnique = !rs.getBoolean("NON_UNIQUE");
        } catch (SQLException e) {
            filterSQLException(e);
        }
        return new Index(this, name, type, isUnique);
    }

    protected IndexColumn createJDBCIndexColumn(Index parent, ResultSet rs) {
        Column column = null;
        int position = 0;
        Ordering ordering = Ordering.NOT_SUPPORTED;
        try {
            column = getColumn(rs.getString("COLUMN_NAME"));
            if (!isOdbc(rs)) {
                position = rs.getInt("ORDINAL_POSITION");
                ordering = JDBCUtils.getOrdering(MetadataUtilities.trimmed(rs.getString("ASC_OR_DESC")));
            }
        } catch (SQLException e) {
            filterSQLException(e);
        }
        if (column == null) {
            logger.info( "Cannot get column for index {0} from {1}",  //NOI18N
                    new Object[] {parent, rs});
            return null;
        }
        return new IndexColumn(parent, column.getName(), column, position, ordering);
    }

        protected void createForeignKeys() {
        Map<String,ForeignKey> newKeys = new LinkedHashMap<String,ForeignKey>();
        try {
            ResultSet rs = MetadataUtilities.getImportedKeys(
                    jdbcSchema.getCatalog().getMetadata().getDmd(),
                    jdbcSchema.getCatalog().getName(), jdbcSchema.getName(),
                    name);
            if (rs != null) {
                try {
                    ForeignKey fkey = null;
                    String currentKeyName = null;
                    while (rs.next()) {
                        String keyName = MetadataUtilities.trimmed(rs.getString("FK_NAME"));
                        // We have to assume that if the foreign key name is null, then this is a *new*
                        // foreign key, even if the last foreign key name was also null.
                    if (fkey == null || keyName == null || !(currentKeyName.equals(keyName))) {
                            fkey = createJDBCForeignKey(keyName, rs);
                            logger.info( "Created foreign key {0}", keyName);  //NOI18N

                            newKeys.put(fkey.getInternalName(), fkey);
                            currentKeyName = keyName;
                        }

                        ForeignKeyColumn col = createJDBCForeignKeyColumn(fkey, rs);
                        fkey.addColumn(col);
                        logger.info( "Added foreign key column {0} to foreign key {1}",  //NOI18N
                                new Object[]{col.getName(), keyName});
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            filterSQLException(e);
        }

        foreignKeys = Collections.unmodifiableMap(newKeys);
    }

    protected ForeignKey createJDBCForeignKey(String name, ResultSet rs) {
        return new ForeignKey(this, name);
    }

    protected ForeignKeyColumn createJDBCForeignKeyColumn(ForeignKey parent, ResultSet rs) {
        Table table;
        String colname;
        Column referredColumn = null;
        Column referringColumn = null;
        int position = 0;

        try {
            table = findReferredTable(rs);
            colname = MetadataUtilities.trimmed(rs.getString("PKCOLUMN_NAME")); // NOI18N
            referredColumn = table.getColumn(colname);
            if (referredColumn == null) {
                throwColumnNotFoundException(table, colname);
            }

            colname = MetadataUtilities.trimmed(rs.getString("FKCOLUMN_NAME"));
            referringColumn = getColumn(colname);

            if (referringColumn == null) {
                throwColumnNotFoundException(this, colname);
            }
            position = rs.getInt("KEY_SEQ");
        } catch (SQLException e) {
            filterSQLException(e);
        }
        return new ForeignKeyColumn(parent, referringColumn.getName(), referringColumn, referredColumn, position);
    }
    
    private void throwColumnNotFoundException(Table table, String colname)
            throws MetadataException {
        String message = MessageFormat.format("The column named {3} in catalog {0}, schema {1}, table {2} can not be found", //NOI18N
                table.getParent().getParent().getName(),
                table.getParent().getName(), table.getName(), colname);
        MetadataException e = new MetadataException(message);
        logger.info(message, e);
        throw e;
    }

    private Table findReferredTable(ResultSet rs) {
        Metadata metadata = jdbcSchema.getCatalog().getMetadata();
        Catalog catalog;
        Schema schema;
        Table table = null;

        try {
            String catalogName = MetadataUtilities.trimmed(rs.getString("PKTABLE_CAT")); // NOI18N
            if (catalogName != null || catalogName.length() != 0) {
                logger.error(MessageFormat.format("The catalog {0} can not be found", catalogName));
            }
            String schemaName = MetadataUtilities.trimmed(rs.getString("PKTABLE_SCHEM")); // NOI18N
            if (schemaName != null || schemaName.length() != 0) {
                logger.error("The schema named {0} can not be found in the catalog {1}", schemaName, catalogName);
            }

            String tableName = MetadataUtilities.trimmed(rs.getString("PKTABLE_NAME"));
            table = getParent().getTable(tableName);

            if (table == null) {
                throw new MetadataException(MessageFormat.format("The table named {2} in catalog {0}, schema {1} can not be found", catalogName, schemaName, tableName));
            }

        } catch (SQLException e) {
            filterSQLException(e);
        }

        return table;
    }

    protected void createPrimaryKey() {
        String pkname = null;
        Collection<Column> pkcols = new ArrayList<Column>();
        try {
            ResultSet rs = MetadataUtilities.getPrimaryKeys(
                    jdbcSchema.getCatalog().getMetadata().getDmd(),
                    jdbcSchema.getCatalog().getName(), jdbcSchema.getName(),
                    name);
            if (rs != null) {
                try {
                    while (rs.next()) {
                        if (pkname == null) {
                            pkname = MetadataUtilities.trimmed(rs.getString("PK_NAME"));
                        }
                        String colName = MetadataUtilities.trimmed(rs.getString("COLUMN_NAME"));
                        pkcols.add(getColumn(colName));
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            filterSQLException(e);
        }

        primaryKey = createJDBCPrimaryKey(pkname, Collections.unmodifiableCollection(pkcols));
    }

    private Map<String, Column> initColumns() {
        if (columns != null) {
            return columns;
        }
        logger.info( "Initializing columns in {0}", this);
        createColumns();
        return columns;
    }

    private Map<String, Index> initIndexes() {
        if (indexes != null) {
            return indexes;
        }
        logger.info( "Initializing indexes in {0}", this);

        createIndexes();
        return indexes;
    }

    private Map<String,ForeignKey> initForeignKeys() {
        if (foreignKeys != null) {
            return foreignKeys;
        }
        logger.info( "Initializing foreign keys in {0}", this);

        createForeignKeys();
        return foreignKeys;
    }

    private PrimaryKey initPrimaryKey() {
        if (primaryKeyInitialized) {
            return primaryKey;
        }
        logger.info( "Initializing columns in {0}", this);
        // These need to be initialized first.
        getColumns();
        createPrimaryKey();
        primaryKeyInitialized = true;
        return primaryKey;
    }

    private void filterSQLException(SQLException x) throws MetadataException {
        if (SQL_EXCEPTION_NOT_YET_IMPLEMENTED.equalsIgnoreCase(x.getMessage())) {
            logger.info(x.getLocalizedMessage(), x);
        } else {
            throw new MetadataException(x);
        }
    }
}
