package com.tools.data.db.metadata;

import com.tools.data.db.exception.MetadataException;
import com.tools.data.db.util.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Schema implements Element{

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    private final Catalog catalog;
    private final String name;
    protected Map<String, Table> tables;
    protected Map<String, View> views;
    protected Map<String, Procedure> procedures;
    protected Map<String, Function> functions;

    public Schema(Catalog catalog, String name) {
        this.catalog = catalog;
        this.name = name;
    }

    public final Element getParent() {
        return catalog;
    }

    public final String getName() {
        return name;
    }

    public final Collection<Table> getTables() {
        return initTables().values();
    }

    public final Table getTable(String name) {
        return MetadataUtils.find(name, initTables());
    }

    public View getView(String name) {
        return MetadataUtils.find(name, initViews());
    }

    public Collection<View> getViews() {
        return initViews().values();
    }

    public Procedure getProcedure(String name) {
        return initProcedures().get(name);
    }

    public Collection<Procedure> getProcedures() {
        return initProcedures().values();
    }

    public Function getFunction(String name) {
        return initFunctions().get(name);
    }

    public Collection<Function> getFunctions() {
        return initFunctions().values();
    }

    protected Table createJDBCTable(String name, boolean system) {
        return new Table(this, name, system);
    }

    protected Procedure createJDBCProcedure(String procedureName) {
        return new Procedure(this, procedureName);
    }

    protected Function createJDBCFunction(String functionName) {
        return new Function(this, functionName);
    }

    protected View createJDBCView(String viewName) {
        return new View(this, viewName);
    }

    protected void createTables() {
        logger.info( "Initializing tables in {0}", this);
        Map<String, Table> newTables = new LinkedHashMap<String, Table>();
        try {
            ResultSet rs = MetadataUtils.getTables(catalog.getMetadata(),
                    catalog.getName(), name, "%", new String[]{"TABLE", "SYSTEM TABLE"}); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String type = MetadataUtils.trimmed(rs.getString("TABLE_TYPE")); //NOI18N
                        String tableName = MetadataUtils.trimmed(rs.getString("TABLE_NAME")); // NOI18N
                        Table table = createJDBCTable(tableName, type.contains("SYSTEM")); //NOI18N
                        newTables.put(tableName, table);
                        logger.info( "Created table {0}", table); //NOI18N
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

    protected void createViews() {
        logger.info( "Initializing views in {0}", this);
        Map<String, View> newViews = new LinkedHashMap<String, View>();
        try {
            ResultSet rs = MetadataUtils.getTables(catalog.getMetadata(),
                    catalog.getName(), name, "%", new String[]{"VIEW"}); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String viewName = MetadataUtils.trimmed(rs.getString("TABLE_NAME")); // NOI18N
                        View view = createJDBCView(viewName);
                        newViews.put(viewName, view);
                        logger.info( "Created view {0}", view); // NOI18N
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        views = Collections.unmodifiableMap(newViews);
    }

    protected void createProcedures() {
        logger.info( "Initializing procedures in {0}", this);
        Map<String, Procedure> newProcedures = new LinkedHashMap<String, Procedure>();
        try {
            ResultSet rs = MetadataUtils.getProcedures(catalog.getMetadata(),
                    catalog.getName(), name, "%"); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String procedureName = MetadataUtils.trimmed(rs.getString("PROCEDURE_NAME")); // NOI18N
                        Procedure procedure = createJDBCProcedure(procedureName);
                        newProcedures.put(procedureName, procedure);
                        logger.info( "Created procedure {0}", procedure); //NOI18N
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        procedures = Collections.unmodifiableMap(newProcedures);
    }

    protected void createFunctions() {
        logger.info( "Initializing functions in {0}", this); //NOI18N
        Map<String, Function> newProcedures = new LinkedHashMap<String, Function>();
        try {
            ResultSet rs = MetadataUtils.getFunctions(catalog.getMetadata(),
                    catalog.getName(), name, "%"); // NOI18N
            if (rs != null) {
                try {
                    while (rs.next()) {
                        String functionName = MetadataUtils.trimmed(rs.getString("FUNCTION_NAME")); // NOI18N
                        Function function = createJDBCFunction(functionName);
                        newProcedures.put(functionName, function);
                        logger.info( "Created function {0}", function); //NOI18N
                    }
                } finally {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
        functions = Collections.unmodifiableMap(newProcedures);
    }

    private Map<String, Table> initTables() {
        if (tables != null) {
            return tables;
        }
        createTables();
        return tables;
    }

    public final Catalog getCatalog() {
        return catalog;
    }

    private Map<String, View> initViews() {
        if (views != null) {
            return views;
        }
        createViews();
        return views;
    }

    private Map<String, Procedure> initProcedures() {
        if (procedures != null) {
            return procedures;
        }

        createProcedures();
        return procedures;
    }

    private Map<String, Function> initFunctions() {
        if (functions != null) {
            return functions;
        }

        createFunctions();
        return functions;
    }
}
