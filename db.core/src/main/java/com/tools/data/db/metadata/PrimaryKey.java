package com.tools.data.db.metadata;

import java.util.Collection;
import java.util.Collections;

public class PrimaryKey {

    private final String name;
    private final Collection<Column> columns;
    private final Table parent;
    
    public PrimaryKey(Table parent, String name, Collection<Column> columns) {
        this.parent = parent;
        this.name = name;
        this.columns = Collections.unmodifiableCollection(columns);
    }
    
    public Collection<Column> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    public Table getParent() {
        return parent;
    }

    public String toString() {
        return "JDBCPrimaryKey[name='" + getName() + "']";
    }

}
