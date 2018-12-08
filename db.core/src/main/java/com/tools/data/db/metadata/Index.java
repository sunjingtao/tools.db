package com.tools.data.db.metadata;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class Index {

    public enum IndexType { CLUSTERED, HASHED, OTHER };

    private final Table parent;
    private final String name;
    private final Map<String, IndexColumn> columns = new LinkedHashMap<String,IndexColumn>();
    private final IndexType indexType;
    private final boolean isUnique;

    public Index(Table parent, String name, IndexType indexType, boolean isUnique) {
        this.parent = parent;
        this.name = name;
        this.indexType = indexType;
        this.isUnique = isUnique;
    }

    public void addColumn(IndexColumn col) {
        columns.put(col.getName(), col);
    }

    public IndexColumn getColumn(String name) {
        return columns.get(name);
    }

    public final Table getParent() {
        return parent;
    }

    public final String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "JDBCIndex[name='" + name + "', type=" +indexType + ", unique=" + isUnique +"]"; // NOI18N
    }

    public Collection<IndexColumn> getColumns() {
        return columns.values();
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isUnique() {
        return isUnique;
    }
}
