package com.tools.data.db.metadata;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ForeignKey {
    private final Table parent;
    private final String name;
    private final Map<String, ForeignKeyColumn> columns = new LinkedHashMap<String,ForeignKeyColumn>();
    private final String internalName;
    private static AtomicLong fkeyCounter = new AtomicLong(0);

    public ForeignKey(Table parent, String name) {
        this.parent = parent;
        this.name = name;
        internalName = parent.getName() + "_FKEY_" + fkeyCounter.incrementAndGet();
    }

    public void addColumn(ForeignKeyColumn col) {
        columns.put(col.getName(), col);
    }

    public final Table getParent() {
        return parent;
    }

    public final String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "JDBCForeignKey[name='" + name + "']"; // NOI18N
    }

    public Collection<ForeignKeyColumn> getColumns() {
        return columns.values();
    }

    public ForeignKeyColumn getColumn(String name) {
        return columns.get(name);
    }

    public String getInternalName() {
        String result = getName();
        return (result != null) ? result : internalName;
    }
}
