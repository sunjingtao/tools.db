package com.tools.data.db.metadata;

import com.tools.data.db.core.Ordering;

public class IndexColumn {
    private final Index parent;
    private final String name;
    private final Column column;
    private final int position;
    private final Ordering ordering;

    public IndexColumn(Index parent, String name, Column column, int position, Ordering ordering) {
        this.parent = parent;
        this.name = name;
        this.column = column;
        this.position = position;
        this.ordering = ordering;
    }

    public Column getColumn() {
        return column;
    }

    public String getName() {
        return name;
    }

    public Ordering getOrdering() {
        return ordering;
    }

    public Index getParent() {
        return parent;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "JDBCIndexColumn[name='" + name + "', ordering=" + ordering + ", position=" + position + ", column=" + column +"]";
    }

}
