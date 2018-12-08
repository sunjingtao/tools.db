package com.tools.data.db.metadata;

public final class ForeignKeyColumn {
    private final ForeignKey parent;
    private final String name;
    private final Column referringColumn;
    private final Column referredColumn;
    private final int position;

    public ForeignKeyColumn(ForeignKey parent, String name, Column referringColumn, Column referredColumn, int position) {
        this.parent = parent;
        this.name = name;
        this.referringColumn = referringColumn;
        this.referredColumn = referredColumn;
        this.position = position;
    }

    public String getName() {
        return name;
    }
    public String toString() {
        return "JDBCForeignKeyColumn[name='" + name + "', position=" + position + "referringColumn=" + referringColumn +", referredColumn=" + referredColumn + "]";
    }

    public ForeignKey getParent() {
        return parent;
    }

    public Column getReferredColumn() {
        return referredColumn;
    }

    public Column getReferringColumn() {
        return referringColumn;
    }

    public int getPosition() {
        return position;
    }


}
