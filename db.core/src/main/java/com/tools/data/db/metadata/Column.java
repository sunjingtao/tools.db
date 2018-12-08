package com.tools.data.db.metadata;

import com.tools.data.db.core.Nullable;
import com.tools.data.db.core.SQLType;

public class Column extends Element{

    private final Element parent;
    private final Value value;
    private final int position;

    public Column(Element parent, int position, Value value) {
        this.parent = parent;
        this.value = value;
        this.position = position;
    }

    public final Element getParent() {
        return parent;
    }

    public final String getName() {
        return value.getName();
    }

    public String toString() {
        return "JDBCColumn[" + value + ", ordinal_position=" + position + "]"; // NOI18N
    }

    public int getPrecision() {
        return value.getPrecision();
    }

    public short getRadix() {
        return value.getRadix();
    }

    public short getScale() {
        return value.getScale();
    }

    public SQLType getType() {
        return value.getType();
    }

    public String getTypeName() {
        return value.getTypeName();
    }

    public int getLength() {
        return value.getLength();
    }

    public Nullable getNullable() {
        return value.getNullable();
    }

    public int getPosition() {
        return position;
    }
}
