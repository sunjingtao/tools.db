package com.tools.data.db.metadata;

import com.tools.data.db.core.Nullable;
import com.tools.data.db.core.SQLType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Parameter {
    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);
    public enum Direction {
        IN, OUT, INOUT;
    };

    private final Element parent;
    private final Direction direction;
    private final int ordinalPosition;
    private final Value value;

    public Parameter(Element parent, Value value, Direction direction, int ordinalPosition) {
        this.parent = parent;
        this.direction = direction;
        this.value = value;
        this.ordinalPosition = ordinalPosition;
    }

    @Override
    public String toString() {
        return "JDBCParameter[" + value + ", direction=" + getDirection() + ", position=" + getOrdinalPosition() + "]"; // NOI18N
    }

    public Element getParent() {
        return parent;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getName() {
        return value.getName();
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

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

}
