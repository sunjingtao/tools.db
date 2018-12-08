package com.tools.data.db.metadata;

import com.tools.data.db.core.Nullable;
import com.tools.data.db.core.SQLType;
import com.tools.data.db.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Value extends Element{

    private static final Logger logger = LoggerFactory.getLogger(Catalog.class);

    private final Element parent;
    private final String name;
    private final SQLType type;
    private final String typeName;
    private final int length;
    private final int precision;
    private final short radix;
    private final short scale;
    private final Nullable nullable;

    /**
     * Create a value from a row in getProcedureColumns()
     *
     * @param rs the result set from getProcedureColumns, assumed to be at a valid row
     * @return a newly created JDBCValue instance
     * @throws java.sql.SQLException
     */
    public static Value createProcedureValue(ResultSet rs, Element parent) throws SQLException {
        String name = MetadataUtilities.trimmed(rs.getString("COLUMN_NAME"));
        SQLType type = JDBCUtils.getSQLType(rs.getInt("DATA_TYPE"));
        String typeName = rs.getString("TYPE_NAME");
        int length = rs.getInt("LENGTH");
        int precision = rs.getInt("PRECISION");
        short scale = rs.getShort("SCALE");
        short radix = rs.getShort("RADIX");
        Nullable nullable = JDBCUtils.getProcedureNullable(rs.getShort("NULLABLE"));

        return new Value(parent, name, type, typeName, length, precision, radix, scale, nullable);
    }

    /**
     * Create a value from a row in getFunctionColumns()
     *
     * @param rs the result set from getFunctionColumns, assumed to be at a
     * valid row
     * @return a newly created JDBCValue instance
     * @throws java.sql.SQLException
     */
    public static Value createFunctionValue(ResultSet rs, Element parent) throws SQLException {
        // Delegate to the procedure Version - currently the same columns are used
        return createProcedureValue(rs, parent);
    }

    /**
     * Create a value from a row in DBMD.getColumns()
     *
     * @param rs the result set from getProcedureColumns, assumed to be at a valid row
     * @return a newly created JDBCValue instance
     * @throws java.sql.SQLException
     */
    public static Value createTableColumnValue(ResultSet rs, Element parent) throws SQLException {
        String name = MetadataUtilities.trimmed(rs.getString("COLUMN_NAME"));
        SQLType type = JDBCUtils.getSQLType(rs.getInt("DATA_TYPE"));
        String typeName = rs.getString("TYPE_NAME");

        int length = 0;
        int precision = 0;

        if (JDBCUtils.isCharType(type)) {
            length = rs.getInt("COLUMN_SIZE");
        } else {
            precision = rs.getInt("COLUMN_SIZE");
        }

        
        short scale = rs.getShort("DECIMAL_DIGITS");
        short radix = rs.getShort("NUM_PREC_RADIX");
        Nullable nullable = JDBCUtils.getColumnNullable(rs.getShort("NULLABLE"));

        return new Value(parent, name, type, typeName, length, precision, radix, scale, nullable);
    }

    /**
     * Create a value from a row in DBMD.getColumns(). For ODBC connection
     * are different names of attributes.
     *
     * @param rs the result set from getColumns, assumed to be at a valid row
     * @return a newly created JDBCValue instance
     * @throws java.sql.SQLException
     */
    public static Value createTableColumnValueODBC(ResultSet rs, Element parent) throws SQLException {
        String name = MetadataUtilities.trimmed(rs.getString("COLUMN_NAME"));
        SQLType type = JDBCUtils.getSQLType(rs.getInt("DATA_TYPE"));
        String typeName = rs.getString("TYPE_NAME");
        int length = 0;
        int precision = 0;
        if (JDBCUtils.isCharType(type)) {
            length = rs.getInt("LENGTH");
        } else {
            precision = rs.getInt("PRECISION");
        }
        short scale = rs.getShort("SCALE");
        short radix = rs.getShort("RADIX");
        Nullable nullable = JDBCUtils.getColumnNullable(rs.getShort("NULLABLE"));

        return new Value(parent, name, type, typeName, length, precision, radix, scale, nullable);
    }

    public Value(Element parent, String name, SQLType type,
                 String typeName, int length, int precision, short radix,
                 short scale, Nullable nullable) {
        this.parent = parent;
        this.name = name;
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.radix = radix;
        this.scale = scale;
        this.nullable = nullable;
        this.typeName = typeName;
    }

    public int getLength() {
        return length;
    }

    public String getName() {
        return name;
    }

    public Nullable getNullable() {
        return nullable;
    }

    public int getPrecision() {
        return precision;
    }

    public short getRadix() {
        return radix;
    }

    public short getScale() {
        return scale;
    }

    public SQLType getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    @Override
    public String toString() {
        return "name=" + name + ", type=" + type + ", length=" + getLength() + ", precision=" + getPrecision() +
                ", radix=" + getRadix() + ", scale=" + getScale() + ", nullable=" + nullable;
    }

    public Element getParent() {
        return this.parent;
    }

}
