package com.tools.data.db.metadata.mysql;

import com.tools.data.db.metadata.*;
import com.tools.data.db.core.Nullable;
import com.tools.data.db.core.SQLType;
import com.tools.data.db.util.JDBCUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLProcedure extends Procedure {
    public MySQLProcedure(Schema jdbcSchema, String name) {
        super(jdbcSchema, name);
    }

    @Override
    protected Parameter createJDBCParameter(int position, ResultSet rs) throws SQLException {
        Parameter.Direction direction = JDBCUtils.getProcedureDirection(rs.getShort("COLUMN_TYPE"));
        return new Parameter(this, createValue(rs, this), direction, position);
    }

    @Override
    protected Value createJDBCValue(ResultSet rs) throws SQLException {
        return createValue(rs, this);
    }

    @Override
    public String toString() {
        return "MySQLProcedure[name=" + getName() + "]";
    }

    /**
     * A "special" version because MySQL returns character lengths in
     * the precision column instead of the length column - sheesh.
     *
     * Logged as a MySQL bug - http://bugs.mysql.com/bug.php?id=41269
     * When this is fixed this workaround will need to be backed out.
     */
    private static Value createValue(ResultSet rs, Element parent) throws SQLException {
        String name = rs.getString("COLUMN_NAME");

        int length = 0;
        int precision = 0;

        SQLType type = JDBCUtils.getSQLType(rs.getInt("DATA_TYPE"));
        String typeName = rs.getString("TYPE_NAME");
        if (JDBCUtils.isNumericType(type)) {
            precision = rs.getInt("PRECISION");
        } else {
            length = rs.getInt("PRECISION");
        }
        short scale = rs.getShort("SCALE");
        short radix = rs.getShort("RADIX");
        Nullable nullable = JDBCUtils.getProcedureNullable(rs.getShort("NULLABLE"));

        return new Value(parent, name, type, typeName, length, precision, radix, scale, nullable);
    }


}
