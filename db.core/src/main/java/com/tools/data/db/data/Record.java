package com.tools.data.db.data;

import com.tools.data.db.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;

public class Record extends HashMap<String,Object> {
    public Record(ResultSet rs,Set<String> columns) throws SQLException {
        while (rs.next()) {
            for(String column : columns){
                put(StringUtils.toCamelCase(column),rs.getString(column));
            }
        }
    }
}
