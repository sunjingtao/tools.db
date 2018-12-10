package com.tools.data.db.util;

import com.tools.data.db.lexer.ParseException;
import com.tools.data.db.lexer.SQLParser;
import com.tools.data.db.lexer.querymodel.*;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SQLParserUtils {

    public static Set<String> getColumnSet(String sql) throws ParseException {
        SQLParser parser = new SQLParser(new ByteArrayInputStream(sql.getBytes()));
        QueryNode queryNode = parser.SQLQuery();
        SelectNode selectNode = (SelectNode) queryNode.getSelect();
        List<Column> columnItemList = selectNode.getSelectItemList();
        Set<String> columnNameSet = new HashSet<>(columnItemList.size());
        for(Column item : columnItemList){
            columnNameSet.add(
                    item.getDerivedColName() == null ? item.getColumnName() : item.getDerivedColName());
        }
        return columnNameSet;
    }
}
