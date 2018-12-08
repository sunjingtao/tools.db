package com.tools.data.db.modules.db.api.sql.execute;

import com.tools.data.db.lexer.SQLParser;
import com.tools.data.db.lexer.querymodel.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

public class SQLParserTest {

    @Test
    public void testSQLParser() throws Exception{
        SQLParser parser = new SQLParser(new ByteArrayInputStream("SELECT A as a2,B,C FROM TTT WHERE A= 'test'".getBytes()));
        QueryNode queryNode = parser.SQLQuery();

    }
}
