package com.tools.data.db.modules.db.api.sql.execute;

import com.tools.data.db.api.DatabaseConnection;
import com.tools.data.db.data.SQLStatementExecutor;
import com.tools.data.db.metadata.Catalog;
import com.tools.data.db.metadata.Column;
import com.tools.data.db.metadata.Schema;
import com.tools.data.db.metadata.Table;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SQLExecutorTest {
    
    private static DatabaseConnection dbconn;

    @BeforeClass
    public static void before() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host","localhost");
        properties.setProperty("port","3306");
        properties.setProperty("db","test");
        properties.setProperty("password","123456a");
        properties.setProperty("user","root");
        properties.setProperty("dbtype","MySQL");
        properties.setProperty("version","");
        properties.setProperty("additional","characterEncoding=utf8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
        dbconn = new DatabaseConnection(properties);
    }

    @Test
    public void testOpenConnection() {
        Connection conn = dbconn.openConnection();
        Assert.assertNotNull(conn);
    }

    @Test
    public void testMetadata(){
        Catalog catalog = new Catalog(dbconn.openConnection());
        Assert.assertNotNull(catalog);
        Schema schema = catalog.getSchema();
        Assert.assertNotNull(schema);
        Collection<Table> tableList = schema.getTables();
        Assert.assertNotNull(tableList);
        Collection<Column> columnCollection = tableList.iterator().next().getColumns();
        Assert.assertNotNull(columnCollection);
    }

    @Test
    public void createRentalTable() throws Exception {
        Assert.assertNotNull(dbconn);

        String sql = "USE test; CREATE TABLE rental ( " +
          "rental_id INT NOT NULL AUTO_INCREMENT, " +
          "rental_date DATETIME NOT NULL, " +
          "inventory_id MEDIUMINT UNSIGNED NOT NULL, " +
          "customer_id SMALLINT UNSIGNED NOT NULL, " +
          "return_date DATETIME DEFAULT NULL, " +
          "staff_id TINYINT UNSIGNED NOT NULL, " +
          "last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
          "PRIMARY KEY (rental_id), " +
          "UNIQUE KEY  (rental_date,inventory_id,customer_id), " +
          "KEY idx_fk_inventory_id (inventory_id), " +
          "KEY idx_fk_customer_id (customer_id), " + 
          "KEY idx_fk_staff_id (staff_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

//        checkExecution(SQLExecutorHe.execute(dbconn, sql));
    }

    @Test
    public void testQueryData(){
        SQLStatementExecutor executor = new SQLStatementExecutor(dbconn);
        String sql = "SELECT name,age,grade,source_info,type,description FROM qazwsx where name ='sun'";
        HashMap map = executor.selectOne(HashMap.class,sql);
        Assert.assertNotNull(map);
        sql = "SELECT name,age,grade,source_info,type,description FROM qazwsx";
        List<HashMap> resultList = executor.selectAll(HashMap.class,sql);
        Assert.assertNotNull(resultList);
        resultList = executor.selectPage(HashMap.class,sql,1,1);
        Assert.assertNotNull(resultList);
        sql = "SELECT qazwsx.* , department.* from qazwsx, department where qazwsx.name = department.name";
        resultList = executor.selectAll(HashMap.class,sql);
        Assert.assertNotNull(resultList);
    }


//    public void testExecute() throws Exception {
//        SQLExecutionInfo info = SQLExecutor.execute(dbconn, "SELECT * FROM " + getTestTableName() + ";");
//        checkExecution(info);
//        Assert.assertTrue(info.getStatementInfos().size() == 1);
//
//        info = SQLExecutor.execute(dbconn, "SELECT * FROM " + getTestTableName() + "; SELECT " + getTestTableIdName() + " FROM " + getTestTableName() + ";");
//        checkExecution(info);
//        Assert.assertTrue(info.getStatementInfos().size() == 2);
//    }

//    public void testBadExecute() throws Exception {
//        SQLExecutionInfo info = SQLExecutor.execute(dbconn, "SELECT * FROM BADTABLE;");
//
//        Assert.assertTrue(info.hasExceptions());
//    }
//
//    private void checkExecution(SQLExecutionInfo info) throws Exception {
//        Assert.assertNotNull(info);
//
//        Throwable throwable = null;
//        if (info.hasExceptions()) {
//            for (StatementExecutionInfo stmtinfo : info.getStatementInfos()) {
//                if (stmtinfo.hasExceptions()) {
//                    System.err.println("The following SQL had exceptions:");
//                } else {
//                    System.err.println("The following SQL executed cleanly:");
//                }
//                System.err.println(stmtinfo.getSQL());
//
//                for  (Throwable t : stmtinfo.getExceptions()) {
//                    t.printStackTrace();
//
//                    throwable = t;
//                }
//            }
//
//            Exception e = new Exception("Executing SQL generated exceptions - see output for details");
//            e.initCause(throwable);
//            throw e;
//        }
//    }
    
    public void testMySQLStoredFunction() throws Exception {

//       SQLExecutor.execute(dbconn, "DROP FUNCTION inventory_in_stock");
//       SQLExecutor.execute(dbconn, "DROP FUNCTION inventory_held_by_customer");
//
       String sql =
            "DELIMITER $$\n" +
            "CREATE FUNCTION inventory_held_by_customer(p_inventory_id INT) RETURNS INT " +
            "READS SQL DATA " +
            "BEGIN " +
              "DECLARE v_customer_id INT; # Testing comment in this context\n" +
              "DECLARE EXIT HANDLER FOR NOT FOUND RETURN NULL; # Another comment\n" +
              "SELECT customer_id INTO v_customer_id " +
              "FROM rental " +
              "WHERE return_date IS NULL " +
              "AND inventory_id = p_inventory_id; " +
              "RETURN v_customer_id; " +
            "END $$ " +
            "DELIMITER ;\n" +

            "DELIMITER $$\n" +
            "CREATE FUNCTION inventory_in_stock(p_inventory_id INT) RETURNS BOOLEAN " +
            "READS SQL DATA " +
            "BEGIN " +
            "    DECLARE v_rentals INT; #Testing comment in this context\n" +
            "    DECLARE v_out     INT; #Another comment\n" +
            
            "    #AN ITEM IS IN-STOCK IF THERE ARE EITHER NO ROWS IN THE rental TABLE\n" +
            "    #FOR THE ITEM OR ALL ROWS HAVE return_date POPULATED\n" +
            "    SELECT COUNT(*) INTO v_rentals " +
            "    FROM rental " +
            "    WHERE inventory_id = p_inventory_id; " +
            "    IF v_rentals = 0 THEN " +
            "      RETURN TRUE; " +
            "    END IF; " +
            "    SELECT COUNT(rental_id) INTO v_out " +
            "    FROM inventory LEFT JOIN rental USING(inventory_id) " +
            "    WHERE inventory.inventory_id = p_inventory_id " +
            "    AND rental.return_date IS NULL; " +
            "    IF v_out > 0 THEN " +
            "      RETURN FALSE; " +
            "    ELSE " +
            "      RETURN TRUE; " +
            "    END IF; " +
            "END";

//        checkExecution(SQLExecutor.execute(dbconn, sql));
    }


}
