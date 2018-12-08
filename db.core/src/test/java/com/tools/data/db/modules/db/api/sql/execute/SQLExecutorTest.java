/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2010 Oracle and/or its affiliates. All rights reserved.
 *
 * Oracle and Java are registered trademarks of Oracle and/or its affiliates.
 * Other names may be trademarks of their respective owners.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common
 * Development and Distribution License("CDDL") (collectively, the
 * "License"). You may not use this file except in compliance with the
 * License. You can obtain a copy of the License at
 * http://www.netbeans.org/cddl-gplv2.html
 * or nbbuild/licenses/CDDL-GPL-2-CP. See the License for the
 * specific language governing permissions and limitations under the
 * License.  When distributing the software, include this License Header
 * Notice in each file and include the License file at
 * nbbuild/licenses/CDDL-GPL-2-CP.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the GPL Version 2 section of the License file that
 * accompanied this code. If applicable, add the following below the
 * License Header, with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * If you wish your version of this file to be governed by only the CDDL
 * or only the GPL Version 2, indicate your decision by adding
 * "[Contributor] elects to include this software in this distribution
 * under the [CDDL or GPL Version 2] license." If you do not indicate a
 * single choice of license, a recipient has the option to distribute
 * your version of this file under either the CDDL, the GPL Version 2 or
 * to extend the choice of license to its licensees as provided above.
 * However, if you add GPL Version 2 code and therefore, elected the GPL
 * Version 2 license, then the option applies only if the new code is
 * made subject to such option by the copyright holder.
 *
 * Contributor(s):
 *
 * Portions Copyrighted 2008 Sun Microsystems, Inc.
 */

package com.tools.data.db.modules.db.api.sql.execute;

import com.tools.data.db.api.DatabaseConnection;
import com.tools.data.db.data.SQLStatementExecutor;
import com.tools.data.db.metadata.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author David Van Couvering
 */
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
        SQLStatementExecutor executor = new SQLStatementExecutor(dbconn.openConnection());
        String sql = "SELECT name,age,grade,source_info,type,description FROM qazwsx";
        HashMap map = executor.selectOne(HashMap.class,sql);
        Assert.assertNotNull(map);
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
