/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 1997-2010 Oracle and/or its affiliates. All rights reserved.
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
 * Contributor(s):
 *
 * The Original Software is NetBeans. The Initial Developer of the Original
 * Software is Sun Microsystems, Inc. Portions Copyright 1997-2006 Sun
 * Microsystems, Inc. All Rights Reserved.
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
 */

package com.tools.data.db.modules.db.explorer;

import com.tools.data.db.api.db.explorer.JDBCDriver;

import java.awt.datatransfer.DataFlavor;

/**
 * This interface is a means to communicate with the db/dbapi module, which
 * contains the actual data flavor and classes for transfer. The db/dbapi module
 * puts an implementation of this interface in the default lookup.
 *
 * @author Andrei Badea
 */
public interface DbMetaDataTransferProvider {

    /**
     * Returns the data flavor representing database connections.
     */
    DataFlavor getConnectionDataFlavor();

    /**
     * Returns the data flavor representing database tables.
     */
    DataFlavor getTableDataFlavor();
    
    /**
     * Returns the data flavor representing database views.
     */
    DataFlavor getViewDataFlavor();

    /**
     * Returns the data flavor representing columns of database tables.
     */
    DataFlavor getColumnDataFlavor();

    /**
     * Returns an object which encapsulates a database connection.
     */
    Object createConnectionData(DatabaseConnection dbconn, JDBCDriver jdbcDriver);
    
    /**
     * Returns an object which encapsulates a database table.
     */
    Object createTableData(DatabaseConnection dbconn, JDBCDriver jdbcDriver, String tableName);
    
    /**
     * Returns an object which encapsulates a database view.
     */
    Object createViewData(DatabaseConnection dbconn, JDBCDriver jdbcDriver, String viewName);
    
    /**
     * Returns an object which encapsulates a column of a database table.
     */
    Object createColumnData(DatabaseConnection dbconn, JDBCDriver jdbcDriver, String tableName, String columnName);
}