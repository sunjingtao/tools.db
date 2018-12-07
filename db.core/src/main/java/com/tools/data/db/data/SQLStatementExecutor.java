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
 * Software is Sun Microsystems, Inc. Portions Copyright 1997-2007 Sun
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
package com.tools.data.db.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;

public abstract class SQLStatementExecutor{
    protected Logger logger = LoggerFactory.getLogger(SQLStatementExecutor.class);

    protected boolean lastCommitState;
    private final boolean runInTransaction;
    private Connection connection;
    private String sql;

    public SQLStatementExecutor(Connection connection, String sql, boolean runInTransaction) {
        this.runInTransaction = runInTransaction;
        if (connection == null) {
            throw new IllegalArgumentException("parameter connection in SQLStatementExecutor cann't be null !");
        }
        this.connection = connection;
        this.sql = sql;
    }

    public void executeSql(){
        try {
            if(runInTransaction) {
                lastCommitState = setAutocommit(false);
            } else {
                lastCommitState = setAutocommit(true);
            }
            execute();
        } catch (Exception ex){
            logger.error(ex.getMessage());
        }finally {
            logger.info(MessageFormat.format("sql execute finished : {0}",sql));
            setAutocommit(lastCommitState);
        }
    }

    private boolean setAutocommit(boolean newState) {
        try {
            if (connection != null) {
                boolean lastState = connection.getAutoCommit();
                connection.setAutoCommit(newState);
                return lastState;
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return newState;
    }
    protected abstract void execute() throws SQLException;

    private boolean commit() {
        if(! runInTransaction) {
            return true;
        }
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException sqlEx) {
            logger.error(sqlEx.getMessage());
            return false;
        }
        return true;
    }

    private void rollback() {
        if(! runInTransaction) {
            logger.error("failed to rollback ,because it is not need to rollback !");
        }
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }
}
