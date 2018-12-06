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

package com.tools.data.db.lib.ddl;

import java.sql.Connection;
import java.util.Properties;

/**
* Connection information.
* This interface defines information needed for connection to database
* (database and driver url, login name, password and schema name). It can create JDBC
* connection and feels to be a bean (has propertychange support and customizer).
* Instances of this class uses api option to store information about
* open connection.
*
* @author Slavek Psenicka, Radko Najman
*/

public interface DBConnection extends java.io.Serializable
{

    /** Returns database URL */
    public String getDatabase();

    /** Returns user login name */
    public String getUser();

    /** Returns password */
    public String getPassword();

    /** Creates JDBC connection
    * Uses DriverManager to create connection to specified database. Throws 
    * DDLException if none of driver/database/user/password is set or if 
    * driver or database does not exist or is inaccessible.
    */
    public Connection openConnection() throws DDLException;

    /**
     * Get additional (besides "user" and "password") connection properties. Use
     * {@link #getUser()} and {@link #getPassword()} to get user and password.
     *
     * @return Object containing additional connection properties, or null if it
     * is not available.
     */
    public Properties getConnectionProperties();
}
