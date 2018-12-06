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
 * Software is Sun Microsystems, Inc. Portions Copyright 1997-2010 Sun
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

package com.tools.data.db.api;

import com.tools.data.db.core.ConnectionUrl;

import java.text.MessageFormat;
import java.util.*;

public class ConnectionUrlManager {

    private static final  Map<String,Map<String,ConnectionUrl>> connectionUrlMap = new HashMap<>();
    private static final String TOKEN_HOST = "<host>";
    private static final String TOKEN_PORT = "<port>";
    private static final String TOKEN_DB = "<db>";
    private static final String TOKEN_ADDITIONAL = "<additional>";
    private static final String TOKEN_INSTANCE = "<instance>";
    private static final String OPTIONAL_START = "[";
    private static final String OPTIONAL_END = "]";
    private static final String REQUIRED = "REQUIRED";
    private static final String SUPPORTED = "SUPPORTED";
    private static final String COMPONENTS = "COMPONENTS";

    private static Map<String,Map<String,Object>> urlTemplateMap = new HashMap<>(10);

    static {
        Map<String,ConnectionUrl> urlMap = new HashMap<>(3);
        ConnectionUrl connectionUrl =  new ConnectionUrl("IBM DB2","net", "COM.ibm.db2.jdbc.net.DB2Driver",
                "jdbc:db2://<host>:<port>/<db>");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrl = new ConnectionUrl("IBM DB2", "local", "COM.ibm.db2.jdbc.app.DB2Driver",
                "jdbc:db2:<db>");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrl = new ConnectionUrl("IBM DB2", "","com.ibm.db2.jcc.DB2Driver",
                "jdbc:db2://<host>:<port>/<db>[:<additional>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrlMap.put(connectionUrl.getType(),urlMap);

        urlMap = new HashMap<>(1);
        connectionUrl = new ConnectionUrl("JDBC-ODBC","Bridge", "sun.jdbc.odbc.JdbcOdbcDriver",
                "jdbc:odbc:<db>");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrlMap.put(connectionUrl.getType(),urlMap);

        urlMap = new HashMap<>(3);
        connectionUrl = new ConnectionUrl("Microsoft SQL Server","Weblogic driver", "weblogic.jdbc.mssqlserver4.Driver",
                "jdbc:weblogic:mssqlserver4:<db>@<host>[:<port>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrl = new ConnectionUrl("Microsoft SQL Server","2000", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
                "jdbc:microsoft:sqlserver://<host>[:<port>][;DatabaseName=<db>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrl = new ConnectionUrl("Microsoft SQL Server", "2005","com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "jdbc:sqlserver://[<host>[\\<instance>][:<port>]][;databaseName=<db>][;<additional>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        urlMap.put("",connectionUrl);
        connectionUrlMap.put(connectionUrl.getType(),urlMap);

        urlMap = new HashMap<>(1);
        connectionUrl = new ConnectionUrl("MySQL","", "com.mysql.jdbc.Driver",
                "jdbc:mysql://<host>:<port>/<db>[?<additional>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrlMap.put(connectionUrl.getType(),urlMap);

        urlMap = new HashMap<>(2);
        connectionUrl = new ConnectionUrl("Oracle Thin", "Service ID (SID)", "oracle.jdbc.OracleDriver",
                "jdbc:oracle:thin:@<host>:<port>:<db>[?<additional>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrl = new ConnectionUrl("Oracle Thin",
                "Service Name", "oracle.jdbc.OracleDriver",
                "jdbc:oracle:thin:@//<host>[:<port>][/<db>][?<additional>]");
        urlMap.put(connectionUrl.getVersion(),connectionUrl);
        connectionUrlMap.put(connectionUrl.getType(),urlMap);
    }

    public static ConnectionUrl getConnectionUrl(String dbtype, String version) {
        checkDatabaseType(dbtype);
        return connectionUrlMap.get(dbtype).get(version);
    }

    public static String getValidUrl(ConnectionUrl connectionUrl,Properties properties){
        if(connectionUrl == null){
            throw new IllegalArgumentException("parameter connectionUrl cann't be null !");
        }
        String template = connectionUrl.getUrlTemplate();
        Map<String,Object> templateMap = urlTemplateMap.get(template);
        if(templateMap == null){
            templateMap = extractUrlComponents(template);
            urlTemplateMap.put(template,templateMap);
        }
        //check if required token has valid value
        HashSet<String> requiredTokens = (HashSet<String>) templateMap.get(REQUIRED);
        for(String token : requiredTokens){
            token = token.substring(1,token.length()-1);
            if(properties.getProperty(token) == null){
                throw new IllegalArgumentException(
                        MessageFormat.format("connect to database :{0} , version: {1} , property : {2} is required !",
                                connectionUrl.getType(),connectionUrl.getVersion(),token));
            }
        }
        //construct connection url with valid value
        ArrayList<String> components = (ArrayList<String>) templateMap.get(COMPONENTS);
        StringBuilder builder = new StringBuilder();
        String comp = null;
        String temp = null;
        String additional = null;
        boolean skipToOptionalEnd = false;
        boolean hasOptionValue = false;
        for(int i = 0; i < components.size(); i++){
            comp = components.get(i);
            // in optional token context
            if(skipToOptionalEnd){
                if(isTokenStart(comp.charAt(0)) && hasOptionValue){
                    builder.append(properties.getProperty(comp.substring(1,comp.length()-1)));
                    continue;
                }
                if(isOptionalEnd(comp.charAt(0))){
                    skipToOptionalEnd = false;
                    hasOptionValue = false;
                    continue;
                }
                if(hasOptionValue){
                    builder.append(comp);
                    continue;
                }
                continue;
            }

            //in main context,handle required token
            if(isTokenStart(comp.charAt(0))){
                builder.append(properties.getProperty(comp.substring(1,comp.length()-1)));
                continue;
            }

            //in main context,handle optional token
            if(isOptionalStart(comp.charAt(0))){
                //checkIfOptionalValueExist
                for(int j = i+1; j < components.size(); j++){
                    temp = components.get(j);
                    if(isTokenStart(temp.charAt(0))){
                        additional = properties.getProperty(temp.substring(1,temp.length()-1));
                        hasOptionValue = (additional != null);
                        break;
                    }
                    continue;
                }
                skipToOptionalEnd = true;
                continue;
            }
            builder.append(comp);
        }
        return builder.toString();
    }

    private static Map<String,Object> extractUrlComponents(String urlTemplate) {
        HashSet<String> requiredTokens = new HashSet<>();
        HashSet<String> supportedTokens = new HashSet<>();
        List urlComponents = new ArrayList<String>();
        int length = urlTemplate.length();
        boolean isToken = false;
        int optionalLevel = 0;
        StringBuffer buf = new StringBuffer();

        for (int i=0 ; i < length ; i++) {
            char ch = urlTemplate.charAt(i);
            if (isTokenStart(ch)) {
                // Can't have two tokens in a row...
                assert(! isToken);

                // Add the text gathered so far as a component of the URL
                buf = addComponent(urlComponents,buf);

                // Start with the next component, which is a token
                buf.append(ch);
                isToken = true;
            } else if (isTokenEnd(ch)) {
                assert(isToken);

                // Add the end-token character
                buf.append(ch);

                String token = buf.toString();

                if (optionalLevel == 0) {
                    requiredTokens.add(token);
                }
                supportedTokens.add(token);

                // Add the token as a component of the URL
                buf = addComponent(urlComponents,buf);

                isToken = false;
            } else if (isOptionalStart(ch)) {
                optionalLevel++;

                // Add the text gathered so far as a component of the URL
                buf = addComponent(urlComponents,buf);

                // Add a new component indicating we're starting an
                // optional section
                urlComponents.add(OPTIONAL_START);
            } else if (isOptionalEnd(ch)) {
                optionalLevel--;

                // Add the text gathered so far as a new component
                buf = addComponent(urlComponents,buf);

                // Add a component indicating we're ending the optional section
                urlComponents.add(OPTIONAL_END);
            } else {
                buf.append(ch);
            }
        }
        Map<String,Object> resultMap = new HashMap<>(3);
        resultMap.put(REQUIRED,requiredTokens);
        resultMap.put(SUPPORTED,supportedTokens);
        resultMap.put(COMPONENTS,urlComponents);
        return resultMap;
    }

    private static StringBuffer addComponent(List urlComponents,StringBuffer text) {
        if (text.length() > 0) {
            urlComponents.add(text.toString());
            return new StringBuffer();
        } else {
            return text;
        }
    }

    private static boolean isOptionalStart(char ch) {
        return ch == '[';
    }

    private static boolean isOptionalEnd(String component) {
        return component.equals("]"); // NOI18N
    }

    private static boolean isOptionalEnd(char ch) {
        return ch == ']';
    }

    private static boolean isTokenStart(char ch) {
        return ch == '<';
    }

    private static boolean isTokenEnd(char ch) {
        return ch == '>';
    }

    public static Set<String> getSupportedDatabase(){
        return connectionUrlMap.keySet();
    }

    public static Set<String> getSupportedVersion(String dbtype){
        checkDatabaseType(dbtype);
        return connectionUrlMap.get(dbtype).keySet();
    }

    private static void checkDatabaseType(String dbtype){
        if(dbtype == null){
            throw new IllegalArgumentException("parameter dbtype cann't be null!");
        }
        if(connectionUrlMap.get(dbtype) == null){
            throw new RuntimeException(MessageFormat.format("unsupported database type : ",dbtype));
        }
    }

    public static void main(String[] args){
        ConnectionUrl connectionUrl = connectionUrlMap.get("MySQL").get("");
        extractUrlComponents(connectionUrl.getUrlTemplate());
        System.out.println("OK !");
    }
}
