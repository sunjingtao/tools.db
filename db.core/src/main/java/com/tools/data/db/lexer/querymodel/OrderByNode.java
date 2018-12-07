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
package com.tools.data.db.lexer.querymodel;

import java.util.ArrayList;
import java.util.Collection;

import com.tools.data.db.core.SQLIdentifiers;

/**
 * Represents a SQL ORDER BY clause
 */
public class OrderByNode implements OrderBy {

    // Fields

    // A vector of generalized column objects (JoinTables)

    ArrayList _sortSpecificationList;


    // Constructors

    public OrderByNode() {
    }

    public OrderByNode(ArrayList sortSpecificationList) {
        _sortSpecificationList = sortSpecificationList;
    }


    // Methods

    // Return the SQL string that corresponds to this From clause
    public String genText(SQLIdentifiers.Quoter quoter) {
        String res = "";    // NOI18N
        if (_sortSpecificationList != null && _sortSpecificationList.size() > 0) {

            res = " ORDER BY " + ((SortSpecification)_sortSpecificationList.get(0)).genText(quoter);  // NOI18N

            for (int i=1; i<_sortSpecificationList.size(); i++) {
                res += ", " + "                    " +    // NOI18N
                  ((SortSpecification)_sortSpecificationList.get(i)).genText(quoter);
            }
        }

        return res;
    }



    // Methods

    // Accessors/Mutators

    void renameTableSpec(String oldTableSpec, String corrName) {
        if (_sortSpecificationList != null) {
            for (int i=0; i<_sortSpecificationList.size(); i++)
                ((SortSpecification)_sortSpecificationList.get(i)).renameTableSpec(oldTableSpec, corrName);
        }
    }

    public void removeSortSpecification(String tableSpec) {
        if (_sortSpecificationList != null) {
            for (int i=0; i<_sortSpecificationList.size(); i++) {
                ColumnNode col = (ColumnNode)((SortSpecification)_sortSpecificationList.get(i)).getColumn();
                if (col.getTableSpec().equals(tableSpec))
                {
                    _sortSpecificationList.remove(i);
                    // item from arraylist is removed, reset index value
                    // as remove shifts any subsequent elements to the left
                    // (subtracts one from their indices).
                    i=i-1;
                }
            }
        }
    }

    public void removeSortSpecification(String tableSpec, String columnName) {
        if (_sortSpecificationList != null) {
            for (int i=0; i<_sortSpecificationList.size(); i++) {
                ColumnNode col = (ColumnNode)((SortSpecification)_sortSpecificationList.get(i)).getColumn();
                if (col.matches(tableSpec, columnName))
                {
                    _sortSpecificationList.remove(i);
                    // item from arraylist is removed, reset index value
                    // as remove shifts any subsequent elements to the left
                    // (subtracts one from their indices).
                    i=i-1;
                }
            }
        }
    }

    public void addSortSpecification(String tableSpec, String columnName, String direction, int order) {
        SortSpecification sortSpec = new SortSpecification(new ColumnNode(tableSpec, columnName), direction);
        // Insert the new one in an appropriate place
        if (_sortSpecificationList == null)
            _sortSpecificationList = new ArrayList();
        _sortSpecificationList.add(order-1, sortSpec);
    }

    public int getSortSpecificationCount() {
        return (_sortSpecificationList != null) ? _sortSpecificationList.size() : 0;
    }

    public SortSpecification getSortSpecification(int i) {
        return (_sortSpecificationList != null) ? ((SortSpecification)_sortSpecificationList.get(i)) : null;
    }

    public void  getReferencedColumns (Collection columns) {
        if (_sortSpecificationList != null) {
            for (int i = 0; i < _sortSpecificationList.size(); i++)
                ((SortSpecification)_sortSpecificationList.get(i)).getReferencedColumns(columns);
        }
    }

}