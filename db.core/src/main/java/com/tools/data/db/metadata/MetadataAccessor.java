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

package com.tools.data.db.metadata;

import com.tools.data.db.metadata.api.IndexColumn;
import com.tools.data.db.metadata.api.Catalog;
import com.tools.data.db.metadata.api.Column;
import com.tools.data.db.metadata.api.ForeignKey;
import com.tools.data.db.metadata.api.ForeignKeyColumn;
import com.tools.data.db.metadata.api.Function;
import com.tools.data.db.metadata.api.Index;
import com.tools.data.db.metadata.api.Metadata;
import com.tools.data.db.metadata.api.MetadataModel;
import com.tools.data.db.metadata.api.Parameter;
import com.tools.data.db.metadata.api.PrimaryKey;
import com.tools.data.db.metadata.api.Procedure;
import com.tools.data.db.metadata.api.Schema;
import com.tools.data.db.metadata.api.Table;
import com.tools.data.db.metadata.api.Value;
import com.tools.data.db.metadata.api.View;
import com.tools.data.db.metadata.spi.CatalogImplementation;
import com.tools.data.db.metadata.spi.ColumnImplementation;
import com.tools.data.db.metadata.spi.ForeignKeyColumnImplementation;
import com.tools.data.db.metadata.spi.ForeignKeyImplementation;
import com.tools.data.db.metadata.spi.FunctionImplementation;
import com.tools.data.db.metadata.spi.IndexColumnImplementation;
import com.tools.data.db.metadata.spi.IndexImplementation;
import com.tools.data.db.metadata.spi.MetadataImplementation;
import com.tools.data.db.metadata.spi.ParameterImplementation;
import com.tools.data.db.metadata.spi.PrimaryKeyImplementation;
import com.tools.data.db.metadata.spi.ProcedureImplementation;
import com.tools.data.db.metadata.spi.SchemaImplementation;
import com.tools.data.db.metadata.spi.TableImplementation;
import com.tools.data.db.metadata.spi.ValueImplementation;
import com.tools.data.db.metadata.spi.ViewImplementation;

/**
 *
 * @author Andrei Badea
 */
public abstract class MetadataAccessor {

    private static volatile MetadataAccessor accessor;

    public static void setDefault(MetadataAccessor accessor) {
        if (MetadataAccessor.accessor != null) {
            throw new IllegalStateException();
        }
        MetadataAccessor.accessor = accessor;
    }

    public static MetadataAccessor getDefault() {
        if (accessor != null) {
            return accessor;
        }
        Class<Metadata> c = Metadata.class;
        try {
            Class.forName(c.getName(), true, c.getClassLoader());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return accessor;
    }

    public abstract ForeignKeyColumn createForeignKeyColumn(ForeignKeyColumnImplementation impl);

    public abstract ForeignKey createForeignKey(ForeignKeyImplementation impl);

    public abstract Index createIndex(IndexImplementation impl);

    public abstract IndexColumn createIndexColumn(IndexColumnImplementation impl);

    public abstract MetadataModel createMetadataModel(MetadataModelImplementation impl);

    public abstract Metadata createMetadata(MetadataImplementation impl);

    public abstract Catalog createCatalog(CatalogImplementation impl);

    public abstract Parameter createParameter(ParameterImplementation impl);

    public abstract PrimaryKey createPrimaryKey(PrimaryKeyImplementation impl);

    public abstract Procedure createProcedure(ProcedureImplementation impl);

    public abstract Function createFunction(FunctionImplementation impl);

    public abstract Schema createSchema(SchemaImplementation impl);

    public abstract Table createTable(TableImplementation impl);

    public abstract Column createColumn(ColumnImplementation impl);

    public abstract Value createValue(ValueImplementation impl);

    public abstract View createView(ViewImplementation impl);

    public abstract CatalogImplementation getCatalogImpl(Catalog catalog);
}
