/*
 * $Id: ExternalTableFactory.java,v 1.9 2007/04/19 18:37:52 nilesh_apte Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The names "Tigris", "Axion", nor the names of its contributors may
 *    not be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 *
 * 4. Products derived from this software may not be called "Axion", nor
 *    may "Tigris" or "Axion" appear in their names without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * =======================================================================
 */
package org.axiondb.engine.tables;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.ExternalTableLoader;
import org.axiondb.Table;
import org.axiondb.TableFactory;

/**
 * Implementation of ExternalTableFactory, to generate instances of concrete
 * implementations of ExternalTable, such as flatfile and remote tables.
 *
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 * @version $Revision: 1.9 $ $Date: 2007/04/19 18:37:52 $
 */
public class ExternalTableFactory implements TableFactory {
    
    public static final String TYPE_DELIMITED = "DELIMITED"; // NOI18N
    public static final String TYPE_FIXEDWIDTH = "FIXEDWIDTH"; // NOI18N
    public static final String TYPE_WEBROWSET = "WEBROWSET"; // NOI18N
    public static final String TYPE_XML = "XML"; // NOI18N
    public static final String TYPE_WEB = "WEB"; // NOI18N
    public static final String TYPE_SPREADSHEET = "SPREADSHEET"; // NOI18N
    public static final String TYPE_RSS = "RSS"; // NOI18N
    public static final String TYPE_TAGGEDEBCDIC = "TAGGEDEBCDIC"; // NOI18N
    public static final String TYPE_REMOTE = "REMOTE"; // NOI18N
    public static final String TYPE_REMOTE_AXION = "REMOTE_AXION"; // NOI18N
    
    public static final HashMap EXTERNAL_LOADERS = new HashMap(4);
    
    static {
        EXTERNAL_LOADERS.put(TYPE_DELIMITED, new DelimitedFlatfileTableLoader());
        EXTERNAL_LOADERS.put(TYPE_FIXEDWIDTH, new FixedWidthFlatfileTableLoader());
        EXTERNAL_LOADERS.put(TYPE_WEBROWSET, new WebRowSetTableLoader());
        EXTERNAL_LOADERS.put(TYPE_XML, new XMLTableLoader());
        EXTERNAL_LOADERS.put(TYPE_WEB, new WebTableLoader());
        EXTERNAL_LOADERS.put(TYPE_RSS, new RSSTableLoader());
        EXTERNAL_LOADERS.put(TYPE_SPREADSHEET, new SpreadsheetTableLoader());
        EXTERNAL_LOADERS.put(TYPE_TAGGEDEBCDIC, new TaggedEBCDICTableLoader());
        EXTERNAL_LOADERS.put(TYPE_REMOTE, new ExternalDatabaseTableLoader());
        EXTERNAL_LOADERS.put(TYPE_REMOTE_AXION, new ExternalAxionDBTableLoader());
    }
    
    public Table createTable(Database database, String name) throws AxionException {
        throw new AxionException("Unsupported method - use overloaded method to "
                + "supply external properties and column metadata.");
    }
    
    public ExternalTable createTable(final Database database, final String name, Properties props,
            List columns) throws AxionException {
        assertValidProperty(props);
        
        String type = props.getProperty(ExternalTable.PROP_LOADTYPE);
        String vendor = props.getProperty(ExternalTable.PROP_VENDOR);
        if(vendor != null && vendor.trim().length() != 0) {
            type = type + "_" +  vendor;
        }
        
        ExternalTableLoader loader = (ExternalTableLoader) EXTERNAL_LOADERS.get(type.toUpperCase());
        
        if (loader != null) {
            ExternalTable table = loader.createExternalTable(database, name);
            
            if (columns == null || columns.isEmpty()) {
                throw new AxionException(
                        "Must supply List of Column instances to populate column metadata");
            }
            
            // Must add column information before invoking setTableProperties(), since we
            // build our remote create and/or select statements from Axion column metadata
            Iterator iter = columns.iterator();
            while (iter.hasNext()) {
                table.addColumn((Column) iter.next());
            }
            
            table.loadExternalTable(props);
            return table;
        }
        throw new AxionException("Unknown external table type: " + type);
    }
    
    public void assertValidProperty(Properties props) throws AxionException {
        if (props == null || props.isEmpty() || props.get(ExternalTable.PROP_LOADTYPE) == null) {
            throw new AxionException("LOADTYPE property required in organization clause.");
        }
    }
}
