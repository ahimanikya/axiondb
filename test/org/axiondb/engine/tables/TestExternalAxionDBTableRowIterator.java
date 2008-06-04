/*
 * $Id: TestExternalAxionDBTableRowIterator.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.ExternalConnectionProvider;
import org.axiondb.ExternalTable;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.Databases;
import org.axiondb.engine.rowiterators.AbstractRowIteratorTest;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 *
 * @author Jonathan Giron
 * @version $Revision: 1.1 $
 */
public class TestExternalAxionDBTableRowIterator extends AbstractRowIteratorTest {

    private static final String DATABASE_NAME = "REMOTEDB";
    private static final String DBLINK_NAME = "AXIONDB";
    
    /**
     * @param testName
     */
    public TestExternalAxionDBTableRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestExternalAxionDBTableRowIterator.class);
        return suite;
    }    
    
    protected void setUp() throws Exception {
        setUpRemote();
        createExternalTable(getRemoteTableName());
    }
    
    
    protected void setUpRemote() throws Exception {
        _db = Databases.getOrCreateDatabase(DATABASE_NAME, null);
        _table = new MemoryTable(getRemoteTableName());
        addColumns(_table);
        _db.addTable(_table);
        addRow();
    }

    protected void tearDown() throws Exception {
        if (_externalTable != null) {
            _externalTable.shutdown();
            _externalTable = null;
        }

        if (_db.hasDatabaseLink(DBLINK_NAME)) {
            _db.dropDatabaseLink(DBLINK_NAME);
        }
        
        _db.dropTable(getRemoteTableName());
        _table.shutdown();        
        _table = null;
        
        _db.shutdown();
        try {
            Databases.forgetDatabase(DATABASE_NAME);
        } catch (Exception e) {
            // ignored
        }
        
    }

    protected RowIterator makeRowIterator() {
        try {
            return _externalTable.getRowIterator(false);
        } catch (AxionException e) {
            throw new UnsupportedOperationException("Could not get RowIterator from external table.");
        }
    }

    protected String getJdbcDriverName() {
        return "org.axiondb.jdbc.AxionDriver";
    }
    
    protected String getJdbcURL() {
        return "jdbc:axiondb:" + DATABASE_NAME;
    }
    
    protected String getUserName() {
        return "ignored";
    }
    
    protected String getPassword() {
        return "ignored";
    }
    
    protected String getSchemaName() {
        return "";
    }

    protected String getRemoteTableName() {
        return "REMOTETABLE";
    }
    
    protected int getSize() {
        return 3;
    }
    
    protected List makeRowList() {
        List list = new ArrayList();
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "a");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "bb");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "ccc");
            list.add(row);
        }
        return list;
    }

    private List buildColumns() {
        List list = new ArrayList(2);
        
        Column id = new Column("id", new IntegerType()); 
        id.setSqlType("integer");
        list.add(id);
        
        Column name = new Column("name", new CharacterVaryingType(10));
        name.setSqlType("varchar");
        list.add(name);
        return list;
    }
    
    private void addColumns(Table t) throws Exception {
        Column id = new Column("id", new IntegerType());
        id.setSqlType("integer");
        t.addColumn(id);
        
        Column name = new Column("name", new CharacterVaryingType(10));
        name.setSqlType("varchar");
        t.addColumn(name);
    }
    
    private Properties getDBLinkProperties() {
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, getJdbcDriverName());
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, getJdbcURL()); 
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, getUserName());
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, getPassword());

        return props;
    }

    private Properties getExternalTableProperties(String name) {
        Properties props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, DBLINK_NAME);
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
        props.setProperty(ExternalTable.PROP_VENDOR, "AXION");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, name);
        return props;
    }

    private void createExternalTable(String name) throws Exception {
        _db.createDatabaseLink(new DatabaseLink(DBLINK_NAME, getDBLinkProperties()));
        
        ExternalTableFactory factory = new ExternalTableFactory();
        _externalTable = (ExternalAxionDBTable) factory.createTable(_db, name + "_EXT", 
            getExternalTableProperties(name), buildColumns());
    }    
    
    private void addRow() throws Exception {
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "a");
            _table.addRow(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "bb");
            _table.addRow(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "ccc");
            _table.addRow(row);
        }
    }
    
    private Database _db;
    private ExternalAxionDBTable _externalTable = null;
    private Table _table = null;
}
