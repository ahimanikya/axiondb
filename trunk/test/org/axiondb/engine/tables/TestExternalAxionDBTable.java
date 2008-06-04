/*
 * $Id: TestExternalAxionDBTable.java,v 1.4 2008/02/21 13:00:27 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.ExternalConnectionProvider;
import org.axiondb.ExternalTable;
import org.axiondb.ExternalTableLoader;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.engine.Databases;
import org.axiondb.engine.commands.RemountCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.FileUtil;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.4 $ $Date: 2008/02/21 13:00:27 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class TestExternalAxionDBTable extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestExternalAxionDBTable(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestExternalAxionDBTable.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private Database _db;
    private Table _table = null;
    private ExternalAxionDBTable _externalTable = null;
    private File _dbDir = new File(new File("."), "testdb2");

    public void setUp() throws Exception {
        _dbDir.mkdirs();
        _db = Databases.getOrCreateDatabase("testdb2", _dbDir);
        _table = new DiskTable("FOO24", _db);
        addColumns(_table);
        _db.addTable(_table);
    }

    public void tearDown() throws Exception {
        _table.shutdown();
        if(_externalTable == null)
            _table.drop();
        _table = null;

        if (_externalTable != null) {
            _externalTable.drop();
            _externalTable = null;
        }

        if (_db.hasDatabaseLink("AXIONDB")) {
            _db.dropDatabaseLink("AXIONDB");
        }

        _db.shutdown();
        FileUtil.delete(_dbDir);
    }

    private void createExternalTable(String name) throws Exception {
        String rtable = "FOO24";
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("axiondb", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, rtable);
        props.setProperty(ExternalTable.PROP_VENDOR, "AXION");
        List columns = new ArrayList(2);
        Column id = new Column("ID", new IntegerType());
        id.setSqlType("integer");
        columns.add(id);
        
        Column colname = new Column("NAME", new CharacterVaryingType(10));
        colname.setSqlType("varchar");
        columns.add(colname);

        ExternalTableFactory factory = new ExternalTableFactory();
        _externalTable = (ExternalAxionDBTable) factory.createTable(_db, name, props, columns);
        addRow();
    }

    private void addColumns(Table t) throws Exception {
        Column id = new Column("ID", new IntegerType());
        id.setSqlType("integer");
        t.addColumn(id);
        
        Column name = new Column("NAME", new CharacterVaryingType(20));
        name.setSqlType("varchar");
        t.addColumn(name);
    }

    private void addRow() throws Exception {
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            _externalTable.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            _externalTable.addRow(row);
        }
    }


    //------------------------------------------------------------------- Tests

    public void testGetName() throws Exception {
        createExternalTable("FOO");
        assertEquals("FOO", _externalTable.getName());
    }

    public void testToString() throws Exception {
        createExternalTable("FOO");
        assertNotNull(_externalTable.toString());
        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertNotNull(iter.toString());
    }

    public void testTableProperties() throws Exception {
        createExternalTable("FOO");
        assertNotNull(_externalTable.getTableProperties());
        assertEquals("REMOTE", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_LOADTYPE));
        assertEquals("AXIONDB", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_DB));
        assertEquals("AXION", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_VENDOR));

    }

    public void testAddThenDropConstraint() throws Exception {
        
    }

    public void testAddThenDropColumn() throws Exception {
        
    }

    public void testGetRowIterator() throws Exception {
        createExternalTable("FOO");

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);

        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());

        assertFalse(iter.hasNext());

        iter.reset();
        assertFalse(iter.hasPrevious());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        assertFalse(iter.hasNext());

        assertTrue(iter.hasPrevious());
        assertNotNull(iter.previous());
        assertTrue(iter.hasPrevious());
        assertNotNull(iter.previous());
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        _externalTable.freeRowId(0); // does nothing
        assertEquals(2, _externalTable.getNextRowId());
    }

    public void testGetMatchingRowsForNull() throws Exception {
        createExternalTable("FOO");
        RowIterator iter = _externalTable.getMatchingRows(null, null, true);
        assertNotNull(iter);
    }

    public void testDrop() throws Exception {
        createExternalTable("FOO");
        _externalTable.drop();

        _externalTable = null;

    }

    public void testRollback() throws Exception {
        createExternalTable("FOO");
        TransactableTable tranTable = _externalTable.makeTransactableTable();
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            tranTable.addRow(row);
        }
        tranTable.rollback();
        assertEquals(2, _externalTable.getRowCount());

        _externalTable.remount();
        tranTable = _externalTable.makeTransactableTable();
        assertEquals(2, _externalTable.getRowCount());
        
        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertFalse(iter.hasNext());
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            tranTable.addRow(row);
        }
        tranTable.commit();
        tranTable.apply();
        assertEquals(3, _externalTable.getRowCount()); 

        iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());

        assertFalse(iter.hasNext()); 

        _externalTable.shutdown(); // try again
        _table.shutdown();
        _externalTable = null;
    }

    public void testRemount() throws Exception {
        testAddRow();
        _db.addTable(_externalTable);
        ((ExternalTable)_externalTable).remount();

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
        
        _table.truncate();
        _table.shutdown();
        
        RemountCommand cmd = new RemountCommand();
        cmd.setTable(new TableIdentifier(_externalTable.getName()));
        cmd.execute(_db);
        iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(!iter.hasNext());

    }
    
    public void testTruncate() throws Exception {
        createExternalTable("FOO");
        _externalTable.truncate();
        addRow();

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertFalse(iter.hasNext());
        _externalTable.truncate();

        iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());
        assertFalse(iter.hasNext());

        addRow();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertFalse(iter.hasNext());
    }

   public void testHasColumn() throws Exception {
        createExternalTable("FOO");
        ColumnIdentifier id = new ColumnIdentifier("ID");

        assertTrue("Should have column", _externalTable.hasColumn(id));
        id.setTableIdentifier(new TableIdentifier("FOO"));
        assertTrue("Should have column", _externalTable.hasColumn(id));

        id.setTableIdentifier(new TableIdentifier("bogus"));
        assertTrue("Should not have column", !_externalTable.hasColumn(id));
    }

    public void testInvalidPropertyKey() throws Exception {
        Properties badProps = new Properties();
        badProps.put(ExternalTable.PROP_LOADTYPE, "remote");
        badProps.put("UNKNOWN_PROPERTY", Boolean.TRUE);
        ExternalTableFactory factory = new ExternalTableFactory();
        try {
            factory.createTable(_db, "BadTable", badProps, new ArrayList());
            fail("Expected AxionException due to unrecognized property name 'UNKNOWN_PROPERTY'");
        } catch (AxionException expected) {
            // Expected AxionException due to unrecognized property name.
        }
    }

    public void testMissingRequiredProperty() throws Exception {
        Properties badProps = new Properties();
        ExternalTableFactory factory = new ExternalTableFactory();
        try {
            factory.createTable(_db, "BadTable", badProps, new ArrayList());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
    }

    public void testMissingDriverProperty() throws Exception {
        // Null driver test
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("axiondb", props));

            fail("Expected AxionException due to null driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.

        }
    }

    public void testBadLoaderType() throws Exception {
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("axiondb", props));

        ExternalTableFactory factory = new ExternalTableFactory();
        props = new Properties();

        try {
            factory.assertValidProperty(props);
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }

        try {
            factory.assertValidProperty(null);
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }

        props.setProperty(ExternalTable.PROP_DB, "axiondb");

        try {
            factory.assertValidProperty(props);
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }

        props.setProperty(ExternalTable.PROP_LOADTYPE, "bogus");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        List columns = new ArrayList(2);
        columns.add(new Column("ID", new IntegerType()));
        columns.add(new Column("NAME", new CharacterVaryingType(10)));

        try {
            _externalTable = (ExternalAxionDBTable) factory.createTable(_db, "bogus");
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }

        try {
            _externalTable = (ExternalAxionDBTable) factory.createTable(_db, "bogus", props,
                columns);
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }
    }

    public void testBadDriverClass() throws Exception {
        // bad driver class
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.AxionDriver");
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("axiondb", props));

            props = new Properties();
            props.setProperty(ExternalTable.PROP_DB, "axiondb");
            props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
            props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
            List columns = new ArrayList(2);
            columns.add(new Column("ID", new IntegerType()));
            columns.add(new Column("NAME", new CharacterVaryingType(10)));

            ExternalTableFactory factory = new ExternalTableFactory();
            _externalTable = (ExternalAxionDBTable) factory.createTable(_db, "FOO24", props,
                columns);
            fail("Expected AxionException due to bad driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.
        }
    }

    public void testBadUrlProperty() throws Exception {
        // bad url
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("axiondb", props));

            props = new Properties();
            props.setProperty(ExternalTable.PROP_DB, "axiondb");
            props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
            props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
            List columns = new ArrayList(2);
            columns.add(new Column("ID", new IntegerType()));
            columns.add(new Column("NAME", new CharacterVaryingType(10)));

            ExternalTableFactory factory = new ExternalTableFactory();
            _externalTable = (ExternalAxionDBTable) factory.createTable(_db, "FOO24", props,
                columns);
            fail("Expected AxionException due to bad driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.
        }
    }

    public void testAddRow() throws Exception {
        createExternalTable("FOO");
    }

    public void testNoNewColumnsAfterRowsAdded() throws Exception {
    }

    public void testGetColumnByIndex() throws Exception {
        createExternalTable("FOO");
        assertEquals("ID", _externalTable.getColumn(0).getName());
        assertEquals("NAME", _externalTable.getColumn(1).getName());
    }

    public void testGetColumnByIndexBadIndex() throws Exception {
        createExternalTable("FOO");
        try {
            _externalTable.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        try {
            _externalTable.getColumn(3);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
    }

    public void testGetColumnByName() throws Exception {
        createExternalTable("FOO");
        assertTrue(_externalTable.getColumn("ID").getDataType() instanceof IntegerType);
        assertTrue(_externalTable.getColumn("NAME").getDataType() instanceof CharacterVaryingType);
    }

    public void testGetColumnByNameBadName() throws Exception {
        createExternalTable("FOO");
        assertNull(_externalTable.getColumn("FOO"));
        assertNull(_externalTable.getColumn("ID1"));
    }

    public void testDataTypes() throws Exception {
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("axiondb", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
        props.setProperty(ExternalTable.PROP_VENDOR, "AXION");

        ExternalTableLoader loader = new ExternalAxionDBTableLoader();
        _externalTable = (ExternalAxionDBTable) loader.createTable(_db, "FOO");

        _externalTable.addColumn(new Column("ID", new IntegerType()));
        _externalTable.addColumn(new Column("NAME", new CharacterVaryingType(30)));

        _externalTable.loadExternalTable(props);

        Object[][] values = new Object[][] {
                new Object[] { new Integer(17), new Integer(0), new Integer(5575), null},
                new Object[] { "", "A String", "Another String", null},};

        Random random = new Random();
        int numRows = 7;

        for (int i = 0; i < numRows; i++) {
            Row row = new SimpleRow(_externalTable.getColumnCount());
            for (int j = 0; j < _externalTable.getColumnCount(); j++) {
                row.set(j, values[j][random.nextInt(values[j].length)]);
            }
            _externalTable.addRow(row);
        }

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        for (int i = 0; i < numRows; i++) {
            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
        }
        assertTrue(!iter.hasNext());
    }

    public void testRemoteUpdate() throws Exception {
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("axiondb", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
        props.setProperty(ExternalTable.PROP_VENDOR, "AXION");

        ExternalTableLoader loader = new ExternalAxionDBTableLoader();
        _externalTable = (ExternalAxionDBTable) loader.createTable(_db, "FOO");

        _externalTable.addColumn(new Column("ID", new IntegerType()));
        _externalTable.addColumn(new Column("NAME", new CharacterVaryingType(20)));

        _externalTable.loadExternalTable(props);

        final String stringTemplate = "My ID is ";
        Object[][] values = new Object[][] {
                new Object[] { new Integer(1), new Integer(2), new Integer(46), new Integer(100) },
                new Object[] { null, null, null, null } };

        Random random = new Random();
        int numRows = 7;

        for (int i = 0; i < numRows; i++) {
            Row row = new SimpleRow(_externalTable.getColumnCount());
            for (int j = 0; j < _externalTable.getColumnCount(); j++) {
                row.set(j, values[j][random.nextInt(values[j].length)]);
            }
            _externalTable.addRow(row);
        }

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        for (int i = 0; i < numRows; i++) {
            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
        }
        assertTrue(!iter.hasNext());
        
        RowIterator modIter = _externalTable.getRowIterator(false);
        while (modIter.hasNext()) {
            Row oldRow = modIter.next();
            Row newRow = new SimpleRow(_externalTable.getColumnCount());
            newRow.set(0, oldRow.get(0));
            newRow.set(1, stringTemplate + oldRow.get(0));
            
            _externalTable.updateRow(oldRow, newRow);
        }
        
        iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        for (int i = 0; i < numRows; i++) {
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            assertEquals(stringTemplate + row.get(0), row.get(1));
        }
        assertTrue(!iter.hasNext());
    }    
}