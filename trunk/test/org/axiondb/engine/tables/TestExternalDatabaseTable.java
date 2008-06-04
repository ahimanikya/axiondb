/*
 * $Id: TestExternalDatabaseTable.java,v 1.4 2008/02/21 13:00:27 jawed Exp $
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
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.NullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.constraints.UniqueConstraint;
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
public class TestExternalDatabaseTable extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestExternalDatabaseTable(String testName) {
        super(testName);
    }

    public static Test suite() {
       TestSuite suite = new TestSuite(TestExternalDatabaseTable.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private Database _db;
    private Table _table = null;
    private ExternalDatabaseTable _externalTable = null;
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
        createExternalTable("FOO", "", "");
    }

    private void createExternalTable(String name, String where) throws Exception {
        createExternalTable("FOO", where, "");
    }

    private void createExternalTable(String name, String where, String orderby) throws Exception {
        String rtable = "FOO24";
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
        props.setProperty(ExternalTable.PROP_WHERE, where);
        props.setProperty(ExternalTable.PROP_ORDERBY, orderby);
        props.setProperty(ExternalTable.PROP_REMOTETABLE, rtable);
        props.setProperty(ExternalTable.PROP_CREATE_IF_NOT_EXIST, Boolean.TRUE.toString());
        List columns = new ArrayList(2);
        Column id = new Column("ID", new IntegerType());
        id.setSqlType("integer");
        columns.add(id);

        Column colname = new Column("NAME", new CharacterVaryingType(10));
        colname.setSqlType("varchar");
        columns.add(colname);

        ExternalTableFactory factory = new ExternalTableFactory();
        _externalTable = (ExternalDatabaseTable) factory.createTable(_db, name, props, columns);
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
        _externalTable.commit();
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
        createExternalTable("FOO", "ID = 1");
        assertNotNull(_externalTable.getTableProperties());
        assertEquals("REMOTE", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_LOADTYPE));
        assertEquals("AXIONDB", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_DB));
        assertEquals("ID = 1", _externalTable.getTableProperties().getProperty(
            ExternalTable.PROP_WHERE));
    }

    public void testAddThenDropConstraint() throws Exception {
        createExternalTable("FOO");
        _db.addTable(_externalTable);
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("PK_FOO");
        pk.addSelectable(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        _externalTable.addConstraint(pk);

        try {
            _externalTable.addConstraint(new PrimaryKeyConstraint("PK_BAR"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            _externalTable.addConstraint(new NotNullConstraint("PK_FOO"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        _externalTable.removeConstraint("this constraint does not exist");
        _externalTable.removeConstraint("PRIMARYKEY");
        _externalTable.addConstraint(pk);
        _externalTable.removeConstraint("PK_FOO");
        _externalTable.removeConstraint("PRIMARYKEY"); // shd be silent

        _externalTable.addConstraint(pk);
        _externalTable.addConstraint(new NotNullConstraint("NN_FOO"));
        _externalTable.removeConstraint("NN_FOO");
        _externalTable.addConstraint(new NullConstraint("N_FOO"));
        _externalTable.removeConstraint("N_FOO");
        _externalTable.addConstraint(new UniqueConstraint("U_BAR"));
        _externalTable.removeConstraint("U_BAR");
        //_externalTable.removeConstraint(null);
        _externalTable.removeConstraint("PK_FOO");
        _db.dropTable(_externalTable.getName());
        _externalTable = null;
    }

   /* public void testAddThenDropColumn() throws Exception {
        // TODO: Make this test pass
    }*/

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
        assertNull(_externalTable.getRow(2));
    }

    public void testGetMatchingRowsForNull() throws Exception {
        createExternalTable("FOO");
        RowIterator iter = _externalTable.getMatchingRows(null, null, true);
        assertNotNull(iter);
    }

    public void testDrop() throws Exception {
        createExternalTable("FOO");
        _externalTable.drop();

        try {
            _table.shutdown();
            _externalTable.drop();
            fail("Expected Exception");
        } catch (AxionException e) {
            // expected
        }

        // Note that remote table does exist,
        // so ExternalDatabaseTable should create one.
        createExternalTable("FOO");
        _externalTable.drop();
        _externalTable.shutdown();
        _externalTable.shutdown(); // try again
        _externalTable.commit(); // does nothing if _conn is null
        _table.shutdown();

        try {
            
            _externalTable.drop();
            fail("Expected Exception");
        } catch (AxionException e) {
            // expected
        }

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
        _externalTable.apply(); // does nothing now
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
        assertNull(_externalTable.getRow(1));

        addRow();
        iter = _externalTable.getRowIterator(true);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertFalse(iter.hasNext());
    }

    public void testTableWithWhere() throws Exception {
        createExternalTable("FOO", "ID = 1");

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertNotNull(iter.current());
        assertFalse(iter.hasNext());
    }

    public void testTableWithWOrderBy() throws Exception {
        createExternalTable("FOO", "ID > 0", "ID,NAME");
        _externalTable.truncate();
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            _externalTable.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            _externalTable.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "aaa");
            _externalTable.addRow(row);
        }

        _externalTable.commit();

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        Row row = iter.next();
        assertEquals(new Integer(2), row.get(0));
        assertEquals("two", row.get(1));

        assertTrue(iter.hasNext());
        row = iter.next();
        assertEquals(new Integer(1), row.get(0));
        assertEquals("one", row.get(1));

        assertTrue(iter.hasNext());
        row = iter.next();
        assertEquals(new Integer(2), row.get(0));
        assertEquals("aaa", row.get(1));

        assertFalse(iter.hasNext());
    }

    public void testHasColumn() throws Exception {
        createExternalTable("FOO");
        ColumnIdentifier id = new ColumnIdentifier("ID");

        assertTrue("Should have column", _externalTable.hasColumn(id));
        id.setTableIdentifier(new TableIdentifier("FOO"));
        assertTrue("Should have column", _externalTable.hasColumn(id));

        id.setTableIdentifier(new TableIdentifier("BOGUS"));
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
        _table.drop();
    }

    public void testMissingRequiredProperty() throws Exception {
        Properties badProps = new Properties();
        ExternalTableFactory factory = new ExternalTableFactory();
        try {
            factory.createTable(_db, "BAD_TABLE", badProps, new ArrayList());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
        _table.drop();
    }

    public void testMissingDriverProperty() throws Exception {
        // Null driver test
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

            fail("Expected AxionException due to null driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.

        }
        _table.drop();
    }

    public void testBadLoaderType() throws Exception {
        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
        _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

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

        props.setProperty(ExternalTable.PROP_LOADTYPE, "BOGUS");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        List columns = new ArrayList(2);
        columns.add(new Column("ID", new IntegerType()));
        columns.add(new Column("NAME", new CharacterVaryingType(10)));

        try {
            _externalTable = (ExternalDatabaseTable) factory.createTable(_db, "BOGUS");
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }

        try {
            _externalTable = (ExternalDatabaseTable) factory.createTable(_db, "BOGUS", props,
                columns);
            fail("Expected Exception");
        } catch (Exception e) {
            // excepted
        }
        _table.drop();
    }

    public void testBadDriverClass() throws Exception {
        // bad driver class
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.AxionDriver");
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb2:testdb2");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

            props = new Properties();
            props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
            props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
            props.setProperty(ExternalTable.PROP_REMOTETABLE, "foo24");
            List columns = new ArrayList(2);
            columns.add(new Column("ID", new IntegerType()));
            columns.add(new Column("NAME", new CharacterVaryingType(10)));

            ExternalTableFactory factory = new ExternalTableFactory();
            _externalTable = (ExternalDatabaseTable) factory.createTable(_db, "FOO24", props,
                columns);
            fail("Expected AxionException due to bad driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.
        }
        _table.drop();
    }

    public void testBadUrlProperty() throws Exception {
        // bad url
        try {
            Properties props = new Properties();
            props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
            props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc");
            props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
            props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");
            _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

            props = new Properties();
            props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
            props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");
            props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
            List columns = new ArrayList(2);
            columns.add(new Column("ID", new IntegerType()));
            columns.add(new Column("NAME", new CharacterVaryingType(10)));

            ExternalTableFactory factory = new ExternalTableFactory();
            _externalTable = (ExternalDatabaseTable) factory.createTable(_db, "FOO24", props,
                columns);
            fail("Expected AxionException due to bad driver class");
        } catch (Exception expected) {
            // Expected AxionException due to unrecognized property name.
        }
        _table.drop();
    }

    public void testAddRow() throws Exception {
        createExternalTable("FOO");
    }

    public void testNoNewColumnsAfterRowsAdded() throws Exception {
        createExternalTable("FOO");
        try {
            _externalTable.addColumn(new Column("NAMETWO", new CharacterVaryingType(10)));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
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
        _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");

        ExternalTableLoader loader = new ExternalDatabaseTableLoader();
        _externalTable = (ExternalDatabaseTable) loader.createTable(_db, "FOO");

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
        _externalTable.commit();

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
        _db.createDatabaseLink(new DatabaseLink("AXIONDB", props));

        props = new Properties();
        props.setProperty(ExternalTable.PROP_DB, "AXIONDB");
        props.setProperty(ExternalTable.PROP_REMOTETABLE, "FOO24");
        props.setProperty(ExternalTable.PROP_LOADTYPE, "remote");

        ExternalTableLoader loader = new ExternalDatabaseTableLoader();
        _externalTable = (ExternalDatabaseTable) loader.createTable(_db, "FOO");

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
        _externalTable.commit();

        RowIterator iter = _externalTable.getRowIterator(true);
        assertNotNull(iter);
        for (int i = 0; i < numRows; i++) {
            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
        }
        assertTrue(!iter.hasNext());

        RowIterator modIter = _externalTable.getRowIterator();
        while (modIter.hasNext()) {
            Row oldRow = modIter.next();
            Row newRow = new SimpleRow(_externalTable.getColumnCount());
            newRow.set(0, oldRow.get(0));
            newRow.set(1, stringTemplate + oldRow.get(0));

            _externalTable.updateRow(oldRow, newRow);
        }
        _externalTable.commit();

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