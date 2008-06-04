/*
 * $Id: AbstractDatabaseTest.java,v 1.3 2007/12/12 13:39:54 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

package org.axiondb;

import java.io.Reader;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.axiondb.engine.commands.AxionQueryContext;
import org.axiondb.engine.commands.CreateTableCommand;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.commands.InsertCommand;
import org.axiondb.engine.commands.SelectCommand;
import org.axiondb.engine.commands.UpdateCommand;
import org.axiondb.event.BaseDatabaseModificationListener;
import org.axiondb.event.DatabaseModifiedEvent;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanFunction;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.3 $ $Date: 2007/12/12 13:39:54 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Doug Sale
 * @author Dave Pekarek Krohn
 * @author Ahimanikya Satapathy
 */
public abstract class AbstractDatabaseTest extends AbstractDbdirTest {

    //------------------------------------------------------------ Conventional

    public AbstractDatabaseTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    private static final String LOBDIR_PROP = "axiondb.lobdir";
    private org.axiondb.Database _db = null;
    private String _origLobDirVal = null;
    protected String _dbName = "dbfoo";

    public void setUp() throws Exception {
        super.setUp();
        _origLobDirVal = System.getProperty(LOBDIR_PROP);
        System.setProperty(LOBDIR_PROP, "testdb");
        _db = createDatabase(_dbName);
    }

    public void tearDown() throws Exception {
        _db.shutdown();
        _db = null;
        if (_origLobDirVal != null) {
            System.getProperties().remove(LOBDIR_PROP);
        }
        super.tearDown();
    }

    //-------------------------------------------------------------------- Util

    protected abstract Database createDatabase(String name) throws Exception;

    protected Database getDb() {
        return _db;
    }

    private void createTableFoo() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("FOO");
        cmd.addColumn("ID", "integer");
        cmd.addColumn("STR", "varchar", "10");
        cmd.execute(_db);
    }

    private void populateTableFoo() throws Exception {
        List columns = new ArrayList(2);
        columns.add(new ColumnIdentifier("ID"));
        columns.add(new ColumnIdentifier("STR"));
        for (int i = 0; i < 10; i++) {
            List values = new ArrayList(2);
            values.add(new Literal(new Integer(i), new IntegerType()));
            values.add(new Literal(String.valueOf(i), new CharacterVaryingType(10)));
            InsertCommand cmd = new InsertCommand(new TableIdentifier("FOO"), columns, values);
            cmd.executeUpdate(_db);
        }
    }

    private void createTableBar() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("BAR");
        cmd.addColumn("BARID", "integer");
        cmd.addColumn("BARSTR", "varchar", "10");
        cmd.execute(_db);
    }

    private void populateTableBar() throws Exception {
        List columns = new ArrayList(2);
        columns.add(new ColumnIdentifier("BARID"));
        columns.add(new ColumnIdentifier("BARSTR"));
        for (int i = 0; i < 10; i++) {
            List values = new ArrayList(2);
            values.add(new Literal(new Integer(i), new IntegerType()));
            values.add(new Literal(String.valueOf(i), new CharacterVaryingType(10)));
            InsertCommand cmd = new InsertCommand(new TableIdentifier("BAR"), columns, values);
            cmd.executeUpdate(_db);
        }
    }

    protected class TestListener extends BaseDatabaseModificationListener {
        private DatabaseModifiedEvent _event = null;

        public void tableAdded(DatabaseModifiedEvent e) {
            _event = e;
        }

        public void tableDropped(DatabaseModifiedEvent e) {
            _event = e;
        }

        public DatabaseModifiedEvent getEvent() {
            return _event;
        }
    }

    protected boolean findStringInTable(String value, Table t, String colName) throws Exception {
        boolean found = false;
        RowIterator it = t.getRowIterator(true);
        int colIdx = t.getColumnIndex(colName);
        while (!found && it.hasNext()) {
            Row cur = it.next();
            Object rowVal = cur.get(colIdx);
            DataType type = t.getColumn(colIdx).getDataType();
            String sVal = type.toString(rowVal);
            found = sVal.equals(value);
        }
        return found;
    }

    //------------------------------------------------------------------- Tests

    public void testGetName() throws Exception {
        assertEquals(_dbName, _db.getName());
    }

    public void testLoadProperties() throws Exception {
        assertNotNull("Should find type", _db.getDataType("integer"));
        Table types = _db.getTable("AXION_TYPES");
        assertNotNull("Should find types table", types);
        boolean found = findStringInTable("INTEGER", types, "TYPE_NAME");
        assertTrue("Should find type in system table", found);
    }

    public void testCreateTableGetTable() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("FOO");
        cmd.addColumn("ID", "INTEGER");

        TestListener listener = new TestListener();
        int baseListenCount = _db.getDatabaseModificationListeners().size();
        _db.addDatabaseModificationListener(listener);
        assertEquals("Should find another listener", baseListenCount + 1, _db.getDatabaseModificationListeners().size());

        assertNull("Should not indicate table added", listener.getEvent());
        cmd.execute(_db);
        assertEquals("Should indicate table added", "FOO", listener.getEvent().getTable().getName());
        assertNotNull("Should get a table back", _db.getTable("FOO"));
        assertNotNull("Should get a table back", _db.getTable("FOO"));

        Table sysTable = getDb().getTable("AXION_TABLES");
        assertNotNull("Should have system table of tables", sysTable);
        boolean found = findStringInTable("FOO", sysTable, "TABLE_NAME");
        assertTrue("Should find entry for table in system table", found);

        Table sysCol = getDb().getTable("AXION_COLUMNS");
        assertNotNull("Should have system table of columns", sysCol);
        found = findStringInTable("ID", sysCol, "COLUMN_NAME");
        assertTrue("Should find entry for column in system table", found);

        Table foo = getDb().getTable("FOO");
        foo.addColumn(new Column("text", new CharacterVaryingType(10)));
        found = findStringInTable("TEXT", sysCol, "COLUMN_NAME");
        assertTrue("Should find entry for new column in system table", found);
    }

    public void testDropTable() throws Exception {

        CreateTableCommand cmd = new CreateTableCommand("FOO1");
        cmd.addColumn("ID", "integer");
        cmd.execute(_db);

        assertNotNull("Should get a table back", _db.getTable("FOO1"));
        TestListener listener = new TestListener();
        _db.addDatabaseModificationListener(listener);
        assertNull("Should not indicate table dropped", listener.getEvent());
        int before = getDb().getTable("AXION_TABLES").getRowCount();
        _db.dropTable("FOO1");
        assertEquals("Should indicate table dropped", "FOO1", listener.getEvent().getTable().getName());
        assertNull(_db.getTable("foo1"));
        int after = getDb().getTable("AXION_TABLES").getRowCount();
        assertEquals("Should have one fewer row", before - 1, after);
    }

    public void testDropTable2() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("FOO2");
        cmd.addColumn("id", "integer");
        cmd.execute(_db);

        assertNotNull("Should get a table back", _db.getTable("FOO2"));
        _db.dropTable("FOO2");
        assertNull(_db.getTable("FOO2"));
    }

    public void testCantCreateDuplicateTable() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("FOO");
        cmd.addColumn("ID", "integer");
        cmd.execute(_db);
        assertNotNull("Should get a table back", _db.getTable("FOO"));
        try {
            cmd.execute(_db);
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
    }

    public void testGetBadName() throws Exception {
        assertNull(_db.getTable("BOGUS"));
    }

    public void testSimpleInsert() throws Exception {
        createTableFoo();
        Table foo = _db.getTable("FOO");
        List columns = new ArrayList(2);
        columns.add(new ColumnIdentifier("ID"));
        columns.add(new ColumnIdentifier("STR"));
        for (int i = 0; i < 10; i++) {
            assertEquals(i, foo.getRowCount());
            List values = new ArrayList(2);
            values.add(new Literal(new Integer(i), new IntegerType()));
            values.add(new Literal(String.valueOf(i), new CharacterVaryingType(10)));
            InsertCommand cmd = new InsertCommand(new TableIdentifier("FOO"), columns, values);
            assertEquals(1, cmd.executeUpdate(_db));
            assertEquals(i + 1, foo.getRowCount());
        }
    }

    public void testInsertWithNullColumn() throws Exception {
        createTableFoo();
        Table foo = _db.getTable("FOO");
        List columns = new ArrayList(2);
        columns.add(new ColumnIdentifier("ID"));
        for (int i = 0; i < 10; i++) {
            assertEquals(i, foo.getRowCount());
            List values = new ArrayList(2);
            values.add(new Literal(new Integer(i), new IntegerType()));
            InsertCommand cmd = new InsertCommand(new TableIdentifier("FOO"), columns, values);
            assertEquals(1, cmd.executeUpdate(_db));
            assertEquals(i + 1, foo.getRowCount());
        }
    }

    public void testUpdateNone() throws Exception {
        createTableFoo();
        populateTableFoo();

        UpdateCommand cmd = new UpdateCommand();
        cmd.setTable(new TableIdentifier("FOO"));
        cmd.addColumn(new ColumnIdentifier("STR"));
        cmd.addValue(new Literal("GREATER_THAN_11", new CharacterVaryingType(10)));
        cmd.addColumn(new ColumnIdentifier("ID"));
        cmd.addValue(new Literal(new Integer(11), new IntegerType()));

        Selectable where = makeLeafWhereNode(new GreaterThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(
            new Integer(11), new IntegerType()));
        cmd.setWhere(where);

        assertEquals(0, cmd.executeUpdate(_db));
    }

    public void testUpdateOne() throws Exception {
        createTableFoo();
        populateTableFoo();

        UpdateCommand cmd = new UpdateCommand();
        cmd.setTable(new TableIdentifier("FOO"));
        cmd.addColumn(new ColumnIdentifier("STR"));
        cmd.addValue(new Literal("VII", new CharacterVaryingType(10)));
        Selectable where = makeLeafWhereNode(new EqualFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(7),
            new IntegerType()));
        cmd.setWhere(where);

        assertEquals(1, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();

        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.setWhere(where);
        SelectCommand select = new SelectCommand(ctx);
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        assertTrue(rset.next());
        assertEquals("VII", rset.getString(1));
        assertTrue(!rset.wasNull());
        assertTrue(!rset.next());
    }

    public void testUpdateSome() throws Exception {
        createTableFoo();
        populateTableFoo();

        UpdateCommand cmd = new UpdateCommand();
        cmd.setTable(new TableIdentifier("FOO"));
        cmd.addColumn(new ColumnIdentifier("STR"));
        cmd.addValue(new Literal("X", new CharacterVaryingType(10)));
        Selectable where = makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(
            5), new IntegerType()));
        cmd.setWhere(where);

        assertEquals(5, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            if (i < 5) {
                assertEquals("X", rset.getString(1));
            } else {
                assertEquals(String.valueOf(i), rset.getString(1));
            }
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testUpdateAll() throws Exception {
        createTableFoo();
        populateTableFoo();

        UpdateCommand cmd = new UpdateCommand();
        cmd.setTable(new TableIdentifier("FOO"));
        cmd.addColumn(new ColumnIdentifier("STR"));
        cmd.addValue(new Literal("X", new CharacterVaryingType(10)));

        assertEquals(10, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals("X", rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt(2));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testDeleteNone() throws Exception {
        createTableFoo();
        populateTableFoo();

        Selectable where = makeLeafWhereNode(new GreaterThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(
            new Integer(11), new IntegerType()));

        DeleteCommand cmd = new DeleteCommand("FOO", where);
        assertEquals(0, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testDeleteOne() throws Exception {
        createTableFoo();
        populateTableFoo();

        Selectable where = makeLeafWhereNode(new EqualFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(7),
            new IntegerType()));

        DeleteCommand cmd = new DeleteCommand("FOO", where);
        assertEquals(1, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            if (i != 7) {
                assertTrue(rset.next());
                assertEquals(String.valueOf(i), rset.getString(1));
                assertTrue(!rset.wasNull());
            }
        }
        assertTrue(!rset.next());
    }

    public void testDeleteSome() throws Exception {
        createTableFoo();
        populateTableFoo();

        Selectable where = makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(
            5), new IntegerType()));

        DeleteCommand cmd = new DeleteCommand("FOO", where);
        assertEquals(5, cmd.executeUpdate(_db));

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 5; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectOneColumnFromOneTable() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("STR"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectClobReader() throws Exception {

        // create a long string
        String text = null;
        {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < 10; i++) {
                buf.append("The quick brown fox jumped over the lazy dogs.");
            }
            text = buf.toString();
        }

        // create the table
        {
            CreateTableCommand cmd = new CreateTableCommand("FOO");
            cmd.addColumn("ID", "integer");
            cmd.addColumn("CLOBSTR", "varchar", text.length() + "");
            cmd.execute(_db);
        }

        // insert into the table
        {
            List columns = new ArrayList(2);
            columns.add(new ColumnIdentifier("ID"));
            columns.add(new ColumnIdentifier("CLOBSTR"));
            IntegerType intType = new IntegerType();
            CharacterVaryingType varcharType = new CharacterVaryingType(text.length());
            for (int i = 0; i < 10; i++) {
                List values = new ArrayList(2);
                values.add(new Literal(new Integer(i), intType));
                values.add(new Literal(text, varcharType));
                InsertCommand cmd = new InsertCommand(new TableIdentifier("FOO"), columns, values);
                assertEquals(1, cmd.executeUpdate(_db));
            }
        }

        // select from the table, and grab the clobs
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "CLOBSTR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            Clob clob = rset.getClob(1);
            assertTrue(!rset.wasNull());
            assertNotNull(clob);
            {
                Reader in = clob.getCharacterStream();
                assertNotNull(in);
                StringBuffer buf = new StringBuffer();
                for (int c = in.read(); c != -1; c = in.read()) {
                    buf.append((char) c);
                }
                assertEquals(text, buf.toString());
            }

            clob = rset.getClob("CLOBSTR");
            assertTrue(!rset.wasNull());
            assertNotNull(clob);
            {
                Reader in = clob.getCharacterStream();
                assertNotNull(in);
                StringBuffer buf = new StringBuffer();
                for (int c = in.read(); c != -1; c = in.read()) {
                    buf.append((char) c);
                }
                assertEquals(text, buf.toString());
            }
        }
        assertTrue(!rset.next());
    }

    public void testSelectTwoColumnsFromOneTable() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("STR"));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt(2));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("ID"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectAllColumnsFromOneTable() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "*"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());
            assertEquals(i, rset.getInt(1));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("ID"));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString(2));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("STR"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectSomeFromTwoTables() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARSTR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addFrom(new TableIdentifier("BAR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertTrue(rset.next());
                assertEquals(String.valueOf(i), rset.getString(1));
                assertTrue(!rset.wasNull());
                assertEquals(String.valueOf(i), rset.getString("BARSTR"));
                assertTrue(!rset.wasNull());
                assertEquals(j, rset.getInt(2));
                assertTrue(!rset.wasNull());
                assertEquals(j, rset.getInt("ID"));
                assertTrue(!rset.wasNull());
            }
        }
        assertTrue(!rset.next());
    }

    public void testSelectAllFromTwoTables() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARSTR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARID"));
        ctx.addFrom(new TableIdentifier("BAR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertTrue(rset.next());

                assertEquals(String.valueOf(i), rset.getString(1));
                assertTrue(!rset.wasNull());
                assertEquals(String.valueOf(i), rset.getString("BARSTR"));
                assertTrue(!rset.wasNull());

                assertEquals(j, rset.getInt(2));
                assertTrue(!rset.wasNull());
                assertEquals(j, rset.getInt("ID"));
                assertTrue(!rset.wasNull());

                assertEquals(String.valueOf(j), rset.getString(3));
                assertTrue(!rset.wasNull());
                assertEquals(String.valueOf(j), rset.getString("STR"));
                assertTrue(!rset.wasNull());

                assertEquals(i, rset.getInt(4));
                assertTrue(!rset.wasNull());
                assertEquals(i, rset.getInt("BARID"));
                assertTrue(!rset.wasNull());

            }
        }
        assertTrue(!rset.next());
    }

    public void testSimpleSelectWithWhere() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addFrom(new TableIdentifier("FOO"));
        Selectable where = makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(
            5), new IntegerType()));
        ctx.setWhere(where);

        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);

        for (int i = 0; i < 5; i++) {
            assertTrue(rset.next());
            assertEquals(i, rset.getInt(1));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("ID"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectWithSimpleJoin() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARSTR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARID"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.addFrom(new TableIdentifier("BAR"));
        Selectable where = makeLeafWhereNode(new EqualFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new ColumnIdentifier(
            new TableIdentifier("BAR"), "BARID"));
        ctx.setWhere(where);

        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 10; i++) {
            assertTrue(rset.next());

            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("barstr"));
            assertTrue(!rset.wasNull());

            assertEquals(i, rset.getInt(2));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("id"));
            assertTrue(!rset.wasNull());

            assertEquals(String.valueOf(i), rset.getString(3));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("str"));
            assertTrue(!rset.wasNull());

            assertEquals(i, rset.getInt(4));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("barid"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectJoinWithMultipartWhere() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARSTR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("BAR"), "BARID"));
        ctx.addFrom(new TableIdentifier("BAR"));
        ctx.addFrom(new TableIdentifier("FOO"));

        AndFunction where = new AndFunction();
        where.addArgument(makeLeafWhereNode(new EqualFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new ColumnIdentifier(
            new TableIdentifier("BAR"), "BARID")));
        where.addArgument(makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(
            new Integer(5), new IntegerType())));
        ctx.setWhere(where);

        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 5; i++) {
            assertTrue(rset.next());

            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("barstr"));
            assertTrue(!rset.wasNull());

            assertEquals(i, rset.getInt(2));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("id"));
            assertTrue(!rset.wasNull());

            assertEquals(String.valueOf(i), rset.getString(3));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("str"));
            assertTrue(!rset.wasNull());

            assertEquals(i, rset.getInt(4));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("barid"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectWithMultipartWhere() throws Exception {
        createTableFoo();
        populateTableFoo();

        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"));
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));

        AndFunction where = new AndFunction();
        where.addArgument(makeLeafWhereNode(new EqualFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new ColumnIdentifier(
            new TableIdentifier("FOO"), "ID")));
        where.addArgument(makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(
            new Integer(5), new IntegerType())));
        ctx.setWhere(where);

        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 0; i < 5; i++) {
            assertTrue(rset.next());

            assertEquals(i, rset.getInt(1));
            assertTrue(!rset.wasNull());
            assertEquals(i, rset.getInt("id"));
            assertTrue(!rset.wasNull());

            assertEquals(String.valueOf(i), rset.getString(2));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("str"));
            assertTrue(!rset.wasNull());

        }
        assertTrue(!rset.next());
    }

    public void testSelectWithWhereOnAnotherColumn() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        Selectable where = makeLeafWhereNode(new LessThanFunction(), new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), new Literal(new Integer(
            5), new IntegerType()));
        ctx.setWhere(where);

        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);

        for (int i = 0; i < 5; i++) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("str"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSelectWithOrderBy() throws Exception {
        createTableFoo();
        populateTableFoo();

        // execute a select upon it
        AxionQueryContext ctx = new AxionQueryContext();
        SelectCommand select = new SelectCommand(ctx);
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "STR"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.addOrderBy(new OrderNode(new ColumnIdentifier(new TableIdentifier("FOO"), "ID"), true));
        ResultSet rset = select.executeQuery(_db);
        assertNotNull(rset);
        for (int i = 9; i >= 0; i--) {
            assertTrue(rset.next());
            assertEquals(String.valueOf(i), rset.getString(1));
            assertTrue(!rset.wasNull());
            assertEquals(String.valueOf(i), rset.getString("STR"));
            assertTrue(!rset.wasNull());
        }
        assertTrue(!rset.next());
    }

    public void testSequence() throws Exception {
        assertNull("Should not find sequence", getDb().getSequence("SEQ1"));
        getDb().createSequence(new Sequence("SEQ1", 0));
        assertNotNull("Should get sequence", getDb().getSequence("SEQ1"));
        getDb().getSequence("SEQ1").evaluate();
        getDb().getSequence("SEQ1").evaluate();
        assertEquals("Should have correct value", BigInteger.valueOf(2), getDb().getSequence("SEQ1").getValue());

        Table seqTable = getDb().getTable("AXION_SEQUENCES");
        assertNotNull("Should find sequence table", seqTable);
        RowIterator it = seqTable.getRowIterator(true);
        Object seqVal = null;
        while (seqVal == null && it.hasNext()) {
            Row row = it.next();
            if ("SEQ1".equals(row.get(0))) {
                seqVal = row.get(1);
            }
        }
        assertNotNull("Should find value", seqVal);
        assertEquals("Value should be updated", new Integer(2), seqVal);
    }

    private Selectable makeLeafWhereNode(Function fun, Selectable left, Selectable right) {
        fun.addArgument(left);
        if (null != right) {
            fun.addArgument(right);
        }
        return fun;
    }

}