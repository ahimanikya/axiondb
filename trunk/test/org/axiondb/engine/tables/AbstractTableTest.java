/*
 * $Id: AbstractTableTest.java,v 1.3 2008/02/21 13:00:27 jawed Exp $
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

package org.axiondb.engine.tables;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.NoSuchElementException;
import java.util.Random;

import org.axiondb.AbstractDbdirTest;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Person;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.NullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.commands.AddConstraintCommand;
import org.axiondb.engine.commands.AlterTableCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.FileUtil;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.BooleanType;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;
import org.axiondb.types.ObjectType;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:27 $
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 */
public abstract class AbstractTableTest extends AbstractDbdirTest {

    //------------------------------------------------------------ Conventional

    public AbstractTableTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    protected Table table = null;

    protected abstract Table createTable(String name) throws Exception;

    protected abstract File getDataFile() throws Exception;

    protected abstract Database getDatabase() throws Exception;

    protected Table getTable() {
        return table;
    }

    protected String getTableName() {
        return "FOO";
    }

    public void setUp() throws Exception {
        super.setUp();
        table = createTable(getTableName());
    }

    public void tearDown() throws Exception {
        if (table != null) {
            table.shutdown();
            table = null;
        }
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testGetName() throws Exception {
        assertEquals(getTableName().toUpperCase(), table.getName());
    }

    public void testToString() throws Exception {
        assertNotNull(table.getName());
        RowIterator iter = table.getRowIterator(false);
        assertNotNull(iter.toString());
    }

    public void testAddThenDropConstraint() throws Exception {
        addColumns();
        addRows();
        Database db = getDatabase();
        db.addTable(table);
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("PK_FOO");
        pk.addSelectable(new ColumnIdentifier(new TableIdentifier(table.getName()), "ID"));
        table.addConstraint(pk);
        Column column = table.getColumn("ID");
        if (!table.isColumnIndexed(column)) {
            assertFalse(table.hasIndex("BOGUS"));
            Index index1 = db.getIndexFactory("btree").makeNewSystemInstance(table, column, db.getDBDirectory() == null);
            Index index2 = db.getIndexFactory("array").makeNewInstance("INDEX_FOO", column, true, db.getDBDirectory() == null);
            db.addIndex(index1, table, true);
            assertTrue(table.hasIndex(index1.getName()));
            db.addIndex(index2, table, true);
            assertTrue(table.hasIndex(index2.getName()));
        }

        try {
            table.addConstraint(new PrimaryKeyConstraint("PK_BAR"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            table.addConstraint(new NotNullConstraint("PK_FOO"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }

        table.addConstraint(new NotNullConstraint("NN_FOO"));
        table.removeConstraint("this constraint does not exist");
        table.removeConstraint("PRIMARYKEY");
        table.addConstraint(pk);
        table.removeConstraint("PK_FOO");
        table.removeConstraint("primarykey"); // shd be silent

        table.addConstraint(pk);
        table.removeConstraint("NN_FOO");
        table.addConstraint(new NullConstraint("N_FOO"));
        table.removeConstraint("N_FOO");
        table.removeConstraint(null);
        db.dropTable(table.getName());
        table = null;
    }

    public void testAddThenDropColumn() throws Exception {
        table.addColumn(new Column("ID", new BigDecimalType()));
        table.addColumn(new Column("NAME", new CharacterVaryingType(3)));
        addRows();
        Database db = getDatabase();
        db.addTable(getTable());

        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("PK_FOO");
        pk.addSelectable(new ColumnIdentifier(new TableIdentifier(getTableName()), "ID", null,
            new BigDecimalType()));

        AddConstraintCommand addCCmd = new AddConstraintCommand(getTableName(), pk);
        addCCmd.execute(db);

        AlterTableCommand alterCmd = new AlterTableCommand(table.getName(), false);
        alterCmd.addColumn("NEWCOL", "varchar", "4", "0", new Literal("Test"), null);
        alterCmd.execute(db);
        table = db.getTable(getTableName());
        assertTrue(table.hasColumn(new ColumnIdentifier("NEWCOL")));
        assertNotNull(table.getConstraints().next());
        RowIterator iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        Row row = iter.next();
        assertEquals("Test", row.get(2));
        assertTrue(iter.hasNext());
        row = iter.next();
        assertEquals("Test", row.get(2));
        assertTrue(!iter.hasNext());

        alterCmd = new AlterTableCommand(table.getName(), false);
        alterCmd.dropColumn("NEWCOL");
        alterCmd.execute(db);
        table = db.getTable(getTableName());
        assertFalse(table.hasColumn(new ColumnIdentifier("NEWCOL")));
        iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        row = iter.next();
        assertEquals("one", row.get(1));
        assertTrue(iter.hasNext());
        row = iter.next();
        assertEquals("two", row.get(1));
        assertTrue(!iter.hasNext());

        try {
            alterCmd = new AlterTableCommand("BOGUS", false);
            alterCmd.dropColumn("NEWCOL");
            alterCmd.execute(db);
            fail("Expected Exception - table does not exist");
        } catch (AxionException e) {
            // expected table does not exist
        }
        
        try {
            alterCmd = new AlterTableCommand(table.getName(), false);
            alterCmd.dropColumn("NEWCOL"); // does not exist
            alterCmd.execute(db);
            fail("Expected Exception - Bad column to drop");
        } catch (AxionException e) {
            // expected table does not exist
        }

        try {
            AlterTableCommand cmd = new AlterTableCommand("FOO", false);
            cmd.executeQuery(db);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testGetMatchingRowsForNull() throws Exception {
        RowIterator iter = table.getMatchingRows(null, null, true);
        assertNotNull(iter);
    }

    public void testHasColumn() throws Exception {
        ColumnIdentifier id = new ColumnIdentifier("FOO");
        assertTrue("Should not have column", !table.hasColumn(id));
        try {
            table.getColumnIndex("FOO");
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        table.addColumn(new Column("FOO", new CharacterVaryingType(10)));
        assertTrue("Should have column", table.hasColumn(id));
        id.setTableIdentifier(new TableIdentifier(getTableName()));
        assertTrue("Should have column", table.hasColumn(id));

        id.setTableIdentifier(new TableIdentifier("BOGUS"));
        assertTrue("Should not have column", !table.hasColumn(id));
    }

    protected void addColumns() throws Exception {
        table.addColumn(new Column("ID", new BigDecimalType()));
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
    }

    public void addRows() throws Exception {
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            table.addRow(row);
        }
        assertEquals("Should have 2 rows", 2, table.getRowCount());
    }

    public void testAddRow() throws Exception {
        addColumns();
        addRows();
    }

    public void testTruncate() throws Exception {
        table.truncate();
        RowIterator iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasNext());

        addColumns();
        addRows();
        iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertFalse(iter.hasNext());

        // create backup file before to test truncate deletes it
        File df = getDataFile();
        if (df != null) {
            File bkupFile = new File(df.getParentFile(), df.getName() + ".backup");
            FileOutputStream out = new FileOutputStream(bkupFile);
            out.write("test".getBytes());
            out.close();
        }

        table.truncate();
        iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertFalse(iter.hasNext());

        addRows();
        iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertFalse(iter.hasNext());

        // TODO: Somehow Unix is not locking bkupFile, so find out a way to lock the backup file and try this test
        // create backup file before to test truncate deletes it
        //        df = getDataFile();
        //        if (df != null) {
        //            File bkupFile = new File(df.getParentFile(), df.getName() + ".backup");
        //            FileOutputStream fos = new FileOutputStream(bkupFile);
        //            FileLock lock = fos.getChannel().lock(0L, Long.MAX_VALUE, false);
        //            
        //            try {
        //                table.truncate();
        //                fail("Expected Exception");
        //            } catch (AxionException e) {
        //                // expected
        //            } finally {
        //                try {
        //                    lock.release();
        //                    fos.close();
        //                    FileUtil.delete(bkupFile);
        //                } catch (IOException ioe) {
        //                    // ignore
        //                }
        //            }
        //        }

    }

    public void testDefrag() throws Exception {
        Database db = getDatabase();

        if (db instanceof DiskDatabase) {

            DiskDatabase diskDB = (DiskDatabase) db;
            addColumns();
            addRows();
            db.addTable(table);
            table.shutdown();

            long oldLength = FileUtil.getLength(getDataFile());
            diskDB.defragTable(getTableName());
            long newLength = FileUtil.getLength(getDataFile());
            assertTrue("Expected " + oldLength + " = " + newLength, oldLength == newLength);

            table = db.getTable(getTableName());
            RowIterator iter = table.getRowIterator(false);
            assertNotNull(iter);

            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
            Row row = iter.current();
            iter.remove();
            table.addRow(new SimpleRow(row));
            iter.reset();

            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
            row = iter.current();
            iter.set(new SimpleRow(row));

            assertTrue(iter.hasNext());
            assertNotNull(iter.next());

            assertFalse(iter.hasNext());

            oldLength = FileUtil.getLength(getDataFile());;
            diskDB.defragTable(getTableName());
            
            newLength = FileUtil.getLength(getDataFile());
            
            assertTrue("Expected " + oldLength + " > " + newLength, oldLength > newLength);

            table = diskDB.getTable(getTableName());
            iter = table.getRowIterator(false);
            assertNotNull(iter);

            assertTrue(iter.hasNext());
            assertNotNull(iter.next());

            assertTrue(iter.hasNext());
            assertNotNull(iter.next());

            assertFalse(iter.hasNext());
        }
    }

    public void testGetRowIterator() throws Exception {
        table.addColumn(new Column("ID", new IntegerType()));
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            table.addRow(row);
        }
        RowIterator iter = table.getRowIterator(false);
        assertNotNull(iter);

        try {
            iter.current();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException ex) {
            // Expected
        }

        try {
            iter.set(null);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            iter.remove();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // Iteration Pass 1
        assertFalse(iter.hasPrevious());

        try {
            iter.previous();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException ex) {
            // Expected
        }

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
        assertEquals(iter.previousIndex(), iter.nextIndex() - 1);
        try {
            iter.next();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException ex) {
            // Expected
        }

        // Iteration Pass 2 : update row
        Row row = new SimpleRow(2);
        row.set(0, new Integer(4));
        row.set(1, "newRow");

        iter.reset();
        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        iter.set(row);
        assertNotNull(iter.current());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        iter.set(row);
        assertNotNull(iter.current());

        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        iter.set(row);
        assertNotNull(iter.current());

        assertFalse(iter.hasNext());

        // Iteration Pass 3
        assertTrue(iter.hasPrevious());
        assertNotNull(iter.previous());
        assertTrue(iter.hasPrevious());
        assertNotNull(iter.previous());
        assertTrue(iter.hasPrevious());
        assertNotNull(iter.previous());

        assertFalse(iter.hasPrevious());

        assertTrue(iter.hasNext());
    }

    public void testNoNewColumnsAfterRowsAdded() throws Exception {
        table.addColumn(new Column("ID", new IntegerType()));
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        Row row = new SimpleRow(2);
        row.set(0, new Integer(1));
        row.set(1, "one");
        table.addRow(row);
        try {
            table.addColumn(new Column("NAMETWO", new CharacterVaryingType(10)));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
    }

    public void testGetColumnByIndex() throws Exception {
        table.addColumn(new Column("ID", new IntegerType()));
        assertEquals("ID", table.getColumn(0).getName());
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        assertEquals("ID", table.getColumn(0).getName());
        assertEquals("NAME", table.getColumn(1).getName());
    }

    public void testGetColumnByIndexBadIndex() throws Exception {
        try {
            table.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        try {
            table.getColumn(0);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        table.addColumn(new Column("ID", new IntegerType()));

        try {
            table.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        try {
            table.getColumn(1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));

        try {
            table.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        try {
            table.getColumn(2);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
    }

    public void testGetAndFreeRowId() throws Exception {
        int id = table.getNextRowId();
        table.freeRowId(id);
        assertEquals(id, table.getNextRowId());
        int id2 = table.getNextRowId();
        assertTrue(id != id2);
    }

    public void testGetColumnByName() throws Exception {
        table.addColumn(new Column("ID", new IntegerType()));
        assertTrue(table.getColumn("ID").getDataType() instanceof IntegerType);
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        assertTrue(table.getColumn("ID").getDataType() instanceof IntegerType);
        assertTrue(table.getColumn("NAME").getDataType() instanceof CharacterVaryingType);
    }

    public void testGetColumnByNameBadName() throws Exception {
        assertNull(table.getColumn("FOO"));
        assertNull(table.getColumn("ID"));
        table.addColumn(new Column("ID", new IntegerType()));
        assertNull(table.getColumn("FOO"));
        assertNull(table.getColumn("NAME"));
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        assertNull(table.getColumn("FOO"));
    }

    public void testDataTypes() throws Exception {
        Table typeTable = createTable("TYPETABLE");

        typeTable.addColumn(new Column("STRCOL", new CharacterVaryingType(30)));
        typeTable.addColumn(new Column("INTCOL", new IntegerType()));
        typeTable.addColumn(new Column("BOOLCOL", new BooleanType()));

        Object[][] values = new Object[][] {
                new Object[] { "", "A String", "Another String", null},
                new Object[] { new Integer(17), new Integer(0), new Integer(5575), null},
                new Object[] { Boolean.TRUE, Boolean.TRUE, Boolean.FALSE, null}};

        Random random = new Random();
        int numRows = 7;

        for (int i = 0; i < numRows; i++) {
            Row row = new SimpleRow(typeTable.getColumnCount());
            for (int j = 0; j < typeTable.getColumnCount(); j++) {
                row.set(j, values[j][random.nextInt(values[j].length)]);
            }
            typeTable.addRow(row);
        }

        RowIterator iter = typeTable.getRowIterator(true);
        assertNotNull(iter);
        for (int i = 0; i < numRows; i++) {
            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
        }
        assertTrue(!iter.hasNext());
        typeTable.shutdown();
    }

    public void testObjectTable() throws Exception {
        Table personTable = createTable("PERSON");

        personTable.addColumn(new Column("KEY", new ObjectType()));
        personTable.addColumn(new Column("VALUE", new ObjectType()));

        Row row = new SimpleRow(personTable.getColumnCount());
        Person person = new Person("James", 1969);

        row.set(0, person.getName());
        row.set(1, person);
        personTable.addRow(row);

        row = new SimpleRow(personTable.getColumnCount());
        person = new Person("Simon", 1971);

        row.set(0, person.getName());
        row.set(1, person);
        personTable.addRow(row);

        RowIterator iter = personTable.getRowIterator(true);
        assertNotNull(iter);

        assertTrue(iter.hasNext());
        row = iter.next();
        assertNotNull(row);

        Object value = row.get(0);
        assertEquals("James", value);
        value = row.get(1);
        assertTrue("Found person object", value instanceof Person);
        person = (Person) value;
        assertEquals("James", person.getName());
        assertEquals(1969, person.getDateOfBirth());

        assertTrue(iter.hasNext());
        row = iter.next();
        assertNotNull(row);

        value = row.get(0);
        assertEquals("Simon", value);
        value = row.get(1);
        assertTrue("Found person object", value instanceof Person);
        person = (Person) value;
        assertEquals("Simon", person.getName());
        assertEquals(1971, person.getDateOfBirth());

        assertTrue(!iter.hasNext());
        personTable.shutdown();
    }

    public void testAddPrimaryKeyConstraintOnPopulatedTable() throws Exception {
        table.addColumn(new Column("ID", new BigDecimalType()));
        table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        {
            Row row = new SimpleRow(2);
            row.set(0, new BigDecimal(1));
            row.set(1, "one");
            table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new BigDecimal(2));
            row.set(1, "two");
            table.addRow(row);
        }

        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("PK_FOO");
        ColumnIdentifier colId = new ColumnIdentifier(new TableIdentifier(table.getName()), "ID");
        pk.addSelectable(colId);

        table.addConstraint(pk);

        // Adding duplicate ID should fail.
        {
            Row row = new SimpleRow(2);
            row.set(0, new BigDecimal(2));
            row.set(1, "two");
            try {
                table.addRow(row);
                fail("Expected AxionException on adding row with duplicate ID to table with PK");
            } catch (AxionException expected) {
                // Expected.
            }
        }

        // Now drop constraint, then add a duplicate row - primary key constraint should
        // fail on add
        table.removeConstraint("PK_FOO");
        {
            Row row = new SimpleRow(2);
            row.set(0, new BigDecimal(2));
            row.set(1, "two");
            table.addRow(row);
        }
        try {
            table.addConstraint(pk);
            fail("Expected AxionException on applying PK constraint to existing table with dup rows");
        } catch (AxionException expected) {
            // Expected.
        }
    }
}

