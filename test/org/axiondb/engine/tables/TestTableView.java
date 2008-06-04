/*
 * $Id: TestTableView.java,v 1.1 2007/11/28 10:01:28 jawed Exp $
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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AbstractDbdirTest;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.engine.commands.CreateViewCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:28 $
 * @author Ahimanikya Satapathy
 */
public class TestTableView extends AbstractDbdirTest {

    //------------------------------------------------------------ Conventional

    public TestTableView(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestTableView.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private DiskDatabase _db = null;
    protected Table table = null;
    protected Table view = null;

    protected Table createTable(String name) throws Exception {
        Table t = new DiskTable(name, _db);
        t.addColumn(new Column("ID", new IntegerType()));
        t.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            t.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            t.addRow(row);
        }
        return t;
    }

    protected Table createView(String name) throws Exception {
        CreateViewCommand cmd = new CreateViewCommand();
        cmd.setObjectName(name);
        cmd.setIfNotExists(true);
        cmd.setSubQuery("select * from " + getTableName());
        cmd.execute(_db);
        return _db.getTable(getViewName());
    }

    public void setUp() throws Exception {
        getDbdir().mkdirs();
        _db = new DiskDatabase(getDbdir());
        table = createTable(getTableName());
        _db.addTable(table);
        view = createView(getViewName());
        super.setUp();
    }

    public void tearDown() throws Exception {
        if (table != null) {
            table.shutdown();
            table = null;
        }
        if (view != null) {
            view.shutdown();
            view = null;
        }
        _db.shutdown();
        super.tearDown();
    }

    protected Table getView() {
        return view;
    }

    protected Table getTable() {
        return table;
    }

    protected String getTableName() {
        return "FOO";
    }

    protected String getViewName() {
        return "FOOVIEW";
    }

    //------------------------------------------------------------------- Tests

    public void testGetName() throws Exception {
        assertEquals(getViewName(), view.getName());
    }

    public void testToString() throws Exception {
        assertNotNull(view.getName());
    }

    public void testGetMatchingRowsForNull() throws Exception {
        RowIterator iter = view.getMatchingRows(null, null, true);
        assertNotNull(iter);
    }

    public void testRowDecorator() throws Exception {
        assertNotNull(view.makeRowDecorator());
        assertNotNull(view.makeRowDecorator());
        assertNotNull(((TableView) view).buildRowDecorator());
        assertNotNull(((TableView) view).getColumnIdentifierList(new TableIdentifier("BOGUS")));
    }

    public void testGetMatchingRows() throws Exception {
        List sels = new ArrayList();
        sels.add(new ColumnIdentifier(new TableIdentifier("FOOVIEW"), "ID"));
        sels.add(new ColumnIdentifier(new TableIdentifier("FOOVIEW"), "ID"));
        List vals = new ArrayList();
        vals.add(new Integer(1));
        vals.add(new Integer(1));

        RowIterator iter = view.getMatchingRows(sels, vals, true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        Row row = iter.next();
        assertNotNull(row);
        assertEquals(new Integer(1), row.get(0));
        assertEquals("one", row.get(1));
        assertTrue(!iter.hasNext());
    }

    public void testHasColumn() throws Exception {
        ColumnIdentifier id = new ColumnIdentifier("FOO");
        assertTrue("Should not have column", !view.hasColumn(id));
        try {
            view.getColumnIndex("FOO");
            fail("Expected AxionException");
        } catch (Exception e) {
            // expected
        }

        id = new ColumnIdentifier("ID");
        assertTrue("Should have column", view.hasColumn(id));

        id.setTableIdentifier(new TableIdentifier(getViewName()));
        assertTrue("Should have column", view.hasColumn(id));

        // TODO: This should pass.
        //id.setTableIdentifier(new TableIdentifier("bogus"));
        //assertTrue("Should not have column", !view.hasColumn(id));
    }

    public void testGetRowIterator() throws Exception {
        RowIterator iter = view.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
    }

    public void testGetColumnByIndex() throws Exception {
        assertEquals("ID", view.getColumn(0).getName());
        assertEquals("NAME", view.getColumn(1).getName());
    }

    public void testGetColumnByIndexBadIndex() throws Exception {
        try {
            view.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }

        try {
            view.getColumn(2);
            fail("Expected IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
    }

    public void testGetColumnByName() throws Exception {
        assertTrue(view.getColumn("ID").getDataType() instanceof IntegerType);
        assertTrue(view.getColumn("NAME").getDataType() instanceof CharacterVaryingType);
    }

    public void testGetColumnByNameBadName() throws Exception {
        assertNull(table.getColumn("FOO"));
    }

    public void testViewDrop() throws Exception {
        File tabledir = new File(getDbdir(), getViewName());
        File meta = new File(tabledir, getViewName() + ".META");
        assertTrue("Table directory should exist", tabledir.exists());
        assertTrue("Meta file should exist", meta.exists());

        _db.dropTable(getViewName());
        assertTrue("Meta file should not exist", !meta.exists());
        assertTrue("Table directory should not exist", !tabledir.exists());
    }

    public void testRestartDB() throws Exception {
        CreateViewCommand cmd = new CreateViewCommand();
        cmd.setObjectName("ANOTHERVIEW");
        cmd.setIfNotExists(true);
        cmd.setSubQuery("select id myid, id+1 myid1, 5 lit from " + getTableName());
        cmd.execute(_db);

        table.shutdown();
        table = null;
        view.shutdown();
        view = null;

        _db.shutdown();

        _db = new DiskDatabase(getDbdir());
        assertTrue(_db.hasTable(getViewName()));
        assertTrue(_db.hasTable(getTableName()));
        table = _db.getTable(getTableName());
        view = _db.getTable(getViewName());

        assertTrue(_db.hasTable("ANOTHERVIEW"));

        testGetName();
        testGetRowIterator();
    }

    public void testReloadView() throws Exception {
        view.shutdown();
        view = null;
        TableViewFactory factory = new TableViewFactory();
        view = factory.createTable(_db, getViewName());
        testGetName();
        testGetRowIterator();
    }

    public void testNotSupported() throws Exception {
        TableView tv = null;
        if (view instanceof TransactableTable) {
            tv = (TableView) ((TransactableTableImpl) view).getTable();
        } else {
            tv = (TableView) view;
        }

        try {
            tv.addColumn(new Column("ID", new IntegerType()));
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.populateIndex(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.addRow(new SimpleRow(1));
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.updateRow(null, new SimpleRow(1));
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        assertEquals(-1, tv.getNextRowId());
        tv.freeRowId(0);
        assertNotNull(tv.toString());

        try {
            tv.applyDeletes(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.applyInserts(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.applyUpdates(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.addConstraint(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.addIndex(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.removeConstraint(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.removeIndex(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.truncate();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            tv.getRow(0);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        assertNull(tv.getIndexForColumn(new Column("ID", new IntegerType())));
        assertFalse(tv.hasIndex(getViewName()));
        assertFalse(tv.isUniqueConstraintExists(""));
        assertFalse(tv.isPrimaryKeyConstraintExists(""));
        tv.remount(null, false);

    }
}