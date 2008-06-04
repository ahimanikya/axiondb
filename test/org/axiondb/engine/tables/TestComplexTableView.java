/*
 * $Id: TestComplexTableView.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.engine.commands.CreateViewCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Ahimanikya Satapathy
 */
public class TestComplexTableView extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestComplexTableView(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestComplexTableView.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private Database _db = null;
    protected Table table1 = null;
    protected Table table2 = null;
    protected Table table3 = null;
    protected Table table4 = null;
    protected Table view = null;

    protected Table createTable(String name) throws Exception {
        Table t = new MemoryTable(name);
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
        cmd.setSubQuery("select t1.* from foo1 t1 left outer join foo2 t2 on t1.id = t2.id " +
                " right outer join foo3 t3 on t2.id = t3.id inner join foo4 t4 on t1.id = t4.id");
        cmd.execute(_db);
        return _db.getTable(getViewName());
    }

    public void setUp() throws Exception {
        _db = new MemoryDatabase("testdb");
        table1 = createTable("FOO1");
        _db.addTable(table1);
        table2 = createTable("FOO2");
        _db.addTable(table2);
        table3 = createTable("FOO3");
        _db.addTable(table3);
        table4 = createTable("FOO4");
        _db.addTable(table4);

        view = createView(getViewName());
        super.setUp();
    }

    public void tearDown() throws Exception {
        table1.shutdown();
        table1 = null;
        table2.shutdown();
        table2 = null;
        table3.shutdown();
        table3 = null;
        table4.shutdown();
        table4 = null;
        view.shutdown();
        view = null;
        _db.shutdown();
        super.tearDown();
    }

    protected Table getView() {
        return view;
    }

    protected String getViewName() {
        return "FOOVIEW";
    }
    
    //------------------------------------------------------------------- Tests

    public void testGetName() throws Exception {
        assertEquals(getViewName().toUpperCase(),view.getName());
    }

    public void testToString() throws Exception {
        assertNotNull(view.getName());
    }
    
    public void testGetMatchingRowsForNull() throws Exception {
        RowIterator iter = view.getMatchingRows(null,null, true);
        assertNotNull(iter);
    }
    
    public void testHasColumn() throws Exception {
        ColumnIdentifier id = new ColumnIdentifier("FOO");
        assertTrue("Should not have column", !view.hasColumn(id));
        try {
            view.getColumnIndex("FOO");
            fail("Expected AxionException");
        } catch(Exception e) {
            // expected
        }
        
        id = new ColumnIdentifier("ID");
        assertTrue("Should have column", view.hasColumn(id));
        
        id.setTableIdentifier(new TableIdentifier(getViewName()));
        assertTrue("Should have column", view.hasColumn(id));
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
        assertEquals("ID",view.getColumn(0).getName());
        assertEquals("NAME",view.getColumn(1).getName());
    }

    public void testGetColumnByIndexBadIndex() throws Exception {
        try {
            view.getColumn(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }

        try {
            view.getColumn(2);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }
    }
    
    public void testGetColumnByName() throws Exception {
        assertTrue(view.getColumn("ID").getDataType() instanceof IntegerType);
        assertTrue(view.getColumn("NAME").getDataType() instanceof CharacterVaryingType);
    }

    public void testGetColumnByNameBadName() throws Exception {
        assertNull(table1.getColumn("FOO"));
    }

    public void testViewDrop() throws Exception {
        _db.dropTable(getViewName());
        try {
            _db.dropTable(getViewName());
            fail("Expected Exception");
        } catch(AxionException e) {
            // expected
        }
    }
    

    public void AmbiguousColumnNameTest() throws Exception {
        CreateViewCommand cmd = new CreateViewCommand();
        cmd.setObjectName("BADVIEW");
        cmd.setSubQuery("select * from foo1 t1 left outer join foo2 t2 on t1.id = t2.id " +
            " right outer join foo3 t3 on t2.id = t3.id inner join t4 on t1.id = t4.id");
        try {
            cmd.execute(_db);
            fail("Expected Exception for AmbiguousColumn");
        } catch (Exception ex) {
            // expected
        }
    }

}