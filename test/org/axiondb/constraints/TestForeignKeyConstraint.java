/*
 * $Id: TestForeignKeyConstraint.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
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

package org.axiondb.constraints;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Rodney Waldhoff
 */
public class TestForeignKeyConstraint extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestForeignKeyConstraint(String testName) {
        super(testName);        
    }
    
    public static Test suite() {
        return new TestSuite(TestForeignKeyConstraint.class);
    }

    //---------------------------------------------------------- TestConstraint

    protected Constraint createConstraint() {
        return new ForeignKeyConstraint(null);
    }
    
    protected Constraint createConstraint(String name) {
        return new ForeignKeyConstraint(name);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        _ftable = new MemoryTable("A");
        _ftable.addColumn(new Column("ID",new IntegerType()));
        _ftable.addRow(new SimpleRow(new Object[] {new Integer(1)}));
        _ftable.addRow(new SimpleRow(new Object[] {new Integer(2)}));
        
        _table = new MemoryTable("B");
        _table.addColumn(new Column("ID",new IntegerType()));
        _table.addColumn(new Column("FID",new IntegerType()));
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _table = null;
        _ftable = null;
    }
    
    protected SimpleRow createRow(int id, int fid) {
        SimpleRow row = new SimpleRow(2);
        row.set(0, new Integer(id));
        row.set(1, new Integer(fid));
        return row;
    }
    
    private Table _table = null;
    private Table _ftable = null;

    //------------------------------------------------------------------- Tests

    public void testCheckWithNoNewRow() throws Exception {
        ForeignKeyConstraint constraint = new ForeignKeyConstraint("C1");
        
        List list = new ArrayList(1);
        list.add(new ColumnIdentifier(new TableIdentifier("B"), "FID", null, new IntegerType()));
        constraint.addColumns(list);

        constraint.setParentTable(_ftable);
        constraint.setChildTable(_table);
        constraint.setParentTableName(_ftable.getName());
        constraint.setChildTableName(_table.getName());
        
        List list2 = new ArrayList(1);
        list2.add(new ColumnIdentifier(new TableIdentifier("A"), "ID", null, new IntegerType()));
        constraint.addForeignColumns(list2);

        SimpleRow row = createRow(1, 1);
        RowEvent event = new RowInsertedEvent(_table, row, null);
        assertTrue(constraint.evaluate(event));
        
        event = new RowInsertedEvent(_table, row, row);
        assertTrue(constraint.evaluate(event));
        
        row = createRow(1, 2);
        event = new RowInsertedEvent(_table, row, row);
        assertTrue(constraint.evaluate(event));
        
        row = createRow(1, 5);
        event = new RowInsertedEvent(_table, row, row);
        assertFalse(constraint.evaluate(event));
        
    }
    
    public void testGeneratedName() throws Exception {
        Constraint constraint = createConstraint();
        assertNotNull(constraint.getName());
        assertEquals(constraint.getName().toUpperCase(),constraint.getName());
        Constraint constraint2 = createConstraint();
        assertNotNull(constraint2.getName());
        assertEquals(constraint2.getName().toUpperCase(),constraint2.getName());
        assertTrue(!constraint.getName().equals(constraint2.getName()));
    }

    public void testNameIsUpperCase() throws Exception {
        Constraint constraint = createConstraint("FOO");
        assertEquals("FOO",constraint.getName());
    }

    public void testGetType() throws Exception {
        Constraint constraint = createConstraint("FOO");
        assertNotNull(constraint.getType());
    }

}

