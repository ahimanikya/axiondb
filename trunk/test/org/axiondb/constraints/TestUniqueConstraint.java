/*
 * $Id: TestUniqueConstraint.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
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
import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Row;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Rodney Waldhoff
 */
public class TestUniqueConstraint extends BaseConstraintTest {

    //------------------------------------------------------------ Conventional

    public TestUniqueConstraint(String testName) {
        super(testName);        
    }
    
    public static Test suite() {
        return new TestSuite(TestUniqueConstraint.class);
    }

    //---------------------------------------------------------- TestConstraint

    protected Constraint createConstraint() {
        return new UniqueConstraint(null);
    }
    
    protected Constraint createConstraint(String name) {
        return new UniqueConstraint(name);
    }

    //--------------------------------------------------------------- Lifecycle

    private Table _table = null;
    
    public void setUp() throws Exception {
        super.setUp();
        _table = new MemoryTable("FOO");
        _table.addColumn(new Column("NAME",new CharacterVaryingType(10)));
        _table.addColumn(new Column("NUM",new IntegerType()));
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _table = null;
    }
    
    //-------------------------------------------------------------------- Util
    
    private SimpleRow addRow(Table table, String name, Integer num) throws Exception {
        SimpleRow row = createRow(name,num);
        table.addRow(row); 
        return row;           
    }
               
    private List getNameColIdList() {
        List list = new ArrayList();
        list.add(new ColumnIdentifier(new TableIdentifier("FOO"),"NAME",null,new CharacterVaryingType(10)));
        return list;
    }

    private List getNumColIdList() {
        List list = new ArrayList();
        list.add(new ColumnIdentifier(new TableIdentifier("FOO"),"NUM",null,new IntegerType()));
        return list;
    }

    private List getNameNumColIdList() {
        List list = new ArrayList();
        list.add(new ColumnIdentifier(new TableIdentifier("FOO"),"NAME",null,new CharacterVaryingType(10)));
        list.add(new ColumnIdentifier(new TableIdentifier("FOO"),"NUM",null,new IntegerType()));
        return list;
    }

    private Constraint makeConstraint(String name, List columns) {
        UniqueConstraint c = new UniqueConstraint(name);
        for(Iterator iter = columns.iterator();iter.hasNext(); ) {
            c.addSelectable((Selectable)(iter.next()));
        }
        return c;
    }

    //------------------------------------------------------------------- Tests

    public void testCheckWithNoNewRow() throws Exception {
        Constraint constraint = makeConstraint("C1", getNumColIdList());
        Row row = createRow(null, null);
        RowEvent event = new RowInsertedEvent(_table, row, null);
        assertTrue(constraint.evaluate(event));
    }

    public void testEvaluateInsertAgainstEmptyTable() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        {
            Row row = createRow(null,null);
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
        {
            Row row = createRow("17",new Integer(17));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
    }

    public void testEvaluateUniqueInsert() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        addRow(_table,"3",new Integer(3));
        addRow(_table,null,null);
        {
            Row row = createRow(null,null);
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
        {
            Row row = createRow("17",new Integer(17));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
    }

    public void testEvaluateInsertWhereOnlyPairIsUnique() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        addRow(_table,"17",new Integer(3));
        {
            Row row = createRow("3",new Integer(3));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(!intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
        {
            Row row = createRow("17",new Integer(17));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(!strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
    }

    public void testEvaluateInsertNotUnique() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        addRow(_table,"17",new Integer(3));
        addRow(_table,"3",new Integer(17));
        addRow(_table,null,null);
        {
            Row row = createRow("3",new Integer(3));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(!intconstraint.evaluate(event));
            assertTrue(!strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
        {
            Row row = createRow("17",new Integer(17));
            RowEvent event = new RowInsertedEvent(_table,null,row);
            assertTrue(!intconstraint.evaluate(event));
            assertTrue(!strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
    }
    
    public void testEvaluateUniqueUpdate() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        Row old = addRow(_table,"3",new Integer(3));
        {
            Row row = createRow(null,null);
            row.setIdentifier(old.getIdentifier());
            RowEvent event = new RowInsertedEvent(_table,old,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
        {
            Row row = createRow("17",new Integer(17));
            row.setIdentifier(old.getIdentifier());
            RowEvent event = new RowInsertedEvent(_table,old,row);
            assertTrue(intconstraint.evaluate(event));
            assertTrue(strconstraint.evaluate(event));
            assertTrue(bothconstraint.evaluate(event));
        }
    }
    
    public void testEvaluateUpdateSame() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        Row old = addRow(_table,"3",new Integer(3));
        Row row = createRow("3",new Integer(3));
        row.setIdentifier(old.getIdentifier());
        RowEvent event = new RowInsertedEvent(_table,old,row);
        assertTrue(intconstraint.evaluate(event));
        assertTrue(strconstraint.evaluate(event));
        assertTrue(bothconstraint.evaluate(event));
    }
    
    public void testEvaluateDelete() throws Exception {
        Constraint intconstraint = makeConstraint("C1",getNumColIdList());
        Constraint strconstraint = makeConstraint("C2",getNameColIdList());
        Constraint bothconstraint = makeConstraint("C3",getNameNumColIdList());
        Row old = addRow(_table,"3",new Integer(3));
        RowEvent event = new RowInsertedEvent(_table,old,null);
        assertTrue(intconstraint.evaluate(event));
        assertTrue(strconstraint.evaluate(event));
        assertTrue(bothconstraint.evaluate(event));
    }
}

