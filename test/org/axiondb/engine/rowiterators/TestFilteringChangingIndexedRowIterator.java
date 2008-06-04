/*
 * $Id: TestFilteringChangingIndexedRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

package org.axiondb.engine.rowiterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.indexes.ObjectArrayIndex;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.StringType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Ahimanikya Satapathy
 */
public class TestFilteringChangingIndexedRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestFilteringChangingIndexedRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestFilteringChangingIndexedRowIterator.class);
        return suite;
    }

    private Table _table;
    private Index _index;
    private RowDecorator _decorator;
    private ConcreteFunction _where;

    protected void setUp() throws Exception {
        Column a = new Column("A", StringType.instance());
        _table = new MemoryTable("X");
        _table.addColumn(a);
        _table.addColumn(new Column("B", StringType.instance()));
        _index = new ObjectArrayIndex("Y", a, false);
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "A");
            _table.addRow(row);
            _index.rowInserted(new RowInsertedEvent(_table, null, row));
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "B");
            _table.addRow(row);
            _index.rowInserted(new RowInsertedEvent(_table, null, row));
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "B");
            row.set(1, "A");
            _table.addRow(row);
            _index.rowInserted(new RowInsertedEvent(_table, null, row));
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "B");
            row.set(1, "B");
            _table.addRow(row);
            _index.rowInserted(new RowInsertedEvent(_table, null, row));
        }

        _where = new EqualFunction();
        _where.addArgument(new ColumnIdentifier("A"));
        _where.addArgument(new Literal("A"));

        Map map = new HashMap();
        map.put(new ColumnIdentifier("A"), new Integer(0));
        map.put(new ColumnIdentifier("B"), new Integer(1));

        _decorator = new RowDecorator(map);
    }

    protected RowIterator makeRowIterator() {
        try {
            ChangingIndexedRowIterator iter = new ChangingIndexedRowIterator(_index, _table, new EqualFunction());
            FilteringChangingIndexedRowIterator fiter = new FilteringChangingIndexedRowIterator(iter, _decorator, _where);
            fiter.setIndexKey("A");
            return fiter;
        } catch (AxionException e) {
            return null;
        }
    }
    
    protected int getSize() {
        return 2;
    }

    protected List makeRowList() {
        List list = new ArrayList();
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "B");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "A");
            list.add(row);
        }
        return list;
    }

    //------------------------------------------------------------------- Tests

    public void test() throws Exception {
        RowIterator iter = makeRowIterator();
        assertTrue(iter.hasNext());
        Row row = iter.next();
        assertNotNull(row);
    }

    public void testAddUnsupported() throws Exception {
        RowIterator rows = makeRowIterator();
        SimpleRow row = new SimpleRow(2);
        row.set(0, "A");
        row.set(1, "A");
        try {
            rows.add(row);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
    
    public void testSetRemove() throws Exception {
        RowIterator rows = makeRowIterator();
        rows.next();
        int nextIndex = rows.nextIndex();

        Row row = new SimpleRow(2);
        row.set(0, "A");
        row.set(1, "A");
        rows.set(row);

        assertEquals(rows.current(), row);
        rows.remove();
        assertEquals(nextIndex - 1 , rows.nextIndex());

        try {
            rows.set(row);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {

        }

        try {
            rows.remove();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {

        }
    }

    public void testIndexNotSet() throws Exception {
        FilteringChangingIndexedRowIterator fiter = (FilteringChangingIndexedRowIterator) makeRowIterator();
        assertFalse(fiter.isEmpty());
        fiter.removeIndexKey();
        assertFalse(fiter.hasNext());
        assertFalse(fiter.hasPrevious());
        assertFalse(fiter.hasCurrent());
        assertFalse(fiter.isEmpty());

        fiter.reset();
    }
}