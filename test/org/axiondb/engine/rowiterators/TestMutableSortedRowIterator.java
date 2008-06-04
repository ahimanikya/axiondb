/*
 * $Id: TestMutableSortedRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003-2005 Axion Development Team.  All rights reserved.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Index;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.BaseTable;
import org.axiondb.types.IntegerType;
import org.axiondb.util.ExceptionConverter;

/**
 * JUnit test case for SortedRowIterator implementation (MutableMergeSort).
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Jonathan Giron
 */
public class TestMutableSortedRowIterator extends AbstractRowIteratorTest {

    protected static final int ROW_SIZE = 100;
    protected static final String ID = "ID";

    //------------------------------------------------------------ Conventional

    public TestMutableSortedRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestMutableSortedRowIterator.class);
        return suite;
    }

    protected RowIterator makeRowIterator() {
        try {
            MockRowSource source = new MockRowSource("TABLE_2345");
            OrderNode node = new OrderNode(new ColumnIdentifier(source.getTableIdentifier(), ID), false);

            ArrayList nodes = new ArrayList();
            nodes.add(node);

            RowIterator rowIter = new SortedRowIterator.MutableMergeSort(source, source.getRowIterator(), nodes, source.makeRowDecorator());
            return rowIter;
        } catch (AxionException e) {
            throw ExceptionConverter.convertToRuntimeException(e);
        }
    }
    
    protected int getSize() {
        return ROW_SIZE;
    }

    protected List makeRowList() {
        List rowList = new ArrayList(ROW_SIZE);
        for (int i = 0; i < ROW_SIZE; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            row.setIdentifier(i);
            rowList.add(row);
        }
        return rowList;
    }

    //------------------------------------------------------------------- Tests
    public void testMergeSort() throws Exception {
        RowIterator rows = makeRowIterator();

        assertFalse(rows.isEmpty());
        assertNotNull(rows.first());
        assertNotNull(rows.next());

        try {
            rows.add(null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        rows.next();
        int nextIndex = rows.nextIndex();

        Row row = new SimpleRow(1);
        row.set(0, new Integer(20));
        rows.set(row);

        assertEquals(rows.current(), row);
        rows.remove();
        assertEquals(nextIndex - 1, rows.nextIndex());

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

    class MockRowSource extends BaseTable implements RowSource {
        public void applyDeletes(IntCollection rowids) throws AxionException {
        }

        public void applyInserts(RowCollection rows) throws AxionException {
        }

        public void applyUpdates(RowCollection rows) throws AxionException {
        }

        public void freeRowId(int id) {
        }

        public int getNextRowId() {
            return 0;
        }

        public int getRowCount() {
            return 0;
        }

        public void populateIndex(Index index) throws AxionException {
        }

        public void truncate() throws AxionException {
        }

        public MockRowSource(String name) throws AxionException{
            super(name);
            addColumn(new Column(ID, new IntegerType()));
            List ids = new ArrayList();
            for (int i = 0; i < ROW_SIZE; i++) {
                ids.add(new Integer(i));
            }

            Collections.shuffle(ids);
            _rows = new ArrayList(ids.size());
            _rowIds = new ArrayIntList(ids.size());

            for (int i = 0; i < ids.size(); i++) {
                Row row = new SimpleRow(1);
                row.set(0, ids.get(i));
                row.setIdentifier(i);

                _rows.add(row);
                _rowIds.add(i);
            }

            _tableIdent = new TableIdentifier(getName());

            _colIdToFieldMap = new HashMap();
            _colIdToFieldMap.put(new ColumnIdentifier(_tableIdent, ID), new Integer(0));
        }

        public Row getRow(int id) {
            return (Row) _rows.get(id);
        }

        public void deleteRow(Row row) throws AxionException {
            _rows.remove(row.getIdentifier());
            _rowIds.removeElementAt(row.getIdentifier());
        }

        public void updateRow(Row oldrow, Row newrow) throws AxionException {
            _rows.add(oldrow.getIdentifier(), newrow);
        }

        public RowDecorator makeRowDecorator() {
            return new RowDecorator(_colIdToFieldMap);
        }

        public int getColumnIndex(String str) {
            return 1;
        }

        public RowIterator getRowIterator() {
            return new LazyRowRowIterator(this, _rowIds.listIterator(), _rowIds.size());
        }

        public TableIdentifier getTableIdentifier() {
            return _tableIdent;
        }

        private List _rows;
        private ArrayIntList _rowIds;
        private Map _colIdToFieldMap;
        private TableIdentifier _tableIdent;
    }
}