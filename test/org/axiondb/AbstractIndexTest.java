/*
 * $Id: AbstractIndexTest.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.IntegerType;

/**
 * Utility test environment for various types of indices
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Chuck Burdick
 */
public abstract class AbstractIndexTest extends AbstractDbdirTest {
    private Log _log = LogFactory.getLog(this.getClass());
    private Table _table = null;
    private Column _col = null;
    private Index _index = null;
    private int _testCount = 50;

    public AbstractIndexTest(String testName) {
        super(testName);
    }

    public void setUp() throws Exception {
        super.setUp();
        _table = new MemoryTable("FOO");
        _table.addColumn(new Column("ID", new IntegerType()));
        _col = new Column("BAR", getDataType());
        _table.addColumn(_col);
        _index = createIndex("BAR", _col, false);
        _table.addIndex(_index);
    }

    public void tearDown() throws Exception {
        _index = null;
        super.tearDown();
    }

    //============================================================= TEST CASES

    public void testHelpers() throws Exception {
        assertEquals("Objects of same sequential index should report equality", getSequentialValue(101), getSequentialValue(101));
    }

    public void testBasics() throws Exception {
        assertNotNull("Should have a table", getTable());
        assertNotNull("Should have a column", getColumn());
        assertNotNull("Should have an index", getIndex());
        assertEquals("Index should report its name", "BAR", getIndex().getName());
        assertEquals("Table should report the index on the column", getTable().getIndexForColumn(getColumn()), getIndex());
        assertEquals("Index should report the indexed column", getColumn(), getIndex().getIndexedColumn());
    }

    public void testChangeRowId() throws Exception {
        // This test assumes I can call getRowIterator,
        // RowIterator.next and Row.getIdentifier
        // with an identifier that doesn't
        // actually exist in the table.
        // This assumption may not always be valid, at which
        // point this test may need to be reconsidered.

        Row row = new SimpleRow(2);
        row.set(0, new Integer(1));
        row.set(1, getSequentialValue(1));
        getTable().addRow(row);

        {
            RowIterator iter = getIndex().getRowIterator(getTable(), new EqualFunction(), getSequentialValue(1));
            Row result = iter.next();
            assertEquals(row.getIdentifier(), result.getIdentifier());
        }

        getIndex().changeRowId(getTable(), row, row.getIdentifier(), row.getIdentifier() + 1000);

        {
            RowIterator iter = getIndex().getRowIterator(getTable(), new EqualFunction(), getSequentialValue(1));
            Row result = iter.next();
            assertEquals(row.getIdentifier() + 1000, result.getIdentifier());
        }

    }

    public void testSequentialAdds() throws Exception {
        List tests = new ArrayList();
        for (int i = 0; i < _testCount; i++) {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(i));
            Object cur = getSequentialValue(i);
            _log.debug("Inserting value " + cur);
            tests.add(cur);
            row.set(1, cur);
            getTable().addRow(row);
        }

        for (int i = 0; i < _testCount; i++) {
            RowIterator it = getIndex().getRowIterator(getTable(), new EqualFunction(), getSequentialValue(i));
            assertTrue("Should have row for item " + i, it.hasNext());
            Row cur = it.next();
            assertEquals("Returned row id should match", new Integer(i), cur.get(0));
            assertEquals("Returned object should match", tests.get(i), cur.get(1));
        }
    }

    public void testRandomAdds() throws Exception {
        Set tests = new HashSet();
        Object cur = null;
        for (int i = 0; i < _testCount; i++) {
            cur = getRandomValue();
            if (tests.contains(cur)) {
                i--;
            } else {
                Row row = new SimpleRow(2);
                row.set(0, new Integer(i));
                tests.add(cur);
                row.set(1, cur);
                getTable().addRow(row);
            }
        }

        Iterator testIt = tests.iterator();
        while (testIt.hasNext()) {
            cur = testIt.next();
            RowIterator it = getIndex().getRowIterator(getTable(), new EqualFunction(), cur);
            assertTrue("Should have row for item " + cur, it.hasNext());
            Row curRow = it.next();
            assertEquals("Returned object should match", cur, curRow.get(1));
        }
    }

    public void testDeletes() throws Exception {
        testRandomAdds();
        RowIterator rIt = getTable().getRowIterator(false);
        while (rIt.hasNext()) {
            Row curRow = rIt.next();
            Object curVal = curRow.get(1);
            RowIterator it = getIndex().getRowIterator(getTable(), new EqualFunction(), curVal);
            assertTrue("Index should find value", it.hasNext());
            rIt.remove();
            it = getIndex().getRowIterator(getTable(), new EqualFunction(), curVal);
            assertTrue("Index should not find value", !it.hasNext());
        }
    }

    public void testSaveAndLoad() throws Exception {
        testRandomAdds();

        File idxDir = new File(getDbdir(), "BAR");
        idxDir.mkdir();

        getIndex().getIndexLoader().saveIndex(getIndex(), idxDir);

        getTable().removeIndex(getIndex());

        Index loadedIndex = getIndex().getIndexLoader().loadIndex(getTable(), idxDir);

        getTable().addIndex(loadedIndex);

        assertEquals("Index should report its name", "BAR", loadedIndex.getName());

        assertEquals("Table should report the index on the column", loadedIndex, getTable().getIndexForColumn(getColumn()));

        assertEquals("Index should report the indexed column", getColumn(), loadedIndex.getIndexedColumn());

        RowIterator rIt = getTable().getRowIterator(true);
        while (rIt.hasNext()) {
            Row curRow = rIt.next();
            Object curVal = curRow.get(1);
            RowIterator it = loadedIndex.getRowIterator(getTable(), new EqualFunction(), curVal);
            assertTrue("Index should find value", it.hasNext());
        }

        deleteFile(idxDir);
    }

    /**
     * Checks for dealing with NULL properly, was throwing an NPE for BTREE indexes
     */
    public void testNull() throws Exception {
        Row row = new SimpleRow(2);
        row.set(0, null);
        getTable().addRow(row);
        RowIterator rIt = getIndex().getRowIterator(getTable(), new EqualFunction(), null);
        assertTrue("Should get empty row iterator back", rIt.isEmpty());
        rIt = getTable().getRowIterator(false);
        while (rIt.hasNext()) {
            rIt.next();
            rIt.remove();
        }
    }

    //========================================================= TEST FRAMEWORK

    /**
     * Subclasses implement so that framework can perform tests on the target index type
     */
    protected abstract Index createIndex(String name, Column col, boolean unique) throws AxionException;

    /**
     * Subclasses implement to inform test table what type of column should be indexed
     */
    protected abstract DataType getDataType();

    /**
     * Subclasses implement to provide a unique value appropriate for the type in {$link
     * getDataType} for each value <code>i</code>.
     */
    protected abstract Object getSequentialValue(int i);

    /**
     * Subclasses implement to provide a random value appropriate for the type in {$link
     * getDataType}.
     */
    protected abstract Object getRandomValue();

    //=============================================== ACCESSORS FOR SUBCLASSES

    /**
     * Test table with two columns. One column is a numeric ID, the other is of the type
     * provided in {@link #getDataType}
     */
    protected Table getTable() {
        return _table;
    }

    /**
     * Column of type provided in {@link #getDataType}
     */
    protected Column getColumn() {
        return _col;
    }

    /**
     * Target index to be tested
     * 
     * @see #getIndexFactory
     * @see #getIndexLoader
     */
    protected Index getIndex() {
        return _index;
    }
}