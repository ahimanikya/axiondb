/*
 * $Id: TestTransactableTableImpl.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.collections.HashBag;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Function;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.indexes.IntArrayIndex;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Rodney Waldhoff
 */
public class TestTransactableTableImpl extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestTransactableTableImpl(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestTransactableTableImpl.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private Table _table = null;
    private TransactableTableImpl _ttable = null;

    public void setUp() throws Exception {
        super.setUp();
        _table = new MemoryTable("TEST");
        _table.addColumn(new Column("ID", new IntegerType()));
        _table.addColumn(new Column("NAME", new CharacterVaryingType(10)));
        _table.addIndex(new IntArrayIndex("ID_INDEX", _table.getColumn(0), false));
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            _table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            _table.addRow(row);
        }
        _ttable = new TransactableTableImpl(_table);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _ttable.shutdown();
        _table = null;
        _ttable = null;
    }

    //------------------------------------------------------------------- Tests

    public void testSelectFromUnmodifiedIndex() throws Exception {
        Function where = new LessThanFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("TEST"), "ID", null,
            new IntegerType()));
        where.addArgument(new Literal(new Integer(10), new IntegerType()));
        RowIterator iter = _ttable.getIndexedRows(where, true);
        assertNotNull("Should have been able to obtain iterator", iter);
        boolean[] found = new boolean[2];
        for (int i = 0; i < 2; i++) {
            assertTrue("Should have a next row", iter.hasNext());
            Row row = iter.next();
            assertNotNull("Row should not be null", row);
            found[((Number) (row.get(0))).intValue() - 1] = true;
        }
        assertTrue(found[0]);
        assertTrue(found[0]);
    }
    
    public void testGetMatchingRowsForNull() throws Exception {
        RowIterator iter = _ttable.getMatchingRows(null,null, true);
        assertNotNull(iter);
    }
    
    public void testDrop() throws Exception {
        _ttable.drop();
    }

    public void testRemount() throws Exception {
        _ttable.remount(null, true);
    }

    public void testSelectFromIndexAfterInsert() throws Exception {
        // add rows to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(4));
            row.set(1, "four");
            _ttable.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }

        Function where = new LessThanFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("TEST"), "ID", null,
            new IntegerType()));
        where.addArgument(new Literal(new Integer(10), new IntegerType()));

        RowIterator iter = _ttable.getIndexedRows(where, true);
        assertNotNull("Should have been able to obtain iterator", iter);
        HashBag expected = new HashBag();
        HashBag found = new HashBag();
        for (int i = 0; i < 4; i++) {
            assertTrue("Should have a next row", iter.hasNext());
            Row row = iter.next();
            assertNotNull("Row should not be null", row);
            assertTrue("Row should be unique, value " + row.get(0) + " already exists in bag "
                + found, !found.contains(row.get(0)));
            found.add(row.get(0));
            expected.add(new Integer(i + 1));
        }
        assertEquals(expected, found);
    }

    public void testSelectFromIndexAfterDelete() throws Exception {
        {
            RowIterator iter = _ttable.getRowIterator(false);
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            iter.remove();
        }

        Function where = new LessThanFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("TEST"), "ID", null,
            new IntegerType()));
        where.addArgument(new Literal(new Integer(10), new IntegerType()));

        RowIterator iter = _ttable.getIndexedRows(where, true);
        assertNotNull("Should have been able to obtain iterator", iter);
        for (int i = 1; i < 2; i++) {
            assertTrue("Should have a next row", iter.hasNext());
            Row row = iter.next();
            assertNotNull("Row should not be null", row);
            assertEquals(new Integer(i + 1), row.get(0));
        }
    }

    public void testSelectFromIndexAfterUpdate() throws Exception {
        {
            RowIterator iter = _ttable.getRowIterator(false);
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            Row updatedRow = new SimpleRow(row);
            updatedRow.set(0, new Integer(11));
            iter.set(updatedRow);
        }

        Function where = new LessThanFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("TEST"), "ID", null,
            new IntegerType()));
        where.addArgument(new Literal(new Integer(10), new IntegerType()));

        RowIterator iter = _ttable.getIndexedRows(where, true);
        assertNotNull("Should have been able to obtain iterator", iter);
        for (int i = 1; i < 2; i++) {
            assertTrue("Should have a next row", iter.hasNext());
            Row row = iter.next();
            assertNotNull("Row should not be null", row);
            assertEquals(new Integer(i + 1), row.get(0));
        }
    }

    public void testDelegation() throws Exception {
        assertEquals(2, _ttable.getRowCount());

        RowIterator iter = _table.getRowIterator(true);
        assertNotNull(iter);
        RowIterator titer = _ttable.getRowIterator(true);
        assertNotNull(titer);
        for (int i = 0; i < 2; i++) {
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            Row trow = titer.next();
            assertNotNull(trow);
            assertEquals(row, trow);
        }
        assertTrue(!iter.hasNext());
        assertTrue(!titer.hasNext());
    }

    public void testInsertIsolation() throws Exception {
        assertEquals(2, _table.getRowCount());
        assertEquals(2, _ttable.getRowCount());

        // add a row to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }

        int insertedRowId = -1;
        // see that we can find it in the ttable
        {
            assertEquals(3, _ttable.getRowCount());
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundit);

            Row row = _ttable.getRow(insertedRowId);
            assertNotNull(row);
            assertEquals("three", row.get(1));
        }

        // but not in the underlying table
        {
            boolean foundit = false;
            assertEquals(2, _table.getRowCount());
            RowIterator iter = _table.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(!foundit);

            assertNull(_table.getRow(insertedRowId));
        }
    }

    public void testDeleteIsolation() throws Exception {
        assertEquals(2, _table.getRowCount());
        assertEquals(2, _ttable.getRowCount());

        // delete a row from the ttable
        Object deletedValue = null;
        int deletedRowId = -1;
        {
            RowIterator iter = _ttable.getRowIterator(false);
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            deletedValue = row.get(0);
            deletedRowId = row.getIdentifier();
            iter.remove();
        }

        // check that it was deleted from the ttable
        {
            assertEquals(1, _ttable.getRowCount());
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 1; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                assertTrue(!deletedValue.equals(row.get(0)));
            }
            assertTrue(!iter.hasNext());
            assertNull(_ttable.getRow(deletedRowId));
        }

        // but not in the underlying table
        boolean foundval = false;
        {
            assertEquals(2, _table.getRowCount());
            RowIterator iter = _table.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if (deletedValue.equals(row.get(0))) {
                    foundval = true;
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundval);
            assertNotNull(_table.getRow(deletedRowId));
        }
    }

    public void testUpdateIsolation() throws Exception {
        assertEquals(2, _table.getRowCount());
        assertEquals(2, _ttable.getRowCount());

        // update a row from the ttable
        Object keyValue = null;
        Object oldValue = null;
        Object newValue = "updated";
        int updatedRowId = -1;
        {
            RowIterator iter = _ttable.getRowIterator(false);
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            keyValue = row.get(0);
            oldValue = row.get(1);
            updatedRowId = row.getIdentifier();
            Row updatedRow = new SimpleRow(row);
            updatedRow.set(1, newValue);
            iter.set(updatedRow);
        }

        // check that it was updated in the ttable
        {
            assertEquals(2, _ttable.getRowCount());
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if (keyValue.equals(row.get(0))) {
                    assertEquals(newValue, row.get(1));
                }
            }
            assertTrue(!iter.hasNext());

            Row row = _ttable.getRow(updatedRowId);
            assertNotNull(row);
            assertEquals(newValue, row.get(1));
        }

        // but not in the underlying table
        {
            assertEquals(2, _table.getRowCount());
            RowIterator iter = _table.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if (keyValue.equals(row.get(0))) {
                    assertTrue(!newValue.equals(row.get(1)));
                }
            }
            assertTrue(!iter.hasNext());

            Row row = _table.getRow(updatedRowId);
            assertNotNull(row);
            assertEquals(oldValue, row.get(1));
        }
    }

    public void testRowIdsAreFreedOnDelete() throws Exception {
        assertEquals(2, _ttable.getRowCount());
        // add a row to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }
        assertEquals(3, _ttable.getRowCount());

        // find the new row, its row id, and delete it
        int insertedRowId = -1;
        {
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                    iter.remove();
                    break;
                }
            }
            assertTrue(foundit);
        }
        assertEquals(2, _table.getRowCount());
        assertEquals(2, _ttable.getRowCount());

        // add a row to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }

        // confirm that the row id is the same
        {
            assertEquals(3, _ttable.getRowCount());
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    assertEquals(insertedRowId, row.getIdentifier());
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundit);
        }
    }

    public void testRowIdsAreFreedOnRollback() throws Exception {
        assertEquals(2, _ttable.getRowCount());
        // add a row to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }
        assertEquals(3, _ttable.getRowCount());

        // find the new row and its row id
        int insertedRowId = -1;
        {
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                    iter.remove();
                    break;
                }
            }
            assertTrue(foundit);
        }

        // rollback
        _ttable.rollback();

        assertEquals(2, _table.getRowCount());

        // see that the row id was returend to the _table
        assertEquals(insertedRowId, _table.getNextRowId());
    }

    public void testApply() throws Exception {
        // TODO: should add update/delete actions to this also
        assertEquals(2, _table.getRowCount());
        assertEquals(2, _ttable.getRowCount());

        // add a row to the ttable
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(3));
            row.set(1, "three");
            _ttable.addRow(row);
        }

        int insertedRowId = -1;
        // see that we can find it in the ttable
        {
            assertEquals(3, _ttable.getRowCount());
            RowIterator iter = _ttable.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundit);

            Row row = _ttable.getRow(insertedRowId);
            assertNotNull(row);
            assertEquals("three", row.get(1));
        }

        // but not in the underlying table
        {
            boolean foundit = false;
            assertEquals(2, _table.getRowCount());
            RowIterator iter = _table.getRowIterator(true);
            assertNotNull(iter);
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(!foundit);

            assertNull(_table.getRow(insertedRowId));
        }

        // apply the change
        _ttable.commit();
        _ttable.apply();

        // see that we can find it in the table now
        {
            assertEquals(3, _table.getRowCount());
            RowIterator iter = _table.getRowIterator(true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundit);

            Row row = _table.getRow(insertedRowId);
            assertNotNull(row);
            assertEquals("three", row.get(1));
        }

        // see that we can find it in the index also
        {
            Function where = new LessThanFunction();
            where.addArgument(new ColumnIdentifier(new TableIdentifier("TEST"), "ID", null,
                new IntegerType()));
            where.addArgument(new Literal(new Integer(10), new IntegerType()));

            RowIterator iter = _table.getIndexedRows(where, true);
            assertNotNull(iter);
            boolean foundit = false;
            for (int i = 0; i < 3; i++) {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                assertNotNull(row);
                if ("three".equals(row.get(1))) {
                    foundit = true;
                    insertedRowId = row.getIdentifier();
                }
            }
            assertTrue(!iter.hasNext());
            assertTrue(foundit);

            Row row = _table.getRow(insertedRowId);
            assertNotNull(row);
            assertEquals("three", row.get(1));
        }

    }

}