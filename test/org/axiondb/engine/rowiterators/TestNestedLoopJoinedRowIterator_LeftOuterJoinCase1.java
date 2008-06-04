/*
 * $Id: TestNestedLoopJoinedRowIterator_LeftOuterJoinCase1.java,v 1.1 2007/11/28 10:01:26 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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
import java.util.Map;
import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.DataType;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.VariableContext;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BooleanType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:26 $
 * @author Chris Johnston
 */
public class TestNestedLoopJoinedRowIterator_LeftOuterJoinCase1 extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestNestedLoopJoinedRowIterator_LeftOuterJoinCase1(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestNestedLoopJoinedRowIterator_LeftOuterJoinCase1.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private ArrayList _listA = null;
    private ArrayList _listB = null;
    private NestedLoopJoinedRowIterator _double = null;

    public void setUp() throws Exception {
        Selectable where = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                Row row = drow.getRow();
                boolean result = (((Integer) (row.get(0))).intValue() == ((Integer) (row.get(1))).intValue());
                return (result ? Boolean.TRUE : Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };

        RowDecorator decorator = new RowDecorator((Map) null);

        _listA = new ArrayList();
        for (int i = 0; i < 6; i++) {
            if (i != 3) {
                Row row = new SimpleRow(1);
                row.set(0, new Integer(i));
                _listA.add(row);
            }
        }
        RowIterator left = new ListRowIterator(_listA);

        _listB = new ArrayList();
        for (int i = 0; i < 7; i++) {
            if ((i != 1) && (i != 5)) {
                Row row = new SimpleRow(1);
                row.set(0, new Integer(i));
                _listB.add(row);
            }
        }
        RowIterator right = new ListRowIterator(_listB);

        _double = new NestedLoopJoinedRowIterator(left, right, 1, true, false);
        _double.setJoinCondition(where, decorator);
    }

    public void tearDown() {
        _listA = null;
        _listB = null;
        _double = null;
    }

    //------------------------------------------------------------------- Tests

    public void testDouble() throws Exception {
        nextPrevFromStart(_double);
        _double.last();
        prevNextFromEnd(_double);
    }

    public void testStickyTail() throws Exception {
        _double.last();
        assertTrue(!_double.hasNext());
        try {
            _double.next();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_double.hasNext());
        try {
            _double.next();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_double.hasNext());
    }

    public void testStickyHead() throws Exception {
        assertTrue(!_double.hasPrevious());
        try {
            _double.previous();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_double.hasPrevious());
        try {
            _double.previous();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_double.hasPrevious());
    }

    public void testWalkForward() throws Exception {
        assertTrue(_double.hasNext());
        assertTrue(!_double.hasPrevious());

        for (int i = 0; i < _listA.size(); i++) {

            assertTrue(_double.hasNext());
            Row row = _double.next();

            switch (i) {
                case 0:
                case 2:
                case 3:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    //assertEquals(((Row)_listB.get(i)).get(0),row.get(1));
                    assertEquals(row.get(0), row.get(1));
                    break;
                case 1:
                case 4:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertTrue(!((Row) _listB.get(i)).get(0).equals(row.get(1)));
                    assertTrue(!row.get(0).equals(row.get(1)));
                    assertEquals(null, row.get(1));
                    break;
                default:
                    assertTrue(false);
            }
        }
        assertTrue(!_double.hasNext());
        assertTrue(_double.hasPrevious());
    }

    public void testWalkForwardBackForward() throws Exception {

        _double.first();
        for (int i = 0; i < _listA.size(); i++) {

            assertTrue(_double.hasNext());
            Row row = _double.next();

            switch (i) {
                case 0:
                case 2:
                case 3:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    //assertEquals(((Row)_listB.get(i)).get(0),row.get(1));
                    assertEquals(row.get(0), row.get(1));
                    break;
                case 1:
                case 4:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertTrue(!((Row) _listB.get(i)).get(0).equals(row.get(1)));
                    assertTrue(!row.get(0).equals(row.get(1)));
                    assertEquals(null, row.get(1));
                    break;
                default:
                    assertTrue(false);
            }
        }

        assertTrue(!_double.hasNext());
        _double.first();

        for (int i = 0; i < _listA.size(); i++) {

            assertTrue(_double.hasNext());
            Row row = _double.next();

            switch (i) {
                case 0:
                case 2:
                case 3:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    //assertEquals(((Row)_listB.get(i)).get(0),row.get(1));
                    assertEquals(row.get(0), row.get(1));
                    break;
                case 1:
                case 4:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertTrue(!((Row) _listB.get(i)).get(0).equals(row.get(1)));
                    assertTrue(!row.get(0).equals(row.get(1)));
                    assertEquals(null, row.get(1));
                    break;
                default:
                    assertTrue(false);
            }
        }

        assertTrue(!_double.hasNext());

    }

    public void testWalkBackward() throws Exception {

        _double.last();
        for (int i = _listA.size() - 1; i >= 0; i--) {

            assertTrue(_double.hasPrevious());
            Row row = _double.previous();

            switch (i) {
                case 0:
                case 2:
                case 3:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    //assertEquals(((Row)_listB.get(i)).get(0),row.get(1));
                    assertEquals(row.get(0), row.get(1));
                    break;
                case 1:
                case 4:
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertTrue(!((Row) _listB.get(i)).get(0).equals(row.get(1)));
                    assertTrue(!row.get(0).equals(row.get(1)));
                    assertEquals(null, row.get(1));
                    break;
                default:
                    assertTrue(false);
            }
        }
    }

    private void nextPrevFromStart(RowIterator iter) throws Exception {
        assertTrue(iter.hasNext());
        assertTrue(!iter.hasPrevious());

        Row a = iter.next();
        assertNotNull(a);
        assertEquals(a, iter.current());

        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());

        Row b = iter.previous();
        assertNotNull(b);
        assertEquals(b, iter.current());
        assertEquals(a, b);

        assertTrue(iter.hasNext());
        assertTrue(!iter.hasPrevious());
    }

    private void prevNextFromEnd(RowIterator iter) throws Exception {
        assertTrue(!iter.hasNext());
        assertTrue(iter.hasPrevious());

        Row a = iter.previous();
        assertNotNull(a);
        assertEquals(a, iter.current());

        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());

        Row b = iter.next();
        assertNotNull(b);
        assertEquals(b, iter.current());
        assertEquals(a, b);

        assertTrue(!iter.hasNext());
        assertTrue(iter.hasPrevious());
    }

}

