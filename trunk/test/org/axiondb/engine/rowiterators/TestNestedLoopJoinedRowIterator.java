/*
 * $Id: TestNestedLoopJoinedRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 * @author Amrish Lal
 */
public class TestNestedLoopJoinedRowIterator extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestNestedLoopJoinedRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestNestedLoopJoinedRowIterator.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private ArrayList _listA = null;
    private ArrayList _listB = null;
    private ArrayList _listC = null;
    private NestedLoopJoinedRowIterator _single = null;
    private NestedLoopJoinedRowIterator _double = null;
    private NestedLoopJoinedRowIterator _triple = null;

    public void setUp() throws Exception {

        _listA = new ArrayList();
        for (int i = 0; i < 2; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            _listA.add(row);
        }
        RowIterator A = new ListRowIterator(_listA);

        _listB = new ArrayList();
        for (int i = 0; i < 2; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            _listB.add(row);
        }

        RowIterator B = new ListRowIterator(_listB);

        _listC = new ArrayList();
        {
            Row row = new SimpleRow(1);
            row.set(0, "A");
            _listC.add(row);
        }
        {
            Row row = new SimpleRow(1);
            row.set(0, "B");
            _listC.add(row);
        }
        {
            Row row = new SimpleRow(1);
            row.set(0, "C");
            _listC.add(row);
        }
        RowIterator C = new ListRowIterator(_listC);

        _single = new NestedLoopJoinedRowIterator(A, new SingleRowIterator(new SimpleRow(0)), 0);
        _double = new NestedLoopJoinedRowIterator(A, B, 1);
        _triple = new NestedLoopJoinedRowIterator(_double, C, 1);

    }

    public void tearDown() {
        _listA = null;
        _listB = null;
        _listC = null;
        _single = null;
        _double = null;
        _triple = null;
    }

    //------------------------------------------------------------------- Tests

    public void testSingle() throws Exception {
        nextPrevFromStart(_single);
        _single.last();
        prevNextFromEnd(_single);
    }

    public void testDouble() throws Exception {
        nextPrevFromStart(_double);
        _double.last();
        prevNextFromEnd(_double);
    }

    public void testTriple() throws Exception {
        nextPrevFromStart(_triple);
        _triple.last();
        prevNextFromEnd(_triple);
    }

    public void testStickyTail() throws Exception {
        _triple.last();
        assertTrue(!_triple.hasNext());
        try {
            _triple.next();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_triple.hasNext());
        try {
            _triple.next();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_triple.hasNext());
    }

    public void testStickyHead() throws Exception {
        assertTrue(!_triple.hasPrevious());
        try {
            _triple.previous();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_triple.hasPrevious());
        try {
            _triple.previous();
            fail("Expected no such element exception");
        } catch (NoSuchElementException e) {
            // ignored
        }
        assertTrue(!_triple.hasPrevious());
    }

    public void testWalkForward() throws Exception {
        assertTrue(_triple.hasNext());
        assertTrue(!_triple.hasPrevious());
        for (int i = 0; i < _listA.size(); i++) {
            for (int j = 0; j < _listB.size(); j++) {
                for (int k = 0; k < _listC.size(); k++) {
                    assertTrue(_triple.hasNext());
                    Row row = _triple.next();
                    assertNotNull(row);
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertEquals(((Row) _listB.get(j)).get(0), row.get(1));
                    assertEquals(((Row) _listC.get(k)).get(0), row.get(2));
                }
            }
        }
        assertTrue(!_triple.hasNext());
        assertTrue(_triple.hasPrevious());
    }

    public void testWalkForwardBackForward() throws Exception {
        for (int i = 0; i < _listA.size(); i++) {
            for (int j = 0; j < _listB.size(); j++) {
                for (int k = 0; k < _listC.size(); k++) {
                    assertTrue(_triple.hasNext());
                    Row row = _triple.next();
                    assertNotNull(row);
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertEquals(((Row) _listB.get(j)).get(0), row.get(1));
                    assertEquals(((Row) _listC.get(k)).get(0), row.get(2));
                }
            }
        }

        assertTrue(!_triple.hasNext());

        _triple.first();

        for (int i = 0; i < _listA.size(); i++) {
            for (int j = 0; j < _listB.size(); j++) {
                for (int k = 0; k < _listC.size(); k++) {
                    assertTrue(_triple.hasNext());
                    Row row = _triple.next();
                    assertNotNull(row);
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertEquals(((Row) _listB.get(j)).get(0), row.get(1));
                    assertEquals(((Row) _listC.get(k)).get(0), row.get(2));
                }
            }
        }

        assertTrue(!_triple.hasNext());

    }

    public void testWalkBackward() throws Exception {
        _triple.last();
        for (int i = _listA.size() - 1; i >= 0; i--) {
            for (int j = _listB.size() - 1; j >= 0; j--) {
                for (int k = _listC.size() - 1; k >= 0; k--) {
                    assertTrue(_triple.hasPrevious());
                    Row row = _triple.previous();
                    assertNotNull(row);
                    assertEquals(((Row) _listA.get(i)).get(0), row.get(0));
                    assertEquals(((Row) _listB.get(j)).get(0), row.get(1));
                    assertEquals(((Row) _listC.get(k)).get(0), row.get(2));
                }
            }
        }
    }

    public void testBug() throws Exception {
        Row brow = new SimpleRow(1);
        brow.set(0, "X");
        List b = new ArrayList();
        b.add(brow);

        List a = new ArrayList();
        List join = new ArrayList();
        for (int i = 0; i < 3; i++) {
            Row arow = new SimpleRow(1);
            arow.set(0, new Integer(i));
            a.add(arow);

            JoinedRow jrow = new JoinedRow();
            jrow.addRow(arow);
            jrow.addRow(brow);
            join.add(jrow);
        }

        ListRowIterator expected = new ListRowIterator(join);

        RowIterator A = new ListRowIterator(a);
        RowIterator B = new ListRowIterator(b);
        NestedLoopJoinedRowIterator testing = new NestedLoopJoinedRowIterator(A, B, 1);

        assertEquals(expected.next(), testing.next());
        assertEquals(expected.next(), testing.next());
        assertEquals(expected.previous(), testing.previous());
        assertEquals(expected.previous(), testing.previous());
    }

    public void testBug2() throws Exception {
        Row brow = new SimpleRow(1);
        brow.set(0, "X");
        List b = new ArrayList();
        b.add(brow);

        List a = new ArrayList();
        List join = new ArrayList();
        for (int i = 0; i < 3; i++) {
            Row arow = new SimpleRow(1);
            arow.set(0, new Integer(i));
            a.add(arow);

            JoinedRow jrow = new JoinedRow();
            jrow.addRow(arow);
            jrow.addRow(brow);
            join.add(jrow);
        }

        ListRowIterator expected = new ListRowIterator(join);
        RowIterator A = new ListRowIterator(a);
        RowIterator B = new ListRowIterator(b);
        NestedLoopJoinedRowIterator testing = new NestedLoopJoinedRowIterator(A, B, 1);

        assertEquals(expected.next(), testing.next());
        assertEquals(expected.next(), testing.next());
        assertEquals(expected.next(), testing.next());
        assertEquals(expected.previous(), testing.previous());
        assertEquals(expected.previous(), testing.previous());
        assertEquals(expected.next(), testing.next());
        assertEquals(expected.next(), testing.next());
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
    
    public void testNegative() throws Exception {
        
        RowIterator A = new BaseRowIterator() {
            public Row current() throws NoSuchElementException {
                return null;
            }

            public int currentIndex() throws NoSuchElementException {
                return 0;
            }

            public boolean hasCurrent() {
                return false;
            }

            public boolean hasNext() {
                return true;
            }

            public boolean hasPrevious() {
                return false;
            }

            public Row next() throws NoSuchElementException, AxionException {
                throw new AxionException("Test bad next");
            }

            public int nextIndex() {
                return 0;
            }

            public Row previous() throws NoSuchElementException, AxionException {
                return null;
            }

            public int previousIndex() {
                return 0;
            }

            public void reset() throws AxionException {
            }
        };
        
        
        Row brow = new SimpleRow(1);
        brow.set(0, "X");
        List b = new ArrayList();
        b.add(brow);
        RowIterator B = new ListRowIterator(b);
        
        NestedLoopJoinedRowIterator testing = new NestedLoopJoinedRowIterator(A, B, 1);

        try{
            testing.next();
            fail("Excepted Exception for bad next");
        } catch(Exception e) {
            // expected
        }
        
    }

}

