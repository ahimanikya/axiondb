/*
 * $Id: TestRowIteratorRowDecoratorIterator.java,v 1.1 2007/11/28 10:01:26 jawed Exp $
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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:26 $
 * @author Ahimanikya Satapathy
 */
public class TestRowIteratorRowDecoratorIterator extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestRowIteratorRowDecoratorIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestRowIteratorRowDecoratorIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    //---------------------------------------------------------- Abstract Impls

    public RowIteratorRowDecoratorIterator makeRowIterator() {
        RowIterator source = new ListIteratorRowIterator(makeRowList().listIterator());
        return new RowIteratorRowDecoratorIterator(source, new RowDecorator(new HashMap()));
    }

    public List makeRowList() {
        ArrayList list = new ArrayList();
        for (int i = 0; i < 10; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            list.add(row);
        }
        return list;
    }

    //------------------------------------------------------------------- Tests

    public void testAdd() throws Exception {
        RowIteratorRowDecoratorIterator rows = makeRowIterator();
        for (Iterator iter = makeRowList().iterator(); iter.hasNext();) {
            rows.add((Row) iter.next());
        }
        for (Iterator iter = makeRowList().iterator(); iter.hasNext();) {
            assertTrue(rows.hasNext());
            assertEquals(iter.next(), rows.next().getRow());
        }
        assertTrue(!rows.hasNext());
        rows.first();
        for (int i = 0; i < 2; i++) {
            for (Iterator iter = makeRowList().iterator(); iter.hasNext();) {
                assertTrue(rows.hasNext());
                assertEquals(iter.next(), rows.next().getRow());
            }
        }
        assertTrue(!rows.hasNext());
    }

    public void testBeforeIterationBegins() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        assertTrue(!iterator.hasCurrent());
        assertEquals(0, iterator.nextIndex());
        assertEquals(-1, iterator.previousIndex());
    }

    public void testToString() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        assertNotNull(iterator.toString());
    }

    public void testNextAfterLast() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        try {
            iterator.next();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
        try {
            iterator.next();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    public void testPreviousBeforeFirst() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        while (iterator.hasPrevious()) {
            iterator.previous();
        }
        try {
            iterator.previous();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
        try {
            iterator.previous();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    public void testReset() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int j = 0; j < 2; j++) {
            assertTrue(!iterator.hasCurrent());
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next().getRow());
                assertTrue(iterator.hasCurrent());
            }
            iterator.reset();
        }
    }

    public void testForwardBackForward() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next().getRow());

            assertEquals(i, iterator.previousIndex());
            assertEquals(i + 1, iterator.nextIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous().getRow());

            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next().getRow());
        }
    }

    public void testWalkForward() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next().getRow());
        }
    }

    public void testBackForwardBack() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        while (iterator.hasNext()) {
            iterator.next();
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous().getRow());

            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next().getRow());

            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous().getRow());
        }
    }

    public void testWalkBackward() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        while (iterator.hasNext()) {
            iterator.next();
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous().getRow());
        }
    }

    public void testFirst() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward a bit
            for (int i = 0; i < list.size() / 2; i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next().getRow());
            }

            // now jump back to the start
            assertEquals(list.get(0), iterator.first().getRow());
            assertEquals(0, iterator.nextIndex());
            assertEquals(-1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertTrue(!iterator.hasPrevious());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(0), iterator.first().getRow());
        } else {
            try {
                iterator.first();
                fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
                // expected
            }
        }
    }

    public void testLast() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward a bit
            for (int i = 0; i < list.size() / 2; i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next().getRow());
            }

            // now jump to the end
            assertEquals(list.get(list.size() - 1), iterator.last().getRow());
            assertEquals(list.size(), iterator.nextIndex());
            assertEquals(list.size() - 1, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertTrue(!iterator.hasNext());
            assertTrue(iterator.hasPrevious());

            assertEquals(list.get(list.size() - 1), iterator.last().getRow());
        } else {
            try {
                iterator.last();
                fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
                // expected
            }
        }
    }

    public void testCurrent() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        try {
            iterator.current();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
        if (iterator.hasNext()) {
            RowDecorator temp = iterator.next();
            assertEquals(temp, iterator.current());
            assertEquals(iterator.current(), iterator.current());
            if (iterator.hasNext()) {
                temp = iterator.next();
                assertEquals(temp, iterator.current());
                assertEquals(iterator.current(), iterator.current());
                temp = iterator.previous();
                assertEquals(temp, iterator.current());
                assertEquals(iterator.current(), iterator.current());
            }
            temp = iterator.previous();
            assertEquals(temp, iterator.current());
            assertEquals(iterator.current(), iterator.current());
        }
    }

    public void testForwardFirstPreviousForward() throws Exception {
        RowIteratorRowDecoratorIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next().getRow());
            }

            // jump to first
            assertEquals(list.get(0), iterator.first().getRow());
            assertEquals(0, iterator.nextIndex());
            assertEquals(-1, iterator.previousIndex());
            assertTrue(!iterator.hasPrevious());
            assertTrue(iterator.hasNext());

            // walk forward
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next().getRow());
            }
        }
    }

    public void testAddSetRemove() throws Exception {
        RowIteratorRowDecoratorIterator rows = makeRowIterator();
        rows.last();
        int nextIndex = rows.nextIndex();

        SimpleRow row = new SimpleRow(1);
        row.set(0, "A");
        rows.add(row);
        assertEquals(rows.last().getRow(), row);

        assertEquals(nextIndex + 1, rows.nextIndex());

        row = new SimpleRow(1);
        row.set(0, "AA");
        rows.set(row);

        assertEquals(rows.last().getRow(), row);
        rows.remove();
        assertEquals(nextIndex, rows.nextIndex());
    }

    public void testGetDecorator() throws Exception {
        RowIteratorRowDecoratorIterator rows = makeRowIterator();
        assertNotNull(rows.getDecorator());
        assertNotNull(rows.getIterator());
    }
}