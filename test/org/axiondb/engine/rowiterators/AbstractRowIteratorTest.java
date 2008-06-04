/*
 * $Id: AbstractRowIteratorTest.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase;

import org.axiondb.Row;
import org.axiondb.RowIterator;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public abstract class AbstractRowIteratorTest extends TestCase {

    //------------------------------------------------------------ Conventional

    public AbstractRowIteratorTest(String testName) {
        super(testName);
    }

    //---------------------------------------------------------------- Abstract

    protected abstract RowIterator makeRowIterator();

    protected abstract List makeRowList();
    
    protected abstract int getSize();

    //------------------------------------------------------------------- Tests

    public void testBeforeIterationBegins() throws Exception {
        RowIterator iterator = makeRowIterator();
        assertTrue(!iterator.hasCurrent());
        assertEquals(-1, iterator.currentIndex());
        assertEquals(0, iterator.nextIndex());
        assertEquals(-1, iterator.previousIndex());
    }

    public void testPeek() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertEquals("" + i, list.get(i), iterator.peekNext());
            assertEquals(list.get(i), iterator.next());
            assertEquals(list.get(i), iterator.peekPrevious());
        }
    }

    public void testNextAfterLast() throws Exception {
        RowIterator iterator = makeRowIterator();
        iterator.last();
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
        RowIterator iterator = makeRowIterator();
        iterator.first();
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
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int j = 0; j < 2; j++) {
            assertTrue(!iterator.hasCurrent());
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next());
                assertTrue(iterator.hasCurrent());
            }
            iterator.reset();
        }
    }

    public void testForwardBackForward() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next());

            assertEquals(i, iterator.previousIndex());
            assertEquals(i + 1, iterator.nextIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous());

            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next());
        }
    }

    public void testWalkForward() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next());
        }
    }

    public void testBackForwardBack() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        while (iterator.hasNext()) {
            iterator.next();
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous());

            assertEquals(i, iterator.nextIndex());
            assertEquals(i - 1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next());

            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous());
        }
    }

    public void testWalkBackward() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        while (iterator.hasNext()) {
            iterator.next();
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            assertEquals(i + 1, iterator.nextIndex());
            assertEquals(i, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertEquals(list.get(i), iterator.previous());
        }
    }

    public void testFirst() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward a bit
            for (int i = 0; i < list.size() / 2; i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next());
            }

            // now jump back to the start
            assertEquals(list.get(0), iterator.first());
            assertEquals(0, iterator.nextIndex());
            assertEquals(-1, iterator.previousIndex());
            assertTrue(iterator.hasNext());
            assertTrue(!iterator.hasPrevious());
            assertTrue(iterator.hasNext());
            assertEquals(list.get(0), iterator.first());
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
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward a bit
            for (int i = 0; i < list.size() / 2; i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next());
            }

            // now jump to the end
            assertEquals(list.get(list.size() - 1), iterator.last());
            assertEquals(list.size(), iterator.nextIndex());
            assertEquals(list.size() - 1, iterator.previousIndex());
            assertTrue(iterator.hasPrevious());
            assertTrue(!iterator.hasNext());
            assertTrue(iterator.hasPrevious());

            assertEquals(list.get(list.size() - 1), iterator.last());
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
        RowIterator iterator = makeRowIterator();
        try {
            iterator.current();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
        if (iterator.hasNext()) {
            Row temp = iterator.next();
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
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        if (list.size() > 0) {
            // walk forward
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next());
            }

            // jump to first
            assertEquals(list.get(0), iterator.first());
            assertEquals(0, iterator.nextIndex());
            assertEquals(-1, iterator.previousIndex());
            assertTrue(!iterator.hasPrevious());
            assertTrue(iterator.hasNext());

            // walk forward
            for (int i = 0; i < list.size(); i++) {
                assertEquals(i, iterator.nextIndex());
                assertEquals(i - 1, iterator.previousIndex());
                assertTrue(iterator.hasNext());
                assertEquals(list.get(i), iterator.next());
            }
        }
    }

    public void testPeekNextAfterLast() throws Exception {
        RowIterator iterator = makeRowIterator();
        iterator.last();
        try {
            iterator.peekNext();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
    }
    
    public void testSize() throws Exception {
        RowIterator iterator = makeRowIterator();
        assertNotNull(iterator.toString());
        assertEquals(getSize(), iterator.size());
    }
    
    public void testPeekPreviousBeforeFirst() throws Exception {
        RowIterator iterator = makeRowIterator();
        iterator.first();
        try {
            iterator.peekPrevious();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    public void testNextAndPrevious() throws Exception {
        RowIterator rows = makeRowIterator();
        rows.first();
        rows.hasPrevious();
        rows.hasPrevious();
        rows.hasPrevious();
        rows.hasNext();
        rows.hasNext();
        rows.hasNext();

        rows.last();
        rows.hasNext();
        rows.hasNext();
        rows.hasNext();
        rows.hasPrevious();
        rows.hasPrevious();
        rows.hasPrevious();
    }
    
    public void testLazyNextprevious() throws Exception {
        RowIterator rows = makeRowIterator();
        rows.next(1);
        assertEquals(1 , rows.nextIndex());
        assertEquals(0 , rows.currentIndex());
        
        rows.next(1);
        assertEquals(2 , rows.nextIndex());
        assertEquals(1 , rows.currentIndex());
        
        rows.previous(1);
        assertEquals(1 , rows.nextIndex());
        assertEquals(1 , rows.currentIndex());
        
        rows.previous(1);
        assertEquals(0 , rows.nextIndex());
        assertEquals(0 , rows.currentIndex());
    }

}

