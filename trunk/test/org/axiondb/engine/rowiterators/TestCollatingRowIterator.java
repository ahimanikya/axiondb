/*
 * $Id: TestCollatingRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Row;
import org.axiondb.RowComparator;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class TestCollatingRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestCollatingRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestCollatingRowIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    public RowIterator makeRowIterator() {
        List list = makeRowList();
        ArrayList a = new ArrayList();
        ArrayList b = new ArrayList();
        a.add(list.get(0));
        b.add(list.get(1));
        a.add(list.get(2));
        b.add(list.get(3));
        b.add(list.get(4));
        a.add(list.get(5));
        b.add(list.get(6));
        a.add(list.get(7));
        b.add(list.get(8));
        a.add(list.get(9));

        HashMap map = new HashMap();
        map.put(new ColumnIdentifier("foo"), new Integer(0));
        RowDecorator decorator = new RowDecorator(map);
        RowComparator comparator = new RowComparator(new ColumnIdentifier(null, "foo", null, new IntegerType()), decorator);
        CollatingRowIterator iterator = new CollatingRowIterator(comparator);
        iterator.addRowIterator(new ListIteratorRowIterator(a.listIterator()));
        iterator.addRowIterator(new ListIteratorRowIterator(b.listIterator()));
        return iterator;
    }

    public List makeRowList() {
        return makeRowList(10);
    }
    
    protected int getSize() {
        return 10;
    }

    private List makeRowList(int size) {
        ArrayList list = new ArrayList();
        for (int i = 0; i < size; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            list.add(row);
        }
        return list;
    }

    //------------------------------------------------------------------- Tests

    public void testRemove() throws Exception {
        RowIterator iterator = makeRowIterator();
        for (int i = 0; i < 5; i++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
        }

        for (int i = 5; i < 10; i++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            iterator.remove();
        }

        List list = makeRowList(5);
        iterator.reset();
        for (int i = 0; i < list.size(); i++) {
            assertTrue(iterator.hasNext());
            assertEquals(list.get(i), iterator.next());
        }
    }

    public void testSet() throws Exception {
        RowIterator iterator = makeRowIterator();
        List list = makeRowList();
        for (int i = 0; i < list.size(); i++) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i + 2));
            iterator.set(row);
        }

        iterator.reset();
        for (int i = 0; i < list.size(); i++) {
            assertTrue(iterator.hasNext());
            Row row = iterator.next();
            assertEquals(new Integer(i + 2), row.get(0));
        }
    }

    public void testNegative() throws Exception {
        RowIterator iterator = makeRowIterator();
        try {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(12));
            iterator.set(row);
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            iterator.remove();
            fail("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // expected
        }

        iterator.hasNext();
        try {
            ((CollatingRowIterator) iterator).addRowIterator(null);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
    }
}

