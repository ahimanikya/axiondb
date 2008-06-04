/*
 * $Id: TestLazyRow.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

package org.axiondb.engine.rows;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowSource;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Ahimanikya Satapathy
 */
public class TestLazyRow extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestLazyRow(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestLazyRow.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //------------------------------------------------------------------- Tests

    public void testGet() throws Exception {
        Row row = new LazyRow(new MockRowSource(), 0);
        assertEquals(row.size(), 3);
        assertEquals(row.get(0), new Integer(1));
        assertEquals(row.get(1), "one");
        assertEquals(row.get(2), "I");
        
        try {
            LazyRow lazyRow = new LazyRow(new MockRowSource(), 4);
            lazyRow.get(2);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    public void testGetOutOfBounds() throws Exception {
        Row row = new LazyRow(new MockRowSource(), 0, 0, "one");

        try {
            row.get(-1);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }

        try {
            row.get(3);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    public void testSetOutOfBounds() throws Exception {
        Row row = new LazyRow(new MockRowSource(), 1);

        try {
            row.set(-1, null);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }

        try {
            row.set(3, null);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }
    
    
    public void testGetRowReturnsNull() throws Exception {
        MockRowSource source = new MockRowSource();
        Row row = new LazyRow(source, 1);
        source.updateRow(1, null);
        assertNull(row.get(1));
    }

    public void testSet() throws Exception {
        Row row = new LazyRow(new MockRowSource(), 0);

        assertEquals(3, row.size());

        row.set(0, null);
        assertNull(row.get(0));
        assertNull(row.get(0));

        row.set(2, "xyzzy");
        assertEquals("xyzzy", row.get(2));
    }

    class MockRowSource implements RowSource {
        private Row[] rows = new Row[2];
        public MockRowSource() {
            rows[0] = new SimpleRow(3);
            rows[0].set(0, new Integer(1));
            rows[0].set(1, "one");
            rows[0].set(2, "I");

            rows[1] = new SimpleRow(3);
            rows[1].set(0, "a");
            rows[1].set(1, "uno");
            rows[1].set(2, "i");
        }

        public Row getRow(int id) throws AxionException {
            try {
                return rows[id];
            } catch (IndexOutOfBoundsException e) {
                throw new AxionException("Invalid Row Index", e);
            }
        }

        public RowDecorator makeRowDecorator() {
            throw new UnsupportedOperationException("Not Implemented");
        }

        public int getColumnCount() {
            return 3;
        }

        public int getColumnIndex(String name) throws AxionException{
            throw new UnsupportedOperationException("Not Implemented");
        }
        
        void updateRow(int i, Row row) {
            rows[i] = row;
        }
    }

}

