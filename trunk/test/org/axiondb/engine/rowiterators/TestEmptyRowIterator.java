/*
 * $Id: TestEmptyRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

package org.axiondb.engine.rowiterators;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 */
public class TestEmptyRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestEmptyRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestEmptyRowIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    public RowIterator makeRowIterator() {
        return new EmptyRowIterator();
    }

    public List makeRowList() {
        return new ArrayList();
    }
    
    protected int getSize() {
        return 0;
    }


    //------------------------------------------------------------------- Tests
    
    public void testEmpty() throws Exception {
        RowIterator iter = new EmptyRowIterator();
        
        for(int i=0;i<3;i++) {
            assertTrue(! iter.hasCurrent() );

            try {
                iter.first();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }
        
            try {
                iter.last();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }
        
            try {
                iter.next();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }
            
            try {
                iter.next(1);
                fail("Expected UnsupportedOperationException");            
            } catch(UnsupportedOperationException e) {
                // expected
            }
        
            try {
                iter.previous();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }
            
            try {
                iter.previous(1);
                fail("Expected UnsupportedOperationException");            
            } catch(UnsupportedOperationException e) {
                // expected
            }

            assertTrue(! iter.hasNext() );
            assertTrue(! iter.hasPrevious() );
            assertTrue( iter.isEmpty() );

            try {
                iter.add(null);
                fail("Expected UnsupportedOperationException");            
            } catch(UnsupportedOperationException e) {
                // expected
            }

            assertEquals(0, iter.nextIndex() );
            assertEquals(-1, iter.previousIndex() );

            assertEquals(-1, iter.currentIndex() );

            try {
                iter.remove();
                fail("Expected UnsupportedOperationException");            
            } catch(UnsupportedOperationException e) {
                // expected
            }

            try {
                iter.set(null);
                fail("Expected UnsupportedOperationException");            
            } catch(UnsupportedOperationException e) {
                // expected
            }

            try {
                iter.peekNext();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }

            try {
                iter.peekPrevious();
                fail("Expected NoSuchElementException");            
            } catch(NoSuchElementException e) {
                // expected
            }
            
            iter.reset();
        }
    }
    
    public void testNextAfterLast() throws Exception {
    }
    
    public void testPreviousBeforeFirst() throws Exception {
    }
    
    public void testPeekNextAfterLast() throws Exception {
    }
    
    public void testNextAndPrevious() throws Exception {
    }
    
    public void testPeekPreviousBeforeFirst() throws Exception {
    }
    
    public void testLazyNextprevious() throws Exception {
    }
    
    public void testBase() throws Exception {
        RowIterator rows = new BaseRowIterator() {
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
                return false;
            }

            public boolean hasPrevious() {
                return false;
            }

            public Row next() throws NoSuchElementException, AxionException {
                return null;
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

        try {
            rows.add(new SimpleRow(1));
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            rows.remove();
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            rows.set(new SimpleRow(1));
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
}