/*
 * $Id: TestIntIteratorIntListIterator.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.util;

import java.util.NoSuchElementException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.IntListIterator;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Rodney Waldhoff
 */
public class TestIntIteratorIntListIterator extends TestCase {

    //------------------------------------------------------------ Conventional
        
    public TestIntIteratorIntListIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestIntIteratorIntListIterator.class);
    }

    //------------------------------------------------------------------- Tests        

    public void testEmpty() throws Exception {
        IntListIterator iter = new IntIteratorIntListIterator((new ArrayIntList()).iterator());
        for(int i=0;i<3;i++) {
            assertTrue(!iter.hasNext());
            assertEquals(0,iter.nextIndex());
            assertTrue(!iter.hasPrevious());
            assertEquals(-1,iter.previousIndex());
            try {
                iter.next();
                fail("Expected NoSuchElementException"); 
            } catch(NoSuchElementException e) {
                // expected
            }
            try {
                iter.previous();
                fail("Expected NoSuchElementException"); 
            } catch(NoSuchElementException e) {
                // expected
            }
        }
    }

    public void testEndToEndWalk() throws Exception {
        IntList list = new ArrayIntList();
        for(int i=0;i<10;i++) {
            list.add(i);
        }
        IntListIterator iter = new IntIteratorIntListIterator(list.iterator());

        for(int j=0;j<3;j++) {        
            assertTrue(! iter.hasPrevious());
        
            for(int i=0;i<10;i++) {
                assertEquals(i,iter.nextIndex());
                assertEquals(i-1,iter.previousIndex());
                assertTrue(iter.hasNext());
                assertEquals(i,iter.next());
                assertEquals(i+1,iter.nextIndex());
                assertEquals(i,iter.previousIndex());
                assertTrue(iter.hasPrevious());
            }

            assertTrue(! iter.hasNext());

            for(int i=9;i>=0;i--) {
                assertEquals(i+1,iter.nextIndex());
                assertEquals(i,iter.previousIndex());
                assertTrue(iter.hasPrevious());
                assertEquals(i,iter.previous());
                assertEquals(i,iter.nextIndex());
                assertEquals(i-1,iter.previousIndex());
                assertTrue(iter.hasNext());
            }

            assertTrue(! iter.hasPrevious());
        }
    }

    public void testSillyWalk() throws Exception {
        IntList list = new ArrayIntList();
        for(int i=0;i<10;i++) {
            list.add(i);
        }
        IntListIterator iter = new IntIteratorIntListIterator(list.iterator());

        for(int k=0;k<3;k++) {        
            assertTrue(! iter.hasPrevious());
        
            for(int j=0;j<10;j++) {
                for(int i=0;i<j;i++) {
                    assertEquals(i,iter.nextIndex());
                    assertEquals(i-1,iter.previousIndex());
                    assertTrue(iter.hasNext());
                    assertEquals(i,iter.next());
                    assertEquals(i+1,iter.nextIndex());
                    assertEquals(i,iter.previousIndex());
                    assertTrue(iter.hasPrevious());
                }
                for(int i=(j-1);i>=0;i--) {
                    assertEquals(i+1,iter.nextIndex());
                    assertEquals(i,iter.previousIndex());
                    assertTrue(iter.hasPrevious());
                    assertEquals(i,iter.previous());
                    assertEquals(i,iter.nextIndex());
                    assertEquals(i-1,iter.previousIndex());
                    assertTrue(iter.hasNext());
                }
            }

            assertTrue(! iter.hasPrevious());

            for(int i=0;i<10;i++) {
                assertEquals(i,iter.nextIndex());
                assertEquals(i-1,iter.previousIndex());
                assertTrue(iter.hasNext());
                assertEquals(i,iter.next());
                assertEquals(i+1,iter.nextIndex());
                assertEquals(i,iter.previousIndex());
                assertTrue(iter.hasPrevious());
            }

            assertTrue(! iter.hasNext());

            for(int j=9;j>=0;j--) {
                for(int i=j;i>=0;i--) {
                    assertEquals(i+1,iter.nextIndex());
                    assertEquals(i,iter.previousIndex());
                    assertTrue(iter.hasPrevious());
                    assertEquals(i,iter.previous());
                    assertEquals(i,iter.nextIndex());
                    assertEquals(i-1,iter.previousIndex());
                    assertTrue(iter.hasNext());
                }
                for(int i=0;i<j;i++) {
                    assertEquals(k+","+j+","+i,i,iter.nextIndex());
                    assertEquals(i-1,iter.previousIndex());
                    assertTrue(iter.hasNext());
                    assertEquals(i,iter.next());
                    assertEquals(i+1,iter.nextIndex());
                    assertEquals(i,iter.previousIndex());
                    assertTrue(iter.hasPrevious());
                }
            }

            assertTrue(! iter.hasPrevious());
        }
    }
    
    public void testNotModifiable() throws Exception {
        IntList list = new ArrayIntList();
        list.add(1); list.add(2); list.add(3);
        IntListIterator iter = new IntIteratorIntListIterator(list.iterator());

        assertTrue(iter.hasNext());
        assertEquals(1,iter.next());
        
        try {
            iter.add(4);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }

        try {
            iter.remove();
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }

        try {
            iter.set(4);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }
}
