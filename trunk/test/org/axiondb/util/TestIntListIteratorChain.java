/*
 * $Id: TestIntListIteratorChain.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
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

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Rodney Waldhoff
 */
public class TestIntListIteratorChain extends TestCase {

    //------------------------------------------------------------ Conventional
        
    public TestIntListIteratorChain(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestIntListIteratorChain.class);
    }

    //--------------------------------------------------------------- Lifecycle
        
    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests        

    public void testEmpty() throws Exception {
        IntListIteratorChain chain = new IntListIteratorChain();

        assertTrue(!chain.hasNext());
        assertEquals(0,chain.nextIndex());
        try {
            chain.next();
            fail("Expected NoSuchElementException");
        } catch(NoSuchElementException e) {
            // expected
        }

        assertTrue(!chain.hasPrevious());
        assertEquals(-1,chain.previousIndex());
        try {
            chain.previous();
            fail("Expected NoSuchElementException");
        } catch(NoSuchElementException e) {
            // expected
        }
        
    }

    public void testCantAddOnceIterating() throws Exception {
        IntListIteratorChain chain = new IntListIteratorChain();
        chain.addIterator(1);
        assertEquals(1,chain.next());
        try {
            chain.addIterator(2);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }

    public void testSingletons() throws Exception {
        IntListIteratorChain chain = new IntListIteratorChain();
        chain.addIterator(1);
        chain.addIterator(2);
        chain.addIterator(3);

        assertTrue(!chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(-1,chain.previousIndex());
        assertEquals(0,chain.nextIndex());
        assertEquals(1,chain.next());
        
        assertTrue(chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(0,chain.previousIndex());
        assertEquals(1,chain.nextIndex());
        assertEquals(2,chain.next());

        assertTrue(chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(1,chain.previousIndex());
        assertEquals(2,chain.nextIndex());
        assertEquals(3,chain.next());

        assertTrue(chain.hasPrevious());
        assertTrue(!chain.hasNext());
        assertEquals(2,chain.previousIndex());
        assertEquals(3,chain.nextIndex());
        assertEquals(3,chain.previous());

        assertTrue(chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(1,chain.previousIndex());
        assertEquals(2,chain.nextIndex());
        assertEquals(2,chain.previous());

        assertTrue(chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(0,chain.previousIndex());
        assertEquals(1,chain.nextIndex());
        assertEquals(1,chain.previous());

        assertTrue(!chain.hasPrevious());
        assertTrue(chain.hasNext());
        assertEquals(-1,chain.previousIndex());
        assertEquals(0,chain.nextIndex());
    }

    public void testNotModifiable() throws Exception {
        IntListIteratorChain chain = new IntListIteratorChain();
        chain.addIterator(1);
        chain.addIterator(2);
        chain.addIterator(3);

        assertTrue(chain.hasNext());
        assertEquals(1,chain.next());
        
        try {
            chain.add(4);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }

        try {
            chain.remove();
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }

        try {
            chain.set(4);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }
}
