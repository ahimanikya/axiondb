/*
 * $Id: TestJoinedRow.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

import org.axiondb.Row;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class TestJoinedRow extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestJoinedRow(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestJoinedRow.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    //------------------------------------------------------------------- Tests

    public void testOneRow() throws Exception {
        Row row = new SimpleRow(3);
        row.set(0,new Integer(1));
        row.set(1,"one");
        row.set(2,"I");        
        
        JoinedRow join = new JoinedRow();
        join.addRow(row);

        assertEquals(row.size(),join.size());
        assertEquals(row.get(0),join.get(0));
        assertEquals(row.get(1),join.get(1));
        assertEquals(row.get(2),join.get(2));
    }

    public void testGetOutOfBounds() throws Exception {
        Row row = new SimpleRow(3);
        row.set(0,new Integer(1));
        row.set(1,"one");
        row.set(2,"I");        
        
        JoinedRow join = new JoinedRow();
        join.addRow(row);

        try {
            join.get(-1);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }


        try {
            join.get(3);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }
    }

    public void testSetOutOfBounds() throws Exception {
        Row row = new SimpleRow(3);
        row.set(0,new Integer(1));
        row.set(1,"one");
        row.set(2,"I");        
        
        JoinedRow join = new JoinedRow();
        join.addRow(row);

        try {
            join.set(-1,null);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }


        try {
            join.set(3,null);
            fail("Expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {
            // expected
        }
    }

    public void testTwoRows() throws Exception {
        Row one = new SimpleRow(3);
        one.set(0,new Integer(1));
        one.set(1,"one");
        one.set(2,"I");        
        
        Row two = new SimpleRow(3);
        two.set(0,"a");
        two.set(1,"uno");
        two.set(2,"i");        

        JoinedRow join = new JoinedRow();
        join.addRow(one);
        join.addRow(two);

        assertEquals(6,join.size());
        assertEquals(one.get(0),join.get(0));
        assertEquals(one.get(1),join.get(1));
        assertEquals(one.get(2),join.get(2));
        assertEquals(two.get(0),join.get(3));
        assertEquals(two.get(1),join.get(4));
        assertEquals(two.get(2),join.get(5));
    }

    public void testSet() throws Exception {
        Row one = new SimpleRow(3);
        one.set(0,new Integer(1));
        one.set(1,"one");
        one.set(2,"I");        
        
        Row two = new SimpleRow(3);
        two.set(0,"a");
        two.set(1,"uno");
        two.set(2,"i");        

        JoinedRow join = new JoinedRow();
        join.addRow(one);
        join.addRow(two);

        assertEquals(6,join.size());
        assertEquals(one.get(0),join.get(0));
        assertEquals(one.get(1),join.get(1));
        assertEquals(one.get(2),join.get(2));
        assertEquals(two.get(0),join.get(3));
        assertEquals(two.get(1),join.get(4));
        assertEquals(two.get(2),join.get(5));
        
        join.set(0,null);
        assertNull(join.get(0));
        assertNull(one.get(0));

        join.set(3,"xyzzy");
        assertEquals("xyzzy",join.get(3));
        assertEquals("xyzzy",two.get(0));        
    }

}

