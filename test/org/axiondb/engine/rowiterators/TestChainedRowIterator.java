/*
 * $Id: TestChainedRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 */
public class TestChainedRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestChainedRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestChainedRowIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    public RowIterator makeRowIterator() {
        ChainedRowIterator iterator = new ChainedRowIterator();
        List list = makeRowList();

        List list1 = new ArrayList();
        for(int i=0;i<5;i++) {
            list1.add(list.get(i));
        }
        iterator.addRowIterator(new ListIteratorRowIterator(list1.listIterator()));
        
        List list2 = new ArrayList();
        for(int i=5;i<10;i++) {
            list2.add(list.get(i));
        }
        iterator.addRowIterator(new ListIteratorRowIterator(list2.listIterator()));
        
        return iterator;
    }
    
    protected int getSize() {
        return 10;
    }

    public List makeRowList() {
        ArrayList list = new ArrayList();
        for(int i=0;i<10;i++) {
            Row row = new SimpleRow(1);
            row.set(0,new Integer(i));
            list.add(row);
        }
        return list;
    }

    //------------------------------------------------------------------- Tests
    
    public void testSetRemove() throws Exception {
        RowIterator rows = makeRowIterator();
        rows.next();
        int nextIndex = rows.nextIndex();

        Row row = new SimpleRow(1);
        row.set(0, new Integer(20));
        rows.set(row);

        assertEquals(rows.current(), row);
        rows.remove();
        assertEquals(nextIndex - 1 , rows.nextIndex());

        try {
            rows.set(row);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {

        }

        try {
            rows.remove();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {

        }
    }
}

