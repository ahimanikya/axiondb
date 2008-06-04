/*
 * $Id: TestIntRowMap.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
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

import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.engine.rowcollection.IntRowMap;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Ahimanikya Satapathy
 */
public class TestIntRowMap extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestIntRowMap(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestIntRowMap.class);
    }

    //------------------------------------------------------------------- Tests

    public void testEmpty() throws Exception {
        IntRowMap map = new IntRowMap();
        RowIterator iter = map.rowValues().rowIterator();
        for (int i = 0; i < 3; i++) {
            assertTrue(!iter.hasNext());
            assertEquals(0, iter.nextIndex());
            assertTrue(!iter.hasPrevious());
            assertEquals(-1, iter.previousIndex());
            try {
                iter.next();
                fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
                // expected
            }
            try {
                iter.previous();
                fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
                // expected
            }
        }
    }
    
    public void testCopy() throws Exception {
        IntRowMap map = new IntRowMap();
        IntRowMap map2 = new IntRowMap();
        for (int i = 0; i < 13; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        map2.putAll(map);
        map.clear();
        map2.putAll(map);
        
        // Test int key iterator
        {
            IntIterator iter =  map2.keyIterator();
            for (int i = 0; i < 13; i++) {
                assertTrue(iter.hasNext());
                assertEquals(i, iter.next());
            }
            assertTrue(!iter.hasNext());
        }
    }
    
    public void testValues() throws Exception {
        IntRowMap map = new IntRowMap();
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        RowCollection values = map.rowValues();
        assertNotNull(values.toString());
        assertEquals(10, values.size());
        
        // Test value iterator
        {
            RowIterator iter = values.rowIterator();
            for (int i = 0; i < 10; i++) {
                assertTrue(iter.hasNext());
                assertEquals(new SimpleRow(new Object[] {new Integer(i)}), iter.next());
            }
            assertTrue(!iter.hasNext());
        }
        
        // Test value list iterator
        {
            RowIterator iter = map.rowValues().rowIterator();
            assertNotNull(iter.toString());
            assertTrue(!iter.hasPrevious());
            for (int i = 0; i < 10; i++) {
                assertEquals(i,iter.nextIndex());
                assertTrue(iter.hasNext());
                assertEquals(new SimpleRow(new Object[] {new Integer(i)}),iter.next());
                assertNotNull(iter.toString());
                assertEquals(i+1,iter.nextIndex());
                assertEquals(i,iter.previousIndex());
                assertTrue(iter.hasPrevious());
            }
            assertTrue(!iter.hasNext());
            
            for (int i = 9; i > 0; i--) {
                assertEquals(i+1,iter.nextIndex());
                assertEquals(i,iter.previousIndex());
                assertTrue(iter.hasPrevious());
                assertEquals(new SimpleRow(new Object[] {new Integer(i)}),iter.previous());
                assertEquals(i,iter.nextIndex());
                assertEquals(i-1,iter.previousIndex());
                assertTrue(iter.hasNext());
            }
            
        }
        
        for (int i = 0; i < 10; i++) {
            assertTrue(values.contains(new SimpleRow(new Object[] {new Integer(i)})));
            assertTrue(values.remove(new SimpleRow(new Object[] {new Integer(i)})));
            assertFalse(values.contains(new SimpleRow(new Object[] {new Integer(i)})));
        }
        assertEquals(0, values.size());
        
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        values = map.rowValues();
        assertEquals(10, values.size());
        values.clear();
        assertEquals(0, values.size());
    }

    public void testIntKeySet() throws Exception {
        IntRowMap map = new IntRowMap();
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        
        // Test int key iterator
        {
            IntIterator iter = map.keyIterator();
            for (int i = 0; i < 10; i++) {
                assertTrue(iter.hasNext());
                assertEquals(i, iter.next());
            }
            assertTrue(!iter.hasNext());
        }
    }
    
    public void testIntEntrySet() throws Exception {
        IntRowMap map = new IntRowMap();
        IntRowMap.Entry[] entries =  new IntRowMap.Entry[10];
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        
        // Test entry iterator
        {
            IntRowMap.EntryIterator iter = map.entryIterator();
            for (int i = 0; i < 10; i++) {
                assertTrue(iter.hasNext());
                entries[i] = iter.nextEntry();
                assertEquals(i, entries[i].getKey());
                assertEquals(new SimpleRow(new Object[] {new Integer(i)}), entries[i].getValue());
                
                assertTrue(entries[i].equals(entries[i]));
                assertFalse(entries[i].equals(entries[(i+1)%9]));
                
                assertTrue(entries[i].hashCode() == entries[i].hashCode());
            }
            assertTrue(!iter.hasNext());
        }
        
        IntRowMap.EntryIterator iter = map.entryIterator();
        while(iter.hasNext()) {
            iter.nextEntry();
            iter.remove();
        }
        assertEquals(0, iter.size());
        
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        iter = map.entryIterator();
        assertEquals(10, iter.size());
        map.clear();
        assertEquals(0, iter.size());
    }
    
    public void testIntHashMap() throws Exception {
        IntRowMap map = new IntRowMap();
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        
        assertEquals(10, map.size());
        
        for (int i = 0; i < 10; i++) {
            assertTrue(map.containsKey(i));
            assertEquals(new SimpleRow(new Object[] {new Integer(i)}), map.remove(i));
            assertFalse(map.containsKey(i));
        }
        assertEquals(0, map.size());
        
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }

        assertEquals(10, map.size());
        map.clear();
        assertEquals(0, map.size());
        
        for (int i = 0; i < 10; i++) {
            map.put(i, new SimpleRow(new Object[] {new Integer(i)}));
        }
        
        for (int i = 0; i < 10; i++) {
            assertTrue(map.containsValue(new SimpleRow(new Object[] {new Integer(i)})));
            assertEquals(new SimpleRow(new Object[] {new Integer(i)}), map.remove(i));
            assertFalse(map.containsValue(new SimpleRow(new Object[] {new Integer(i)})));
        }
        
        assertEquals(0, map.size());
        
    }
}