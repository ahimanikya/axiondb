/*
 * $Id: TestIntArrayIndex.java,v 1.1 2007/11/28 10:01:24 jawed Exp $
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

package org.axiondb.engine.indexes;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Column;
import org.axiondb.Index;
import org.axiondb.engine.indexes.IntArrayIndex;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanFunction;
import org.axiondb.functions.GreaterThanOrEqualFunction;
import org.axiondb.functions.IsNotNullFunction;
import org.axiondb.functions.IsNullFunction;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.functions.LessThanOrEqualFunction;
import org.axiondb.functions.NotEqualFunction;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:24 $
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 */
public class TestIntArrayIndex extends AbstractIntIndexTest {

    //------------------------------------------------------------ Conventional

    public TestIntArrayIndex(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestIntArrayIndex.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestIntArrayIndex.class);
    }

    //--------------------------------------------------------------- Lifecycle
        
    private Column _column = null;
    
    public void setUp() throws Exception {
        super.setUp();
        _column = new Column("val",new IntegerType());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _column = null;
    }

    //------------------------------------------------------------------- Tests

    public void testFindRequired() throws Exception {
        IntArrayIndex index = new IntArrayIndex("MYINDEX",_column,true);
        findRequired(index);
    }

    public void testFindRequiredNonUnique() throws Exception {
        IntArrayIndex index = new IntArrayIndex("MYINDEX",_column,false);
        findRequired(index);
    }

    public void testFind() throws Exception {
        IntArrayIndex index = new IntArrayIndex("MYINDEX",_column,true);
        find(index);
    }

    public void testFindNonUnique() throws Exception {
        IntArrayIndex index = new IntArrayIndex("MYINDEX",_column,false);
        find(index);
    }

    public void testSupportsFunction() throws Exception {
        IntArrayIndex index = new IntArrayIndex("testindex",_column,true);
        assertTrue(index.supportsFunction(new EqualFunction()));
        assertTrue(index.supportsFunction(new GreaterThanFunction()));
        assertTrue(index.supportsFunction(new GreaterThanOrEqualFunction()));
        assertTrue(! index.supportsFunction(new IsNotNullFunction()));
        assertTrue(! index.supportsFunction(new IsNullFunction()));
        assertTrue(index.supportsFunction(new LessThanFunction()));
        assertTrue(index.supportsFunction(new LessThanOrEqualFunction()));
        assertTrue(! index.supportsFunction(new NotEqualFunction()));
    }
    
    public void testRemoveKey() throws Exception {
        IntArrayIndex index = new IntArrayIndex("testindex",_column,true);
        index.insertKey(10);
        assertEquals(0,index.find(10,true));
        index.removeKey(10);
        assertEquals(-1,index.find(10,true));
        index.removeKey(10);
        assertEquals(-1,index.find(10,true));
    }
    
    public void testTruncate() throws Exception {
        IntArrayIndex index = new IntArrayIndex("MYINDEX",_column,true);
        
        index.insertKey(10);
        index.truncate();
        assertEquals(-1, index.find(10, true));
    }    

    private void findRequired(IntArrayIndex index) throws Exception {
        assertEquals(-1,index.find(0,true));
        assertEquals(-1,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(-1,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(-1,index.find(5,true));
        assertEquals(-1,index.find(6,true));
        index.insertKey(5);
        assertEquals(-1,index.find(0,true));
        assertEquals(-1,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(-1,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(0,index.find(5,true));
        assertEquals(-1,index.find(6,true));
        index.insertKey(1);
        assertEquals(-1,index.find(0,true));
        assertEquals(0,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(-1,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(1,index.find(5,true));
        assertEquals(-1,index.find(6,true));
        index.insertKey(3);
        assertEquals(-1,index.find(0,true));
        assertEquals(0,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(1,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(2,index.find(5,true));
        assertEquals(-1,index.find(6,true));
        index.insertKey(6);
        assertEquals(-1,index.find(0,true));
        assertEquals(0,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(1,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(2,index.find(5,true));
        assertEquals(3,index.find(6,true));
        index.insertKey(0);
        assertEquals(0,index.find(0,true));
        assertEquals(1,index.find(1,true));
        assertEquals(-1,index.find(2,true));
        assertEquals(2,index.find(3,true));
        assertEquals(-1,index.find(4,true));
        assertEquals(3,index.find(5,true));
        assertEquals(4,index.find(6,true));
    }

   
    private void find(IntArrayIndex index) throws Exception {
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(0,index.find(1,false));
        assertEquals(0,index.find(2,false));
        assertEquals(0,index.find(3,false));
        assertEquals(0,index.find(4,false));
        assertEquals(0,index.find(5,false));
        assertEquals(0,index.find(6,false));
        assertEquals(0,index.find(7,false));
        index.insertKey(5);
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(0,index.find(1,false));
        assertEquals(0,index.find(2,false));
        assertEquals(0,index.find(3,false));
        assertEquals(0,index.find(4,false));
        assertEquals(0,index.find(5,false));
        assertEquals(1,index.find(6,false));
        assertEquals(1,index.find(7,false));
        index.insertKey(1);
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(0,index.find(1,false));
        assertEquals(1,index.find(2,false));
        assertEquals(1,index.find(3,false));
        assertEquals(1,index.find(4,false));
        assertEquals(1,index.find(5,false));
        assertEquals(2,index.find(6,false));
        assertEquals(2,index.find(7,false));
        index.insertKey(3);
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(0,index.find(1,false));
        assertEquals(1,index.find(2,false));
        assertEquals(1,index.find(3,false));
        assertEquals(2,index.find(4,false));
        assertEquals(2,index.find(5,false));
        assertEquals(3,index.find(6,false));
        assertEquals(3,index.find(7,false));
        index.insertKey(6);
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(0,index.find(1,false));
        assertEquals(1,index.find(2,false));
        assertEquals(1,index.find(3,false));
        assertEquals(2,index.find(4,false));
        assertEquals(2,index.find(5,false));
        assertEquals(3,index.find(6,false));
        assertEquals(4,index.find(7,false));
        index.insertKey(0);
        assertEquals(0,index.find(-1,false));
        assertEquals(0,index.find(0,false));
        assertEquals(1,index.find(1,false));
        assertEquals(2,index.find(2,false));
        assertEquals(2,index.find(3,false));
        assertEquals(3,index.find(4,false));
        assertEquals(3,index.find(5,false));
        assertEquals(4,index.find(6,false));
        assertEquals(5,index.find(7,false));
    }

    //========================================================= TEST FRAMEWORK

    protected Index createIndex(String name, Column col, boolean unique) {
        return new IntArrayIndex(name, col, unique);
    }
}
