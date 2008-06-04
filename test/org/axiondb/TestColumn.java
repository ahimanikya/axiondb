/*
 * $Id: TestColumn.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
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

package org.axiondb;

import java.io.Serializable;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Rod Waldhoff
 */
public class TestColumn extends BaseSerializableTest {

    //------------------------------------------------------------ Conventional

    public TestColumn(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestColumn.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //--------------------------------------------------------------- Framework
    
    public Serializable makeSerializable() {
        return new Column("name", new IntegerType());
    }

    //------------------------------------------------------------------- Tests
    
    public void testNPEOnConstructor() {
        try {
            new Column(null,null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
        try {
            new Column("name",null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
        try {
            new Column(null,new IntegerType());
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
        try {
            new Column(null,null,null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
        try {
            new Column("name",null,null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
        try {
            new Column(null,new IntegerType(),null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
    }

    public void testNameTypeConstructor() {
        Column col = new Column("name", new IntegerType());
        assertEquals("name",col.getName());
        assertTrue(col.getDataType() instanceof IntegerType);
    }

    public void testNameTypeDefaultConstructor() {
        Column col = new Column("name", new IntegerType(), new Literal(new Integer(17),new IntegerType()));
        assertEquals("name",col.getName());
        assertTrue(col.getDataType() instanceof IntegerType);
        assertTrue(col.getDefault() instanceof Literal);
    }

    public void testEqualsAndHashCode() {
        Column col1 = new Column("name", new IntegerType());
        Column col2 = new Column("name", new CharacterVaryingType(10));
        Column col3 = new Column("anothername", new IntegerType());
        assertEquals(col1,col1);
        assertEquals(col1,col2);
        assertEquals(col2,col1);
        assertEquals(col1.hashCode(),col2.hashCode());
        
        assertTrue(! col1.equals(col3) );
        assertTrue(col1.hashCode() != col3.hashCode() );
        assertTrue(! col1.equals(null) );
        assertTrue(! col1.equals(new Object()) );
    }
}
