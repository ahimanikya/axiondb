/*
 * $Id: TestObjectType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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

package org.axiondb.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.DataType;
import org.axiondb.Person;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author James Strachan
 */
public class TestObjectType extends BaseDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestObjectType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestObjectType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        type = new ObjectType();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        type = null;
    }

    //------------------------------------------------------------------- Super
    
    protected DataType getDataType() {
        return type;
    }

    //------------------------------------------------------------------- Tests

    public void testSupportsSuccessor() throws Exception {
        assertTrue(!type.supportsSuccessor());
    }

    public void testAccepts() throws Exception {
        assertTrue("Should accept Integer",type.accepts(new Integer(3)));
        assertTrue("Should accept String",type.accepts("3.14159"));
        assertTrue("Should accept Person",type.accepts(new Person("James", 1969)));
        assertTrue("Should not accept non-serializable",! type.accepts(new Object()));
    }
    
    public void testConvertUnacceptable() throws Exception {
        try {
            type.convert(new Object());
            fail("Expected IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            // expected
        }
    }
    
    public void testConvertAccetable() throws Exception {
        assertNotNull(type.convert(new Person("James",1969)));
    }
    
    public void testSuccessor() throws Exception {
        try {
            type.successor("xyzzy");
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }
    
    public void testWriteReadNonNull() throws Exception {
        Object orig = new Person("James", 1969);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(orig,new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig,read);
    }

    public void testWriteReadNonNull2() throws Exception {
        Object orig = "testing";
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(buf);               
        type.write(orig,out);        
        out.flush();
        out.close();
        Object read = type.read(new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig,read);
    }

    public void testWriteReadSeveral() throws Exception {
        Object[] data = {
            null,
            new Person("James", 1969),
            null,
            new Person("Simon", 1971),
            new Person("Lisa", 1974),
            null
        };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(int i=0;i<data.length;i++) {
            type.write(data[i],new DataOutputStream(out));
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for(int i=0;i<data.length;i++) {
            Object read = type.read(in);        
            if(null == data[i]) {
                assertNull(read);
            } else {
                assertEquals(data[i],read);
            }
        }
    }
    
    public void testGetColumnDisplaySize() {
        assertEquals(0, type.getColumnDisplaySize());
    }
}

