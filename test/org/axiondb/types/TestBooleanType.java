/*
 * $Id: TestBooleanType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestBooleanType extends BaseDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestBooleanType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBooleanType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        type = new BooleanType();
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

    public void testGetPrecision() throws Exception {
        assertEquals(1,getDataType().getPrecision());
    }
    
    public void testAccepts() throws Exception {
        assertTrue("Should not accept Integer",!type.accepts(new Integer(3)));
        assertTrue("Should accept Boolean.TRUE",type.accepts(Boolean.TRUE));
        assertTrue("Should accept Boolean.FALSE",type.accepts(Boolean.FALSE));
        assertTrue("Should accept \"TRUE\"",type.accepts("TRUE"));
        assertTrue("Should accept \"true\"",type.accepts("true"));
        assertTrue("Should accept \"FALSE\"",type.accepts("FALSE"));
        assertTrue("Should accept \"false\"",type.accepts("false"));
        assertTrue("Should not accept unknown object", !type.accepts(new Object()));
    }

    public void testToBoolean() throws Exception {
        assertEquals(true,type.toBoolean(Boolean.TRUE));
        assertEquals(false,type.toBoolean(Boolean.FALSE));
        
        try {
            type.toBoolean(null);
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // Expected.
        }
        
        try {
            type.toBoolean(new Object());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // Expected.
        }
    }

    public void testWriteReadNonNull() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(Boolean.TRUE,new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(Boolean.TRUE,read);
    }

    public void testWriteReadSeveral() throws Exception {
        Object[] data = {
            null,
            Boolean.TRUE,
            null,
            Boolean.FALSE,
            Boolean.TRUE,
            Boolean.FALSE,
            Boolean.FALSE,
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
                assertEquals(type.convert(data[i]),read);
            }
        }
    }

    public void testReadInvalid() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new DataOutputStream(out).writeByte((byte) 127);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        try {
            type.read(in);
            fail("Expected IOException due to invalid byte input value.");
        } catch (IOException expected) {
            // Expected.
        }
    }
    
    public void testComparator() throws Exception {
        Comparator comp = ((BaseDataType) type).getComparator();
        Object[] ascending = new Object[] { Boolean.FALSE, Boolean.TRUE, null };

        for (int i = 0; i < ascending.length - 1; i++) {
            assertEquals(-1, comp.compare(ascending[i], ascending[i + 1]));
        }

        for (int i = ascending.length - 1; i >= 1; i--) {
            assertEquals(1, comp.compare(ascending[i], ascending[i - 1]));
        }
        
        Object[] itemsToSort = new Object[] { Boolean.TRUE, null, null, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null, Boolean.FALSE };
        final Object[] expected = new Object[] { Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.TRUE, Boolean.TRUE, null, null, null };
        Arrays.sort(itemsToSort, comp);
        
        for (int i = 0; i < itemsToSort.length; i++) {
            assertEquals("Did not find expected item in comparator sorted list", expected[i], itemsToSort[i]);
        }
    }
}

