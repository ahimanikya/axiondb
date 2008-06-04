/*
 * $Id: TestVarBinaryType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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

package org.axiondb.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 */
public class TestVarBinaryType extends BaseDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestVarBinaryType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestVarBinaryType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
        _type = new VarBinaryType(SIZE);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _type = null;
    }

    //------------------------------------------------------------------- Super
    
    protected DataType getDataType() {
        return _type;
    }

    //------------------------------------------------------------------- Tests

    public void testGetPrecision() throws Exception {
        assertEquals(SIZE, getDataType().getPrecision());
        assertEquals(10, getDataType().getPrecisionRadix());
    }
    
    public void testAccepts() throws Exception {
        // currently VarBinaryType accepts everything, because it drops back to toString().getBytes()
        assertTrue("Should accept byte[]", _type.accepts("the quick brown fox jumped over the lazy dogs".getBytes()));
        assertTrue("Should accept String", _type.accepts("the quick brown fox jumped over the lazy dogs"));
        assertTrue("accepts null", _type.accepts(null));
    }

    public void testConvert() throws Exception {
        assertNull(_type.convert(null));
        assertTrue(Arrays.equals("The quick brown fox".getBytes(),(byte[])_type.convert("The quick brown fox")));
        assertTrue(Arrays.equals("17".getBytes(),(byte[])_type.convert(new Integer(17))));
        assertTrue(Arrays.equals(new byte[] { (byte)1, (byte)-1 },(byte[])_type.convert(new byte[] { (byte)1, (byte)-1 })));
    }

    public void testWriteReadNonNull() throws Exception {
        byte[] orig = "The quick brown fox jumped over the lazy dogs.".getBytes();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        _type.write(orig, new DataOutputStream(buf));
        Object read = _type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));
        assertTrue(Arrays.equals(orig,(byte[])read));        
    }

    public void testCompare() throws Exception {
        byte[][] list = new byte[][] {
            new byte[0],
            new byte[] { Byte.MIN_VALUE },
            new byte[] { Byte.MIN_VALUE, Byte.MIN_VALUE },
            new byte[] { Byte.MIN_VALUE, -1 },
            new byte[] { Byte.MIN_VALUE, 0 },
            new byte[] { Byte.MIN_VALUE, 1 },
            new byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE },
            new byte[] { -1 },
            new byte[] { -1, Byte.MIN_VALUE },
            new byte[] { -1, -1 },
            new byte[] { -1, 0 },
            new byte[] { -1, 1 },
            new byte[] { -1, Byte.MAX_VALUE },
            new byte[] { 0 },
            new byte[] { 0, Byte.MIN_VALUE },
            new byte[] { 0, -1 },
            new byte[] { 0, 0 },
            new byte[] { 0, 1 },
            new byte[] { 0, Byte.MAX_VALUE },
            new byte[] { 1 },
            new byte[] { 1, Byte.MIN_VALUE },
            new byte[] { 1, -1 },
            new byte[] { 1, 0 },
            new byte[] { 1, 1 },
            new byte[] { 1, Byte.MAX_VALUE },
            new byte[] { Byte.MAX_VALUE },
            new byte[] { Byte.MAX_VALUE, Byte.MIN_VALUE },
            new byte[] { Byte.MAX_VALUE, -1 },
            new byte[] { Byte.MAX_VALUE, 0 },
            new byte[] { Byte.MAX_VALUE, 1 },
            new byte[] { Byte.MAX_VALUE, Byte.MAX_VALUE },
        };
        for(int i=0;i<list.length;i++) {
            for(int j=0;j<list.length;j++) {
                if(i==j) {
                    assertEquals(tos(list[i]) + " = " + tos(list[j]),0,_type.compare(list[i],list[j]));
                } else if(i<j) {
                    assertTrue(tos(list[i]) + " < " + tos(list[j]),_type.compare(list[i],list[j]) < 0);
                } else if(i>j) {
                    assertTrue(tos(list[i]) + " > " + tos(list[j]),_type.compare(list[i],list[j]) > 0);
                }
            }
        }
    }
    
    public void testSuccessor() throws Exception {
        {
            byte[] v = new byte[] { (byte)0 };
            for(int i=0;i<10;i++) {
                assertEquals(i,v[0]);
                byte[] w = (byte[])_type.successor(v);
                assertTrue(_type.compare(v,w) < 0);
                assertEquals(i+1,w[0]);
                v = w;
            }
        }
        {
            byte[] v = new byte[] { (byte)0, (byte)0 };
            for(int i=0;i<10;i++) {
                assertEquals(0,v[0]);
                assertEquals(i,v[1]);
                byte[] w = (byte[])_type.successor(v);
                assertTrue(_type.compare(v,w) < 0);
                assertEquals(0,w[0]);
                assertEquals(i+1,w[1]);
                v = w;
            }
        }

        {
            byte[] v = new byte[] { (byte)0, Byte.MAX_VALUE };
            byte[] expected = new byte[] { (byte)0, Byte.MAX_VALUE, Byte.MIN_VALUE };
            Arrays.equals(expected, (byte[])_type.successor(v));
            assertTrue(_type.compare(v,expected) < 0);
        }

        Arrays.equals(new byte[] { Byte.MIN_VALUE }, (byte[])_type.successor(new byte[0]));
    }
    
    public void testSupportsSuccessor() throws Exception {
        assertTrue(_type.supportsSuccessor());
    }
    
    public void testRangeCheck() throws Exception {
        byte[] badArray = new byte[SIZE + 1];
        for (int i = 0; i < badArray.length; i++) {
            badArray[i] = (byte) (i % 10);
        }
        
        final String msg = "Expected AxionException (22001) - string data, right truncation";
        try {
            _type.convert(badArray);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22001", expected.getSQLState());
        }
    }
    
    public void testWriteReadSeveral() throws Exception {
        Object[] data = {
            null,
            new byte[0],
            null,
            "quick".getBytes(),
            new byte[10],
            new byte[] { (byte)1, (byte)2, (byte)-1 },
            "brown ".getBytes(),
            " fox".getBytes(),
            null
        };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(int i=0;i<data.length;i++) {
            _type.write(data[i],new DataOutputStream(out));
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for(int i=0;i<data.length;i++) {
            Object read = _type.read(in);        
            if(null == data[i]) {
                assertNull(read);
            } else {
                Arrays.equals((byte[])data[i],(byte[])read);
            }
        }
    }
    
    public void testInvalidWriteFails() throws Exception {
        byte[] badArray = new byte[SIZE + 1];
        for (int i = 0; i < badArray.length; i++) {
            badArray[i] = (byte) (i % 10);
        }
        
        assertInvalidWriteFails(badArray);
    }

    public void testToString() throws Exception {
        assertEquals("the quick brown fox",_type.toString("the quick brown fox"));
        assertEquals("the quick brown fox",_type.toString("the quick brown fox".getBytes()));
    }
    
    public void testGetColumnDisplaySize() {
        assertEquals(Integer.MAX_VALUE, _type.getColumnDisplaySize());
    }    
    
    private String tos(byte[] val) {
        StringBuffer buf = new StringBuffer();
        buf.append("[");
        for(int i = 0; i < val.length; i++) {
            if(i != 0) {
                buf.append(",");
            }
            buf.append(i);
        }
        buf.append("]");
        return buf.toString();
    }
    
    private static final int SIZE = 46;
    private DataType _type = null;
}

