/*
 * $Id: TestAnyType.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.DataTypeFactory;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public class TestAnyType extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestAnyType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestAnyType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        type = new AnyType();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        type = null;
    }

    //------------------------------------------------------------------- Tests

    public void testAccepts() throws Exception {
        // AnyType accepts anything
        assertTrue("Should accept null",type.accepts(null));
        assertTrue("Should accept Byte",type.accepts(new Byte((byte)3)));
        assertTrue("Should accept Short",type.accepts(new Short((short)3)));
        assertTrue("Should accept Integer",type.accepts(new Integer(3)));
        assertTrue("Should accept Long",type.accepts(new Long(3L)));
        assertTrue("Should accept Double",type.accepts(new Double(3.14D)));
        assertTrue("Should accept Float",type.accepts(new Float(3.14F)));
        assertTrue("Should accept Integer String",type.accepts("3"));
        assertTrue("Should accept non-integer String",type.accepts("The quick brown fox."));
        assertTrue("Should Date",type.accepts(new Date(System.currentTimeMillis())));
        assertTrue("Should Timestamp",type.accepts(new Timestamp(System.currentTimeMillis())));
        assertTrue("Should Time",type.accepts(new Time(System.currentTimeMillis())));
    }

    public void testConvert() throws Exception {
        // AnyType.convert just returns the given object
        assertNull(type.convert(null));
        assertEquals(new Byte((byte)17),type.convert(new Byte((byte)17)));
        assertEquals(new Short((short)17),type.convert(new Short((short)17)));
        assertEquals(new Integer(17),type.convert(new Integer(17)));
        assertEquals(new Long(17),type.convert(new Long(17)));
        assertEquals(new Float(17.99),type.convert(new Float(17.99)));
        assertEquals(new Double(17.99),type.convert(new Double(17.99)));
        assertEquals("The quick brown fox.",type.convert("The quick brown fox."));
    }
    
    public void testWrite() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            type.write("foo",new DataOutputStream(buf));
            fail("Exepcted IOException.");
        } catch(IOException e) {
            // expected
        }
    }

    public void testRead() throws Exception {
        try {
            type.read(null);
            fail("Exepcted IOException.");
        } catch(IOException e) {
            // expected
        }
    }

    public void testGetPreferredValueClassName() throws Exception {
        assertEquals("java.lang.String",type.getPreferredValueClassName());
    }

    public void testGetJdbcType() throws Exception {
        assertEquals(java.sql.Types.OTHER,type.getJdbcType());
    }
    
    public void testToNumber() throws Exception {
        assertNull(((AnyType)type).toNumber(null));
        assertEquals(new Integer(3),((AnyType)type).toNumber(new Integer(3)));
        assertEquals(new Integer(3),((AnyType)type).toNumber("3"));
    }

    public void testSuccessor() throws Exception {
        assertTrue(!type.supportsSuccessor());
        try {
            type.successor(new Integer(3));
            fail("Exepcted UnsupportedOperationException.");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }

    public void testToBoolean() throws Exception {
        try {        
            type.toBoolean(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {        
            type.toBoolean(new Integer(17));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {        
            type.toBoolean("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        assertEquals(false,type.toBoolean("false"));
        assertEquals(true,type.toBoolean("true"));
        assertEquals(true,type.toBoolean(Boolean.TRUE));
        assertEquals(false,type.toBoolean(Boolean.FALSE));
    }

    public void testToByte() throws Exception {
        assertEquals((byte)17,type.toByte(new Byte((byte)17)));
        assertEquals((byte)17,type.toByte(new Short((short)17)));
        assertEquals((byte)17,type.toByte(new Integer(17)));
        assertEquals((byte)17,type.toByte(new Long(17)));
        assertEquals((byte)17,type.toByte(new Float(17)));
        assertEquals((byte)17,type.toByte(new Double(17)));
        assertEquals((byte)17,type.toByte("17"));
        try {
            type.toByte(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toByte("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toByte(Boolean.TRUE);
            fail("Exepected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }
    
    public void testToByteArray() throws Exception {
        Byte b17 = new Byte((byte)17);
        assertEquals(new String(String.valueOf(b17).getBytes()), new String(type.toByteArray(b17)));
        Short s17= new Short((short)17);
        assertEquals(new String(String.valueOf(s17).getBytes()),new String(type.toByteArray(s17)));
        Integer i17 = new Integer(17);
        assertEquals(new String(String.valueOf(i17).getBytes()),new String(type.toByteArray(i17)));
        Long l17 = new Long(17);
        assertEquals(new String(String.valueOf(l17).getBytes()),new String(type.toByteArray(l17)));
        Float f17= new Float(17);
        assertEquals(new String(String.valueOf(f17).getBytes()),new String(type.toByteArray(f17)));
        Double d17 = new Double(17);
        
        assertEquals(new String(String.valueOf(d17).getBytes()),new String(type.toByteArray(d17)));        
        assertEquals(new String("17".getBytes()),new String(type.toByteArray("17")));        
        assertEquals(new String("17".getBytes()),new String(type.toByteArray("17".getBytes())));                
        
        try {
            type.toByteArray(null);            
            fail("Exepected AxionException");
        } catch(AxionException e) {
            // expected
        } 
    }

    public void testToDouble() throws Exception {
        assertEquals(17d,type.toDouble(new Byte((byte)17)),0d);
        assertEquals(17d,type.toDouble(new Short((short)17)),0d);
        assertEquals(17d,type.toDouble(new Integer(17)),0d);
        assertEquals(17d,type.toDouble(new Long(17)),0d);
        assertEquals(17.35d,type.toDouble(new Float(17.35f)),0.001d);
        assertEquals(17.35d,type.toDouble(new Double(17.35d)),0.001d);
        assertEquals(17.35d,type.toDouble("17.35"),0d);
        try {
            type.toDouble(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toDouble("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toDouble(Boolean.TRUE);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToFloat() throws Exception {
        assertEquals(17f,type.toFloat(new Byte((byte)17)),0f);
        assertEquals(17f,type.toFloat(new Short((short)17)),0f);
        assertEquals(17f,type.toFloat(new Integer(17)),0f);
        assertEquals(17f,type.toFloat(new Long(17)),0f);
        assertEquals(17.35f,type.toFloat(new Float(17.35)),0f);
        assertEquals(17.35f,type.toFloat(new Double(17.35)),0f);
        assertEquals(17.35f,type.toFloat("17.35"),0f);
        try {
            type.toFloat(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toFloat("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toFloat(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }


    public void testToInt() throws Exception {
        assertEquals(17,type.toInt(new Byte((byte)17)));
        assertEquals(17,type.toInt(new Short((short)17)));
        assertEquals(17,type.toInt(new Integer(17)));
        assertEquals(17,type.toInt(new Long(17)));
        assertEquals(17,type.toInt("17"));
        assertEquals(17,type.toInt(new Float(17.35)));
        assertEquals(17,type.toInt(new Double(17.35)));
        try {
            type.toInt(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toInt("17.35");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toInt("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toInt(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToLong() throws Exception {
        assertEquals(17,type.toLong(new Byte((byte)17)));
        assertEquals(17,type.toLong(new Short((short)17)));
        assertEquals(17,type.toLong(new Integer(17)));
        assertEquals(17,type.toLong(new Long(17)));
        assertEquals(17,type.toLong("17"));
        assertEquals(17,type.toLong(new Float(17.35)));
        assertEquals(17,type.toLong(new Double(17.35)));
        try {
            type.toLong(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toLong("17.35");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toLong("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toLong(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        long millis = System.currentTimeMillis();
        assertEquals(millis,type.toLong(String.valueOf(millis)));
        assertEquals(millis,type.toLong(new Date(millis)));
        assertEquals(millis,type.toLong(new Time(millis)));
        // JDBC 2/3 and/or JDK 1.3/1.4 seem to differ slightly in
        // the Timestamp millisecond resolution (JDK 1.3 gives time to seconds,
        // JDK 1.4 to milliseconds).  Here we'll trim both sides to allow the
        // test to pass in both environments
        long found = type.toLong(new Timestamp(millis));
        assertEquals(millis-(millis%1000L),found-(found%1000L));
    }

    public void testToShort() throws Exception {
        assertEquals(17,type.toShort(new Byte((byte)17)));
        assertEquals(17,type.toShort(new Short((short)17)));
        assertEquals(17,type.toShort(new Integer(17)));
        assertEquals(17,type.toShort(new Long(17)));
        assertEquals(17,type.toShort("17"));
        assertEquals(17,type.toShort(new Float(17.35)));
        assertEquals(17,type.toShort(new Double(17.35)));
        try {
            type.toShort(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toShort("17.35");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toShort("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toShort(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToString() throws Exception {
        assertEquals("17",type.toString(new Byte((byte)17)));
        assertEquals("17",type.toString(new Short((short)17)));
        assertEquals("17",type.toString(new Integer(17)));
        assertEquals("17",type.toString(new Long(17)));
        assertEquals("17",type.toString("17"));
        assertEquals("17.35",type.toString(new Float(17.35)));
        assertEquals("17.35",type.toString(new Double(17.35)));
        assertEquals("17.35",type.toString("17.35"));
        assertNull(type.toString(null));
        assertEquals("foo",type.toString("foo"));
        assertEquals("true",type.toString(new Boolean(true)));
        assertEquals("false",type.toString(new Boolean(false)));
    }

    public void testToDate() throws Exception {
        assertNull(type.toDate(null));
        long millis = System.currentTimeMillis();
        Date date = new Date(millis);
        assertEquals(date,type.toDate(date));
        assertEquals(date,type.toDate(new Long(millis)));
        try {
            type.toDate("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toDate(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToTime() throws Exception {
        assertNull(type.toTime(null));
        long millis = System.currentTimeMillis();
        Time time = new Time(millis);
        assertEquals(time,type.toTime(time));
        assertEquals(time,type.toTime(new Long(millis)));
        try {
            type.toTime("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toTime(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToTimestamp() throws Exception {
        assertNull(type.toTimestamp(null));
        long millis = System.currentTimeMillis();
        Timestamp time = new Timestamp(millis);
        assertEquals(time,type.toTimestamp(time));
        assertEquals(time,type.toTimestamp(new Long(millis)));
        try {
            type.toTimestamp("foo");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toTimestamp(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToClob() throws Exception {
        assertNull(type.toClob(null));
        assertNotNull(type.toClob("foo"));
        assertNotNull(type.toClob(new Integer(17)));
        StringClob expected = new StringClob("xyzzy");
        assertSame(expected,type.toClob(expected)); // wouldn't really have to be the same, but this is easier than a comparision for now        
    }

    public void testToBlob() throws Exception {
        assertNull(type.toBlob(null));
        try {
            type.toBlob(new Integer(17));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toBlob(new Boolean(true));
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        ByteArrayBlob expected = new ByteArrayBlob("xyzzy".getBytes());
        assertSame(expected,type.toBlob(expected)); // wouldn't really have to be the same, but this is easier than a comparision for now        
    }

    public void testMakeNewInstance() throws Exception {
        DataType made = ((DataTypeFactory)type).makeNewInstance();
        assertNotNull(made);
        assertTrue(made instanceof AnyType);
        assertTrue(made != type);
        assertTrue(made != ((DataTypeFactory)type).makeNewInstance());
    }
    
    public void testGetColumnDisplaySize() {
        assertEquals(0, type.getColumnDisplaySize());
    }
}

