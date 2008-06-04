/*
 * $Id: TestBigDecimalType.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public class TestBigDecimalType extends BaseNumericDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestBigDecimalType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBigDecimalType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        type = new BigDecimalType(6, 2);
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
        assertEquals(6, getDataType().getPrecision());
    }

    public void testConvertNonNumericString() throws Exception {
        expectExceptionWhileConvertingNonNumericString("22018");
    }
    
    public void testAccepts() throws Exception {
        assertTrue("Should accept Byte",type.accepts(new Byte((byte)3)));
        assertTrue("Should accept Short",type.accepts(new Short((short)3)));
        assertTrue("Should accept Integer",type.accepts(new Integer(3)));
        assertTrue("Should accept Long",type.accepts(new Long(3L)));
        
        assertTrue("Should accept Double",type.accepts(new Double(3.14D)));
        assertTrue("Should accept Float",type.accepts(new Float(3.14F)));

        assertTrue("Should accept BigInteger",type.accepts(new BigInteger("12345")));
        assertTrue("Should accept BigDecimal",type.accepts(new BigDecimal("12345.00")));
        
        assertTrue("Should accept integer String",type.accepts("3"));
        assertTrue("Should accept non-integer String",type.accepts("3.14"));
    }
    
    public void testConvertLiterals() throws Exception {
        assertEquals(new BigDecimal("17.00"),type.convert("17"));
        assertEquals(new BigDecimal("17.00"),type.convert(" 17 "));
        assertEquals(new BigDecimal("17.90"),type.convert("17.9"));
        assertEquals(new BigDecimal("-17.90"),type.convert("    -17.9 "));
        assertEquals(new BigDecimal("17.99"),type.convert("  17.99           "));
        assertEquals(new BigDecimal("17.99"),type.convert("17.990            "));
        
        try {
            type.convert("abcd");
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22018) - invalid character value for cast",
                "22018", expected.getSQLState());
        }
        
        try {
            type.convert(new Float(Float.NaN));
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22003) - numeric value out of range", 
                "22003", expected.getSQLState());
        }
        
        try {
            type.convert(new Float(-Float.NaN));
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22003) - numeric value out of range", 
                "22003", expected.getSQLState());
        }
        
        try {
            type.convert(new Float(Float.POSITIVE_INFINITY));
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22003) - numeric value out of range", 
                "22003", expected.getSQLState());
        }
        
        try {
            type.convert(new Float(Float.NEGATIVE_INFINITY));
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22003) - numeric value out of range", 
                "22003", expected.getSQLState());
        }
    }
    
    public void testRounding() throws Exception {
        assertEquals(new BigDecimal("17.99"),type.convert("17.9900"));
        assertEquals(new BigDecimal("17.99"),type.convert("17.9910"));
        assertEquals(new BigDecimal("17.99"),type.convert("17.9940"));        
        assertEquals(new BigDecimal("18.00"),type.convert("17.9950"));
        assertEquals(new BigDecimal("18.00"),type.convert("17.9990"));
        assertEquals(new BigDecimal("18.00"),type.convert("18.0000"));
    }
    
    public void testNumericOverflow() throws Exception {
        try {
            type.convert("10000"); // numeric(5,0) - too large for numeric(6,2)
            fail("Expected SQLException (22003) - numeric value out of range");
        } catch (AxionException expected) {
            assertEquals("Expected SQLException (22003) - numeric value out of range", 
                "22003", expected.getSQLState());
        }
    }

    public void testConvertIntegerTypes() throws Exception {
        assertEquals(new BigDecimal("17.00"),type.convert(new Byte((byte)17)));
        assertEquals(new BigDecimal("17.00"),type.convert(new Short((short)17)));
        assertEquals(new BigDecimal("17.00"),type.convert(new Integer(17)));
        assertEquals(new BigDecimal("17.00"),type.convert(new Long(17)));
        assertEquals(new BigDecimal("17.00"),type.convert(new BigInteger("17")));
    }

    public void testConvertDecimalTypes() throws Exception {
        assertEquals(new BigDecimal("17.99"),type.convert(new Float(17.99f)));
        assertEquals(new BigDecimal("17.90"),type.convert(new Float(17.9f)));
        assertEquals(new BigDecimal("17.90"),type.convert(new Float(17.900f)));

        assertEquals(new BigDecimal("17.99"),type.convert(new Double(17.99d)));
        assertEquals(new BigDecimal("17.90"),type.convert(new Double(17.9d)));
        assertEquals(new BigDecimal("17.90"),type.convert(new Double(17.900d)));

        assertEquals(new BigDecimal("17.00"),type.convert(new BigDecimal("17")));
        assertEquals(new BigDecimal("17.90"),type.convert(new BigDecimal("17.9")));
        assertEquals(new BigDecimal("17.99"),type.convert(new BigDecimal("17.99")));
        assertEquals(new BigDecimal("17.99"),type.convert(new BigDecimal("17.990")));
    }

    public void testWriteReadNonNull() throws Exception {
        BigDecimal orig = new BigDecimal("17.36");
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(orig, new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig,read);
    }

    public void testToBigDecimal() throws Exception {
        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new Byte((byte)17)));
        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new Short((short)17)));
        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new Integer(17)));
        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new Long(17)));
        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new BigInteger("17")));

        assertEquals(new BigDecimal("17.99"),type.toBigDecimal(new Float(17.99f)));
        assertEquals(new BigDecimal("17.90"),type.toBigDecimal(new Float(17.9f)));
        assertEquals(new BigDecimal("17.90"),type.toBigDecimal(new Float(17.900f)));

        assertEquals(new BigDecimal("17.99"),type.toBigDecimal(new Double(17.99d)));
        assertEquals(new BigDecimal("17.90"),type.toBigDecimal(new Double(17.9d)));
        assertEquals(new BigDecimal("17.90"),type.toBigDecimal(new Double(17.900d)));

        assertEquals(new BigDecimal("17.00"),type.toBigDecimal(new BigDecimal("17")));
        assertEquals(new BigDecimal("17.90"),type.toBigDecimal(new BigDecimal("17.9")));
        assertEquals(new BigDecimal("17.99"),type.toBigDecimal(new BigDecimal("17.99")));
        assertEquals(new BigDecimal("17.99"),type.toBigDecimal(new BigDecimal("17.990")));
    }

    public void testWriteReadSeveral() throws Exception {
        BigDecimal[] data = {
            new BigDecimal("1.30"),
            new BigDecimal("-1.30"),
            null,
            new BigDecimal("0.00"),
            null,
            null,
            new BigDecimal("17.00"),
            null
        };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0;i < data.length; i++) {
            type.write(data[i],new DataOutputStream(out));
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for (int i = 0;i < data.length; i++) {
            Object read = type.read(in);        
            if (null == data[i]) {
                assertNull(read);
            } else {
                assertEquals(data[i],read);
            }
        }
    }
    
    public void testGetColumnDisplaySize() {
        assertCorrectColumnDisplaySize(2, 2, new BigDecimalType(2,2));
        assertCorrectColumnDisplaySize(4, 0, new BigDecimalType(4,0));
        assertCorrectColumnDisplaySize(10, 3, new BigDecimalType(10, 3));
        
        BigDecimalType defType = new BigDecimalType();
        assertCorrectColumnDisplaySize(defType.getPrecision(), defType.getScale(), defType);
    }
    
    public void testInvalidWriteFails() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            // value exceeds (precision,scale) of (6,2) 
            type.write(new BigDecimal("123456789"), new DataOutputStream(bos));
            fail("Expected IOException due to invalid write data");
        } catch (IOException expected) {
            // expected
        }
    }    
    
    public void testInvalidConvertFails() {
        try {
            type.convert(new Object());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // Expected.
        }
    }
    
    public void testPrecisionScaleOfConvertedValues() throws AxionException {
        BigDecimalType bdType; 

        bdType = new BigDecimalType(new BigDecimal("0"));
        assertEquals(1, bdType.getPrecision());
        assertEquals(0, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal(".0"));
        assertEquals(1, bdType.getPrecision());
        assertEquals(1, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal("0.00"));
        assertEquals(3, bdType.getPrecision());
        assertEquals(2, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal("0.00300000"));
        assertEquals(9, bdType.getPrecision());
        assertEquals(8, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal(".11"));
        assertEquals(2, bdType.getPrecision());
        assertEquals(2, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal("-.11"));
        assertEquals(2, bdType.getPrecision());
        assertEquals(2, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal(".10"));
        assertEquals(2, bdType.getPrecision());
        assertEquals(2, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal("-.10"));
        assertEquals(2, bdType.getPrecision());
        assertEquals(2, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal(".1"));
        assertEquals(1, bdType.getPrecision());
        assertEquals(1, bdType.getScale());
        
        bdType = new BigDecimalType(new BigDecimal("-.1"));
        assertEquals(1, bdType.getPrecision());
        assertEquals(1, bdType.getScale());

        bdType = new BigDecimalType(new BigDecimal("4.510"));
        assertEquals(4, bdType.getPrecision());
        assertEquals(3, bdType.getScale());

        bdType = new BigDecimalType(new BigDecimal("123456789.0"));
        assertEquals(10, bdType.getPrecision());
        assertEquals(1, bdType.getScale());

        bdType = new BigDecimalType(new BigDecimal("-123456789.0"));
        assertEquals(10, bdType.getPrecision());
        assertEquals(1, bdType.getScale());

        bdType = new BigDecimalType(new BigDecimal("1234567890"));
        assertEquals(10, bdType.getPrecision());
        assertEquals(0, bdType.getScale());

        bdType = new BigDecimalType(new BigDecimal("0123456789"));
        assertEquals(9, bdType.getPrecision());
        assertEquals(0, bdType.getScale());
    }
    
    private void assertCorrectColumnDisplaySize(final int precision, final int scale, DataType bdType) {
        // # significant digits + decimal point (if any, as indicated by scale) + sign
        assertEquals(precision + (scale == 0 ? 0 : 1) + 1, bdType.getColumnDisplaySize());
    }
}