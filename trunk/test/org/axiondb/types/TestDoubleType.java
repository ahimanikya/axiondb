/*
 * $Id: TestDoubleType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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
import java.math.BigDecimal;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType} representing a double precision floating-point value.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Jonathan Giron
 */
public class TestDoubleType extends BaseNumericDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestDoubleType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDoubleType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType _type = null;

    public void setUp() throws Exception {
        super.setUp();
        _type = new DoubleType();
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

    public void testSupportsSuccessor() throws Exception {
        assertTrue(_type.supportsSuccessor());
    }

    public void testGetPrecision() throws Exception {
        assertEquals(22, getDataType().getPrecision());
    }

    public void testConvertNonNumericString() throws Exception {
        expectExceptionWhileConvertingNonNumericString("22018");
    }       
    
    public void testSuccessor() throws Exception {
        assertTrue(_type.supportsSuccessor());
        
        Double d = new Double(-Double.MAX_VALUE);
        Double returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);
        
        d = new Double(-2.43235987e231);
        returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);

        d = new Double(-4.9875e-15);
        returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);
        
        d = new Double(-Double.MIN_VALUE);
        returned = (Double) _type.successor(d);
        assertEquals(new Double(-0.0), returned);
        
        d = returned;
        returned = (Double) _type.successor(d);
        assertEquals(new Double(0.0), returned);
        
        d = returned;
        returned = (Double) _type.successor(d);
        assertEquals(new Double(Double.MIN_VALUE), returned);
        
        d = new Double(8.583e-12);
        returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);
        
        d = new Double(15.0);
        returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);
        
        d = new Double(2.43235987e231);
        returned = (Double) _type.successor(d);
        assertTrue(returned.compareTo(d) == 1);
        
        d = new Double(Double.MAX_VALUE);
        returned = (Double) _type.successor(d);
        assertEquals(new Double(Double.MAX_VALUE), returned);
    }
    
    public void testRangeChecks() throws Exception {
        final String msg = "Expected AxionException (22003)";
        
        final BigDecimal exceedsMaxPos = new BigDecimal(Double.MAX_VALUE).add(new BigDecimal("1E308"));
        final BigDecimal exceedsMaxNeg = new BigDecimal(-Double.MAX_VALUE).subtract(new BigDecimal("1E308"));
        
        try {
            _type.convert(exceedsMaxPos);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
        
        try {
            _type.convert(exceedsMaxNeg);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
        
        try {
            _type.convert(exceedsMaxPos.toString());
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        } 
        
        try {
            _type.convert(exceedsMaxNeg.toString());
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
    }
    
    public void testCompare() throws Exception {
        compareEm(
            new Double[] {
                new Double(Double.NEGATIVE_INFINITY),
                new Double(-1f * Double.MAX_VALUE),            
                new Double(-1),
                new Double(-1f*Double.MIN_VALUE),
                new Double(0),
                new Double(Double.MIN_VALUE),
                new Double(1),
                new Double(Double.MAX_VALUE),            
                new Double(Double.POSITIVE_INFINITY)
            });
    }

    public void testAccepts() throws Exception {
        assertTrue("Should accept Byte", _type.accepts(new Byte((byte)3)));
        assertTrue("Should accept Short", _type.accepts(new Short((short)3)));
        assertTrue("Should accept Integer", _type.accepts(new Integer(3)));
        assertTrue("Should accept Long", _type.accepts(new Long(3L)));
        
        assertTrue("Should accept Double", _type.accepts(new Double(3.14D)));
        assertTrue("Should accept Double", _type.accepts(new Double(3.14F)));
        
        assertTrue("Should accept integer String", _type.accepts("3"));
        assertTrue("Should accept non-integer String", _type.accepts("3.14159"));
        
        assertFalse("Should not accept Object", _type.accepts(new Object()));
        assertFalse("Should not accept Timestamp", _type.accepts(new Timestamp(0L)));
    }

    public void testConvertIntegerTypes() throws Exception {
        assertEquals(new Double(17.0), _type.convert(new Byte((byte)17)));
        assertEquals(new Double(17.0), _type.convert(new Short((short)17)));
        assertEquals(new Double(17.0), _type.convert(new Integer(17)));
        assertEquals(new Double(17.0), _type.convert(new Long(17)));
    }

    public void testConvertDecimalTypes() throws Exception {
        assertEquals(new Double(17.99), _type.convert(new Double(17.99)));
        assertEquals(new Double(17.99), _type.convert(new Double(17.99)));
        assertEquals(new Double(17.99), _type.convert("17.99"));
    }
    
    public void testConvertLiterals() throws Exception {
        assertEquals(new Double(17.0), _type.convert("17.0"));
        assertEquals(new Double(-17.0), _type.convert("-17.0"));
        
        assertEquals(new Double(17.0), _type.convert("        17"));
        assertEquals(new Double(-17.0), _type.convert(" -17         "));
        
        assertEquals(new Double(17.0), _type.convert("17.0   "));
        assertEquals(new Double(-17.0), _type.convert(" -17.0"));
        
        try {
            _type.convert("- 150.0");
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22018) - invalid character value for cast",
                "22018", expected.getSQLState());
        }
        
        try {
            _type.convert("abcd");
            fail("Expected AxionException");
        } catch (AxionException expected) {
            assertEquals("Expected AxionException (22018) - invalid character value for cast",
                "22018", expected.getSQLState());
        }
    }        

    public void testWriteReadNonNull() throws Exception {
        Double orig = new Double(17.3);
        
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        _type.write(orig, new DataOutputStream(buf));
        
        Object read = _type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig, read);
    }

    public void testWriteReadSeveral() throws Exception {
        Double[] data = {
            new Double(1.3),
            new Double(-1.3),
            null,
            new Double(0.0),
            new Double(Double.MAX_VALUE),
            null,
            new Double(Double.MIN_VALUE),
            new Double(17.0),
            null
        };
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < data.length; i++) {
            _type.write(data[i], new DataOutputStream(out));
        }
        
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for (int i = 0; i < data.length; i++) {
            Object read = _type.read(in);        
            if (null == data[i]) {
                assertNull(read);
            } else {
                assertEquals(data[i], read);
            }
        }
    }
    
    public void testInvalidWriteFails() throws Exception {
        assertInvalidWriteFails(new Double(Double.POSITIVE_INFINITY));
        assertInvalidWriteFails(new Double(Double.NEGATIVE_INFINITY));
        assertInvalidWriteFails(new Double(Double.NaN));
    }
}