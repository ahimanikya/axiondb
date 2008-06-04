/*
 * $Id: TestFloatType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestFloatType extends BaseNumericDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestFloatType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestFloatType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType _type = null;

    public void setUp() throws Exception {
        super.setUp();
        _type = new FloatType();
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
        assertEquals(12, getDataType().getPrecision());
        assertEquals(10, getDataType().getPrecisionRadix());
    }

    public void testConvertNonNumericString() throws Exception {
        expectExceptionWhileConvertingNonNumericString("22018");
    }       
    
    public void testSuccessor() throws Exception {
        assertTrue(_type.supportsSuccessor());
        
        // Test successor of negative value with largest magnitude.
        Float f = new Float(-Float.MAX_VALUE);
        Float result = (Float) _type.successor(f);
        assertEquals(new Float(Float.intBitsToFloat(Float.floatToIntBits(-Float.MAX_VALUE) - 1)), result);
        
        f = result;
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f));

        
        // Test a few negative float values.
        f = new Float(-1.512341235e10);
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f)); // ensure successor is greater than source value
        
        f = new Float(-5.0);
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f)); // ensure successor is greater than source value
        
        
        // Exercise the successor(float) method from just less then the smallest negative value
        // to just greater than the smallest positive value.
        f = new Float(Float.intBitsToFloat(Float.floatToIntBits(-Float.MIN_VALUE) + 1));
        result = (Float) _type.successor(f);
        assertEquals(new Float(-Float.MIN_VALUE), result);
        
        f = result;
        result = (Float) _type.successor(f);
        assertEquals(new Float(-0.0), result);
        
        f = result;
        result = (Float) _type.successor(f);
        assertEquals(new Float(0.0), result);
        
        f = result;
        result = (Float) _type.successor(f);
        assertEquals(new Float(Float.MIN_VALUE), result);
        
        f = result;
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f));


        // Test a few positive float values.
        f = new Float(5.0);
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f)); // ensure successor is greater than source value
        
        f = new Float(1.512341235e10);
        result = (Float) _type.successor(f);
        assertEquals(1, result.compareTo(f)); // ensure successor is greater than source value

        
        // Successor of [Float.MAX_VALUE - (min decrement)] should be Float.MAX_VALUE.
        f = new Float(Float.intBitsToFloat(Float.floatToIntBits(Float.MAX_VALUE) - 1));
        result = (Float) _type.successor(f);
        assertEquals(new Float(Float.MAX_VALUE), result);
        
        assertEquals(result, _type.successor(result));
    }
    
    public void testRangeChecks() throws Exception {
        final String msg = "Expected AxionException (22003)";
        
        final Double exceedsMaxPos = new Double(Float.MAX_VALUE + 1e38);
        final Double exceedsMaxNeg = new Double(-Float.MAX_VALUE - + 1e38);
        
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
            new Float[] {
                new Float(Float.NEGATIVE_INFINITY),
                new Float(-1f * Float.MAX_VALUE),            
                new Float(-1),
                new Float(-1f*Float.MIN_VALUE),
                new Float(0),
                new Float(Float.MIN_VALUE),
                new Float(1),
                new Float(Float.MAX_VALUE),            
                new Float(Float.POSITIVE_INFINITY)
            });
    }

    public void testAccepts() throws Exception {
        assertTrue("Should accept Byte",_type.accepts(new Byte((byte)3)));
        assertTrue("Should accept Short",_type.accepts(new Short((short)3)));
        assertTrue("Should accept Integer",_type.accepts(new Integer(3)));
        assertTrue("Should accept Long",_type.accepts(new Long(3L)));
        
        assertTrue("Should accept Double",_type.accepts(new Double(3.14D)));
        assertTrue("Should accept Float",_type.accepts(new Float(3.14F)));
        
        assertTrue("Should accept integer String",_type.accepts("3"));
        assertTrue("Should accept non-integer String",_type.accepts("3.14159"));
        
        assertFalse("Should not accept Object", _type.accepts(new Object()));
        assertFalse("Should not accept Timestamp", _type.accepts(new Timestamp(0L)));        
    }

    public void testConvertIntegerTypes() throws Exception {
        assertEquals(new Float(17f),_type.convert(new Byte((byte)17)));
        assertEquals(new Float(17f),_type.convert(new Short((short)17)));
        assertEquals(new Float(17f),_type.convert(new Integer(17)));
        assertEquals(new Float(17f),_type.convert(new Long(17)));
    }

    public void testConvertDecimalTypes() throws Exception {
        assertEquals(new Float(17.99f),_type.convert(new Float(17.99)));
        assertEquals(new Float(17.99f),_type.convert(new Double(17.99)));
        assertEquals(new Float(17.99f),_type.convert("17.99"));
    }
    
    public void testConvertLiterals() throws Exception {
        assertEquals(new Float(17f),_type.convert("17.0"));
        assertEquals(new Float(-17f),_type.convert("-17.0"));
        
        assertEquals(new Float(17f),_type.convert("        17"));
        assertEquals(new Float(-17),_type.convert(" -17         "));
        
        assertEquals(new Float(17),_type.convert("17.0   "));
        assertEquals(new Float(-17),_type.convert(" -17.0"));
        
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
        Float orig = new Float(17.3);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        _type.write(orig,new DataOutputStream(buf));
        Object read = _type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig,read);
    }

    public void testWriteReadSeveral() throws Exception {
        Float[] data = {
            new Float(1.3),
            new Float(-1.3),
            null,
            new Float(0.0),
            new Float(Float.MAX_VALUE),
            null,
            new Float(Float.MIN_VALUE),
            new Float(17.0),
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
                assertEquals(data[i],read);
            }
        }
    }
    
    public void testInvalidWriteFails() throws Exception {
        assertInvalidWriteFails(new Float(Float.POSITIVE_INFINITY));
        assertInvalidWriteFails(new Float(Float.NEGATIVE_INFINITY));
        assertInvalidWriteFails(new Float(Float.NaN));
    }
    
    public void testGetColumnDisplaySize() {
        assertEquals(String.valueOf(Float.MAX_VALUE).length(), _type.getColumnDisplaySize());
    }    
}

