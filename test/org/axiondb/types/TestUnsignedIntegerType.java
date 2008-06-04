/*
 * $Id: TestUnsignedIntegerType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestUnsignedIntegerType extends BaseNumericDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestUnsignedIntegerType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestUnsignedIntegerType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType _type = null;

    public void setUp() throws Exception {
        super.setUp();
        _type = new UnsignedIntegerType();
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
        assertEquals(10,getDataType().getPrecision());
        assertEquals(10, getDataType().getPrecisionRadix());
    }

    public void testGetColumnDisplayType() throws Exception {
        assertEquals(String.valueOf((Integer.MAX_VALUE + 1L) * 2L).length(), _type.getColumnDisplaySize());
    }

    public void testConvertNonNumericString() throws Exception {
        expectExceptionWhileConvertingNonNumericString("22018");
    }

    public void testSuccessor() throws Exception {
        Long i = new Long(4);
        assertTrue(_type.supportsSuccessor());
        assertEquals(new Long(5), _type.successor(i));
        assertEquals(new Long(UnsignedIntegerType.MAX_VALUE), _type.successor(new Long(UnsignedIntegerType.MAX_VALUE)));
    }
    
    public void testRangeChecks() throws Exception {
        final String msg = "Expected AxionException (22003)";
        
        final long exceedsMax = UnsignedIntegerType.MAX_VALUE + 1L;
        final long exceedsMin = UnsignedIntegerType.MIN_VALUE - 1L;
        
        try {
            _type.convert(new Long(exceedsMax));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
        
        try {
            _type.convert(new Long(exceedsMin));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
        
        try {
            _type.convert(String.valueOf(exceedsMax));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        } 
        
        try {
            _type.convert(String.valueOf(exceedsMin));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        }
        
        try {
            _type.convert(new BigDecimal(String.valueOf(exceedsMax)));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        } 
        
        try {
            _type.convert(new BigDecimal(String.valueOf(exceedsMin)));
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22003", expected.getSQLState());
        } 
    }    
    
    public void testAccepts() throws Exception {
        assertTrue("Should accept Byte", _type.accepts(new Byte((byte)3)));
        assertTrue("Should accept Short", _type.accepts(new Short((short)3)));
        assertTrue("Should accept Integer", _type.accepts(new Integer(3)));
        assertTrue("Should accept Long", _type.accepts(new Long(3L)));
        
        assertTrue("Should accept Double", _type.accepts(new Double(3.14D)));
        assertTrue("Should accept Float", _type.accepts(new Float(3.14F)));
        
        assertTrue("Should accept integer String", _type.accepts("3"));
        assertTrue("Should accept Number String (will truncate)", _type.accepts("3.14159"));
    }

    public void testDoesNotAccept() throws Exception {
        assertFalse("Should not accept non-number object", _type.accepts(new Object()));
        assertFalse("Should not accept integer > largest unsigned integer", 
            _type.accepts(new Long(UnsignedIntegerType.MAX_VALUE + 1L)));
        assertFalse("Should not accept negative value", _type.accepts(new Integer(-1)));    
        assertFalse("Should not accept Timestamp", _type.accepts(new Timestamp(0L)));
    }

    public void testConvertIntegerTypes() throws Exception {
        assertEquals(new Long(17), _type.convert(new Byte((byte)17)));
        assertEquals(new Long(17), _type.convert(new Short((short)17)));
        assertEquals(new Long(17), _type.convert(new Integer(17)));
        assertEquals(new Long(17), _type.convert(new Long(17)));
        assertEquals(new Long(UnsignedIntegerType.MAX_VALUE), _type.convert(new Long(UnsignedIntegerType.MAX_VALUE)));
    }

    public void testConvertDecimalTypes() throws Exception {
        assertEquals(new Long(17), _type.convert(new Float(17.99)));
        assertEquals(new Long(17), _type.convert(new Double(17.99)));
    }
    
    public void testConvertLiteralStrings() throws Exception {
        assertEquals(new Long(17), _type.convert("17"));
        assertEquals(new Long(17), _type.convert("  17"));
        assertEquals(new Long(17), _type.convert("17   "));
        assertEquals(new Long(17), _type.convert("17.0  "));
        assertEquals(new Long(17), _type.convert(" 17.0 "));
        
        try {
            _type.convert("-17");
            fail("Expected AxionException (22003)");
        } catch (AxionException expected) {
            assertEquals("22003", expected.getSQLState());
        }
        
        try {
            _type.convert(String.valueOf((Integer.MAX_VALUE + 1L) * 2L));
            fail("Expected AxionException (22003)");
        } catch (AxionException expected) {
            assertEquals("22003", expected.getSQLState());
        }        
        
        try {
            _type.convert("");
            fail("Expected AxionException (22018)");
        } catch (AxionException expected) {
            assertEquals("22018", expected.getSQLState());
        }
    }
    
    public void testInvalidSuccessorFails() {
        try {
            _type.successor(new Long(UnsignedIntegerType.MAX_VALUE + 1L));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // Expected.
        }
    }
    
    public void testWriteReadNonNull() throws Exception {
        Long orig = new Long(17);
        
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        _type.write(orig,new DataOutputStream(buf));
        
        Object read = _type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig,read);
    }

    public void testWriteReadSeveral() throws Exception {
        Long[] data = {
            new Long(1L),
            new Long(Integer.MAX_VALUE),
            null,
            new Long(0L),
            new Long(0xFFFFFFFFL),
            null,
            new Long(Integer.MAX_VALUE+1L),
            new Long(17L),
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
        assertInvalidWriteFails(new Long(UnsignedIntegerType.MAX_VALUE + 1));
        assertInvalidWriteFails(new Long(UnsignedIntegerType.MIN_VALUE - 1));
        assertInvalidWriteFails(new Object());
    } 
}