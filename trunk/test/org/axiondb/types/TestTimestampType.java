/*
 * $Id: TestTimestampType.java,v 1.3 2008/02/21 13:00:28 jawed Exp $
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
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:28 $
 * @author Chuck Burdick
 */
public class TestTimestampType extends BaseDataTypeTest {
    private long now = System.currentTimeMillis();

    //------------------------------------------------------------ Conventional

    public TestTimestampType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestTimestampType.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
        type = new TimestampType();
        
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

    public void testToTime() throws Exception {
        assertNotNull(getDataType().toTime(new Date()));
    }

    public void testToBigDecimal() throws Exception {
        assertNotNull(getDataType().toBigDecimal(new Date()));
    }
    
    public void testToString() throws Exception {
        assertNotNull(getDataType().toString(new Date()));
    }

    public void testGetPrecision() throws Exception { 
        assertEquals(10, getDataType().getPrecision());
        assertEquals(10, getDataType().getPrecisionRadix());
    }
    
    public void testGetColumnDisplaySize() throws Exception {
        assertEquals(29, getDataType().getColumnDisplaySize());
    }
    
    public void testAccepts() throws Exception {
        assertTrue("Should accept Long",
                   type.accepts(new Long(now)));
        assertTrue("Should accept util Date",
                   type.accepts(new java.util.Date()));
        assertTrue("Should accept sql Date",
                   type.accepts(new java.sql.Date(now)));
        assertTrue("Should accept sql Time",
                   type.accepts(new java.sql.Time(now)));
        assertTrue("Should accept Timestamp",
                   type.accepts(new Timestamp(now)));
        assertTrue("Should accept String",
                   type.accepts("2002-08-01"));
        assertTrue("Should not accept Object",
                   !type.accepts(new Object()));
    }

    public void testSupportsSuccessor() {
        assertTrue(type.supportsSuccessor()); 
    }

    public void testConvertObject() {
        try {
            type.convert(new Object());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.convert("this is not a date");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toDate(new Object());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
        try {
            type.toTimestamp(new Object());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testConvertNumberTypes() throws AxionException {
        assertEquals(new Timestamp(now), type.convert(new Long(now)));
    }

    public void testConvertDateTypes() throws AxionException {
        Timestamp expected = new Timestamp(now);
        assertEquals(expected, type.convert(new java.util.Date(now)));
        assertEquals(expected, type.convert(new java.sql.Date(now)));
        assertEquals(expected, type.convert(new java.sql.Time(now)));

        expected.setNanos(123);
        assertEquals(expected, type.convert(expected));
    }

    public void testToDate() throws Exception {
        Object result = type.toDate(new Timestamp(now));
        assertNotNull("Should get object back", result);
        assertTrue("Should get Date back", result instanceof java.sql.Date);
    }   

    public void testToTimestamp() throws Exception {
        Object result = type.toTimestamp(new Timestamp(now));
        assertNotNull("Should get object back", result);
        assertTrue("Should get Timestamp back", result instanceof java.sql.Timestamp);
    }

    public void testConvert24HourString() throws Exception {        
        // August 1, 2002 12:00 AM GMT
        Timestamp expected = new Timestamp(
            32*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31)*24*60*60*1000L + // August 1
            15 * (60*60*1000L) + // hours
            18 * (60*1000L) + // minutes
            29 * (1000L) // seconds
            );
        assertEquals(expected, type.convert("2002-08-01 15:18:29"));
    }

    public void testConvertStringTypes() throws Exception {        
        // August 1, 2002 12:00 AM GMT
        Timestamp expected = new Timestamp(
            32*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31)*24*60*60*1000L + // August
            0 // time
            );
        assertEquals("Should accept", expected, type.convert("2002-08-01"));

        // August 1, 2002 12:00 PM CDT
        expected = new Timestamp(expected.getTime() + 
                                 12*60*60*1000L // time (daylight savings)
            );
        DateFormat fmt = DateFormat.getDateTimeInstance(
            DateFormat.LONG, DateFormat.LONG, TimestampType.LOCALE);
        fmt.setTimeZone(TimestampType.getTimeZone());
        assertEquals("Make sure 'expected' is as expected",
                     "01 August 2002 12:00:00 GMT", fmt.format(expected));

        String toConvert = type.toString(expected); 
        assertEquals("Check format", "2002-08-01 12:00:00.000", toConvert);
        
        // August 1, 2002 12:00:00.001 PM CDT
        expected = new Timestamp(expected.getTime() + 
            1L // 1 millisecond
        );
        toConvert = type.toString(expected);
        assertEquals("Check format", "2002-08-01 12:00:00.001", toConvert);
        
        // August 1, 2002 12:00:00.050 PM CDT
        expected = new Timestamp(expected.getTime() + 
            49L // 49 milliseconds
        );
        toConvert = type.toString(expected);
        assertEquals("Check format", "2002-08-01 12:00:00.050", toConvert);
        assertEquals("Should be identical", expected, type.convert("2002-08-01 12:00:00.050"));
        
        // August 1, 2002 12:00:00.050 PM CDT
        expected = new Timestamp(expected.getTime() + 
            100L // 100 milliseconds
        );
        toConvert = type.toString(expected);
        assertEquals("Check format", "2002-08-01 12:00:00.150", toConvert);
        assertEquals("Should be identical", expected, type.convert("2002-08-01 12:00:00.150"));
    }

    public void testTimestampNanoProps() {
        Timestamp foo = new Timestamp(now);
        int[] good = { 0, 1, 999, 1000, 999999, 1000000, 999999999 };
        int[] bad = { -1, 1000000000 };

        for (int i = 0; i < good.length; i++) {
            foo.setNanos(good[i]);
            assertEquals("Should get nanos back", good[i], foo.getNanos());
        }

        for (int i = 0; i < bad.length; i++) {
            try {
                foo.setNanos(bad[i]);
                fail("Should not be able to set nanos to " + bad[i]);
            } catch (Exception e) {
                // expected
            }
        }
    }

    public void testCompare() throws Exception {
        Object[] list = new Object[] {
            new java.util.Date(Long.MIN_VALUE),
            new java.util.Date(-1L),
            new java.util.Date(0L),
            new java.util.Date(1L),
            java.sql.Date.valueOf("2003-11-10"),
            java.sql.Date.valueOf("2003-11-11"),
            java.sql.Timestamp.valueOf("2003-11-11 12:34:56.1"),
            java.sql.Timestamp.valueOf("2003-11-11 12:34:56.2"),
            java.sql.Timestamp.valueOf("2003-11-11 12:34:56.200001"),
            java.sql.Timestamp.valueOf("2003-11-11 12:34:57"),
            java.sql.Date.valueOf("2003-11-12"),
            new java.util.Date(Long.MAX_VALUE)
        };
        compareEm(list);
    }

    public void testWriteReadNonNull() throws Exception {
        Timestamp orig = new Timestamp(now);
        orig.setNanos(123);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(orig,new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertEquals(orig, read);
    }

    public void testWriteReadSeveral() throws Exception {
        Timestamp[] data = {
            new Timestamp(System.currentTimeMillis()),
            // 06:34:12 am
            new Timestamp(6*60*60*1000 + 34*60*1000 + 12*1000),
            null,
            new Timestamp(0L),
            null };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(int i=0;i<data.length;i++) {
            type.write(data[i],new DataOutputStream(out));
        }
        DataInputStream in = new DataInputStream(
            new ByteArrayInputStream(out.toByteArray()));
        for(int i=0;i<data.length;i++) {
            Object read = type.read(in);        
            if(null == data[i]) {
                assertNull(read);
            } else {
                assertEquals(data[i],read);
            }
        }
    }
    
    public void testInvalidWriteFails() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            type.write(new Object(), new DataOutputStream(out));
        } catch (IOException expected) {
            // Expected.
        }
    }

    public void testSuccessor() {
        Timestamp value = new Timestamp(System.currentTimeMillis());
        Timestamp next = (Timestamp)type.successor(value);
        assertTrue("Should be greater", value.before(next));
        assertEquals("Should get next nanos",
                     value.getNanos() + 1, next.getNanos());

        value.setNanos(999999999);
        next = (Timestamp)type.successor(value);
        assertTrue("Should be greater", value.before(next));
        assertEquals("Should get next nanos", 0, next.getNanos());
    }
    
    public void testInvalidConversion() throws Exception {
        try {
            type.toBigDecimal(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // Expected.
        }
        
        try {
            type.toDate(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // Expected.
        }
        
        try {
            type.toString(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // Expected.
        }
        
        try {
            type.toTime(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // Expected.
        }
                
        try {
            type.toTimestamp(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // Expected.
        }
        
    }    
}
