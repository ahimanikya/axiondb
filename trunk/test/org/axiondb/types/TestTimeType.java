/*
 * $Id: TestTimeType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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
 *    not be used to endorse or promote  products derived from this 
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
import java.io.OutputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Jonathan Giron
 */
public class TestTimeType extends BaseDataTypeTest {
    private long now = System.currentTimeMillis();

    //------------------------------------------------------------ Conventional

    public static final void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestTimeType.class);
        return suite;
    }

    public TestTimeType(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
        TimeType.setTimeZone("GMT");
        type = new TimeType();

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
        assertEquals(8, getDataType().getPrecision());
        assertEquals(10, getDataType().getPrecisionRadix());
    }
    
    public void testGetColumnDisplaySize() throws Exception {
        assertEquals(8, getDataType().getColumnDisplaySize());
    }

    public void testAccepts() throws Exception {
        assertTrue("Should accept Long", type.accepts(new Long(now)));
        assertTrue("Should accept util Date", type.accepts(new java.util.Date()));
        assertTrue("Should accept sql Date", type.accepts(new java.sql.Date(now)));
        assertTrue("Should accept sql Time", type.accepts(new java.sql.Time(now)));
        assertTrue("Should accept Timestamp", type.accepts(new Timestamp(now)));
        assertTrue("Should accept String", type.accepts("11:11:11"));
        assertTrue("Should not accept Object", !type.accepts(new Object()));
    }

    public void testSupportsSuccessor() {
        assertTrue(type.supportsSuccessor());
    }

    public void testConvertObject() {
        try {
            type.convert(new Object());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
        try {
            type.convert("this is not a date");
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
        try {
            type.toDate(new Object());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
        try {
            type.toTimestamp(new Object());
            fail("Expected AxionException");
        } catch (AxionException expected) {
            // expected
        }
    }

    public void testConvertNumberTypes() throws Exception {
        assertEquals(new Time(TimeType.normalizeTime(now)), type.toTime(new Long(now)));
    }

    public void testConvertTimeTypes() throws Exception {
        long normalizedNow = TimeType.normalizeTime(now);
        Time expectedTime = new Time(normalizedNow);
        assertEquals(expectedTime, type.toTime(new java.util.Date(now)));
        assertEquals(expectedTime, type.toTime(new java.sql.Date(now)));
        assertEquals(expectedTime, type.toTime(expectedTime));
        
        try {
            type.toTime(new Object());
            fail("Expected AxionException(22007) - cannot parse/convert object to timestamp");
        } catch (AxionException expected) {
            assertEquals("22007", expected.getSQLState());
        }        
    }

    public void testToDate() throws Exception {
        try {
            type.toDate(new Date(now));
            fail("Expected AxionException - not implemented.");
        } catch (AxionException expected) {
            // expected
        }
    }
    
    public void testToTime() throws Exception {
        Object result = type.toTime(new Time(now));
        assertNotNull("Should get object back", result);
        assertTrue("Should get Time back", result instanceof java.sql.Time);
    }

    public void testToTimestamp() throws Exception {
        Object result = type.toTimestamp(new Time(now));
        assertNotNull("Should get object back", result);
        assertTrue("Should get Timestamp back", result instanceof java.sql.Timestamp);
    }

    public void testConvert24HourString() throws Exception {
        // August 1, 2002 15:18:29 AM GMT
        Time expected = new Time(
            15 * (60 * 60 * 1000L) + // hours
            18 * (60 * 1000L) + // minutes
            29 * (1000L) // seconds
        );
        Time received = type.toTime("15:18:29");
        assertEquals(expected, received);
    }

    public void testConvertStringTypes() throws Exception {
        Time expectedTime = new Time(0);
        assertEquals("Should accept - ", expectedTime, type.toTime("00:00:00"));

        // 12:00 PM
        expectedTime = (Time) type.convert(new Long(expectedTime.getTime() + 12 * 60 * 60 * 1000L)); 
        DateFormat fmt = DateFormat.getTimeInstance(DateFormat.LONG,
            TimestampType.LOCALE);
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        assertEquals("Make sure 'expected' is as expected", "12:00:00 GMT", fmt
            .format(expectedTime));

        fmt = new SimpleDateFormat("HH:mm:ss");
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        expectedTime = new Time(((12L * 60L * 60L) + (5L * 60L)) * 1000L);
        assertEquals(fmt.format(expectedTime), type.toString("12:05:00"));
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
                new java.util.Date(Long.MAX_VALUE)};
        compareEm(list);
    }

    public void testWriteReadNonNull() throws Exception {
        Time orig = new Time(TimeType.normalizeTime(now));
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(orig, new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));
        assertEquals(orig, read);
    }

    public void testWriteReadSeveral() throws Exception {
        Time[] data = { 
                new Time(System.currentTimeMillis()), 
                // 06:34:12 am
                new Time(6 * 60 * 60 * 1000 + 34 * 60 * 1000 + 12 * 1000), 
                null, 
                new Time(0L), 
                null};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < data.length; i++) {
            type.write(data[i], new DataOutputStream(out));
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for (int i = 0; i < data.length; i++) {
            Object read = type.read(in);
            if (null == data[i]) {
                assertNull(read);
            } else {
                assertEquals(type.convert(data[i]), read);
            }
        }
    }

    public void testWriteTimestampReadTime() throws Exception {
        Timestamp[] tsData = { new Timestamp(System.currentTimeMillis()), 
        // 06:34:12 am
                new Timestamp(6 * 60 * 60 * 1000 + 34 * 60 * 1000 + 12 * 1000), null,
                new Timestamp(0L), null};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < tsData.length; i++) {
            type.write(tsData[i], new DataOutputStream(out));
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for (int i = 0; i < tsData.length; i++) {
            Object read = type.read(in);
            if (null == tsData[i]) {
                assertNull(read);
            } else {
                Time date = (Time) type.convert(new Long(tsData[i].getTime()));
                assertEquals(date, read);
            }
        }
    }
    
    public void testInvalidWriteFails() throws Exception {
        try {
            OutputStream out = new ByteArrayOutputStream();
            type.write(new Object(), new DataOutputStream(out));
            fail("Expected IOException - invalid object for write");
        } catch (IOException expected) {
            // Expected.
        }
    }

    public void testSuccessor() throws Exception {
        Time value = (Time) type.convert(new Long(System.currentTimeMillis()));
        Time next = (Time) type.successor(value);
        assertTrue("Should be greater", value.before(next));
        assertEquals("Should be greater by exactly " + TimeType.INCREMENT_MS + " ms.", value.getTime()
            + TimeType.INCREMENT_MS, next.getTime());
        
        try {
            type.successor(new Object());
            fail("Expected IllegalArgumentException - invalid object for successor()");
        } catch (IllegalArgumentException expected) {
            // Expected
        }
    }
    
    public void testNormalizeTime() {
        // Check that already-normalized values don't get modified. 
        TimestampType.setTimeZone("UTC");
        assertEquals(0, TimeType.normalizeTime(0L));
        
        long value = 13L * 60L * 60L * 1000L;
        assertEquals(value, TimeType.normalizeTime(value));
        
        Time normalOne = new Time(value);
        assertEquals(normalOne.getTime(), TimeType.normalizeTime(value));
        
        // Now assume we're in America/Los_Angeles (-0800 PST, -0700 PDT)
        TimestampType.setTimeZone("America/Los_Angeles");
        
        // Pre DST
        long largeValue = value 
                + (34L * 365L * 24L * 60L * 60L * 1000L) // 2004
                + (8L * 24L * 60L * 60L * 1000L) // leap years (not including 2004)
                + ((31L + 29L + 15L) * 24L * 60L * 60L * 1000L); // March 15
        assertEquals(value, TimeType.normalizeTime(largeValue));
        
        // Post DST
        largeValue = value 
                + (34L * 365L * 24L * 60L * 60L * 1000L) // 2004
                + (8L * 24L * 60L * 60L * 1000L) // leap years (not including 2004)
                + ((31L + 29L + 31L + 30L + 15) * 24L * 60L * 60L * 1000L); // May 15
        int offset = TimeType.getTimeZone().getDSTSavings();
        assertEquals(value + offset, TimeType.normalizeTime(largeValue));
    }
}
