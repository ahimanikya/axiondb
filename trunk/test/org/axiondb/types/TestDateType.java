/*
 * $Id: TestDateType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Jonathan Giron
 * @author Girish Patil
 *
 */
public class TestDateType extends BaseDataTypeTest {
    private static final SimpleDateFormat DATE_FIRMAT = new SimpleDateFormat("yyyy-MM-dd", TimestampType.LOCALE);
    private long now = System.currentTimeMillis();
    private Date expected = null;
    private long expectedMillis = 0l;

    //------------------------------------------------------------ Conventional

    public static final void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDateType.class);
        return suite;
    }

    public TestDateType(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    private DataType type = null;

    public void setUp() throws Exception {
        super.setUp();
        type = new DateType();

        Calendar dt = Calendar.getInstance();
        dt.set(Calendar.MILLISECOND, 0);
        dt.set(Calendar.SECOND, 0);
        dt.set(Calendar.MINUTE, 0);
        dt.set(Calendar.HOUR_OF_DAY, 0);
        expectedMillis = dt.getTimeInMillis();
        expected = new Date(expectedMillis);
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
        assertEquals(11, getDataType().getPrecision());
    }

    public void testAccepts() throws Exception {
        assertTrue("Should accept Long", type.accepts(new Long(now)));
        assertTrue("Should accept util Date", type.accepts(new java.util.Date()));
        assertTrue("Should accept sql Date", type.accepts(new java.sql.Date(now)));
        assertTrue("Should accept sql Time", type.accepts(new java.sql.Time(now)));
        assertTrue("Should accept Timestamp", type.accepts(new Timestamp(now)));
        assertTrue("Should accept String", type.accepts("2004-04-15"));
        assertTrue("Should not accept Object", !type.accepts(new Object()));
    }

    public void testSupportsSuccessor() {
        assertTrue(type.supportsSuccessor());
    }

    public void testConvertObject() {
        try {
            type.convert(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            type.convert("this is not a date");
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            type.toDate(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            type.toTimestamp(new Object());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
    }

    public void testConvertNumberTypes() throws Exception {
        Date generated = (Date) type.convert(new Long(now));
        assertEquals(expected, generated);
    }

    public void testConvertDateTypes() throws Exception {
        assertEquals(expected, type.toDate(new java.util.Date(now)));
        assertEquals(expected, type.toDate(new java.sql.Date(now)));
        assertEquals(expected, type.toDate(new java.sql.Time(now)));
    }

    public void testToBigDecimal() throws Exception {
        BigDecimal tExpected = BigDecimal.valueOf(expectedMillis);
        assertEquals(tExpected, type.toBigDecimal(new Long(now)));
    }

    public void testToDate() throws Exception {
        Object result = type.toDate(new Date(now));
        assertNotNull("Should get object back", result);
        assertTrue("Should get Date back", result instanceof java.sql.Date);
        assertEquals(expectedMillis, ((Date) result).getTime());
    }

    public void testToTimestamp() throws Exception {
        Object result = type.toTimestamp(new Date(now));
        assertTrue("Should get Timestamp back", result instanceof java.sql.Timestamp);
        assertEquals(expectedMillis, ((Timestamp) result).getTime());
    }

    public void testConvert24HourString() throws Exception {
        // originally 15:18:29 GMT August 1, 2002 -> 12:00 AM GMT August 1, 2002
        Date tExpected = type.toDate(new Long(32 * 365 * 24 * 60 * 60 * 1000L + //year
            8 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31 + 28 + 31 + 30 + 31 + 30 + 31) * 24 * 60 * 60 * 1000L + // August 1
            15 * (60 * 60 * 1000L) + // hours
            18 * (60 * 1000L) + // minutes
            29 * (1000L) // seconds
        ));
        assertEquals(tExpected, type.toDate("2002-08-01 15:18:29"));
    }

    public void testConvertStringTypes() throws Exception {
        // August 1, 2002 12:00 AM GMT
        long GMT_2002_0801 = 32 * 365 * 24 * 60 * 60 * 1000L + //year
        8 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
        (31 + 28 + 31 + 30 + 31 + 30 + 31) * 24 * 60 * 60 * 1000L + // August
        0  ; // time

        Date expectedDate = new Date(GMT_2002_0801 - TimeZone.getDefault().getOffset(GMT_2002_0801)
        );
        assertEquals("Should accept", expectedDate, type.toDate("2002-08-01"));

        // August 1, 2002 12:00 PM CDT
        expectedDate = new Date(expectedDate.getTime() + 12 * 60 * 60 * 1000L // time (daylight
        // savings)
        );
        DateFormat fmt = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG,
            TimestampType.LOCALE);
        fmt.setTimeZone(TimeZone.getDefault());
        assertEquals("Make sure 'expected' is as expected", "01 August 2002 12:00:00 "
                + TimeZone.getDefault().getDisplayName(true, TimeZone.SHORT), fmt.format(expectedDate));

        String toConvert = DATE_FIRMAT.format(expectedDate);
        assertEquals("Check format", "2002-08-01", toConvert);

        String tExpected = "2002-08-01";
        String converted = type.toString(tExpected);
        assertEquals(tExpected, converted);

        String expectedPlusTime = tExpected + " 08:45:00";
        converted = type.toString(expectedPlusTime);
        assertEquals(tExpected, converted);

        expectedPlusTime = tExpected + " 23:59:59";
        converted = type.toString(expectedPlusTime);
        assertEquals(tExpected, converted);
    }

    public void testCompare() throws Exception {
        Object[] list = new Object[] { new java.util.Date(Long.MIN_VALUE), new java.util.Date(-1L),
                new java.util.Date(0L), new java.util.Date(1L),
                java.sql.Date.valueOf("2003-11-10"), java.sql.Date.valueOf("2003-11-11"),
                java.sql.Timestamp.valueOf("2003-11-11 12:34:56.1"),
                java.sql.Timestamp.valueOf("2003-11-11 12:34:56.2"),
                java.sql.Timestamp.valueOf("2003-11-11 12:34:56.200001"),
                java.sql.Timestamp.valueOf("2003-11-11 12:34:57"),
                java.sql.Date.valueOf("2003-11-12"), new java.util.Date(Long.MAX_VALUE)};
        compareEm(list);
    }

    public void testWriteReadNonNull() throws Exception {
        Date orig = new Date(DateType.normalizeToUTCZeroHour(now));
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        type.write(orig, new DataOutputStream(buf));
        Object read = type.read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));
        assertEquals(orig, read);
    }

    public void testWriteReadSeveral() throws Exception {
        Date[] data = {
                new Date(DateType.normalizeToUTCZeroHour(System.currentTimeMillis())),
                // 06:34:12 am
                new Date(DateType.normalizeToUTCZeroHour(6 * 60 * 60 * 1000 + 34 * 60 * 1000 + 12 * 1000)),
                null,
                new Date(0L),
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
                assertEquals(data[i], read);
            }
        }
    }

    public void testWriteTimestampReadDate() throws Exception {
        long pointInTime = 6 * 60 * 60 * 1000 + 34 * 60 * 1000 + 12 * 1000;
        Timestamp[] tsInData = {
                new Timestamp(now),
                //  06:34:12 am
                new Timestamp(pointInTime - TimeZone.getDefault().getOffset(pointInTime)),
                null,
                new Timestamp(0L - TimeZone.getDefault().getOffset(pointInTime))};

        Date[] dtOutData = {
                expected,
                new Date(0L  - TimeZone.getDefault().getOffset(pointInTime)),
                null,
                new Date(0L - TimeZone.getDefault().getOffset(pointInTime))};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < tsInData.length; i++) {
            type.write(tsInData[i], new DataOutputStream(out));
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        for (int i = 0; i < tsInData.length; i++) {
            Object read = type.read(in);
            if (null == tsInData[i]) {
                assertNull(read);
            } else {
                assertEquals(dtOutData[i], read);
            }
        }
    }

    public void testSuccessor() {
        long normalizedValue = expectedMillis;

        Date value = new Date(now);
        Date next = (Date) type.successor(value);

        assertTrue("Should be greater", value.before(next));
        assertEquals("Should be greater by exactly " + DateType.INCREMENT_DAY + " ms.", normalizedValue
            + DateType.INCREMENT_DAY, next.getTime());
    }
}
