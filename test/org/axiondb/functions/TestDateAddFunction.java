/*
 * $Id: TestDateAddFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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

package org.axiondb.functions;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.axiondb.ColumnIdentifier;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.DateType;
import org.axiondb.types.TimestampType;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author radval To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Generation - Code and Comments
 */
public class TestDateAddFunction extends BaseFunctionTest {

    public TestDateAddFunction(String testName) {
        super(testName);
    }

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.axiondb.functions.BaseFunctionTest#makeFunction()
     */
    protected ConcreteFunction makeFunction() {
        return new DateAddFunction();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDateAddFunction.class);
        return suite;
    }

    public void testAddYearsToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //April 01 2004 GMT
        Timestamp input = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
            9 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96, 2000,
                                       // 2004
            (31 + 28 + 31 + 01) * 24 * 60 * 60 * 1000L // April 01
        );

        //April 01 2005 GMT
        Timestamp result = new Timestamp(35 * 365 * 24 * 60 * 60 * 1000L + //year
            9 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96, 2000,
                                       // 2004
            (31 + 28 + 31 + 01) * 24 * 60 * 60 * 1000L // April 01
        );

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "YEAR", new Integer(1), input}));

        assertEquals(result, function.evaluate(dec));
    }

    public void testAddQuartersToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Feb 29 1960 GMT
        Timestamp input = new Timestamp(-1 * (10 * 365 * 24 * 60 * 60 * 1000L + //year
            3 * 24 * 60 * 60 * 1000L + // leap years '60,'64,'68
            (31 + 29) * 24 * 60 * 60 * 1000L) // Feb 29
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.MONTH, 75 * 3);

        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "QUARTER", new Integer(75), input}));

        assertEquals(result, function.evaluate(dec));
    }

    public void testAddMonthsToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Feb 01 1980 GMT
        Timestamp input = new Timestamp(10 * 365 * 24 * 60 * 60 * 1000L + //year
            3 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80
            (31 + 01) * 24 * 60 * 60 * 1000L // Feb 01
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.MONTH, 125);
        //1990-07-01
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MONTH", new Integer(125), input}));
        assertEquals(result, function.evaluate(dec));
    }

    public void testAddDaysToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Feb 29 1980 GMT
        Timestamp input = new Timestamp(10 * 365 * 24 * 60 * 60 * 1000L + //year
            3 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80
            (31 + 29) * 24 * 60 * 60 * 1000L // Feb 29
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.DAY_OF_YEAR, 377);
        //1981-03-12
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(377), input}));

        assertEquals(result, function.evaluate(dec));
    }

    public void testAddHoursToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //April 29 1999 11:20:33 GMT
        Timestamp input = new Timestamp(29 * 365 * 24 * 60 * 60 * 1000L + //year
            7 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,84,88,92,96
            (31 + 28 + 31 + 29) * 24 * 60 * 60 * 1000L + // Feb 29
            11 * 60 * 60 * 1000L + //Hours
            20 * 60 * 1000L + //minutes
            33 * 1000L //seconds
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.HOUR_OF_DAY, 160);

        //1999-05-06 03:20:33.0
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "HOUR", new Integer(160), input}));
        assertEquals(result, function.evaluate(dec));
    }

    public void testAddMinutesToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Nov 30 2001 18:56:49 GMT
        Timestamp input = new Timestamp(31 * 365 * 24 * 60 * 60 * 1000L + //year
            8 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,84,88,92,96,2000
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30) * 24 * 60 * 60 * 1000L + // Nov
                                                                                            // 30
            18 * 60 * 60 * 1000L + //Hours
            56 * 60 * 1000L + //minutes
            49 * 1000L //seconds
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.MINUTE, 2089);

        //2001-12-02 05:45:49.0
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MINUTE", new Integer(2089), input}));
        assertEquals(result, function.evaluate(dec));
    }

    public void testAddSecondsToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Dec 30 2000 21:56:49 GMT
        Timestamp input = new Timestamp(30 * 365 * 24 * 60 * 60 * 1000L + //year
            8 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,84,88,92,96,2000
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 30) * 24 * 60 * 60 * 1000L + // Dec
                                                                                                 // 30
            21 * 60 * 60 * 1000L + //Hours
            56 * 60 * 1000L + //minutes
            49 * 1000L //seconds
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.SECOND, 9999);

        //2000-12-31 00:43:28.0
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "SECOND", new Integer(9999), input}));
        assertEquals(result, function.evaluate(dec));
    }

    public void testAddMilliSecondsToDate() throws Exception {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        Map map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        //Dec 31 2000 23:56:49 GMT
        Timestamp input = new Timestamp(30 * 365 * 24 * 60 * 60 * 1000L + //year
            8 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,84,88,92,96,2000
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) * 24 * 60 * 60 * 1000L + // Dec
                                                                                                 // 30
            23 * 60 * 60 * 1000L + //Hours
            56 * 60 * 1000L + //minutes
            49 * 1000L //seconds
        );

        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.MILLISECOND, 198774);

        //2001-01-01 00:00:07.774
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MILLISECOND", new Integer(198774), input}));
        assertEquals(result, function.evaluate(dec));
    }

    public void testNullIntervalInputYieldsNull() {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", null, new Timestamp(System.currentTimeMillis())}));
        try {
            assertNull("Expected null return for DateAdd with null for interval input.", function.evaluate(dec));
        } catch (Exception e) {
            fail("Null for interval input of DateAdd should not have thrown an Exception: " + e);
        }
    }

    public void testDateTypeInput() throws Exception {
        final int INCR = 5;

        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier date = new ColumnIdentifier("date");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(date);

        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(date, new Integer(2));

        Date input = new Date(DateType.normalizeToUTCZeroHour(System.currentTimeMillis()));
        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.DATE, INCR);
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(INCR), input}));
        assertEquals("DateAdd failed for DATE input - ", result, function.evaluate(dec));
    }

    public void testTimeTypeInput() throws Exception {
        final int INCR = 2;

        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier time = new ColumnIdentifier("time");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(time);

        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(time, new Integer(2));

        Time input = new Time(0L);
        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(input.getTime());
        c.add(Calendar.DATE, INCR);
        Timestamp result = new Timestamp(c.getTimeInMillis());

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(INCR), input}));
        assertEquals("DateAdd failed for TIME input - ", result, function.evaluate(dec));
    }

    public void testNullTimestampInputYieldsNull() {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(1), null}));
        try {
            assertNull("Expected null return for DateAdd with null for timestamp input.", function.evaluate(dec));
        } catch (Exception e) {
            fail("Null for timestamp input of DateAdd should not have thrown an Exception: " + e);
        }
    }

    public void testInvalidArguments() {
        DateAddFunction function = new DateAddFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier interval = new ColumnIdentifier("interval");
        ColumnIdentifier timestamp = new ColumnIdentifier("timestamp");

        function.addArgument(intervalType);
        function.addArgument(interval);
        function.addArgument(timestamp);

        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));
        map.put(interval, new Integer(1));
        map.put(timestamp, new Integer(2));

        RowDecorator dec = new RowDecorator(map);
        Date input = new Date(DateType.normalizeToUTCZeroHour(System.currentTimeMillis()));
        dec.setRow(new SimpleRow(new Object[] { "DAY", "integer", input}));
        try {
            function.evaluate(dec);
            fail("Expected conversion error");
        } catch (Exception e) {
            // Expected
        }
        
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(1), "input"}));
        try {
            function.evaluate(dec);
            fail("Expected conversion error");
        } catch (Exception e) {
            // Expected
       }
        
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Integer(1), new Object[] {"input"}}));
        try {
            function.evaluate(dec);
            fail("Expected conversion error");
        } catch (Exception e) {
            // Expected
        }

    }

    public void testMakeNewInstance() {
        DateAddFunction function = new DateAddFunction();
        assertTrue(function.makeNewInstance() instanceof DateAddFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testInvalid() throws Exception {
        DateAddFunction function = new DateAddFunction();
        assertTrue(!function.isValid());
    }

    public void testValid() throws Exception {
        DateAddFunction function = new DateAddFunction();
        function.addArgument(new ColumnIdentifier("interval"));
        function.addArgument(new ColumnIdentifier("intervalType"));
        function.addArgument(new ColumnIdentifier("timestamp"));
        assertTrue(function.isValid());
    }
}