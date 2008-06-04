/*
 * $Id: TestDateDiffFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

import org.axiondb.ColumnIdentifier;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.DateType;
import org.axiondb.types.TimestampType;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author radval
 *
 * To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class TestDateDiffFunction extends BaseFunctionTest {

    public TestDateDiffFunction(String testName) {
        super(testName);
    }
    
    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
    }
    
    /* (non-Javadoc)
     * @see org.axiondb.functions.BaseFunctionTest#makeFunction()
     */
    protected ConcreteFunction makeFunction() {
        return new DateDiffFunction();
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite(TestDateDiffFunction.class);
        return suite;
    }
    
    public void testYearsBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //November 30 1973  GMT
        Timestamp t1 = new Timestamp(
            3*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+31+30+31+30)*24*60*60*1000L // November 30
            );
        
        //April 25 2004  GMT
        Timestamp t2 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+25)*24*60*60*1000L // April 25
            );
        
        Calendar c1 = Calendar.getInstance(TimestampType.getTimeZone());
        c1.setTimeInMillis(t1.getTime());
        
        Calendar c2 = Calendar.getInstance(TimestampType.getTimeZone());
        c2.setTimeInMillis(t2.getTime());
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "YEAR" , t1, t2}));
        Long result = new Long(c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testYearsBetweenDates2() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //November 30 1973  GMT
        Timestamp t2 = new Timestamp(
            3*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+31+30+31+30)*24*60*60*1000L // November 30
            );
        
        //April 25 2004  GMT
        Timestamp t1 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+25)*24*60*60*1000L // April 25
            );
        
        Calendar c1 = Calendar.getInstance(TimestampType.getTimeZone());
        c1.setTimeInMillis(t1.getTime());
        
        Calendar c2 = Calendar.getInstance(TimestampType.getTimeZone());
        c2.setTimeInMillis(t2.getTime());
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "YEAR" , t1, t2}));
        Long result = new Long(c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testWeeksBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //November 30 1973  GMT
        Timestamp t1 = new Timestamp(
            3*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+31+30+31+30)*24*60*60*1000L // November 30
            );
        
        //April 25 2004  GMT
        Timestamp t2 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+25)*24*60*60*1000L // April 25
            );
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "WEEK" , t1, t2}));
        int i = (t2.getTime()-t1.getTime())%(7*24*60*60*1000) == 0? 0: 1;
        Long result = new Long((t2.getTime()-t1.getTime())/(7*24*60*60*1000) + i);
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testWeeksBetweenDates2() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //November 30 1973  GMT
        Timestamp t2 = new Timestamp(
            3*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+31+30+31+30)*24*60*60*1000L // November 30
            );
        
        //April 25 2004  GMT
        Timestamp t1 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+25)*24*60*60*1000L // April 25
            );
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "WEEK" , t1, t2}));
        int i = (t2.getTime()-t1.getTime())%(7*24*60*60*1000) == 0? 0: -1;
        Long result = new Long((t2.getTime()-t1.getTime())/(7*24*60*60*1000) + i);
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testQuartersBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        // November 30 1973  GMT
        Timestamp t1 = new Timestamp(
            3*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+31+30+31+30)*24*60*60*1000L // November 30
            );
        
        //April 25 2004  GMT
        Timestamp t2 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+25)*24*60*60*1000L // April 25
            );
        
//      dumb way to calculate months difference but this is another way
        //to verify , we are doing a better way in datediff function
        Calendar c1 = Calendar.getInstance(TimestampType.getTimeZone());
        c1.setTimeInMillis(t1.getTime());
        c1.clear(Calendar.MILLISECOND);
        c1.clear(Calendar.SECOND);
        c1.clear(Calendar.MINUTE);
        c1.clear(Calendar.HOUR_OF_DAY);
        c1.clear(Calendar.DATE);


        
        Calendar c2 = Calendar.getInstance(TimestampType.getTimeZone());
        c2.setTimeInMillis(t2.getTime());
        c2.clear(Calendar.MILLISECOND);
        c2.clear(Calendar.SECOND);
        c2.clear(Calendar.MINUTE);
        c2.clear(Calendar.HOUR_OF_DAY);
        c2.clear(Calendar.DATE);

        int months = 0;
        while ( c1.before(c2) ) {
            c1.add(Calendar.MONTH, 1);
            months++;
         }

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "QUARTER", t1, t2}));
        Long result = new Long(months/3);
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testMonthsBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //August 22 1990  GMT
        Timestamp t2 = new Timestamp(
            20*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+31+22)*24*60*60*1000L // August 22
            );
        
        //July 23 2002  GMT
        Timestamp t1 = new Timestamp(
            32*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+23)*24*60*60*1000L // //July 23
            );
        
        //dumb way to calculate months difference but this is another way
        //to verify , we are doing a better way in datediff function
        Calendar c1 = Calendar.getInstance(TimestampType.getTimeZone());
        c1.setTimeInMillis(t1.getTime());
        c1.clear(Calendar.MILLISECOND);
        c1.clear(Calendar.SECOND);
        c1.clear(Calendar.MINUTE);
        c1.clear(Calendar.HOUR_OF_DAY);
        c1.clear(Calendar.DATE);

        Calendar c2 = Calendar.getInstance(TimestampType.getTimeZone());
        c2.setTimeInMillis(t2.getTime());
        c2.clear(Calendar.MILLISECOND);
        c2.clear(Calendar.SECOND);
        c2.clear(Calendar.MINUTE);
        c2.clear(Calendar.HOUR_OF_DAY);
        c2.clear(Calendar.DATE);

        int months = 0;
        while ( c2.before(c1) ) {
            c2.add(Calendar.MONTH, 1);
            months++;
         }

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MONTH" , t1, t2}));
        Long result = new Long(-months);
        assertEquals( result , function.evaluate(dec));
        
        dec.setRow(new SimpleRow(new Object[] { "MONTH" , t2, t2}));
        assertEquals( new Long(0) , function.evaluate(dec));
    }
    
    public void testDaysBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //August 15 1947  GMT
        Timestamp t1 = new Timestamp(
            -1*(23*365*24*60*60*1000L + //year
            6*24*60*60*1000L + // leap years '48,'52,'56,'60,'64,'68,
            (31+28+31+30+31+30+31+15)*24*60*60*1000L) // August 15
            );
        
        //July 23 2000  GMT
        Timestamp t2 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+30+31+30+23)*24*60*60*1000L // //July 23
            );
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY" , t1, t2}));
        Long result = new Long((t2.getTime()-t1.getTime())/(24*60*60*1000));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testHoursBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //December 15 1999 11:11:23 GMT
        Timestamp t1 = new Timestamp(
            29*365*24*60*60*1000L + //year
            7*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96
            (31+28+31+30+31+30+31+31+30+31+30+15)*24*60*60*1000L + // December 15
            11*60*60*1000L + //Hours
            11*60*1000L + //minutes
            23*1000L //seconds
            );
        
        //April 23 2004 21:22:21  GMT
        Timestamp t2 = new Timestamp(
            34*365*24*60*60*1000L + //year
            9*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000,2004
            (31+28+31+23)*24*60*60*1000L + //April 23
            21*60*60*1000L + //Hours
            21*60*1000L + //minutes
            21*1000L //seconds
            );
        
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "HOUR" , t1, t2}));
        Long result = new Long((t2.getTime()-t1.getTime())/(60*60*1000));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testMinutesBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //August 15 2000 12:04:22 GMT
        Timestamp t1 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96, 2000
            (31+28+31+30+31+30+31+15)*24*60*60*1000L + // August 15
            12*60*60*1000L + //Hours
            04*60*1000L + //minutes
            22*1000L //seconds
            );
        
        //April 23 2000 11:06:22  GMT
        Timestamp t2 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+23)*24*60*60*1000L + //April 23
            11*60*60*1000L + //Hours
            06*60*1000L + //minutes
            22*1000L //seconds
            );
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MINUTE", t1, t2}));
        Long result = new Long((t2.getTime()-t1.getTime())/(60*1000));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testSecondsBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //May 15 2000 12:04:22 GMT
        Timestamp t1 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96, 2000
            (31+28+31+30+15)*24*60*60*1000L + // May 15
            12*60*60*1000L + //Hours
            04*60*1000L + //minutes
            22*1000L //seconds
            );
        
        //April 23 2000 11:06:22  GMT
        Timestamp t2 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+23)*24*60*60*1000L + //April 23
            11*60*60*1000L + //Hours
            06*60*1000L + //minutes
            22*1000L //seconds
            );
        
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "SECOND", t1, t2}));
        Long result = new Long((t2.getTime()-t1.getTime())/(1000));
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testMilliSecondsBetweenDates() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
//      April 15 2000 12:04:22 GMT
        Timestamp t1 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96, 2000
            (31+28+31+15)*24*60*60*1000L + // April 15
            12*60*60*1000L + //Hours
            04*60*1000L + //minutes
            22*1000L //seconds
            );
        
        //April 23 2000 11:06:22  GMT
        Timestamp t2 = new Timestamp(
            30*365*24*60*60*1000L + //year
            8*24*60*60*1000L + // leap years '72,'76,'80,'84,'88,'92,'96,2000
            (31+28+31+23)*24*60*60*1000L + //April 23
            11*60*60*1000L + //Hours
            06*60*1000L + //minutes
            22*1000L //seconds
            );
        
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "MILLISECOND", t1, t2}));
        Long result = new Long(t2.getTime()-t1.getTime());
        assertEquals( result , function.evaluate(dec));
    }
    
    public void testNullTimestampInputYieldsNull() {
        DateDiffFunction function = new DateDiffFunction();
        ColumnIdentifier intervalType = new ColumnIdentifier("intervalType");
        ColumnIdentifier timestamp1 = new ColumnIdentifier("timestamp1");
        ColumnIdentifier timestamp2 = new ColumnIdentifier("timestamp2");
        
        function.addArgument(intervalType);
        function.addArgument(timestamp1);
        function.addArgument(timestamp2);
        
        HashMap map = new HashMap();
        map.put(intervalType, new Integer(0));                
        map.put(timestamp1,new Integer(1));
        map.put(timestamp2, new Integer(2));
        
        //August 15 1947  GMT
        final Timestamp ts = new Timestamp(
            -1*(23*365*24*60*60*1000L + //year
            6*24*60*60*1000L + // leap years '48,'52,'56,'60,'64,'68,
            (31+28+31+30+31+30+31+15)*24*60*60*1000L) // August 15
            );
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] {"DAY", ts, null}));
        try {
            assertNull("Expected null return for DateDiff with null input for t2.", function.evaluate(dec));
        } catch (Exception e) {
            fail("Null for input t2 of DateDiff should not have thrown an Exception: " + e);
        }
        // Now test with reversed inputs:  t1 = null, t2 = (some valid timestamp)
        dec.setRow(new SimpleRow(new Object[] {"DAY", null, ts}));
        try {
            assertNull("Expected null return for DateDiff with null input for t1.", function.evaluate(dec));
        } catch (Exception e) {
            fail("Null for input t1 of DateDiff should not have thrown an Exception: " + e);
        }
        
        // Now test with both inputs null
        dec.setRow(new SimpleRow(new Object[] {"DAY", null, null}));
        try {
            assertNull("Expected null return for DateDiff with both inputs null.", function.evaluate(dec));
        } catch (Exception e) {
            fail("Nulls for both inputs of DateDiff should not have thrown an Exception: " + e);
        }
    }
    
    public void testInvalidArguments() {
        DateDiffFunction function = new DateDiffFunction();
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
        
        dec.setRow(new SimpleRow(new Object[] { "DAY", new Object[] {"integer"}, input}));
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
        DateDiffFunction function = new DateDiffFunction();
        assertTrue(function.makeNewInstance() instanceof DateDiffFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
    
    public void testInvalid() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        assertTrue(! function.isValid());
    }

    public void testValid() throws Exception {
        DateDiffFunction function = new DateDiffFunction();
        function.addArgument(new ColumnIdentifier("foo"));
        function.addArgument(new ColumnIdentifier("bar"));
        function.addArgument(new ColumnIdentifier("foobar"));
        assertTrue(function.isValid());
    }

}
