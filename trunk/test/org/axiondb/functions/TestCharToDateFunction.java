/*
 * $Id: TestCharToDateFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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

import java.sql.Timestamp;
import java.util.HashMap;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.TimestampType;

/**
 * Unit tests for CharToDate function.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $ 
 * @author Jonathan Giron
 */
public class TestCharToDateFunction extends BaseFunctionTest {

    public TestCharToDateFunction(String testName) {
        super(testName);
    }

    public static final void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(TestCharToDateFunction.class);
    }

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
    }

    /*
     * @see org.axiondb.functions.BaseFunctionTest#makeFunction()
     */
    protected ConcreteFunction makeFunction() {
        return new CharToDateFunction();
    }

    public void testIsValid() {
        ConcreteFunction function = makeFunction();
        assertFalse(function.isValid());
        function.addArgument(new ColumnIdentifier("column"));
        assertFalse(function.isValid());
        function.addArgument(new ColumnIdentifier("column2"));
        assertTrue(function.isValid());
        function.addArgument(new ColumnIdentifier("column3"));
        assertTrue(function.isValid());
    }

    public void testValidFormats() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier dateStr = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(dateStr);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(dateStr, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);

        // Sample time: 2004-04-01 12:00:00Z
        TimestampType.setTimeZone("GMT");
        final Timestamp target = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
                9 * 24 * 60 * 60 * 1000L + // leap years
                                           // '72,'76,'80,'84,'88,'92,'96, 2000,
                                           // 2004
                (31 + 28 + 31) * 24 * 60 * 60 * 1000L // April 01
        );
        
        // Test YYYY-MM-DD date only format, with dash separators
        Literal format = new Literal("yyyy-mm-dd");
        dec.setRow(new SimpleRow(new Object[] { "2004-04-01", format }));

        Object returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);

        // Test US civilian time only format, with colon separators
        format = new Literal("hh:mi:ss am");
        dec.setRow(new SimpleRow(new Object[] { "00:00:00 AM", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", new Timestamp(0), returnVal);

        // Test NATO/military time only format, with colon separators and
        // milliseconds
        format = new Literal("hh24:mi:ss.ff");
        dec.setRow(new SimpleRow(new Object[] { "16:30:00.00", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", 
                new Timestamp( ((16 * 60) + 30) * 60L * 1000L), 
                returnVal);

        // Test compact ISO 8601 format
        format = new Literal("yyyymmddThh24miss");
        dec.setRow(new SimpleRow(new Object[] { "20040401T000000", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);

        // Test three-letter month abbreviation
        format = new Literal("dd mon yyyy hh24:mi:ss.ff");
        dec.setRow(new SimpleRow(new Object[] { "01 APR 2004 00:00:00.00", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target,
                returnVal);

        // Test German-style date format, with dot separators
        format = new Literal("dd.mm.yyyy");
        dec.setRow(new SimpleRow(new Object[] { "01.04.2004", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);

        // Test US-style date format with slash separators
        format = new Literal("mm/dd/yyyy");
        dec.setRow(new SimpleRow(new Object[] { "04/01/2004", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);
        
        // Test pattern with all capital letters
        format = new Literal("YYYYMMDDTHH24MISS");
        dec.setRow(new SimpleRow(new Object[] { "20040401T000000", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);
        
        // Test day in year + 2 digit year (upper case form)
        format = new Literal("DDDYY");
        dec.setRow(new SimpleRow(new Object[] { "09204", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);
        
        // Test day in year + 2 digit year (lower case form)
        format = new Literal("dddyy");
        dec.setRow(new SimpleRow(new Object[] { "09204", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);
    }
    
    public void testValidFormatWithNonZeroSeconds() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier dateStr = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(dateStr);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(dateStr, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        // Sample time: 2004-04-01 12:00:45Z
        TimestampType.setTimeZone("GMT");
        final Timestamp target = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
                9 * 24 * 60 * 60 * 1000L // leap years
                                         // '72,'76,'80,'84,'88,'92,'96, 2000,
                                         // 2004
                + (31 + 28 + 31) * 24 * 60 * 60 * 1000L // April 01
                + 45 * 1000L // 00:00:45
        );
        
        // Test pattern with all capital letters and fractional seconds
        Literal format = new Literal("YYYYMMDDTHH24MISS");
        dec.setRow(new SimpleRow(new Object[] { "20040401T000045", format }));
        Object returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target, returnVal);
        
        final Timestamp target2 = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
                9 * 24 * 60 * 60 * 1000L // leap years
                                         // '72,'76,'80,'84,'88,'92,'96, 2000,
                                         // 2004
                + (31 + 28 + 31) * 24 * 60 * 60 * 1000L // April 01
                + 45 * 1000L // 00:00:45
                + 678L // 0.678
        ); 
        format = new Literal("YYYY-MM-DD HH24:MI:SS.FF");
        dec.setRow(new SimpleRow(new Object[] { "2004-04-01 00:00:45.678", format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", target2, returnVal);
        
        final Timestamp target3 = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
            9 * 24 * 60 * 60 * 1000L // leap years
                                     // '72,'76,'80,'84,'88,'92,'96, 2000,
                                     // 2004
            + (31 + 28 + 31) * 24 * 60 * 60 * 1000L // April 01
            + 0 * 1000L // 00:00:00
            + 678L // 0.678
        ); 
        format = new Literal("YYYY-MM-DD HH24:MI:SS.FF");
        dec.setRow(new SimpleRow(new Object[] { "2004-04-01 00:00:00.678", format}));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format " + format.toString() + "; ", target3, returnVal);        
    }
    
    public void testNullDateStrYieldsNull() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("yyyy.mm.dd");
        dec.setRow(new SimpleRow(new Object[] { null, format }));
        
        Object returnVal = function.evaluate(dec);
        assertNull("Null value for date-str input should have returned null", 
                returnVal);
    }

    public void testInvalidDateStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("yyyy-mm-dd");
        dec.setRow(new SimpleRow(new Object[] { "abcdef", format }));
        try {
            function.evaluate(dec);
            fail("Unparseable value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { null , format }));
        assertNull(function.evaluate(dec));
    }
    
    public void testOutOfRangeYearInDateStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        Literal format = new Literal("yyyy-mm-dd");
        dec.setRow(new SimpleRow(new Object[] { "-434-13-02", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range month value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testOutOfRangeMonthInDateStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        Literal format = new Literal("yyyy-mm-dd");
        dec.setRow(new SimpleRow(new Object[] { "2004-13-02", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range month value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { "2004-00-01", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range month value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testOutOfRangeDayInDateStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        Literal format = new Literal("yyyymmdd");
        dec.setRow(new SimpleRow(new Object[] { "20050229", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range day value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        format = new Literal("yyyymmdd");
        dec.setRow(new SimpleRow(new Object[] { "200502-1", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range day value for date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        format = new Literal("dddyy");
        dec.setRow(new SimpleRow(new Object[] { "36603", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range leap year day value for non-leap year date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testOutOfRangeHourInTimeStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        // Test 24-hour format range
        Literal format = new Literal("hh24:mi:ss");
        dec.setRow(new SimpleRow(new Object[] { "24:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (24) for hh24 date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { "-1:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (-1) for hh24 date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        // Test 12-hour format range (1-12) [hh12]
        format = new Literal("hh12:mi:ss");
        dec.setRow(new SimpleRow(new Object[] { "13:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (13) for hh12 date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }        

        dec.setRow(new SimpleRow(new Object[] { "-1:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (-1) for hh12 date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }        
        
        // Test 12-hour format range (1-12) [hh - identical to hh12]
        format = new Literal("hh:mi:ss");
        dec.setRow(new SimpleRow(new Object[] { "13:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (13) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { "-1:00:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range hour value (-1) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }

    public void testOutOfRangeMinuteInTimeStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        // Test minute range (00-59)
        Literal format = new Literal("hh24:mi:ss");    
        dec.setRow(new SimpleRow(new Object[] { "10:61:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range minute value (61) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { "10:-1:00", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range minute value (-1) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testOutOfRangeSecondInTimeStrThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        // Test second range (00-59)
        Literal format = new Literal("hh24:mi:ss");
        dec.setRow(new SimpleRow(new Object[] { "10:00:61", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range second value (61) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        dec.setRow(new SimpleRow(new Object[] { "10:00:-1", format }));
        try {
            function.evaluate(dec);
            fail("Out-of-range second value (-1) for hh date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }        
    }    
    
    public void testMalformedAMPMThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        
        // Test second range (00-59)
        Literal format = new Literal("hh24:mi:ss am");
        dec.setRow(new SimpleRow(new Object[] { "10:00:00 DM", format }));
        try {
            function.evaluate(dec);
            fail("Malformed text for AM date-str input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
    }
    
    public void testInvalidFormatThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("bcdef");
        dec.setRow(new SimpleRow(new Object[] { "2004-04-01", format }));
        try {
            function.evaluate(dec);
            fail("Invalid value for format input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }       
    }    

    public void testNullFormatThrowsException() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "2004-04-15", null }));
        try {
            function.evaluate(dec);
            fail("Null for format input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testMakeNewInstance() {
        CharToDateFunction function = new CharToDateFunction();
        assertTrue(function.makeNewInstance() instanceof CharToDateFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
    
    public void testInvalid() throws Exception {
        CharToDateFunction function = new CharToDateFunction();
        assertTrue(! function.isValid());
    }

}