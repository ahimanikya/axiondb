/*
 * $Id: TestIsValidDateTimeFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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
 * Unit tests for IsValidDateTime function.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Jonathan Giron
 */
public class TestIsValidDateTimeFunction extends BaseFunctionTest {

    public TestIsValidDateTimeFunction(String testName) {
        super(testName);
    }

    public static final void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(TestIsValidDateTimeFunction.class);
    }

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
    }

    /*
     * @see org.axiondb.functions.BaseFunctionTest#makeFunction()
     */
    protected ConcreteFunction makeFunction() {
        return new IsValidDateTimeFunction();
    }

    public void testMakeNewInstance() {
        assertNotNull((new IsValidDateTimeFunction()).makeNewInstance());
    }

    public void testIsValid() {
        ConcreteFunction function = makeFunction();
        assertFalse(function.isValid());
        function.addArgument(new ColumnIdentifier("column"));
        assertFalse(function.isValid());
        function.addArgument(new ColumnIdentifier("column2"));
        assertTrue(function.isValid());
        function.addArgument(new ColumnIdentifier("column3"));
        assertFalse(function.isValid());
    }

    public void testValidFormats() throws Exception {
        //
        // Test YYYY-MM-DD date only format, with various separators
        //
        String formatStr = "yyyy-mm-dd";
        String testItem = "2004-04-01";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "yyyy/mm/dd";
        testItem = "2004/04/01";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "yyyy.mm.dd";
        testItem = "2005.01.01";
        assertValidDateTime(testItem, formatStr);        
        
        formatStr = "yyyy mm dd";
        testItem = "2004 04 01";
        assertValidDateTime(testItem, formatStr);        
        
        formatStr = "yyyymmdd";
        testItem = "20040401";
        assertValidDateTime(testItem, formatStr);
        
        
        // Test US civilian time only format, with colon separators
        formatStr = "hh:mi:ss am";
        testItem = "00:00:00 AM";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "hh:mi:ss";
        assertValidDateTime("00:00:00", formatStr);
        assertValidDateTime("11:45:36", formatStr);
        

        // Test NATO/military time only formats
        formatStr = "hh24:mi:ss";
        testItem = "19:45:00";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "hh24:mi:ss.ff";
        testItem = "16:30:00.00";
        assertValidDateTime(testItem, formatStr);
        

        //Test date/time combinations
        formatStr = "MON DD YYYY HH:MIAM";
        testItem = "APR 01 2004 01:45AM";
        assertValidDateTime(testItem, formatStr);

        formatStr = "DD MON YYYY HH24:MI:SS";
        testItem = "01 APR 2004 13:24:36";
        assertValidDateTime(testItem, formatStr);

        formatStr = "YYYY-MM-DD HH24:MI:SS.FF";
        testItem = "2004-04-01 18:45:22.5";
        assertValidDateTime(testItem, formatStr);

        formatStr = "DD MON YYYY HH:MI:SS.FFAM";
        testItem = "01 APR 2004 06:30:45.5pm";
        assertValidDateTime(testItem, formatStr);

        formatStr = "DD/MM/YYYY HH:MI:SS.FFAM";
        testItem = "01/04/2004 10:15:30.45AM";
        assertValidDateTime(testItem, formatStr);


        // Test compact ISO 8601 format
        formatStr = "yyyymmddThh24miss";
        testItem = "20040401T000000";
        assertValidDateTime(testItem, formatStr);
        

        // Test three-letter month abbreviation
        formatStr = "dd mon yyyy hh24:mi:ss.ff";
        testItem = "01 APR 2004 00:00:00.00";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "dd mon yyyy";
        testItem = "01 AUG 2004";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "mon dd, yyyy";
        testItem = "FEB 29, 2004";
        assertValidDateTime(testItem, formatStr);
        

        // Test German-style date format with various separators
        formatStr = "dd.mm.yyyy";
        testItem = "01.04.2004";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "dd/mm/yyyy";
        testItem = "13/01/2004";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "dd-mm-yyyy";
        testItem = "30-04-2004";
        assertValidDateTime(testItem, formatStr);

        
        // Test US-style date format with various separators
        formatStr = "mm/dd/yyyy";
        testItem = "04/01/2004";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "mm-dd-yyyy";
        testItem = "04-01-2004";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "mm.dd.yyyy";
        testItem = "04.01.2004";
        assertValidDateTime(testItem, formatStr);
        
        
        // Test pattern with all capital letters
        formatStr = "YYYYMMDDTHH24MISS";
        testItem = "20040401T000000";
        assertValidDateTime(testItem, formatStr);
        
        
        // Test patterns with 2-digit year
        formatStr = "YYMMDD";
        testItem = "080229";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "mm/dd/yy";
        testItem = "02/29/04";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "DD/MM/YY";
        testItem = "29/02/04";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "YY/MM/DD";        
        testItem = "04/02/29";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "mm-dd-yy";
        testItem = "02-29-00";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "DD-MM-YY";
        testItem = "29-02-00";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "YY-MM-DD";        
        testItem = "04-02-29";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "MM.DD.YY";
        testItem = "02.29.96";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "DD.MM.YY";
        testItem = "29.02.96";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "YY.MM.DD";
        testItem = "96.02.29";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "MON DD, YY";
        testItem = "FEB 29, 92";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "DD MON YY";
        testItem = "29 FEB 92";
        assertValidDateTime(testItem, formatStr);
        
        formatStr = "YY MON DD";
        testItem = "92 FEB 29";
        assertValidDateTime(testItem, formatStr);
    }

    public void testNullDateStrYieldsNull() throws Exception {
        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("yyyy.mm.dd");
        dec.setRow(new SimpleRow(new Object[] { null, format}));

        Object returnVal = function.evaluate(dec);
        assertNull("Null value for date-str input should have returned null", returnVal);
    }
    
    public void testInvalidDateStrReturnsFalse() throws Exception {
        String formatStr = "yyyy-mm-dd";
        assertInvalidDateTime("abcdef", formatStr);
    }

    public void testOutOfRangeYearInDateStrReturnsFalse() throws Exception {
        String formatStr = "yyyy-mm-dd";
        assertInvalidDateTime("testItem", formatStr);
    }

    public void testOutOfRangeMonthInDateStrReturnsFalse() throws Exception {
        String formatStr = "yyyy-mm-dd";
        assertInvalidDateTime("2004-13-02", formatStr);
        assertInvalidDateTime("2004-00-01", formatStr);
    }

    public void testOutOfRangeDayInDateStrReturnsFalse() throws Exception {
        String formatStr = "yyyymmdd";
        assertInvalidDateTime("20050229", formatStr);
        assertInvalidDateTime("200502-1", formatStr);
        assertInvalidDateTime("20052903", formatStr);
    }

    public void testOutOfRangeHourInTimeStrReturnsFalse() throws Exception {
        // Test 24-hour format range
        String formatStr = "hh24:mi:ss";
        assertInvalidDateTime("24:00:00", formatStr);
        assertInvalidDateTime("-1:00:00", formatStr);

        // Test 12-hour format range (1-12) [hh12]
        formatStr = "hh12:mi:ss";
        assertInvalidDateTime("13:00:00", formatStr);
        assertInvalidDateTime("-1:00:00", formatStr);

        // Test 12-hour format range (1-12) [hh - identical to hh12]
        formatStr = "hh:mi:ss";
        assertInvalidDateTime("13:00:00", formatStr);
        assertInvalidDateTime("-1:00:00", formatStr);
    }

    public void testOutOfRangeMinuteInTimeStrReturnsFalse() throws Exception {
        // Test minute range (00-59)
        String formatStr = "hh24:mi:ss";
        assertInvalidDateTime("10:61:00", formatStr);
        assertInvalidDateTime("10:-1:00", formatStr);
    }

    public void testOutOfRangeSecondInTimeStrReturnsFalse() throws Exception {
        // Test second range (00-59)
        String formatStr = "hh24:mi:ss";
        assertInvalidDateTime("10:00:61", formatStr);
        assertInvalidDateTime("10:00:-1", formatStr);
    }

    public void testMalformedAMPMReturnsFalse() throws Exception {
        String formatStr = "hh24:mi:ss am";
        assertInvalidDateTime("10:00:00 DM", formatStr);
    }

    public void testInvalidFormatThrowsException() throws Exception {
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "2004-04-15", "bcdef"}));
        try {
            function.evaluate(dec);
            fail("Expected AxionException(22007) for invalid format string");
        } catch (AxionException e) {
            if (!"22007".equals(e.getSQLState())) {
                fail ("Expected AxionException(22007) for invalid format string");
            }
            // expected
        }
        
        // mmm is not a valid date/time part mnemonic
        dec.setRow(new SimpleRow(new Object[] { "2004-04-15", "yyyy-mmm-dd"}));
        try {
            function.evaluate(dec);
            fail("Expected AxionException(22007) for invalid format string: yyyy-mmm-dd");
        } catch (AxionException e) {
            if (!"22007".equals(e.getSQLState())) {
                fail ("Expected AxionException(22007) for invalid format string: yyyy-mmm-dd");
            }
            // expected
        }        
    }

    public void testNullFormatThrowsException() throws Exception {
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "2004-04-15", null}));
        try {
            function.evaluate(dec);
            fail("Expected AxionException(22004) for null format string");
        } catch (AxionException e) {
            if (!"22004".equals(e.getSQLState())) {
                fail ("Expected AxionException(22004) for null format string");
            }
            // expected
        }
    }
    
    public void testMismatchedFormatsAndValues() throws Exception {
        String formatStr = "dd/mm/yy";
        assertInvalidDateTime("03/24/2002", formatStr);
        assertInvalidDateTime("11/29/1999 12:37:09.18PM", formatStr);
        
        formatStr = "hh:mi:ss";
        assertInvalidDateTime("03/24/2002", formatStr);
    }

    public void testPartialMatchesReturnFalse() throws Exception {
        String formatStr = "dd/mm/yyyy";
        assertInvalidDateTime("23 03 2002 15:23:45", formatStr);
        
        formatStr = "hh24:mi:ss am";
        assertInvalidDateTime("15:23:45", formatStr);
    }
    
    public void testTwoDigitYearPatternRejectsFourYearString() throws Exception {
        assertInvalidDateTime("1971-03-31", "yy-mm-dd");
        assertInvalidDateTime("03-31-1971 15:45", "mm-dd-yy hh24:mi");
        assertInvalidDateTime("03-31-1971", "mm-dd-yy");

        assertInvalidDateTime("1971 03 31", "yy mm dd");
        assertInvalidDateTime("03 31 1971 15:45", "mm dd yy hh24:mi");
        assertInvalidDateTime("03 31 1971", "mm dd yy");

        assertInvalidDateTime("1971/03/31", "yy/mm/dd");
        assertInvalidDateTime("03/31/1971 15:45", "mm/dd/yy hh24:mi");
        assertInvalidDateTime("03/31/1971", "mm/dd/yy");

        assertInvalidDateTime("1971.03.31", "yy.mm.dd");
        assertInvalidDateTime("03.31.1971 15:45", "mm.dd.yy hh24:mi");
        assertInvalidDateTime("03.31.1971", "mm.dd.yy");
        
        assertInvalidDateTime("19710331", "yymmdd");
        assertInvalidDateTime("03311971 15:45", "mmddyy hh24:mi");
        assertInvalidDateTime("03311971", "mmddyy");
        
        assertInvalidDateTime("19710331 125900", "yymmdd hhmiss");
        assertInvalidDateTime("01312005123456", "mmddyyThhmiss");
    }
    
    public void testFourDigitYearPatternRejectsTwoYearString() throws Exception {
        assertInvalidDateTime("71-03-31", "yyyy-mm-dd");
        assertInvalidDateTime("03-31-71 15:45", "mm-dd-yyyy hh24:mi");
        assertInvalidDateTime("03-31-71", "mm-dd-yyyy");

        assertInvalidDateTime("71 03 31", "yyyy mm dd");
        assertInvalidDateTime("03 31 71 15:45", "mm dd yyyy hh24:mi");
        assertInvalidDateTime("03 31 71", "mm dd yyyy");

        assertInvalidDateTime("71/03/31", "yyyy/mm/dd");
        assertInvalidDateTime("03/31/71 15:45", "mm/dd/yyyy hh24:mi");
        assertInvalidDateTime("03/31/71", "mm/dd/yyyy");

        assertInvalidDateTime("71.03.31", "yyyy.mm.dd");
        assertInvalidDateTime("03.31.71 15:45", "mm.dd.yyyy hh24:mi");
        assertInvalidDateTime("03.31.71", "mm.dd.yyyy");
        
        assertInvalidDateTime("710331", "yyyymmdd");
        assertInvalidDateTime("033171 15:45", "mmddyyyy hh24:mi");
        assertInvalidDateTime("033171", "mmddyyyy");
        
        assertInvalidDateTime("710331 125900", "yyyymmdd hhmiss");
        assertInvalidDateTime("013105123456", "mmddyyyyhhmiss");
        
        assertInvalidDateTime("013105T123456", "mmddyyyyThhmiss");
    }
        
    public void testDuplicatedMnemonicInPatternRejected() throws AxionException {
        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "20042004", "yyyyyyyy"}));
        try {
            function.evaluate(dec);
            fail("Expected AxionException(22007) for bad format string");
        } catch (AxionException e) {
            if (!"22007".equals(e.getSQLState())) {
                fail ("Expected AxionException(22007) for bad format string");
            }
            // expected
        }

        dec.setRow(new SimpleRow(new Object[] { "1212", "mmmm" } ));
        try {
            function.evaluate(dec);
            fail("Expected AxionException(22007) for bad format string");
        } catch (AxionException e) {
            if (!"22007".equals(e.getSQLState())) {
                fail ("Expected AxionException(22007) for bad format string");
            }
            // expected
        }        
    }

    private void assertValidDateTime(String testItem, String formatStr) throws AxionException {
        Literal format = new Literal(formatStr);

        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { testItem, format}));
        Object returnVal = function.evaluate(dec);

        assertEquals("Expected true for valid string " + testItem + "; ", Boolean.TRUE, returnVal);
    }

    private void assertInvalidDateTime(String testItem, String formatStr) throws AxionException {
        Literal format = new Literal(formatStr);

        ColumnIdentifier timestampLbl = new ColumnIdentifier("dateStr");
        ColumnIdentifier formatLbl = new ColumnIdentifier("formatLiteral");

        IsValidDateTimeFunction function = new IsValidDateTimeFunction();
        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { testItem, format}));
        
        Object returnVal = function.evaluate(dec);
        assertEquals("Expected false for invalid string " + testItem + "; ", Boolean.FALSE, returnVal);
    }
}