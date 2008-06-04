/*
 * $Id: TestDatePartFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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
import java.text.DateFormatSymbols;
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
 * Unit tests for DatePart function.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Jonathan Giron
 */
public class TestDatePartFunction extends BaseFunctionTest {

    public TestDatePartFunction(String testName) {
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
        return new DatePartFunction();
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite(TestDatePartFunction.class);
        return suite;
    }
    
    public void testValidDateParts() throws Exception {
        DatePartFunction function = new DatePartFunction();
        ColumnIdentifier datePartLbl = new ColumnIdentifier("datePartLbl");
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestampLbl");
        
        function.addArgument(datePartLbl);
        function.addArgument(timestampLbl);
        
        HashMap map = new HashMap();
        map.put(datePartLbl, new Integer(0));                
        map.put(timestampLbl, new Integer(1));

        // Sample time: 2004-04-01 20:30:58.450Z
        TimestampType.setTimeZone("GMT");
        final Timestamp input = new Timestamp(
                34L * 365L * 24L * 60L * 60L * 1000L //year
                + 9L * 24L * 60L * 60L * 1000L + // leap years
                                           // '72,'76,'80,'84,'88,'92,'96, 2000,
                                           // 2004
                + (31L + 28L + 31L) * 24L * 60L * 60L * 1000L // April 01
                + ((20L * 60L) + 30L) * 60L * 1000L // 8:30 PM
                + 58 * 1000L // 58 seconds
                + 450L // 450 milliseconds
        );
        
        final Timestamp inputAm = new Timestamp(
                34L * 365L * 24L * 60L * 60L * 1000L //year
                + 9L * 24L * 60L * 60L * 1000L + // leap years
                                           // '72,'76,'80,'84,'88,'92,'96, 2000,
                                           // 2004
                + (31L + 28L + 31L) * 24L * 60L * 60L * 1000L // April 01
                + ((8L * 60L) + 30L) * 60L * 1000L // 8:30 AM
                + 58 * 1000L // 58 seconds
                + 450L // 450 milliseconds
        );        
        
        RowDecorator dec = new RowDecorator(map);
        
        // Test year.
        Literal part = new Literal("YEAR");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        Object returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "2004", returnVal);
        
        // Test month.
        part = new Literal("MONTH");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "04", returnVal);
        
        // Test year.
        part = new Literal("DAY");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "01", returnVal);
        
        // Test hour (single- or double-digit, 0-23)
        part = new Literal("HOUR");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "20", returnVal);

        dec.setRow(new SimpleRow(new Object[] { part, inputAm }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "8", returnVal);
        
        // Test hour (padded double-digit, 01-12)
        part = new Literal("HOUR12");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "08", returnVal);

        dec.setRow(new SimpleRow(new Object[] { part, inputAm }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "08", returnVal);        
        
        // Test hour (padded double-digit, 0-23)
        part = new Literal("HOUR24");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "20", returnVal);

        dec.setRow(new SimpleRow(new Object[] { part, inputAm }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "08", returnVal);        
        
        // Test minute.
        part = new Literal("MINUTE");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "30", returnVal);
        
        // Test second.
        part = new Literal("SECOND");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "58", returnVal);
        
        // Test millisecond.
        part = new Literal("MILLISECOND");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "450", returnVal);
        
        // Test am/pm.
        part = new Literal("AMPM");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "PM", returnVal);
        
        // Test quarter.
        part = new Literal("QUARTER");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "2", returnVal);
        
        // Test week.
        part = new Literal("WEEK");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "14", returnVal);
        
        // Test three-letter month abbreviation.
        part = new Literal("MONTH3");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "APR", returnVal);
        
        // Test full month.
        part = new Literal("MONTHFULL");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "APRIL", returnVal);
        
        // Test day of week (number).
        part = new Literal("WEEKDAY");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", "5", returnVal);
        
        // Test three-letter day of week abbreviation
        part = new Literal("WEEKDAY3");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", 
                new DateFormatSymbols().getShortWeekdays()[5].toUpperCase(), returnVal);
        
        // Test full day of week.
        part = new Literal("WEEKDAYFULL");
        dec.setRow(new SimpleRow(new Object[] { part, input }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for date-part input '"
                + part.toString() + "'; ", 
                new DateFormatSymbols().getWeekdays()[5].toUpperCase(), returnVal);
        
    }
    
    public void testNullTimestampInputYieldsNull() {
        DatePartFunction function = new DatePartFunction();
        ColumnIdentifier datePartLbl = new ColumnIdentifier("datePartLbl");
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestampLbl");
        
        function.addArgument(datePartLbl);
        function.addArgument(timestampLbl);
        
        HashMap map = new HashMap();
        map.put(datePartLbl, new Integer(0));                
        map.put(timestampLbl, new Integer(1));
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { "DAY", null }));
        try {
            assertNull("Expected null return for DatePart with null for timestamp input.", 
                function.evaluate(dec));
        } catch (Exception e) {
            fail("Null for timestamp input of DatePart should not have thrown an Exception: " + e);
        }
    }
    
    public void testNullDatePartThrowsException() {
        DatePartFunction function = new DatePartFunction();
        ColumnIdentifier datePartLbl = new ColumnIdentifier("datePartLbl");
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestampLbl");
        
        function.addArgument(datePartLbl);
        function.addArgument(timestampLbl);
        
        HashMap map = new HashMap();
        map.put(datePartLbl, new Integer(0));                
        map.put(timestampLbl, new Integer(1));
        
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { null, new Timestamp(0) }));
        
        try {
            function.evaluate(dec);
            fail("Null value for date-part should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - do nothing.
        }             
    }
    
    public void testInvalidPartIdentThrowsException() {
        DatePartFunction function = new DatePartFunction();
        ColumnIdentifier datePartLbl = new ColumnIdentifier("datePartLbl");
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestampLbl");
        
        function.addArgument(datePartLbl);
        function.addArgument(timestampLbl);
        
        HashMap map = new HashMap();
        map.put(datePartLbl, new Integer(0));                
        map.put(timestampLbl, new Integer(1));
        
        RowDecorator dec = new RowDecorator(map);
        
        try {
            dec.setRow(new SimpleRow(new Object[] { "CRAP", new Timestamp(0) }));
            function.evaluate(dec);
            fail("Invalid value for date-part should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - do nothing.
        }     

        try {
            dec.setRow(new SimpleRow(new Object[] { "DAY", new Object[] {"string"} }));
            function.evaluate(dec);
            fail("Invalid value for date-source should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - do nothing.
        }
        
        try {
            dec.setRow(new SimpleRow(new Object[] { "DAY", "string" }));
            function.evaluate(dec);
            fail("Invalid value for date-source should have thrown an Exception");
        } catch (IllegalArgumentException e) {
            // Desired effect - do nothing.
        } catch (AxionException e) {
           
        }

    }

    public void testMakeNewInstance() {
        DatePartFunction function = new DatePartFunction();
        assertTrue(function.makeNewInstance() instanceof DatePartFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
    
    public void testInvalid() throws Exception {
        DatePartFunction function = new DatePartFunction();
        assertTrue(! function.isValid());
    }

    public void testValid() throws Exception {
        DatePartFunction function = new DatePartFunction();
        function.addArgument(new ColumnIdentifier("foo"));
        function.addArgument(new ColumnIdentifier("bar"));
        assertTrue(function.isValid());
    }
}
