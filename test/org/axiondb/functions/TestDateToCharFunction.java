/*
 * $Id: TestDateToCharFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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
 * Unit tests for DateToChar function.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $ 
 * @author Jonathan Giron
 */
public class TestDateToCharFunction extends BaseFunctionTest {

    public TestDateToCharFunction(String testName) {
        super(testName);
    }

    public static final void main(String[] args) {
        junit.textui.TestRunner.run(suite());
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
        return new DateToCharFunction();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDateToCharFunction.class);
        return suite;
    }

    public void testValidFormats() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);

        // Sample time: 2004-04-01 12:00:00Z
        TimestampType.setTimeZone("GMT");
        final Timestamp input = new Timestamp(34 * 365 * 24 * 60 * 60 * 1000L + //year
                9 * 24 * 60 * 60 * 1000L + // leap years
                                           // '72,'76,'80,'84,'88,'92,'96, 2000,
                                           // 2004
                (31 + 28 + 31) * 24 * 60 * 60 * 1000L // April 01
        );
        
        // Test YYYY-MM-DD date only format, with dash separators
        Literal format = new Literal("yyyy-mm-dd");
        dec.setRow(new SimpleRow(new Object[] { input, format }));

        Object returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "2004-04-01", returnVal);

        // Test US civilian time only format, with colon separators
        format = new Literal("hh:mi:ss am");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "12:00:00 AM", returnVal);

        // Test NATO/military time only format, with colon separators and
        // milliseconds
        format = new Literal("hh24:mi:ss.ff");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "00:00:00.000", returnVal);

        // Test compact ISO 8601 format
        format = new Literal("yyyymmddThh24miss");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "20040401T000000", returnVal);

        // Test three-letter month abbreviation
        format = new Literal("dd mon yyyy hh24:mi:ss.ff");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "01 APR 2004 00:00:00.000",
                returnVal);

        // Test German-style date format, with dot separators
        format = new Literal("dd.mm.yyyy");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "01.04.2004", returnVal);

        // Test US-style date format with slash separators
        format = new Literal("mm/dd/yyyy");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "04/01/2004", returnVal);
        
        // Test pattern with all capital letters
        format = new Literal("YYYYMMDDTHH24MISS");
        dec.setRow(new SimpleRow(new Object[] { input, format }));
        returnVal = function.evaluate(dec);
        assertEquals("Expected valid return for format "
                + format.toString() + "; ", "20040401T000000", returnVal);
    }
    
    public void testNullDateExprYieldsNull() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("bcdef");
        dec.setRow(new SimpleRow(new Object[] { null, format }));
        
        Object returnVal = function.evaluate(dec);
        assertNull("Null value for date-expr input should have returned null", 
                returnVal);
    }

    public void testInvalidDateExprThrowsException() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

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
            fail("Invalid value for date-expr input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
        
        try {
            dec.setRow(new SimpleRow(new Object[] { new Object[] {"abcdef"}, format }));
            function.evaluate(dec);
            fail("Invalid value for date-expr input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testInvalidFormatThrowsException() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        Literal format = new Literal("bcdef");
        dec.setRow(new SimpleRow(new Object[] { new Timestamp(0), format }));
        try {
            function.evaluate(dec);
            fail("Invalid value for format input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }       
    }

    public void testNullFormatThrowsException() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        ColumnIdentifier timestampLbl = new ColumnIdentifier("timestamp");
        ColumnIdentifier formatLbl = new ColumnIdentifier("format");

        function.addArgument(timestampLbl);
        function.addArgument(formatLbl);

        HashMap map = new HashMap();
        map.put(timestampLbl, new Integer(0));
        map.put(formatLbl, new Integer(1));

        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] {
                new Timestamp(System.currentTimeMillis()), null }));
        try {
            function.evaluate(dec);
            fail("Null for format input should have thrown an Exception");
        } catch (AxionException e) {
            // Desired effect - ignore.
        }
    }
    
    public void testMakeNewInstance() {
        DateToCharFunction function = new DateToCharFunction();
        assertTrue(function.makeNewInstance() instanceof DateToCharFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
    
    public void testInvalid() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        assertTrue(! function.isValid());
    }

    public void testValid() throws Exception {
        DateToCharFunction function = new DateToCharFunction();
        function.addArgument(new ColumnIdentifier("foo"));
        function.addArgument(new ColumnIdentifier("bar"));
        assertTrue(function.isValid());
    }

}