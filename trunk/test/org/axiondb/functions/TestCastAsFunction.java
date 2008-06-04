/*
 * $Id: TestCastAsFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004-2005 Axion Development Team.  All rights reserved.
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
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TimeZone;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BooleanType;
import org.axiondb.types.CharacterType;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.FloatType;
import org.axiondb.types.IntegerType;
import org.axiondb.types.BigIntType;
import org.axiondb.types.TimestampType;

/**
 * @author Ritesh Adval
 * @author Ahimanikya Satapathy
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 */
public class TestCastAsFunction extends BaseFunctionTest {
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", TimestampType.LOCALE);
    public TestCastAsFunction(String testName) {
        super(testName);
    }

    public void setUp() throws Exception {
        super.setUp();
        TimestampType.setTimeZone("GMT");
        TimeZone tZone = TimeZone.getTimeZone("GMT");
        if (tZone != null) {
            ISO_DATE_FORMAT.setTimeZone(tZone);
        }
    }

    /**
     * @see org.axiondb.functions.BaseFunctionTest#makeFunction()
     */
    protected ConcreteFunction makeFunction() {
        return new CastAsFunction();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestCastAsFunction.class);
        return suite;
    }

    public void testMakeNewInstance() {
        assertNotNull((new CastAsFunction()).makeNewInstance());
    }

    public void testIsValid() {
        ConcreteFunction function = makeFunction();
        assertFalse(function.isValid());
        function.addArgument(new ColumnIdentifier("COLUMN"));
        assertFalse(function.isValid());
        function.addArgument(new Literal("DATATYPE", new TimestampType()));
        assertTrue(function.isValid());
        function.addArgument(new ColumnIdentifier("COLUMN"));
        assertFalse(function.isValid());
    }

    public void testCastStringAsDate() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new TimestampType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        //Nov 30 1973 GMT
        Timestamp result = new Timestamp(3 * 365 * 24 * 60 * 60 * 1000L + //year
            1 * 24 * 60 * 60 * 1000L + // leap years '72
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30) * 24 * 60 * 60 * 1000L // Nov
        // 30
        );

        String toConvert = ISO_DATE_FORMAT.format(result);
        dec.setRow(new SimpleRow(new Object[] { toConvert, "timestamp"}));

        assertEquals(result, function.evaluate(dec));
    }

    public void testCastStringAsDateFailure() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new TimestampType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        //Dec 12 1973 GMT
        Timestamp date = new Timestamp(3 * 365 * 24 * 60 * 60 * 1000L + //year
            1 * 24 * 60 * 60 * 1000L + // leap years '72
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 12) * 24 * 60 * 60 * 1000L // Dec
        // 12
        );

        //Dec 12 1973 00:00:01 GMT
        Timestamp result = new Timestamp(3 * 365 * 24 * 60 * 60 * 1000L + //year
            1 * 24 * 60 * 60 * 1000L + // leap years '72
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 12) * 24 * 60 * 60 * 1000L + // Dec
            // 12
            1 * 1000L);

        dec.setRow(new SimpleRow(new Object[] { date.toString(), "timestamp"}));
        assertNotSame(result, function.evaluate(dec));
    }

    public void testCastBigIntAsDate() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new TimestampType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        //Dec 31 1999 11:43:54.334 GMT
        Timestamp result = new Timestamp(29 * 365 * 24 * 60 * 60 * 1000L + //year
            7 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) * 24 * 60 * 60 * 1000L + // Dec
            // 31
            11 * 60 * 60 * 1000L + //Hours
            43 * 60 * 1000L + //minutes
            54 * 1000L + //seconds
            334 //millisecond
        );

        dec.setRow(new SimpleRow(new Object[] { new Long(result.getTime()), "timestamp"}));

        assertEquals(result, function.evaluate(dec));
    }

    public void testCastDateAsBigInt() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new BigIntType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        //Dec 31 1999 11:43:54.334 GMT
        Timestamp date = new Timestamp(29 * 365 * 24 * 60 * 60 * 1000L + //year
            7 * 24 * 60 * 60 * 1000L + // leap years '72,'76,'80,'84,'88,'92,'96
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) * 24 * 60 * 60 * 1000L + // Dec
            // 31
            11 * 60 * 60 * 1000L + //Hours
            43 * 60 * 1000L + //minutes
            54 * 1000L + //seconds
            334 //millisecond
        );

        dec.setRow(new SimpleRow(new Object[] { date, "bigint"}));
        Long result = new Long(date.getTime());

        assertEquals(result, function.evaluate(dec));
    }

    public void testCastDateAsString() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new CharacterVaryingType(21));

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        //Nov 11 1973 12:11:23 GMT
        Timestamp date = new Timestamp(3 * 365 * 24 * 60 * 60 * 1000L + //year
            1 * 24 * 60 * 60 * 1000L + // leap years '72
            (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 11) * 24 * 60 * 60 * 1000L + // Nov
            // 31
            12 * 60 * 60 * 1000L + //Hours
            11 * 60 * 1000L + //minutes
            23 * 1000L //seconds
        );

        dec.setRow(new SimpleRow(new Object[] { date, "varchar"}));
        String result = date.toString();
        assertEquals(result, function.evaluate(dec));
        assertTrue(function.getDataType() instanceof CharacterVaryingType);
    }

    public void testCastFloatAsInteger() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new IntegerType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { new Float(125.7), "integer"}));
        Integer result = new Integer(125);
        assertEquals(result, function.evaluate(dec));
        assertTrue(function.getDataType() instanceof IntegerType);
    }

    public void testCastIntegerAsFloat() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new FloatType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { new Integer(125), "float"}));
        Float result = new Float(125.0);
        assertEquals(result, function.evaluate(dec));
    }

    public void testInvalidCast() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new FloatType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { null, "float"}));
        assertNull(function.evaluate(dec));

        try {
            dec.setRow(new SimpleRow(new Object[] { "string", "float"}));
            function.evaluate(dec);
            fail("Expected Conversion exception");
        } catch (AxionException e) {
            // expected
        }
    }

    public void testCastNull() throws Exception {
        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal dataType = new Literal("DATATYPE", new IntegerType());

        function.addArgument(column);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { null, "integer"}));
        assertNull(function.evaluate(dec));
        assertTrue(function.getDataType() instanceof IntegerType);
    }

    public void testCastBooleanToCharWithTruncation() throws Exception {
        final int length = 5;

        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("COLUMN");
        Literal literal = new Literal(Boolean.FALSE, new BooleanType());
        Literal dataType = new Literal("DATATYPE", new CharacterType(length));

        function.addArgument(literal);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { literal, "boolean" }));
        assertEquals("false", function.evaluate(dec));
        assertTrue(function.getDataType() instanceof CharacterType);
    }

    public void testCastCharLiteralToCharWithTruncation() throws Exception {
        final String literalStr = "1234567890";
        final int downcastLength = 5;

        CastAsFunction function = new CastAsFunction();
        ColumnIdentifier column = new ColumnIdentifier("column");
        Literal literal = new Literal(literalStr, new CharacterType(literalStr.length()));
        Literal dataType = new Literal("datatype", new CharacterType(downcastLength));

        function.addArgument(literal);
        function.addArgument(dataType);

        HashMap map = new HashMap();
        map.put(column, new Integer(0));
        map.put(dataType, new Integer(1));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { literalStr, "varchar"}));
        assertEquals(literalStr.substring(0, downcastLength), function.evaluate(dec));
        assertTrue(function.getDataType() instanceof CharacterType);
    }
}