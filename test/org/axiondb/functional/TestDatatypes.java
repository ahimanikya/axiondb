/*
 * $Id: TestDatatypes.java,v 1.3 2008/02/21 13:00:26 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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
package org.axiondb.functional;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.types.BigDecimalType;

/**
 *
 * @author Jonathan Giron
 * @version $Revision: 1.3 $
 */
public class TestDatatypes extends AbstractFunctionalTest {

    public TestDatatypes(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDatatypes.class);
    }

    public void testCustomDataType() throws Exception {
        final BigDecimal first = new BigDecimal("12.56");
        final BigDecimal second = new BigDecimal("1.2");
        
        _rset = _stmt.executeQuery("select 1 as col1, cast('12.56' as NUMERIC(4,2)) as col2, cast('1.23' as NUMERIC(3,1)) as col3");
        assertTrue(_rset.next());
         
        BigDecimal result = (BigDecimal) _rset.getObject(2);
        assertEquals(first, result);
        
        result = (BigDecimal) _rset.getObject(3);
        assertEquals(second, result);        
    }

    public void testNumericWithDefaultScale() throws Exception {
        _stmt.executeUpdate("create table num_def_test (myvalue numeric(10))");
        _stmt.executeUpdate("insert into num_def_test values (10.40)");
        
        _rset = _stmt.executeQuery("select myvalue from num_def_test");
        assertTrue(_rset.next());
        assertEquals(new BigDecimal("10"), _rset.getObject(1));
        
    }
    
    public void testNumericWithZeroScale() throws Exception {
        final int scale = 0;
        final String originalValueStr = "10.41"; 
        
        _stmt.executeUpdate("create table num_zero_test (myvalue numeric(10, " + scale + "))");
        _stmt.executeUpdate("insert into num_zero_test values (" + originalValueStr  + ")");
        
        _rset = _stmt.executeQuery("select myvalue from num_zero_test");
        assertTrue(_rset.next());
        assertEquals(new BigDecimal(originalValueStr).setScale(scale, BigDecimalType.ROUNDING_RULE), 
            _rset.getObject(1));
        
        final String addValueStr = "10.4444";
        _rset = _stmt.executeQuery("select myvalue + " + addValueStr + " from num_zero_test");
        assertTrue(_rset.next());
        assertEquals(new BigDecimal("20.4444"), _rset.getObject(1));
        
        _stmt.executeUpdate("update num_zero_test set myvalue = myvalue + " + addValueStr);
        _rset = _stmt.executeQuery("select myvalue from num_zero_test");
        assertTrue(_rset.next());
        assertEquals(new BigDecimal("20"), _rset.getObject(1));
    }
    
    public void testNumericWithNonzeroScale() throws Exception {
        final int scale = 3;
        final BigDecimal[] values = new BigDecimal[] {
                new BigDecimal("10"),
                new BigDecimal("10.1"),
                new BigDecimal("10.22"),
                new BigDecimal("10.333"),
                new BigDecimal("10.4444"),
                new BigDecimal("11")
        };
        final String addValueStr = "10.4444";
        
        BigDecimal[] scaledValues = new BigDecimal[values.length];
        BigDecimal[] addValues = new BigDecimal[values.length];
        BigDecimal[] updateValues = new BigDecimal[values.length];
        
        _stmt.executeUpdate("create table num_nonzero_test (myvalue numeric(10, " + scale + "))");
        for (int i = 0; i < values.length; i++) {
            scaledValues[i] = values[i].setScale(scale, BigDecimalType.ROUNDING_RULE);
            addValues[i] = scaledValues[i].add(new BigDecimal(addValueStr));
            updateValues[i] = addValues[i].setScale(scale, BigDecimalType.ROUNDING_RULE);
            assertEquals(1, _stmt.executeUpdate("insert into num_nonzero_test values (" + values[i].toString() + ")"));
        }
        
        _rset = _stmt.executeQuery("select myvalue from num_nonzero_test");
        for (int i = 0; i < scaledValues.length; i++) {
            assertTrue(_rset.next());
            assertEquals(scaledValues[i], _rset.getObject(1));
        }
        
        _rset = _stmt.executeQuery("select myvalue + " + addValueStr + " from num_nonzero_test");
        for (int i = 0; i < addValues.length; i++) {
            assertTrue(_rset.next());
            assertEquals(addValues[i], _rset.getObject(1));
        }
        
        _stmt.executeUpdate("update num_nonzero_test set myvalue = myvalue + " + addValueStr);
        _rset = _stmt.executeQuery("select myvalue from num_nonzero_test");
        for (int i = 0; i < updateValues.length; i++) {
            assertTrue(_rset.next());
            assertEquals(updateValues[i], _rset.getObject(1));
        }
        
    }
    
    public void testNumericValueAddition() throws Exception {
        final BigDecimal first = new BigDecimal("2.4444");
        final BigDecimal second = new BigDecimal("1.05");
        
        _rset = _stmt.executeQuery("select " + first + " + " + second);
        assertTrue(_rset.next());
         
        BigDecimal result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.add(second), result);
        
        assertEquals(Math.max(first.scale(), second.scale()), result.scale());

    }
    
    public void testNumericValueSubtraction() throws Exception {
        final BigDecimal first = new BigDecimal("2.4444");
        final BigDecimal second = new BigDecimal("1.05");

        _rset = _stmt.executeQuery("select " + first + " - " + second);
        assertTrue(_rset.next());
         
        BigDecimal result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.subtract(second), result);
        
        assertEquals(Math.max(first.scale(), second.scale()), result.scale());
    }
    
    public void testNumericValueMultiplication() throws Exception {
        final BigDecimal first = new BigDecimal("2.4444");
        final BigDecimal second = new BigDecimal("1.05");
        
        // Multiplication
        _rset = _stmt.executeQuery("select " + first + " * " + second);
        assertTrue(_rset.next());
         
        BigDecimal result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.multiply(second), result);
        
        assertEquals(first.scale() + second.scale(), result.scale());
    }
        
    public void testNumericValueDivision() throws Exception {
        final BigDecimal first = new BigDecimal("2.4444");
        final BigDecimal second = new BigDecimal("1.05");

        _rset = _stmt.executeQuery("select " + first + " / " + second);
        assertTrue(_rset.next());
         
        BigDecimal result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.divide(second, BigDecimalType.ROUNDING_RULE), result);
        
        assertEquals(first.scale(), result.scale());
    }
    
    public void testNumericChainedArithmetic() throws Exception {
        final BigDecimal first = new BigDecimal("2.4444");
        final BigDecimal second = new BigDecimal("1.05");
        final BigDecimal third = new BigDecimal("3.1234567");
        final BigDecimal fourth = new BigDecimal("2.0");
        
        _rset = _stmt.executeQuery("select " + first + " + " + second + " - " + third + " - " + fourth);
        assertTrue(_rset.next());
        
        BigDecimal result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.add(second).subtract(third).subtract(fourth), result);
        assertEquals(Math.max(Math.max(Math.max(first.scale(), second.scale()), third.scale()), fourth.scale()), 
            result.scale());

        
        _rset = _stmt.executeQuery("select " + first + " * " + second + " * " + third);
        assertTrue(_rset.next());
        
        result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.multiply(second).multiply(third), result);
        assertEquals(first.scale() + second.scale() + third.scale(), result.scale());
        
        
        _rset = _stmt.executeQuery("select " + first + " * " + second + " - " + third + " * " + fourth);
        assertTrue(_rset.next());
        
        result = (BigDecimal) _rset.getObject(1);
        assertEquals(first.multiply(second).subtract(third.multiply(fourth)), result);
        assertEquals(Math.max(first.scale() + second.scale(), third.scale() + fourth.scale()), result.scale());
    }
    
    public void testNegativeNullArithmetic() throws Exception {
        _rset = _stmt.executeQuery("select null + 1.05");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select 1.05 + null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null + null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null - 1.05");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select 1.05 - null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null - null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null * 1.05");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select 1.05 * null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null * null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null / 1.05");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select 1.05 / null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
        
        _rset = _stmt.executeQuery("select null / null");
        assertTrue(_rset.next());
        assertEquals(null, _rset.getBigDecimal(1));
        assertEquals(null, _rset.getObject(1));
        assertTrue(_rset.wasNull());
    }

    public void testPrecisionOverflow() throws Exception {
        _stmt.executeUpdate("create table smallval (id numeric(4, 2))");
        
        assertEquals(1, _stmt.executeUpdate("insert into smallval values (0.01)"));
        assertEquals(1, _stmt.executeUpdate("insert into smallval values (0.1)"));
        assertEquals(1, _stmt.executeUpdate("insert into smallval values (1)"));
        assertEquals(1, _stmt.executeUpdate("insert into smallval values (10)"));
        
        try {
            _stmt.executeUpdate("insert into smallval values (100)");
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            // TODO:  When BigDecimalType throws SQLException(22003), test expected for that sqlstate code.
        }
        
        _stmt.executeUpdate("create table fractions (id numeric(2,2))");
        assertEquals(1, _stmt.executeUpdate("insert into fractions values (0)"));
        assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.10)"));
        assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.512345)"));
        assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.99)"));
        assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.994)"));
        
        try {
            assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.995)"));
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            assertEquals("Expected SQLException 22003 - inserted value exceeds specified precision",
                "22003", expected.getSQLState());
        }
        
        try {
            assertEquals(1, _stmt.executeUpdate("insert into fractions values (0.99 + 0.005)"));
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            assertEquals("Expected SQLException 22003 - inserted value exceeds specified precision",
                "22003", expected.getSQLState());
        }

        try {
            assertEquals(1, _stmt.executeUpdate("insert into fractions values (1.0)"));
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            assertEquals("Expected SQLException 22003 - inserted value exceeds specified precision",
                "22003", expected.getSQLState());
        }

        try {
            assertEquals(1, _stmt.executeUpdate("insert into fractions values (1)"));
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            assertEquals("Expected SQLException 22003 - inserted value exceeds specified precision",
                "22003", expected.getSQLState());
        }
        
        _stmt.executeUpdate("create table intonly (id numeric(2))");
        
        assertEquals(1, _stmt.executeUpdate("insert into intonly values (1)"));
        assertEquals(1, _stmt.executeUpdate("insert into intonly values (10)"));
        assertEquals(1, _stmt.executeUpdate("insert into intonly values (20.1)"));
        
        try {
            assertEquals(1, _stmt.executeUpdate("insert into intonly values (100)"));
            fail("Expected SQLException - inserted value exceeds specified precision.");
        } catch (SQLException expected) {
            // Expected
            assertEquals("Expected SQLException 22003 - inserted value exceeds specified precision",
                "22003", expected.getSQLState());
        }
        
        _rset = _stmt.executeQuery("select * from intonly");
        
        assertTrue(_rset.next());
        assertEquals("1", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("10", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("20", _rset.getString(1));
        
        assertFalse(_rset.next());
    }
    
    public void testNegativeVarcharInvalidSize() {
        try {
            _stmt.executeUpdate("create table error (myvalue varchar(a))");
            fail("Expected SQLException - value must be a non-negative integer.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(-1))");
            fail("Expected SQLException - value must be a positive integer.");
        } catch (SQLException expected) {
            // Expected
        }
    }
    
    public void testNegativeNumericInvalidPrecisionScale() {
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(a))");
            fail("Expected SQLException - precision value must be a positive integer.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(-1))");
            fail("Expected SQLException - precision value must be a positive integer.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(0))");
            fail("Expected SQLException - precision value must be a positive integer.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(5, zzz))");
            fail("Expected SQLException - scale value must be a non-negative integer.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _stmt.executeUpdate("create table error (myvalue numeric(5, -1))");
            fail("Expected SQLException - scale value must be a non-negative integer.");
        } catch (SQLException expected) {
            // Expected
        }
    }
    
    public void testVarcharType() throws Exception {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < 255; i++) {
            buf.append(i % 10);
        }
    
        final String str_5 = "12345";
        final String str_10 = "1234567890";
        final String str_20 = "12345678901234567890";
        final String str_20_pad = "12345678901234567890        ";
        final String str_255 = buf.toString();
        
        _stmt.executeUpdate("create table vc_test (vc_20 varchar(20), vc_255 varchar(255))");
        ResultSet dbmdRs = _conn.getMetaData().getColumns(null, null, "vc_test", "vc_20");
        assertTrue(dbmdRs.next());
        assertEquals(20, dbmdRs.getInt("COLUMN_SIZE"));
        
        dbmdRs = _conn.getMetaData().getColumns(null, null, "vc_test", "vc_255");
        assertTrue(dbmdRs.next());
        assertEquals(255, dbmdRs.getInt("COLUMN_SIZE"));
        
        assertEquals(1, _stmt.executeUpdate(
            "insert into vc_test values ('" + str_20 + "', '" + str_255 + "')"));

        assertEquals(1, _stmt.executeUpdate(
            "insert into vc_test values (concat('" + str_5 + "', '" + str_10 + "'), '" + str_255 + "')"));

        assertEquals(1, _stmt.executeUpdate(
            "insert into vc_test values ('" + str_20_pad + "', '" + str_255 + "')"));
        
        _rset = _stmt.executeQuery("select vc_20, vc_255 from vc_test");
        
        ResultSetMetaData rsmd = _rset.getMetaData();
        assertEquals(20, rsmd.getPrecision(1));
        assertEquals(255, rsmd.getPrecision(2));
        
        assertTrue(_rset.next());
        assertEquals(str_20, _rset.getObject(1));
        assertEquals(str_255, _rset.getObject(2));
        
        assertTrue(_rset.next());
        assertEquals(str_5 + str_10, _rset.getObject(1));
        assertEquals(str_255, _rset.getObject(2));
        
        assertTrue(_rset.next());
        assertEquals(str_20, _rset.getObject(1));
        assertEquals(str_255, _rset.getObject(2));

        assertFalse(_rset.next());
        
        _rset = _stmt.executeQuery("select length(vc_20), length(vc_255) from vc_test");
        
        assertTrue(_rset.next());
        assertEquals(20, _rset.getInt(1));
        assertEquals(255, _rset.getInt(2));
        
        assertTrue(_rset.next());
        assertEquals(15, _rset.getInt(1));
        assertEquals(255, _rset.getInt(2));
        
        assertTrue(_rset.next());
        assertEquals(20, _rset.getInt(1));
        assertEquals(255, _rset.getInt(2));
        
        assertFalse(_rset.next());
    }
    
    public void testNegativeVarcharOverflow() throws Exception {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < 255; i++) {
            buf.append(i % 10);
        }
    
        final String str_10 = "1234567890";
        final String str_20 = "12345678901234567890";
        final String str_255 = buf.toString();
        final String overFlowStr = buf.append("_").toString();
        
        _stmt.executeUpdate("create table vc_test (vc_20 varchar(20), vc_255 varchar(255))");
        
        try {
            _stmt.executeUpdate(
                "insert into vc_test values ('12345678901234567890', '" + overFlowStr + "')");
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }

        assertEquals(1, _stmt.executeUpdate(
            "insert into vc_test values ('" + str_20 + "', '" + str_255 + "')"));
        try {
            assertEquals(1, _stmt.executeUpdate(
                "insert into vc_test values (concat('" + str_20 + "', '" + str_10 + "'), '" + str_255 + "')"));
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }
        
        try {
            assertEquals(1, _stmt.executeUpdate(
                "update vc_test set vc_20 = concat(vc_20, '" + str_20 + "')"));
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }
    }
    
    public void testCharType() throws Exception {
        final String str_5 = "12345";
        final String str_10 = "1234567890";
        
        _stmt.executeUpdate("create table char_test (char_10 char(10))");

        ResultSet dbmdRs = _conn.getMetaData().getColumns(null, null, "char_test", "char_10");
        assertTrue(dbmdRs.next());
        assertEquals(10, dbmdRs.getInt("COLUMN_SIZE"));
        
        assertEquals(1, _stmt.executeUpdate("insert into char_test values ('" + str_5 + "')"));
        assertEquals(1, _stmt.executeUpdate("insert into char_test values ('" + str_10 + "')"));
        assertEquals(1, _stmt.executeUpdate(
            "insert into char_test values (concat('" + str_5 + "', '" + str_5 + "'))"));

        
        _rset = _stmt.executeQuery("select char_10 from char_test");
        
        ResultSetMetaData rsmd = _rset.getMetaData();
        assertEquals(10, rsmd.getPrecision(1));
        
        assertTrue(_rset.next());
        assertEquals(padValue(str_5, 10, ' '), _rset.getObject(1));
        
        assertTrue(_rset.next());
        assertEquals(padValue(str_10, 10, ' '), _rset.getObject(1));
        
        assertTrue(_rset.next());
        assertEquals(padValue(str_5 + str_5, 10, ' '), _rset.getObject(1));
        
        assertFalse(_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select length(char_10) from char_test");
        
        assertTrue(_rset.next());
        assertEquals(10, _rset.getInt(1));
        
        assertTrue(_rset.next());
        assertEquals(10, _rset.getInt(1));
        
        assertTrue(_rset.next());
        assertEquals(10, _rset.getInt(1));
        
        assertFalse(_rset.next());
    }
    
    public void testNegativeCharOverflow() throws Exception {
        final String str_10 = "1234567890";
        final String str_20 = "12345678901234567890";

        _stmt.executeUpdate("create table char_test (char_10 char(10))");
        try {
            _stmt.executeUpdate("insert into char_test values ('" + str_20 + "')");
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }
        
        try {
            assertEquals(1, _stmt.executeUpdate(
                "insert into char_test values (concat('" + str_10 + "', '" + str_10 + "'))"));
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }        
        
        assertEquals(1, _stmt.executeUpdate("insert into char_test values ('" + str_10 + "')"));
        try {
            _stmt.executeUpdate("update char_test set char_10 = '" + str_20 + "'");
            fail("Expected SQLException with SQLSTATE 22001 - string data, right truncation.");
        } catch (SQLException expected) {
            assertEquals("Expected SQLException with SQLSTATE 22001 - string data, right truncation.", 
                "22001", expected.getSQLState());
        }
    }
    
    public void testDataTypes_DelimitedFlatfile() throws Exception {
        final String eol = "\n";

        File data = new File(".", "chartest_delim.csv");
        FileWriter out = new FileWriter(data);
        out.write("ID,NAME,CNTDOWN,VAL" + eol); // Header
        out.write("1,a         ,1234567890,1.0" + eol); // 1
        out.write("2,bb        ,123456789 ,2.0" + eol); // 2
        out.write("3,ccc       ,12345678  ,3.0" + eol); // 3
        out.write("4,dddd      ,1234567   ,4.0" + eol); // 4
        out.write("5,eeeee     ,123456    ,5.0" + eol); // 5
        out.write("6,ffffff    ,12345     ,6.0" + eol); // 6
        out.write("7,ggggggg   ,1234      ,7.0" + eol); // 7
        out.write("8,hhhhhhhh  ,123       ,8.0" + eol); // 8
        out.write("9,iiiiiiiii ,12        ,9.0" + eol); // 9
        out.write("10,jjjjjjjjjj,1         ,10.0" + eol); // 10
        out.write("11,invalid row,invalid row,11.0" + eol); // 11 - invalid (char fields too long)
        out.write("12,          ,          ," + eol); // 12
        out.close();
        
        _stmt.executeUpdate("create external table chartest_delim ( id numeric(2), name char(10), "
            + "cntdown char(10), val numeric(3,1)) organization (loadtype='delimited' "
            + "filename='./chartest_delim.csv' isfirstlineheader='true' "
            + "recorddelimiter='\\n')");

        try {
            _rset = _stmt.executeQuery("select count(id) from chartest_delim");
            assertTrue(_rset.next());
            assertEquals(11, _rset.getInt(1));
            _rset.close();
            
            final String cntdownTemplate = "1234567890";
            _rset = _stmt.executeQuery("select id, name, cntdown, val from chartest_delim");
            for (int i = 0; i < 10; i++) {
                StringBuffer namebuf = new StringBuffer(10);
                for (int j = 0; j < (i + 1) % 11; j++) {
                    String charVal = String.valueOf((char) ('a' + i)); 
                    namebuf.append(charVal);
                }
                
                assertTrue(_rset.next());
                assertEquals(new BigDecimal(i + 1), _rset.getBigDecimal(1));
                assertEquals(padValue(namebuf.toString(), 10, ' '), _rset.getString(2));

                String expectedStr = padValue(cntdownTemplate.substring(0, 10 - i), 10, ' ');
                assertEquals(expectedStr, _rset.getString(3));
                
                BigDecimal expected = new BigDecimal(String.valueOf(i + 1) + ".0");
                expected = expected.setScale(1);
                assertEquals(expected, _rset.getBigDecimal(4));
            }

            assertTrue(_rset.next());
            assertEquals(new BigDecimal(12), _rset.getBigDecimal(1));
            assertNull(_rset.getString(2));
            assertTrue(_rset.wasNull());
            assertNull(_rset.getString(3));
            assertTrue(_rset.wasNull());
            assertEquals(0,_rset.getInt(4));
            assertTrue(_rset.wasNull());

            
            assertFalse(_rset.next());
            _rset.close();
            
            _rset = _stmt.executeQuery("select count(id) from chartest_delim");
            assertTrue(_rset.next());
            assertEquals(11, _rset.getInt(1));
            _rset.close();
        } finally {
            try {
                if (_rset != null) {
                    _rset.close();
                }
                
                if (_stmt != null) {
                    _stmt.execute("drop table chartest_delim");
                    _stmt.close();
                }
            } catch (SQLException ignore) {
                // ignore
            }
            data.delete();
        }
    }
    
    private String padValue(String raw, int fieldSize, char padChar) {
        StringBuffer buf = new StringBuffer(raw);
        for (int i = 0; i < fieldSize - raw.length(); i++) {
            buf.append(padChar);
        }
        return buf.toString();
    }
}