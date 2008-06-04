/*
 * $Id: TestSpecials.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Ritesh Adval
 */
public class TestSpecials extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestSpecials(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestSpecials.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests
    public void testAddIndexAfterInsertUpdateDelete() throws Exception {
        _stmt.execute("create table foo (id int, val varchar(10))");
        _stmt.executeUpdate("insert into foo (id, val) values ( 1, 'one' )");
        _stmt.executeUpdate("insert into foo (id, val) values ( 2, 'two' )");
        _stmt.executeUpdate("insert into foo (id, val) values ( 3, null )");
        _stmt.executeUpdate("delete from foo where id = 2");
        _stmt.executeUpdate("update foo set val = 'three' where id = 3");
        _stmt.execute("create index foo_ndx on foo (id)");
        assertResult("one", "select val from foo where id = 1");
        assertResult("three", "select val from foo where id = 3");
    }

    public void testBindTypes() throws Exception {
        byte[] bytes = new byte[] { (byte) 1, (byte) 2, (byte) 3};

        final long now = System.currentTimeMillis();
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        java.util.Date expDate = cal.getTime();
        java.sql.Date date = new java.sql.Date(now);
        java.sql.Time time = new java.sql.Time(now);
        java.sql.Timestamp timestamp = new java.sql.Timestamp(now);
        URL url = new URL("http://127.0.0.1/");

        _stmt.execute("create table foo ( boolv boolean, byteav varbinary(3), bytev byte, shortv short, intv integer, longv bigint, floatv float, doublev float, strv varchar(100), nullv boolean, datev date, timev time, tsv timestamp, urlv varchar(100) )");
        PreparedStatement pstmt = _conn.prepareStatement("insert into foo values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");
        pstmt.setBoolean(1, true);
        pstmt.setBytes(2, bytes);
        pstmt.setByte(3, (byte) 3);
        pstmt.setShort(4, (short) 4);
        pstmt.setInt(5, 5);
        pstmt.setLong(6, 6L);
        pstmt.setFloat(7, 7.0f);
        pstmt.setDouble(8, 8.0d);
        pstmt.setString(9, "string");
        pstmt.setNull(10, Types.BOOLEAN);
        pstmt.setDate(11, date);
        pstmt.setTime(12, time);
        pstmt.setTimestamp(13, timestamp);
        pstmt.setURL(14, url);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        _rset = _stmt.executeQuery("select * from foo");
        assertTrue(_rset.next());
        assertEquals(true, _rset.getBoolean(1));
        assertTrue(Arrays.equals(bytes, _rset.getBytes(2)));
        assertEquals((byte) 3, _rset.getByte(3));
        assertEquals((short) 4, _rset.getShort(4));
        assertEquals(5, _rset.getInt(5));
        assertEquals(6L, _rset.getLong(6));
        assertEquals(7.0f, _rset.getFloat(7), 0f);
        assertEquals(8.0d, _rset.getDouble(8), 0d);
        assertEquals("string", _rset.getString(9));
        assertNull(_rset.getObject(10));
        assertEquals(expDate, _rset.getDate(11));
        assertEquals(expDate, _rset.getDate("datev"));
        assertEquals(time, _rset.getTime(12));
        assertEquals(time, _rset.getTime("timev"));
        assertEquals(timestamp, _rset.getTimestamp(13));
        assertEquals(timestamp, _rset.getTimestamp("tsv"));
        assertEquals(url, _rset.getURL(14));
        assertEquals(url, _rset.getURL("urlv"));

    }

    public void testSQLExceptionOnBadSyntax() throws Exception {
        try {
            _stmt.execute("xyzzy XYZZY xyzzy");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testFunctionNamesAreNotReservedWords() throws Exception {
        _stmt.execute("create table max ( min int )");
        _stmt.execute("insert into max values ( 1 )");
        _stmt.execute("select min from max");
        _stmt.execute("drop table max");

        _stmt.execute("create table datediff ( dateadd int )");
        _stmt.execute("insert into datediff values ( 1 )");
        _stmt.execute("select dateadd from datediff");
        _stmt.execute("drop table datediff");
    }

    public void testGetURL() throws Exception {
        ResultSet rset = _stmt.executeQuery("select 'http://localhost:8080/'");
        assertTrue(rset.next());
        assertEquals(new URL("http://localhost:8080/"), rset.getURL(1));
        assertTrue(!rset.next());
        rset.close();
    }

    public void testSimpleExplain() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("explain select * from FOO where NUM < 3");

        assertTrue(rset.next());
        assertEquals("Unmod(MemoryTable(FOO))", rset.getString(1));
        assertTrue(rset.next());
        assertEquals("Filtering(LESSTHAN((FOO).NUM,3))", rset.getString(1));
        assertTrue(!rset.next());
        rset.close();
    }

    public void testLiteralBooleanInWhere() throws Exception {
        _stmt.execute("create table foo ( id int )");
        _stmt.executeUpdate("insert into foo values ( 1 )");

        assertOneRow("select id from foo where TRUE");
        assertOneRow("select id from foo where true");
        assertOneRow("select id from foo where true and true");
        assertOneRow("select id from foo where true or true");
        assertOneRow("select id from foo where true or false");
        assertOneRow("select id from foo where true and not(false)");
        assertOneRow("select id from foo where not(false)");
        assertOneRow("select id from foo where not(true and false)");

        assertNoRows("select id from foo where true and false");
        assertNoRows("select id from foo where false or false");
        assertNoRows("select id from foo where false");
        assertNoRows("select id from foo where not(true)");
    }

    public void testClearBindVariableInFunction() throws Exception {
        PreparedStatement stmt = _conn.prepareStatement("select upper(?)");
        stmt.setString(1, "test");
        assertResult("TEST", stmt.executeQuery());
        stmt.clearParameters();
        stmt.setString(1, "test2");
        assertResult("TEST2", stmt.executeQuery());
        stmt.close();
    }

    public void testSequenceWithBadPseduoColumn() throws Exception {
        _stmt.execute("create sequence foo_id_seq");
        try {
            _stmt.executeQuery("select foo_id_seq.xyzzy");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testSequenceCurrvalInWhereClause() throws Exception {
        _stmt.execute("create sequence foo_id_seq");
        _stmt.execute("create table foo ( id int default foo_id_seq.nextval, val varchar(10) )");
        _conn.setAutoCommit(false);
        assertEquals(1, _stmt.executeUpdate("insert into foo ( val ) values ('value')"));
        _rset = _stmt.executeQuery("select id, val from foo where id = foo_id_seq.currval");
        assertTrue(_rset.next());
        assertNotNull(_rset.getString(1));
        assertEquals("value", _rset.getString(2));
        assertTrue(!_rset.next());
    }

    public void testSequenceNextvalOutsideOfTransaction() throws Exception {
        _stmt.execute("create sequence foo_seq start with 0");
        for (int i = 0; i < 3; i++) {
            _rset = _stmt.executeQuery("select foo_seq.nextval");
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }
    
    // TODO: As per ANSI 2003 spec, All (NEXT VALUE FOR expressions) specifyed of
    // the same sequence generator within a single statement evaluate to the
    // same value for a given row. Make this test pass.
    //public void testSequenceMultipleNextvalOutsideOfTransaction() throws Exception {
    //    _stmt.execute("create sequence foo_seq start with 0");
    //    for (int i = 0; i < 3; i++) {
    //        _rset = _stmt.executeQuery("select foo_seq.nextval, foo_seq.nextval");
    //        assertTrue(_rset.next());
    //        assertEquals(i, _rset.getInt(1));
    //        assertEquals(i, _rset.getInt(2));
    //        assertTrue(!_rset.next());
    //        _rset.close();
    //    }
    //}

    public void testSequenceCurrvalOutsideOfTransaction() throws Exception {
        _stmt.execute("create sequence foo_seq start with 0");

        try {
            _rset = _stmt.executeQuery("select foo_seq.currval");
            assertTrue(_rset.next());
            _rset.getInt(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        for (int i = 0; i < 3; i++) {
            _rset = _stmt.executeQuery("select foo_seq.nextval, foo_seq.currval");
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals(i, _rset.getInt(2));
            assertTrue(!_rset.next());
            _rset.close();

            try {
                _rset = _stmt.executeQuery("select foo_seq.currval");
                assertTrue(_rset.next());
                _rset.getInt(1);
                fail("Expected SQLException");
            } catch (SQLException e) {
                // expected
            }
        }
    }

    public void testSequenceNextvalInsideOfTransaction() throws Exception {
        _stmt.execute("create sequence foo_seq start with 0");
        _conn.setAutoCommit(false);
        for (int i = 0; i < 3; i++) {
            _rset = _stmt.executeQuery("select foo_seq.nextval");
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    public void testSequenceCurrvalInsideOfTransaction() throws Exception {
        _stmt.execute("create sequence foo_seq start with 0");
        _conn.setAutoCommit(false);
        for (int i = 0; i < 3; i++) {
            _rset = _stmt.executeQuery("select foo_seq.nextval, foo_seq.currval");
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals(i, _rset.getInt(2));
            assertTrue(!_rset.next());
            _rset.close();

            _rset = _stmt.executeQuery("select foo_seq.currval");
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    public void testCheckFileState() throws Exception {
        _rset = _stmt.executeQuery("CHECKFILESTATE");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNotNull(_rset.getString(1));
        _rset.close();
    }

    public void test_select_star_metadata() throws Exception {
        createTableFoo();
        populateTableFoo();
        _rset = _stmt.executeQuery("select * from FOO");
        ResultSetMetaData mdata = _rset.getMetaData();
        assertNotNull(mdata);
        assertTrue("NUM".equalsIgnoreCase(mdata.getColumnName(1)));
        assertTrue("STR".equalsIgnoreCase(mdata.getColumnName(2)));
        assertTrue("NUMTWO".equalsIgnoreCase(mdata.getColumnName(3)));
        assertTrue("FOO".equalsIgnoreCase(mdata.getTableName(1)));
        assertTrue("FOO".equalsIgnoreCase(mdata.getTableName(2)));
        assertTrue("FOO".equalsIgnoreCase(mdata.getTableName(3)));
        assertNotNull(mdata.getColumnTypeName(1));
        assertNotNull(mdata.getColumnTypeName(2));
        assertNotNull(mdata.getColumnTypeName(3));
    }

    public void testCreateJavaObjectTable() throws Exception {
        _stmt.execute("create table foo( key_object java_object, entry_object java_object )");
    }

    public void testFirst() throws Exception {
        createTableFoo();
        populateTableFoo();

        _rset = _stmt.executeQuery("select str from foo");
        assertNotNull("Should have been able to create ResultSet", _rset);

        assertTrue(_rset.isBeforeFirst());
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertTrue(!_rset.isBeforeFirst());
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertEquals(String.valueOf(i), val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());

        _rset.first();

        assertTrue(!_rset.isBeforeFirst());
        for (int i = 1; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertTrue(!_rset.isBeforeFirst());
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertEquals(String.valueOf(i), val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());

        _rset.close();
    }

    public void testBeforeFirstAfterLastEtc() throws Exception {
        createTableFoo();
        populateTableFoo();

        _rset = _stmt.executeQuery("select str from foo");
        assertNotNull("Should have been able to create ResultSet", _rset);

        assertTrue(_rset.isBeforeFirst());
        assertTrue(!_rset.isFirst());
        assertTrue(!_rset.isAfterLast());
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertTrue(!_rset.isBeforeFirst());
            if (0 == i) {
                assertTrue(_rset.isFirst());
            } else {
                assertTrue(!_rset.isFirst());
            }
            
            assertTrue(!_rset.isAfterLast());
            if (NUM_ROWS_IN_FOO - 1 == i) {
                assertTrue(_rset.isLast());
            }
            
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertEquals(String.valueOf(i), val);
        }
        assertTrue(!_rset.isFirst());
        assertTrue(!_rset.isBeforeFirst());
        
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        assertTrue(_rset.isAfterLast());

        _rset.beforeFirst();

        assertTrue(_rset.isBeforeFirst());
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertTrue(!_rset.isBeforeFirst());
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertEquals(String.valueOf(i), val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());

        _rset.close();
    }

    public void testResultSetRelative() throws Exception {
        createTableFoo();
        populateTableFoo();

        _rset = _stmt.executeQuery("select str from foo");

        assertTrue(_rset.next());
        for (int i = 0; i < NUM_ROWS_IN_FOO + 1; i++) {
            assertTrue(_rset.relative(0));
        }
        assertTrue(!_rset.relative(NUM_ROWS_IN_FOO + 1));
    }

    public void test_select_literal_without_from() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select 'Literal'";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("ResultSet should contain more rows", _rset.next());
        String val = _rset.getString(1);
        assertNotNull("Returned String should not be null", val);
        assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        assertEquals("Returned string should equal \"Literal\".", "Literal", val);
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_non_literal_without_from() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select foo.num";
        try {
            _stmt.executeQuery(sql);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void test_select_bindvar_without_from() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select ?");
        pstmt.setInt(1, 1);
        _rset = pstmt.executeQuery();
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertTrue(!_rset.next());
        pstmt.clearParameters();
        pstmt.setInt(1, 2);
        _rset = pstmt.executeQuery();
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        pstmt.close();
    }

    public void test_select_bindvar_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select ? from FOO");

        pstmt.setString(1, "bound");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals("bound", _rset.getString(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();

        pstmt.clearParameters();
        pstmt.setInt(1, 2);
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(2, _rset.getInt(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void testBindVariableAsFunctionArgument() throws Exception {
        createTableFoo();
        populateTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select NUM, UPPER(?) from FOO where UPPER(STR) = UPPER(?)");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setString(1, "will become upper");
            pstmt.setString(2, String.valueOf(i));
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals("WILL BECOME UPPER", _rset.getString(2));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            pstmt.clearParameters();
        }
        pstmt.close();
    }

    public void testGetBigDecimal() throws Exception {
        createTableFoo();
        populateTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select NUM from FOO");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(new BigDecimal(i), _rset.getBigDecimal(1));
        }
        assertTrue(!_rset.next());
        pstmt.close();
        _rset.close();
    }

    public void testInsertBigDecimal() throws Exception {
        _stmt.execute("create table bigdectable ( \"value\" number )");
        PreparedStatement pstmt = _conn.prepareStatement("insert into bigdectable values ( ? )");
        // insert as integer
        {
            pstmt.setInt(1, Integer.MAX_VALUE);
            assertEquals(1, pstmt.executeUpdate());
            ResultSet rset = _stmt.executeQuery("select \"value\" from bigdectable");
            assertTrue(rset.next());
            assertEquals(Integer.MAX_VALUE, rset.getInt(1));
            assertEquals(new BigDecimal(String.valueOf(Integer.MAX_VALUE)), rset.getBigDecimal(1));
            assertEquals(new BigDecimal(String.valueOf(Integer.MAX_VALUE)), rset.getBigDecimal("VALUE"));
            assertTrue(!rset.next());
            rset.close();
            assertEquals(1, _stmt.executeUpdate("delete from bigdectable"));
        }
        // insert as string
        {
            pstmt.setString(1, String.valueOf(Integer.MAX_VALUE));
            assertEquals(1, pstmt.executeUpdate());
            ResultSet rset = _stmt.executeQuery("select \"value\" from bigdectable");
            assertTrue(rset.next());
            assertEquals(Integer.MAX_VALUE, rset.getInt(1));
            assertEquals(new BigDecimal(String.valueOf(Integer.MAX_VALUE)), rset.getBigDecimal(1));
            assertTrue(!rset.next());
            rset.close();
            assertEquals(1, _stmt.executeUpdate("delete from bigdectable"));
        }
        // insert as bigdecimal
        {
            pstmt.setBigDecimal(1, new BigDecimal(String.valueOf(Integer.MAX_VALUE)));
            assertEquals(1, pstmt.executeUpdate());
            ResultSet rset = _stmt.executeQuery("select \"value\" from bigdectable");
            assertTrue(rset.next());
            assertEquals(Integer.MAX_VALUE, rset.getInt(1));
            assertEquals(new BigDecimal(String.valueOf(Integer.MAX_VALUE)), rset.getBigDecimal(1));
            assertTrue(!rset.next());
            rset.close();
            assertEquals(1, _stmt.executeUpdate("delete from bigdectable"));
        }
        // insert big decimal value as string
        {
            String value = "1234567890123456789012";
            pstmt.setString(1, value);
            assertEquals(1, pstmt.executeUpdate());
            ResultSet rset = _stmt.executeQuery("select \"value\" from bigdectable");
            assertTrue(rset.next());
            assertEquals(value, rset.getString(1));
            assertEquals(new BigDecimal(value), rset.getBigDecimal(1));
            assertTrue(!rset.next());
            rset.close();
            assertEquals(1, _stmt.executeUpdate("delete from bigdectable"));
        }
        // insert big decimal value as BigDecimal
        {
            String value = "1234567890123456789012";
            pstmt.setBigDecimal(1, new BigDecimal(value));
            assertEquals(1, pstmt.executeUpdate());
            ResultSet rset = _stmt.executeQuery("select \"value\" from bigdectable");
            assertTrue(rset.next());
            assertEquals(value, rset.getString(1));
            assertEquals(new BigDecimal(value), rset.getBigDecimal(1));
            assertTrue(!rset.next());
            rset.close();
            assertEquals(1, _stmt.executeUpdate("delete from bigdectable"));
        }
        pstmt.close();
    }

    public void testNumberType() throws Exception {
        _stmt.execute("create table XYZZY ( MYCOL NUMBER(10,2) )");
        assertEquals(1, _stmt.executeUpdate("insert into XYZZY values ( 1 )"));
        assertEquals(1, _stmt.executeUpdate("insert into XYZZY values ( 1.01 )"));
    }

    public void testSysdate() throws Exception {
        createAndPopulateDual();
        ResultSet rset = _stmt.executeQuery("select SYSDATE, NOW()");
        assertTrue(rset.next());
        Date date1 = rset.getDate(1);
        Date date2 = rset.getDate(2);
        assertNotNull(date1);
        assertNotNull(date2);
        assertTrue(!date1.after(date2));
        // TODO: it'd be nice to assert
        // date1.equals(date2)
    }

    public void testMultipleTransactionsOnASingleConnection() throws Exception {
        createTableFoo();
        _conn.setAutoCommit(false);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, 'xyzzy', null)");
        for (int i = 0; i < 10; i++) {
            assertRowCount(i, _stmt);
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
            assertRowCount(i + 1, _stmt);
            _conn.commit();
            assertRowCount(i + 1, _stmt);
        }
        pstmt.close();
    }

    public void testColumnsByName() throws Exception {
        _stmt.execute("create table foo ( col1 int, col2 float, col3 varchar(10), col4 varchar(10), col5 boolean )");
        _stmt.executeUpdate("insert into foo values ( 17, 3.14159, 'seventeen', null, true )");
        _rset = _stmt.executeQuery("select col1, col2, col3, col4, col5 from foo");
        assertEquals(1, _rset.findColumn("COL1"));
        assertEquals(2, _rset.findColumn("COL2"));
        assertEquals(3, _rset.findColumn("COL3"));
        assertEquals(4, _rset.findColumn("COL4"));
        assertEquals(5, _rset.findColumn("COL5"));

        try {
            // since next() has not yet been called, this should fail
            _rset.getInt("COL1"); 
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        assertTrue(_rset.next());
        assertEquals(17, _rset.getInt("COL1"));
        assertEquals(17, _rset.getShort("COL1"));
        assertEquals(17, _rset.getLong("COL1"));
        assertEquals(17, _rset.getByte("COL1"));
        assertEquals(3.14159, _rset.getFloat("COL2"), 0.000001);
        assertEquals(3.14159, _rset.getDouble("COL2"), 0.000001);
        assertEquals("seventeen", _rset.getString("COL3"));
        assertEquals("seventeen", _rset.getObject("COL3"));
        assertEquals(0, _rset.getInt("COL4"));
        assertEquals(0, _rset.getShort("COL4"));
        assertEquals(0, _rset.getLong("COL4"));
        assertEquals(0, _rset.getByte("COL4"));
        assertEquals(0, _rset.getFloat("COL4"), 0.000001);
        assertEquals(0, _rset.getDouble("COL4"), 0.000001);
        assertNull(_rset.getString("COL4"));
        assertNull(_rset.getObject("COL4"));
        assertEquals(true, _rset.getBoolean("COL5"));
        try {
            _rset.findColumn("THIS_COLUMN_DOES_NOT_EXIST");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        _rset.close();
    }

    public void testResultSetGetStatement() throws Exception {
        createAndPopulateDual();
        ResultSet rset = _stmt.executeQuery("select * from dual");
        assertEquals(_stmt, rset.getStatement());
        rset.close();
    }

    public void testLiteralSelect() throws Exception {
        createAndPopulateDual();
        ResultSet rset = _stmt.executeQuery("select 1,2,3 from dual");
        assertTrue(rset.next());
        assertEquals(2, rset.getInt(2));
        rset.close();
    }

    public void testCastAsInSelect() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 varchar(4), Category int )");
        stmt.execute("CREATE TABLE text2 (Id varchar(1), Text2 varchar(4), Category varchar(1))");
        stmt.executeUpdate("INSERT INTO text2 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text2 Values(2, 'John', 0)");

        assertEquals(2, stmt.executeUpdate("insert into text1 select cast(id as int) id, " + " text2, cast(Category as int) cat from text2"));

        ResultSet rset = _stmt.executeQuery("select * from text1");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));
        assertEquals(0, rset.getInt(3));

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("John", rset.getString(2));
        assertEquals(0, rset.getInt(3));

        assertTrue(!rset.next());

        try {
            stmt.executeQuery("select cast(id as bogus) from text2");
            fail("Exception expected: bad datatype");
        } catch (Exception e) {
            // expected
        }

        rset.close();
        stmt.close();
    }

    public void testBasicGroupBy() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("create table orders ( who varchar(10), what varchar(10), cost integer)");
        assertEquals(1, stmt.executeUpdate("insert into orders values ( 'Joe', 'Book', 10 )"));
        assertEquals(1, stmt.executeUpdate("insert into orders values ( 'Joe', 'Book', 20 )"));
        assertEquals(1, stmt.executeUpdate("insert into orders values ( 'Joe', 'CD', 20 )"));
        assertEquals(1, stmt.executeUpdate("insert into orders values ( 'Jane', 'Book', 20 )"));
        assertEquals(1, stmt.executeUpdate("insert into orders values ( 'Jane', 'CD', 10 )"));
        {
            ResultSet rset = stmt.executeQuery("select who, sum(cost) from orders group by who order by who");
            assertTrue(rset.next());
            assertEquals("Jane", rset.getString(1));
            assertEquals(30, rset.getInt(2));
            assertTrue(rset.next());
            assertEquals("Joe", rset.getString(1));
            assertEquals(50, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }
        {
            ResultSet rset = stmt.executeQuery("select what, sum(cost) from orders " + " group by what order by what");
            assertTrue(rset.next());
            assertEquals("Book", rset.getString(1));
            assertEquals(50, rset.getInt(2));
            assertTrue(rset.next());
            assertEquals("CD", rset.getString(1));
            assertEquals(30, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }
        {
            ResultSet rset = stmt.executeQuery("select cost, count(*) from" + " orders group by cost order by cost desc");
            assertTrue(rset.next());
            assertEquals(20, rset.getInt(1));
            assertEquals(3, rset.getInt(2));
            assertTrue(rset.next());
            assertEquals(10, rset.getInt(1));
            assertEquals(2, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }
        {
            ResultSet rset = stmt.executeQuery("select S.what, S.totalcost from" + " (select what, sum(cost) totalcost from orders "
                + "group by what order by what) AS S");
            assertTrue(rset.next());
            assertEquals("Book", rset.getString(1));
            assertEquals(50, rset.getInt(2));
            assertTrue(rset.next());
            assertEquals("CD", rset.getString(1));
            assertEquals(30, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }

        {
            try {
                stmt.executeQuery("select cost, count(*) from orders");
                fail("Expected Exception");
            } catch (Exception ex) {
                // expected
            }

            try {
                stmt.executeQuery("select count(*), who || what  from orders");
                fail("Expected Exception");
            } catch (Exception ex) {
                // expected
            }

            try {
                stmt.executeQuery("select who || what, sum(cost) from orders group by who order by who");
                fail("Expected Exception");
            } catch (Exception ex) {
                // expected
            }

            try {
                stmt.executeQuery("select what, sum(cost) from orders group by who order by who");
                fail("Expected Exception");
            } catch (Exception ex) {
                // expected
            }

            try {
                stmt.executeQuery("select what, cost from orders group by what");
                fail("Expected Exception");
            } catch (Exception ex) {
                // expected
            }
        }

        {
            ResultSet rset = stmt.executeQuery("select sum(subtotal), count(*) from (select sum(cost) subtotal, count(*) subcount from orders group by what order by what) AS S");
            assertTrue(rset.next());
            assertEquals(80, rset.getInt(1));
            assertEquals(2, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }

        {
            ResultSet rset = stmt.executeQuery("select S.totalcost, S.totalcount from (select sum(cost) totalcost, count(*) totalcount from orders) AS S");
            assertTrue(rset.next());
            assertEquals(80, rset.getInt(1));
            assertEquals(5, rset.getInt(2));
            assertTrue(!rset.next());
            rset.close();
        }

        stmt.close();
    }

    public void testGroupByWithWhere() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 varchar(5), Category int, PRIMARY KEY ( Id ) )");
        stmt.execute("CREATE INDEX text1_text ON text1(text1)");
        stmt.executeUpdate("INSERT INTO text1 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(2, 'John', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(3, 'Bill', 1)");
        stmt.executeUpdate("INSERT INTO text1 Values(4, 'dave', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(7, 'John', 1)");

        ResultSet rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1 where Category=0 group BY text1");

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(!rset.next());

    }

    public void testDropSysIndex() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 Varchar(5), Category int, PRIMARY KEY ( Id ) )");

        ResultSet rset = stmt.executeQuery("select index_name from AXION_INDEX_INFO where table_name = 'TEXT1'");

        assertTrue(rset.next());
        String indexName = rset.getString(1);
        assertTrue(!rset.next());

        try {
            stmt.execute("DROP INDEX " + indexName);
            fail("Expected Exception SYS generated Index can't be droped");
        } catch (Exception e) {
            // expected
        }

        // make sure the index still exist.
        rset = stmt.executeQuery("select index_name from AXION_INDEX_INFO where table_name = 'TEXT1'");

        assertTrue(rset.next());
        assertEquals(indexName, rset.getString(1));
        assertTrue(!rset.next());

    }

    public void testGroupByWithoutWhere() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 Varchar(5), Category int, PRIMARY KEY ( Id ) )");
        stmt.execute("CREATE INDEX text_text ON text1(text1)");
        stmt.executeUpdate("INSERT INTO text1 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(2, 'John', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(3, 'Bill', 1)");
        stmt.executeUpdate("INSERT INTO text1 Values(4, 'dave', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(7, 'John', 1)");

        ResultSet rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1  group BY text1");

        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        assertEquals("Bill", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(9, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(!rset.next());
    }

    public void testOrderByWithWhere() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 varchar(5), Category int, PRIMARY KEY ( Id ) )");
        stmt.execute("CREATE INDEX text_text ON text1(text1)");
        stmt.executeUpdate("INSERT INTO text1 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(2, 'John', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(3, 'Bill', 1)");
        stmt.executeUpdate("INSERT INTO text1 Values(4, 'dave', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(7, 'John', 1)");

        ResultSet rset = stmt.executeQuery("SELECT * FROM text1 WHERE Category = 0 ORDER BY text1");

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("John", rset.getString(2));
        assertEquals(0, rset.getInt(3));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));
        assertEquals(0, rset.getInt(3));

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));
        assertEquals(0, rset.getInt(3));

        assertTrue(!rset.next());
    }

    public void testGroupByOrderByWithWhere() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 varchar(5), Category int, PRIMARY KEY ( Id ) )");
        stmt.execute("CREATE INDEX text_text ON text1(text1)");
        stmt.executeUpdate("INSERT INTO text1 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(2, 'John', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(3, 'Bill', 1)");
        stmt.executeUpdate("INSERT INTO text1 Values(4, 'dave', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(7, 'John', 1)");

        ResultSet rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1 where Category=0 group BY text1 order by text1");

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(!rset.next());

        //desc order
        rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1 where Category=0 group BY text1 order by text1 desc");
        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(!rset.next());
    }

    public void testGroupByOrderByWithoutWhere() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("CREATE TABLE text1 (Id int, Text1 varchar(5), Category int, PRIMARY KEY ( Id ) )");
        stmt.execute("CREATE INDEX text_text ON text1(text1)");
        stmt.executeUpdate("INSERT INTO text1 Values(1, 'Mike', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(2, 'John', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(3, 'Bill', 1)");
        stmt.executeUpdate("INSERT INTO text1 Values(4, 'dave', 0)");
        stmt.executeUpdate("INSERT INTO text1 Values(7, 'John', 1)");

        ResultSet rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1 group BY text1 order by text1");

        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        assertEquals("Bill", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(9, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(!rset.next());

        //desc order
        rset = stmt.executeQuery("SELECT sum(id), text1 FROM text1 group BY text1 order by text1 desc");

        assertTrue(rset.next());
        assertEquals(4, rset.getInt(1));
        assertEquals("dave", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("Mike", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(9, rset.getInt(1));
        assertEquals("John", rset.getString(2));

        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        assertEquals("Bill", rset.getString(2));

        assertTrue(!rset.next());

    }

    private void create_table_x() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists x ");
        stmt.execute("create table x(id int, name varchar(3))");
        assertEquals(1, stmt.executeUpdate("insert into x values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into x values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into x values(3,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into x values(4,'bbb')"));
        stmt.close();
    }
    
    public void testSelectSameColumnTwice() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();
        
        ResultSet rset = stmt.executeQuery("select id colid, (name || 'ss') as colname2, name colname from x");
        assertTrue(rset.next());
        
        rset.close();
        stmt.close();
    }

    public void testBasicSubSelect() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("create table y(id int, name varchar(3))");
        assertEquals(4, stmt.executeUpdate("insert into y select * from x"));

        // exists with sub-select
        ResultSet rset = stmt.executeQuery("select * from x where exists (select id from x)");
        assertTrue(rset.next());
        
        // not exists with sub-select
        rset = stmt.executeQuery("select * from x where not exists (select y.id from y where x.id = y.id)");
        assertTrue(!rset.next());
        
        // not exists with sub-select
        rset = stmt.executeQuery("select * from x where not exists (select id from x)");
        assertTrue(!rset.next());

        // in with sub-select
        rset = stmt.executeQuery("select * from x where id in (select id from x)");
        assertTrue(rset.next());
        
        // in with sub-select
        rset = stmt.executeQuery("select * from x where id in (select y.id from y where x.id = y.id)");
        assertTrue(rset.next());

        // UPDATE t1 SET column2 = (SELECT MAX(column1) FROM t1);

        // A correlated subquery is a subquery that contains a
        // reference to a table that also appears in the outer query

        rset = stmt.executeQuery("select * from x where id = (select y.id from y where x.id = y.id)");
        assertTrue(rset.next());

        // scalar sub-select column visibility test
        rset = stmt.executeQuery(" select x.id, (select (select s.name from y s where y.id = x.id) from y, x where y.id = x.id and y.id = 2) from x");
        assertTrue(rset.next());
        
        // object not found
        try {
            stmt.executeUpdate("insert into y select * from x where id = (select y.id from y where xxx.id = y.id)");
            fail("Expected SQLException");
        } catch (SQLException e) {
            assertEquals("Expected Column not found exception", "42703", e.getSQLState());
        }

        // too many values - test
        try {
            rset = stmt.executeQuery(" select x.id,  (select (select * from y s where y.id = x.id) "
                + " from y, x where y.id = x.id and y.id = 2) from x");
            rset.next();
            rset.getInt(1);
            rset.getString(2);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // single-row subquery returns more than one row - test
        try {
            rset = stmt.executeQuery(" select x.id, (select (select s.name from y s where y.id = x.id) from y, x where y.id = x.id and y.id = x.id) from x");
            rset.next();
            rset.getInt(1);
            rset.getString(2);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        rset = stmt.executeQuery("select * from (select id, name from y) as x1 right outer join "
            + " (select id xid  from x) as y1 on x1.id = y1.xid");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select * from x where name = (select 'aaa')");
        assertTrue(rset.next());

        rset = stmt.executeQuery("SELECT UPPER((SELECT distinct name FROM x where id =2)) FROM y;");
        assertTrue(rset.next());

        // Pretending to be a Table
        rset = stmt.executeQuery("SELECT foo  FROM (SELECT 1 AS foo) AS tbl;");
        assertTrue(rset.next());

        // sub-select as FromNode
        rset = stmt.executeQuery("select  * from (select * from x) s");
        assertTrue(rset.next());

        // sub-select as FromNode
        rset = stmt.executeQuery("select  s.id from (select * from x) s");
        assertTrue(rset.next());

        // sub-select as FromNode
        rset = stmt.executeQuery("select  * from (select * from x ) s where s.id = 2");
        assertTrue(rset.next());

        // sub-select as FromNode
        rset = stmt.executeQuery("select  * from (select * from x where id not in (select * from x)) s where s.id = 2");
        assertTrue(!rset.next());
        
        stmt.executeUpdate("create view v1 as select  * from (select * from x)");
        rset = stmt.executeQuery("select * from v1 where v1.id in (select id from x)");
        assertTrue(rset.next());

        // not in with sub-select
        rset = stmt.executeQuery("select * from x where id not in (select id from x)");
        assertTrue(!rset.next());
        
        // not in with sub-select
        rset = stmt.executeQuery("select * from x where 10 not in (select id from x)");
        assertTrue(rset.next());

        // aggregate function as salar value with sub-select
        rset = stmt.executeQuery("select * from x where id = (select max(id) from x)");
        assertTrue(rset.next());

        // Test scalar sub-select visibility in where cond using alias
        rset = stmt.executeQuery("select  x.id, (select distinct y.id from y where y.id =2) myid from x where x.id = myid");
        assertTrue(rset.next());

        // duplicate column test
        try {
            stmt.executeUpdate("create view v2 as select * from x,y");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // ambiguous column test
        try {
            stmt.executeUpdate("create view v2 as select id,name from x,y");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        stmt.executeUpdate("create view v2 as select t.id tid, t.name tname  from (select * from v1) t");
        rset = stmt.executeQuery("select v2.tid, v2.tname from v2 where v2.tid in (select id from x)");
        assertTrue(rset.next());

        // ambiguous column test
        try {
            stmt.executeUpdate("insert into y select * from x,y");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        stmt.execute("create table z(id1 int, name1 varchar(3))");
        assertEquals(4, stmt.executeUpdate("insert into z select * from x"));

        // no ambiguous column but too many values
        try {
            stmt.executeUpdate("insert into y select * from x,z");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // no ambiguous column but too many values
        try {
            stmt.executeUpdate("select (select (select (select 1, 2)))");
            rset.next();
            rset.getObject(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        assertEquals(16, stmt.executeUpdate("insert into y select x.* from x,z"));
        assertEquals(16, stmt.executeUpdate("insert into y select x.id, x.name from x,z"));
        assertEquals(16, stmt.executeUpdate("insert into y select x.id,z.name1 from x,z"));

        //insert as scalar sub-subselect
        assertEquals(1, stmt.executeUpdate("insert into y values((select x.id from x where id =2), (select x.name from x where x.id=2))"));

        // aggregate function with subselect in insert
        assertEquals(1,
            stmt.executeUpdate("insert into y values ((select max(id) from x), (select distinct x.name from x where id = (select max(id) from x)))"));

        // test delete with sub-select
        rset = stmt.executeQuery("select count(*) from x");
        assertTrue(rset.next());
        int count = rset.getInt(1);
        assertEquals(count, stmt.executeUpdate("delete from x where id in (select id from x)"));
        assertEquals(0, stmt.executeUpdate("delete from x where exists (select id from x)"));

        rset = stmt.executeQuery("select count(*) from y where id = 2");
        assertTrue(rset.next());
        count = rset.getInt(1);
        assertEquals(count, stmt.executeUpdate("delete from y where id = (select distinct id from y where id = 2)"));
        assertEquals(0, stmt.executeUpdate("delete from y where id = (select id from y where id = 2)"));

        rset = stmt.executeQuery("select (select (select (select 1)))");
        assertTrue(rset.next());

        rset.close();
        stmt.close();
    }

    public void testColumnAliasInSelect() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("create table y (id int, name varchar(3))");
        assertEquals(4, stmt.executeUpdate("insert into y select * from x"));

        ResultSet rset = stmt.executeQuery("select id as myid, name as myname from x where myid = 4");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select id as myid, name as myname  from x where myid in (select id from x)");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select sum(id) mysum, name myname  from x group by x.myname");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select id as myid, name as myname  from x order by x.id");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select x.id xid, x.name myname, y.id yid, y.name yname from x left outer join y on(xid = yid)");
        assertTrue(rset.next());

        rset = stmt.executeQuery("select 'aaa' as myname  from x where x.name = myname");
        assertTrue(rset.next());

        try {
            rset = stmt.executeQuery("select 'AAA' from x where x.name = AAA");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        rset.close();
        stmt.close();
    }

    public void testBadFunctionAndTableName() throws Exception {
        Statement stmt = _conn.createStatement();

        try {
            stmt.executeQuery("select bogus('AAA')");
            fail("Expected SQLException: invalid function name");
        } catch (SQLException e) {
            // expected
        }

        try {
            stmt.executeQuery("select abs()");
            fail("Expected SQLException: invalid function name");
        } catch (SQLException e) {
            // expected
        }

        try {
            stmt.executeQuery("select S1.id from S1");
            fail("Expected SQLException: table not found");
        } catch (SQLException e) {
            // expected
        }

        try {
            stmt.executeQuery("select * from S1");
            fail("Expected SQLException: table not found");
        } catch (SQLException e) {
            // expected
        }

        stmt.close();
    }

    public void test_upsert_via_pstmt() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("drop table if exists y ");
        stmt.execute("create table y(id int, name varchar(4))");
        assertEquals(2, stmt.executeUpdate("insert into y select * from x where name = 'aaa'"));

        PreparedStatement pstmt = _conn.prepareStatement("upsert into y as D " + " using x as S on(S.id = D.id and S.id = ?) "
            + " when matched then update set D.name = '_' || S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name)");

        pstmt.setInt(1, 1);

        assertEquals(4, pstmt.executeUpdate());
        pstmt.close();
        stmt.close();
    }

    public void testBasicUpsert() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("drop table if exists y ");
        stmt.execute("create table y(id int, name varchar(3))");
        assertEquals(2, stmt.executeUpdate("insert into y select * from x where name = 'aaa'"));

        // We get two exact row, so merge/upsert will skip it,
        // hence we will expect 2 mod count.
        assertEquals(2, stmt.executeUpdate("upsert into y as D using (select id, name from x) as S on(S.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name)"));

        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using (select id, name from x) as S on(S.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name)"));

        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using (select id myid, name from x) as S on(S.myid = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (myid, S.name)"));

        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using (select id myid, name from x) as S on(S.myid = D.id and S.myid = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (myid, S.name)"));

        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using (select id myid, name from x) as S on(S.myid = D.id and S.myid = 2)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (myid, S.name)"));

        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using x as S on(x.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (y.id, D.name) values (x.id, S.name)"));

        // unstable row set
        try {
            stmt.executeUpdate(" merge into y as D using (select x.id, x.name from x,y) as S on(s.id = D.id)"
                + " when matched then update set D.name = S.name" + " when not matched then insert (D.id, D.name) values (s.id, s.name)");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // inner join in sub-query : shd return zero since y is empty
        stmt.execute("delete from y");
        assertEquals(0, stmt.executeUpdate(" merge into y as D using (select x.id, x.name from x,y)" + " as S on(s.id = D.id)"
            + " when matched then update set D.name = S.name" + " when not matched then insert (D.id, D.name) values (s.id, s.name)"));

        stmt.executeUpdate("insert into y values(5,'fff')");
        assertEquals(4, stmt.executeUpdate(" merge into y as D using (select x.id, x.name from x,y)" + " as S on(s.id = D.id)"
            + " when matched then update set D.name = S.name" + " when not matched then insert (D.id, D.name) values (s.id, s.name)"));

        // ambiguous column test
        try {
            stmt.executeUpdate(" merge into y as D using (select * from x,y) as S on(s.id = D.id)" + " when matched then update set D.name = S.name"
                + " when not matched then insert (D.id, D.name) values (s.id, s.name)");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // Updates Not allowed for cols used in Merge/Upsert Condition
        try {
            stmt.executeUpdate(" merge into y as D using (select x.id, x.name from x,y) as S on(s.id = D.id)"
                + " when matched then update set D.name = S.name, D.id = S.id" + " when not matched then insert (D.id, D.name) values (s.id, s.name)");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        // Issue #: 21
        stmt.execute("delete from y");
        assertEquals(4, stmt.executeUpdate("merge into y as D using x as S on(x.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (y.id, D.name) values (x.id, S.name)"));
        // if source table have 0 rows
        stmt.execute("delete from x");
        assertEquals(0, stmt.executeUpdate("merge into y as D using x as S on(x.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (y.id, D.name) values (x.id, S.name)"));

        try {
            stmt.executeUpdate("merge into y as D using (select id myid, name from x) as S on(S.myid = D.id)"
                + " when matched then update set S.name = D.name when not matched then " + " insert (D.id, D.name) values (myid, S.name)");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        stmt.close();
    }

    public void testUpsertExceptionWhenClause() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("drop table if exists y ");
        stmt.execute("create table y(id int, name varchar(3))");

        stmt.execute("drop table if exists z ");
        stmt.execute("create table z(id int, name varchar(3))");

        assertEquals(2, stmt.executeUpdate("insert into y select * from x where name = 'aaa'"));

        assertEquals(2, stmt.executeUpdate("upsert into y as D using (select id, name from x) as S on(S.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name) "
            + " exception when S.id < 3 then Insert into z"));

        stmt.execute("delete from y");
        assertEquals(2, stmt.executeUpdate("merge into y as D using (select id, name from x) as S on(S.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name)"
            + " exception when S.id < 3 then Insert into z values(S.id, S.name)"));

        stmt.execute("delete from y");
        assertEquals(2, stmt.executeUpdate("merge into y as D using (select id myid, name from x) as S on(S.myid = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (myid, S.name)"
            + " exception when S.myid < 3 then Insert into z(z.id, z.name) values(S.myid, S.name)"));

        stmt.execute("delete from y");
        assertEquals(2, stmt.executeUpdate("merge into y as D using x as S on(S.id = D.id)"
            + " when matched then update set D.name = S.name when not matched then " + " insert (D.id, D.name) values (S.id, S.name)"
            + " exception when S.id < 3 then Insert into z(z.id, z.name) values(S.id, S.name)"));

    }

    public void testBasicUpdateSelect() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(8))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') " + "FROM tmp T, emp S WHERE T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = ('RRRR' || 'Test') " + "FROM tmp T, emp S WHERE T.tid = S.id"));

        assertResult("RRRRTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = 'Test' " + "FROM tmp T, emp S WHERE T.tid = S.id"));

        assertResult("Test", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");

        // switch the table order in from
        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = 'Test' " + "FROM tmp T, emp S WHERE T.tid = S.id"));

        assertResult("Test", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");

        assertEquals(1, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = 'aaa' " + "from tmp where tid=1"));

        assertResult("aaa", "select tname from tmp where tid=1");

        stmt.close();

    }

    public void testBasicUpdateSelect1() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp SET tmp.tname = (S.name || 'Test') " + "FROM tmp T right outer join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect2() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2,
            stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') " + "FROM tmp T left outer join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect3() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T right outer join emp S on T.tid = S.id where tmp.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("cccTest", "select tname from tmp where tid=2");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect4() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T left outer join emp S on T.tid = S.id where tmp.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("cccTest", "select tname from tmp where tid=2");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect5() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        //wrong table alias T.tid in join on conditon exception expected
        try {
            stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
                + "FROM emp S1 inner join tmp T1  on T.tid = S1.id where T.tid = S1.id");
        } catch (SQLException ex) {
            //expected
        }

        assertEquals(2, stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
            + "FROM emp S1 inner join tmp T1  on T1.tid = S1.id where T.tid = S1.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("cccTest", "select tname from tmp where tid=2");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect6() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        //wrong table alias T.tid in join on conditon exception expected
        try {
            stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
                + "FROM emp S1 inner join tmp T1  on T.tid = S1.id where T.tid = S1.id");
        } catch (SQLException ex) {
            //expected
        }

        assertEquals(2, stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
            + "FROM emp S1 inner join tmp T1  on T1.tid = S1.id where T.tid = S1.id and T.tid >= 1"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("cccTest", "select tname from tmp where tid=2");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelect7() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        //wrong table alias T.tid in join on conditon exception expected
        try {
            stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
                + "FROM emp S1 inner join tmp T1  on T.tid = S1.id where T.tid = S1.id");
        } catch (SQLException ex) {
            //expected
        }

        //test to make sure that all the where condition are applied at LOJ (target LOJ
        // source) we
        //create in update command, otherwise we will get a FilteringRowIterator
        //and a ClassCastException is thrown in update command
        assertEquals(2, stmt.executeUpdate("UPDATE tmp T " + "SET T.tname = (S1.name || 'Test') "
            + "FROM emp S1 inner join tmp T1  on T1.tid = S1.id where T.tid = S1.id and T.tname like 'bbb' "));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("cccTest", "select tname from tmp where tid=2");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelectUsingIndexInnerJoin() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        stmt.execute("create btree index tmp_idx on tmp(tid)");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') " + "FROM tmp T inner join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelectUsingIndexLeftOuterJoin() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        stmt.execute("create btree index tmp_idx on tmp(tid)");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2,
            stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') " + "FROM tmp T left outer join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testBasicUpdateSelectUsingIndexRightOuter() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        stmt.execute("create btree index emp_idx on emp(id)");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T right outer join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testThreeTableBasicUpdateSelectUsingIndex() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        stmt.execute("create table bmp(bid int, bname varchar(3))");
        stmt.execute("create btree index tmp_idx on tmp(tid)");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into bmp values(1,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into bmp values(2,'ccc')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T left outer join emp S on T.tid = S.id " + "left outer join bmp B on S.id = B.bid"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testThreeTableBasicUpdateSelect() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("drop table if exists emp ");
        stmt.execute("drop table if exists tmp ");
        stmt.execute("create table emp(id int, name varchar(3))");
        stmt.execute("create table tmp(tid int, tname varchar(7))");
        stmt.execute("create table bmp(bid int, bname varchar(3))");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into bmp values(1,'ccc')"));
        assertEquals(1, stmt.executeUpdate("insert into bmp values(2,'ccc')"));

        assertEquals(2, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T left outer join emp S on T.tid = S.id " + "left outer join bmp B on S.id = B.bid"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testGeneratedColumn() throws Exception {
        Statement stmt = _conn.createStatement();

        stmt.execute("drop table if exists emp ");
        stmt.execute("create table emp(id int, name varchar(3), id_name" + " generated always as (id || name) )");
        assertEquals(1, stmt.executeUpdate("insert into emp values(1,'aaa')"));

        ResultSet rset = stmt.executeQuery("SELECT id, name, id_name FROM emp");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("aaa", rset.getString(2));
        assertEquals("1aaa", rset.getString(3));
        assertTrue(!rset.next());

        assertEquals(1, stmt.executeUpdate("UPDATE emp " + "SET emp.name = 'bbb'"));
        rset = stmt.executeQuery("SELECT id, name, id_name FROM emp");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("bbb", rset.getString(2));
        assertEquals("1bbb", rset.getString(3));
        assertTrue(!rset.next());

        assertEquals(1, stmt.executeUpdate("alter table emp" + " add column name_id generated always as (name || id)"));

        rset = stmt.executeQuery("SELECT id, name, id_name, name_id FROM emp");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("bbb", rset.getString(2));
        assertEquals("1bbb", rset.getString(3));
        assertEquals("bbb1", rset.getString(4));
        assertTrue(!rset.next());

        assertEquals(1, stmt.executeUpdate("UPDATE emp " + "SET emp.name = 'ccc'"));

        rset = stmt.executeQuery("SELECT id, name, id_name, name_id FROM emp");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("ccc", rset.getString(2));
        assertEquals("1ccc", rset.getString(3));
        assertEquals("ccc1", rset.getString(4));
        assertTrue(!rset.next());

        assertEquals(1, stmt.executeUpdate("insert into emp values(2,'ddd')"));

        rset = stmt.executeQuery("SELECT id, name, id_name, name_id FROM emp");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("ccc", rset.getString(2));
        assertEquals("1ccc", rset.getString(3));
        assertEquals("ccc1", rset.getString(4));

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("ddd", rset.getString(2));
        assertEquals("2ddd", rset.getString(3));
        assertEquals("ddd2", rset.getString(4));
        assertTrue(!rset.next());
    }

    public void testVariousUnsupported() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("select NUM from FOO");
        try {
            rset.getArray(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.getArray("NUM");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.getCursorName();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        rset.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        assertEquals(ResultSet.FETCH_UNKNOWN, rset.getFetchDirection());
        rset.setFetchSize(0);
        assertEquals(0, rset.getFetchSize());
        try {
            rset.getObject(1, (Map) null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.getObject("NUM", (Map) null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.getRef(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.getRef("NUM");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.moveToCurrentRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.moveToInsertRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.refreshRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.deleteRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        assertTrue(!rset.rowDeleted());
        try {
            rset.updateRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        assertTrue(!rset.rowUpdated());
        try {
            rset.insertRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        assertTrue(!rset.rowInserted());
        try {
            rset.updateAsciiStream(1, null, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateAsciiStream("NUM", null, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBigDecimal(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBigDecimal("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBinaryStream(1, null, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBinaryStream("NUM", null, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBoolean(1, true);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBoolean("NUM", true);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateByte(1, (byte) 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateByte("NUM", (byte) 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateShort(1, (short) 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateShort("NUM", (short) 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateInt(1, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateInt("NUM", 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateLong(1, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateLong("NUM", 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateFloat(1, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateFloat("NUM", 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateNull(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateNull("NUM");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateString(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateString("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateDate(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateDate("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateTimestamp(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateTimestamp("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateTime(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateTime("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateObject(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateObject("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateObject("NUM", null, 2);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBlob(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateBlob("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateClob(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateClob("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateRef(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateRef("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateArray(1, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            rset.updateArray("NUM", null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    private void assertRowCount(int i, Statement stmt) throws SQLException {
        ResultSet rset = stmt.executeQuery("select count(*) from foo");
        assertTrue(rset.next());
        assertEquals(i, rset.getInt(1));
        assertTrue(!rset.next());
        rset.close();
    }

    public void testQuotedIdentifiers() throws Throwable {
        // Expect statement using reserved word as table identifier to fail.
        try {
            _stmt.execute("create table table (a int, b varchar(30))");
            fail("Expected SQLException upon using reserved word as table identifier.");
        } catch (SQLException ignore) {
            // keep going
        }

        // Expect statement using reserved word as column identifier to fail.
        try {
            _stmt.execute("create table table_1 (asc int, b varchar(30))");
            fail("Expected SQLException upon using reserved word as column identifier.");
        } catch (SQLException ignore) {
            // keep going
        }

        // Expect statement using reserved word as table or column aliases to fail.
        _stmt.execute("create table table_1 (a int, b varchar(30))");
        try {
            _stmt.execute("select drop.a from table_1 drop");
            fail("Expected SQLException upon using reserved word as table alias.");
        } catch (SQLException ignore) {
            // keep going
        }

        try {
            _stmt.execute("select a as drop from table_1");
            fail("Expected SQLException upon using reserved word as table alias.");
        } catch (SQLException ignore) {
            // keep going
        }

        _stmt.execute("drop table table_1");

        // Expect statement with incompletely quoted identifer to fail.
        try {
            _stmt.execute("create table \"table (a int, b varchar(30))");
            fail("Expected SQLException upon using incompletely quoted identifier.");
        } catch (SQLException ignore) {
            // keep going
        }

        // Expect statement with invalid characters in quoted identifer to fail.
        try {
            _stmt.execute("create table \"_!@#$%\" (a int, b varchar(30))");
            fail("Expected SQLException upon using invalid quoted identifier.");
        } catch (SQLException ignore) {
            // keep going
        }

        // Create new table "table"
        _stmt.execute("create table \"table\" ( \"asc\" int not null, \"desc\" varchar(30))");

        // Insert data into "table"
        assertEquals("Could not insert using quoted identifier.", 1, _stmt.executeUpdate("insert into \"table\" values (1, 'first')"));
        assertEquals("Could not insert using quoted identifier.", 1, _stmt.executeUpdate("insert into \"table\" values (2, 'second')"));

        // Expect statement using reserved word as table or column aliases to fail.
        _stmt.execute("create table table_1 (a int, b varchar(30))");
        try {
            _stmt.execute("select drop.a from table_1 drop");
            fail("Expected SQLException upon using reserved word as table alias.");
        } catch (SQLException ignore) {
            // keep going
        }

        try {
            _stmt.execute("select a as drop from table_1");
            fail("Expected SQLException upon using reserved word as table alias.");
        } catch (SQLException ignore) {
            // keep going
        }

        // Create new table "sequence"
        _stmt.execute("create table \"sequence\" ( \"asc\" int, \"desc\" varchar(30))");

        // Insert-Select into "sequence" from "table"
        assertEquals("Could not execute insert-select using quoted identifiers.", 2,
            _stmt.executeUpdate("insert into \"sequence\" select \"table\".\"asc\", " + "\"table\".\"desc\" from \"table\""));

        // Select using inner join between "table" and "sequence"
        ResultSet rs = _stmt.executeQuery("select \"table\".\"asc\", \"sequence\".\"desc\" from \"table\" inner join \"sequence\" "
            + "on (\"table\".\"asc\" = \"sequence\".\"asc\") order by \"table\".\"asc\" desc");

        // Note: order by desc...
        assertTrue("Could not advance ResultSet as generated from select (inner join) using quoted " + "identifiers", rs.next());
        assertEquals("Could not get expected data from select (inner join) using quoted identifiers.", 2, rs.getInt(1));
        assertEquals("Could not get expected data from select (inner join) using quoted identifiers.", "second", rs.getString(2));
        assertTrue("Could not advance ResultSet as generated from select (inner join) using quoted " + "identifiers", rs.next());
        assertEquals("Could not get expected data from select (inner join) using quoted identifiers.", 1, rs.getInt("ASC"));
        assertEquals("Could not get expected data from select (inner join) using quoted identifiers.", "first", rs.getString("DESC"));
        assertFalse("Expected not to advance ResultSet as generated from select (inner join) using " + "quoted identifiers", rs.next());

        rs.close();

        // Update "sequence"
        assertEquals(1, _stmt.executeUpdate("update \"sequence\" set \"desc\" = 'KillMe!' where \"asc\" = 2"));
        rs = _stmt.executeQuery("select \"desc\" from \"sequence\" where \"desc\" = 'KillMe!'");
        assertTrue("Could not advance ResultSet as generated from select after update using quoted " + "identifiers.", rs.next());
        assertEquals("Could not get expected data from select after update using quoted identifiers.", "KillMe!", rs.getString(1));

        // Delete and drop "table"
        assertEquals("Could not delete using quoted identifier.", 2, _stmt.executeUpdate("delete from \"table\""));
        assertEquals("Could not drop using quoted identifier.", 0, _stmt.executeUpdate("drop table \"table\""));
    }
    
    public void testConcurrency() throws Exception {
        _stmt.execute("create table foo (id int, name varchar(50))");
        ResultSet rset = _stmt.executeQuery("select count(*) from foo");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rset.getConcurrency());
    }
}