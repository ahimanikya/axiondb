/*
 * $Id: TestDQL.java,v 1.1 2007/11/28 10:01:30 jawed Exp $
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

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.HashBag;
import org.axiondb.jdbc.AxionConnection;

/**
 * Database Query Language tests.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:30 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Dave Pekarek Krohn
 * @author Jonathan Giron
 */
public class TestDQL extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestDQL(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDQL.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testCrossproductStyleInnerJoin() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        _rset = _stmt.executeQuery("select * from FOO F, BAR B where F.NUM = B.ID");
        assertNotNull(_rset);
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(_rset.next());
            assertEquals(_rset.getString("STR"), _rset.getString("DESCR"));
        }
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select count(*) from FOO F, BAR B where F.NUM = F.NUM");
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), NUM_ROWS_IN_FOO * NUM_ROWS_IN_BAR);
        _rset.close();
    }

    public void testCrossproductStyleInnerJoinWithWhere() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        PreparedStatement pstmt = _conn.prepareStatement("select B.DESCR, F.STR from FOO F, BAR B where F.NUM = B.ID and F.NUM = ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            assertResult(String.valueOf(i), pstmt.executeQuery());
        }
        pstmt.close();
    }

    public void testAnsiStyleInnerJoin() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        _rset = _stmt.executeQuery("select F.STR, B.DESCR from FOO F inner join BAR B on F.NUM = B.ID");
        assertNotNull(_rset);
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(_rset.next());
            assertEquals(_rset.getString(1), _rset.getString(2));
        }
        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testAnsiStyleInnerJoinWithWhere() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        PreparedStatement pstmt = _conn.prepareStatement("select STR, DESCR from FOO F inner join BAR  B on F.NUM = B.ID where F.NUM = ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            assertResult(String.valueOf(i), pstmt.executeQuery());
        }
        pstmt.close();
    }

    public void testGetRow() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("select * from foo");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(rset.next());
            assertEquals(i + 1, rset.getRow());
            assertTrue(rset.relative(0));
            assertEquals(i + 1, rset.getRow());
        }
        rset.close();
    }

    public void testGetRow2() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("select * from foo");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(rset.relative(1));
            assertEquals(i + 1, rset.getRow());
            assertTrue(rset.relative(0));
            assertEquals(i + 1, rset.getRow());
        }
        rset.close();
    }

    public void testGetRow3() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("select * from foo");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i += 2) {
            assertTrue(rset.relative(2));
            assertEquals(i + 2, rset.getRow());
            assertTrue(rset.relative(0));
            assertEquals(i + 2, rset.getRow());
        }
        rset.close();
    }

    public void testGetRowWithDot() throws Exception {
        createTableFoo();
        populateTableFoo();
        ResultSet rset = _stmt.executeQuery("select foo.* from foo");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(rset.next());
            assertEquals(i + 1, rset.getRow());
        }
        rset.close();
    }

    public void testLimitByMaxRows() throws Exception {
        createTableFoo();
        populateTableFoo();
        _stmt.setMaxRows(NUM_ROWS_IN_FOO - 1);
        ResultSet rset = _stmt.executeQuery("select * from foo");
        for (int i = 0; i < NUM_ROWS_IN_FOO - 1; i++) {
            assertTrue(rset.next());
            assertEquals(i + 1, rset.getRow());
        }
        assertTrue(!rset.next());
        rset.close();
    }

    public void testSingleRowButNotEqualFromIndexBug() throws Exception {
        createTableFoo();
        populateTableFoo();

        // select the last row from the index using a statement
        {
            Statement stmt = null;
            ResultSet rset = null;
            try {
                stmt = _conn.createStatement();
                rset = stmt.executeQuery("select NUM, STR from FOO where NUM > " + (NUM_ROWS_IN_FOO - 2));
                while (rset.next()) {
                    int num = rset.getInt(1);
                    String str = rset.getString(2);
                    assertEquals(String.valueOf(num), str);
                }
            } finally {
                try {
                    rset.close();
                } catch (Exception t) {
                }
                try {
                    stmt.close();
                } catch (Exception t) {
                }
            }
        }
        // select the last row from the index using a prepared statement
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            try {
                stmt = _conn.prepareStatement("select NUM, STR from FOO where NUM > ?");
                for (int i = 0; i < 2; i++) {
                    stmt.clearParameters();
                    stmt.setInt(1, NUM_ROWS_IN_FOO - 2);
                    rset = stmt.executeQuery();
                    while (rset.next()) {
                        int num = rset.getInt(1);
                        String str = rset.getString(2);
                        assertEquals(String.valueOf(num), str);
                    }
                    rset.close();
                }
            } finally {
                try {
                    rset.close();
                } catch (Exception t) {
                }
                try {
                    stmt.close();
                } catch (Exception t) {
                }
            }
        }
    }

    public void testSelectWithAutocommitTurnedOff() throws Exception {
        createTableFoo();
        populateTableFoo();

        _conn.setAutoCommit(false);
        {
            String sql = "select STR from FOO";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);

            // can't assume the order in which rows will be returned
            // so populate a set and compare 'em
            Bag expected = new HashBag();
            Bag found = new HashBag();

            for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
                assertTrue("ResultSet should contain more rows [" + i + "]", _rset.next());
                expected.add(String.valueOf(i));
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
        _conn.commit();
        {
            String sql = "select STR from FOO";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);

            // can't assume the order in which rows will be returned
            // so populate a set and compare 'em
            Bag expected = new HashBag();
            Bag found = new HashBag();

            for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
                assertTrue("ResultSet should contain more rows [" + i + "]", _rset.next());
                expected.add(String.valueOf(i));
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_str_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();
        String sql = "select STR from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows [" + i + "]", _rset.next());
            expected.add(String.valueOf(i));
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
            found.add(val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_select_str_from_foo_semicolon() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO;";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(String.valueOf(i));
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
            found.add(val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_select_distinct() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select distinct NUMTWO from FOO where NUM >= ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = (i / 2); j < (NUM_ROWS_IN_FOO / 2); j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows ", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
        pstmt.close();
    }

    public void testSelectDuplicateRows() throws Exception {
        createTableFoo(false);
        populateTableFoo();
        populateTableFoo(); // create duplicate set of rows

        String sql = "select all NUM, STR from FOO where NUM > 0";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int k = 0; k < 2; k++) {
            for (int i = 1; i < NUM_ROWS_IN_FOO; i++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                expected.add(new Integer(i));
                int val = _rset.getInt(1);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                found.add(new Integer(val));
            }
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void testSelectDuplicateRows2() throws Exception {
        createTableFoo(false);
        populateTableFoo();
        populateTableFoo(); // create duplicate set of rows

        String sql = "select NUM, STR from FOO where NUM > 0";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int k = 0; k < 2; k++) {
            for (int i = 1; i < NUM_ROWS_IN_FOO; i++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                expected.add(new Integer(i));
                int val = _rset.getInt(1);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                found.add(new Integer(val));
            }
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_select_literal_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select 'Literal' from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertEquals("Returned string should equal \"Literal\".", "Literal", val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_str_num_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR, NUM, NUMTWO from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a bag and compare 'em

        Bag expectedStr = new HashBag();
        Bag foundStr = new HashBag();
        Bag expectedNum = new HashBag();
        Bag foundNum = new HashBag();
        Bag expectedNumtwo = new HashBag();
        Bag foundNumtwo = new HashBag();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            expectedNum.add(new Integer(i));
            expectedNumtwo.add(new Integer(i / 2));
            expectedStr.add(String.valueOf(i));

            assertTrue("ResultSet should contain more rows", _rset.next());
            String strVal = _rset.getString(1);
            assertNotNull("Returned String should not be null", strVal);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertTrue("Shouldn't have seen \"" + strVal + "\" yet", !foundStr.contains(strVal));
            foundStr.add(strVal);

            int intVal = _rset.getInt(2);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            assertTrue("Shouldn't have seen \"" + intVal + "\" yet", !foundNum.contains(new Integer(intVal)));
            foundNum.add(new Integer(intVal));

            int intVal2 = _rset.getInt(3);
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
            foundNumtwo.add(new Integer(intVal2));

        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expectedStr, foundStr);
        assertEquals(expectedNum, foundNum);
        assertEquals(expectedNumtwo, foundNumtwo);
    }

    public void test_select_str_from_foo_where_num_eq_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where NUM = " + i;
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should not be empty", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("STR"));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("str"));
            assertTrue(!_rset.wasNull());
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
    }

    public void test_select_str_num_from_foo_where_num_eq_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR, NUM from FOO where NUM = " + i;
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should not be empty", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("STR"));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("str"));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt(2));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt("NUM"));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt("num"));
            assertTrue(!_rset.wasNull());
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
    }

    public void test_select_foostr_foonum_from_foo_where_foonum_eq_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select FOO.STR, FOO.NUM from FOO where FOO.NUM = " + i;
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should not be empty", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("STR"));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("str"));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt(2));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt("NUM"));
            assertTrue(!_rset.wasNull());
            assertEquals(i, _rset.getInt("num"));
            assertTrue(!_rset.wasNull());
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
    }

    public void test_select_str_from_foo_where_num_lt_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where NUM < " + i;
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = 0; j < i; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should not be empty", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_str_from_foo_where_str_gt_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where STR >= '" + i + "'";
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j >= i; j--) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should not be empty", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_str_from_foo_where_num_gteq_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select STR from FOO where NUM >= ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j >= i; j--) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should not be empty", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
        pstmt.close();
    }

    public void test_select_str_from_foo_where_literal_lt_num() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < 5; i++) {
            String sql = "select STR from FOO where " + i + " < NUM";
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j > i; j--) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should contain more rows", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_str_from_foo_where_num_eq_literal_or_num_gt_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where NUM = " + i + " or NUM > " + i;
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j >= i; j--) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should not be empty", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_str_from_foo_where_num_bewtween_literal_and_literal() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where NUM between " + i + " and " + (i + 2);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = i; j <= i + 2 && j < NUM_ROWS_IN_FOO; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows (i=" + i + ",j=" + j + ")", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_with_bindvar_limit() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select STR from FOO limit ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO + 1; i++) {
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            for (int j = 0; j < i && j < NUM_ROWS_IN_FOO; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows (i=" + i + ")", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
            pstmt.clearParameters();
        }
        pstmt.close();
    }

    public void test_select_with_literal_limit() throws Exception {
        createTableFoo();
        populateTableFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            String sql = "select STR from FOO where NUM >= " + i + " limit 3";
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = i; j < i + 3 && j < NUM_ROWS_IN_FOO; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows (i=" + i + ",j=" + j + ")", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows (i=" + i + ")", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
    }

    public void test_select_with_bindvar_offset() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select STR from FOO offset ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = i; j < NUM_ROWS_IN_FOO; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows (i=" + i + ")", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
            pstmt.clearParameters();
        }
        pstmt.close();
    }

    public void test_select_with_literal_offset() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select STR from FOO where NUM >= ? offset 3");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = i + 3; j < NUM_ROWS_IN_FOO; j++) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should have more rows (i=" + i + ",j=" + j + ")", _rset.next());
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows (i=" + i + ")", !_rset.next());
            _rset.close();
            assertEquals(expected, found);
        }
        pstmt.close();
    }

    public void test_select_order_by_offset_limit() throws Exception {
        createTableFoo();
        populateTableFoo();

        int offset = 1;
        int limit = 3;
        PreparedStatement pstmt = _conn.prepareStatement("select NUM from FOO order by NUM desc limit " + limit + " offset " + offset);
        ResultSet rset = pstmt.executeQuery();
        for (int i = 0; i < limit; i++) {
            assertTrue(rset.next());
            assertEquals(NUM_ROWS_IN_FOO - offset - i - 1, rset.getInt(1));
        }
        assertTrue(!rset.next());
        rset.close();
        pstmt.close();
    }

    public void test_select_foo_dot_asterisk_from_foo_bar_where_num_gt_literal_and_id_eq_num() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        PreparedStatement pstmt = _conn.prepareStatement("select A.* from FOO A, BAR B where A.NUM >= ? and B.ID = A.NUM");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j >= i; j--) {
                int numval = 0;
                int num2val = 0;
                String strval = null;
                assertTrue("ResultSet should not be empty", _rset.next());
                {
                    numval = _rset.getInt(1);
                    assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                }
                {
                    strval = _rset.getString(2);
                    assertNotNull("Returned String should not be null", strval);
                    assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                }
                {
                    num2val = _rset.getInt(3);
                    assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                }
                assertEquals(numval, Integer.parseInt(strval));
                assertEquals(num2val, Integer.parseInt(strval) / 2);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        pstmt.close();
    }

    public void test_select_str_descr_from_foo_bar_where_num_gt_literal_and_id_eq_num() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        PreparedStatement pstmt = _conn.prepareStatement("select STR, DESCR from FOO, BAR where NUM >= ? and ID = NUM");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            Bag expected = new HashBag();
            Bag foundStr = new HashBag();
            Bag foundDescr = new HashBag();
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = NUM_ROWS_IN_FOO - 1; j >= i; j--) {
                expected.add(String.valueOf(j));
                assertTrue("ResultSet should not be empty", _rset.next());
                {
                    String strval = _rset.getString(1);
                    assertNotNull("Returned String should not be null", strval);
                    assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                    assertTrue("Shouldn't have seen \"" + strval + "\" yet", !foundStr.contains(strval));
                    foundStr.add(strval);
                }
                {
                    String descrval = _rset.getString(2);
                    assertNotNull("Returned String should not be null", descrval);
                    assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                    assertTrue("Shouldn't have seen \"" + descrval + "\" yet", !foundDescr.contains(descrval));
                    foundDescr.add(descrval);
                }
                assertEquals(foundDescr, foundStr);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, foundStr);
            assertEquals(expected, foundDescr);
        }
        pstmt.close();
    }

    public void test_select_str_descr_from_foo_bar_where_id_eq_num_and_num_lt_literal_join_w_bind() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        String sql = "select foo.STR from FOO, BAR barro where barro.ID = NUM and STR = ?";
        PreparedStatement stmt = _conn.prepareStatement(sql);
        stmt.setString(1, "bogus");
        _rset = stmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("Should not have any rows", !_rset.next());

        stmt.close();
    }

    public void testPreparedStatement() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO where NUM = ?";
        PreparedStatement stmt = _conn.prepareStatement(sql);

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            stmt.clearParameters();
            stmt.setInt(1, i);
            _rset = stmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should not be empty", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("STR"));
            assertTrue(!_rset.wasNull());
            assertEquals(String.valueOf(i), _rset.getString("str"));
            assertTrue(!_rset.wasNull());
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        try {
            stmt.close();
        } catch (Exception t) {
        }
    }

    public void test_select_char_of_num_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select NUM, CHR(NUM) from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            int num = _rset.getInt(1);
            String str = _rset.getString(2);
            assertEquals((char) num, str.charAt(0));
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_upper_of_descr2_from_bar() throws Exception {
        createTableBar();
        populateTableBar();

        String sql = "select DESCR2, UPPER(DESCR2) from BAR";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_BAR; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            String plainstr = _rset.getString(1);
            String upperstr = _rset.getString(2);
            assertTrue(!plainstr.equals(upperstr));
            assertEquals(plainstr.toUpperCase(), upperstr);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_lower_of_descr2_from_bar() throws Exception {
        createTableBar();
        populateTableBar();

        String sql = "select DESCR2, LOWER(DESCR2) from BAR";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_BAR; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            String plainstr = _rset.getString(1);
            String lowerstr = _rset.getString(2);
            assertTrue(!plainstr.equals(lowerstr));
            assertEquals(plainstr.toLowerCase(), lowerstr);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_concat_literal_descr2_from_bar() throws Exception {
        createTableBar();
        populateTableBar();

        String sql = "select DESCR2, CONCAT('TEST', DESCR2) from BAR";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_BAR; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            String plainstr = _rset.getString(1);
            String concat = _rset.getString(2);
            assertTrue(!plainstr.equals(concat));
            assertEquals("TEST" + plainstr, concat);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void testNullPlusNonNullIsNull() throws Exception {
        createTableBar();
        populateTableBar();

        String sql = "select DESCR2, CONCAT(NULL, DESCR2) from BAR";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_BAR; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            String concat = _rset.getString(2);
            assertTrue(_rset.wasNull());
            assertNull(concat);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_contains_literal_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "SELECT contains('team', 'i'), str FROM foo";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        assertTrue("Should have a row", _rset.next());
        assertTrue("Should return false", !_rset.getBoolean(1));
        _rset.close();
    }

    public void test_select_contains_fieldval_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "SELECT str, contains(lower(str), '0') FROM foo ORDER BY num";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        boolean[] results = new boolean[] { true, false};

        for (int i = 0; i < results.length; i++) {
            assertTrue("Should have a row", _rset.next());
            String str = _rset.getString(1);
            assertEquals("Should return expected result for contains(" + str + ", '0')", results[i], _rset.getBoolean(2));
        }
        _rset.close();
    }

    public void test_select_from_foo_where_contains_fieldval_eq_true() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "SELECT str FROM foo WHERE contains(str, '0') = true";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        assertTrue("Should have a row", _rset.next());
        assertTrue("Should have only one row but also found " + _rset.getString(1), !_rset.next());
        _rset.close();
    }

    public void test_select_from_foo_where_contains_fieldval_unary() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "SELECT str FROM foo WHERE contains(str, '0')";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        assertTrue("Should have a row", _rset.next());
        assertTrue("Should have only one row but also found " + _rset.getString(1), !_rset.next());
        _rset.close();
    }

    public void test_select_state_where_matches_north() throws Exception {
        createTableStates();
        populateTableStates();

        String sql = "SELECT state FROM states WHERE matches(state, 'north') ORDER BY state";
        String[] expected = new String[] { "north carolina", "north dakota"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_state_where_matches_caret_a() throws Exception {
        createTableStates();
        populateTableStates();

        String sql = "SELECT state FROM states WHERE matches(state, '^a') ORDER BY state";
        String[] expected = new String[] { "alabama", "alaska", "arizona", "arkansas"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_state_where_state_like_al_percent() throws Exception {
        createTableStates();
        populateTableStates();

        String sql = "SELECT state FROM states WHERE state LIKE 'al%' ORDER BY state";
        String[] expected = new String[] { "alabama", "alaska"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_state_where_state_like_a_percent_a() throws Exception {
        createTableStates();
        populateTableStates();

        String sql = "SELECT state FROM states WHERE state LIKE 'a%a' ORDER BY state";
        String[] expected = new String[] { "alabama", "alaska", "arizona"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_word_where_word_like_b_t() throws Exception {
        createTableWords();
        populateTableWords();

        String sql = "SELECT word FROM words WHERE word LIKE 'b_t' ORDER BY word";
        String[] expected = new String[] { "bat", "bet", "bit", "bot", "but"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_word_where_word_not_like_b_t() throws Exception {
        createTableWords();
        populateTableWords();

        String sql = "SELECT word FROM words WHERE word LIKE 'b%t' AND NOT word LIKE 'b_t' ORDER BY word";
        String[] expected = new String[] { "bait", "bent", "bolt", "bunt"};
        helpTestStringResults(sql, expected);
    }

    public void test_select_word_where_word_like_b() throws Exception {
        createTableWords();
        populateTableWords();

        String sql = "SELECT word FROM words WHERE word LIKE 'b' ORDER BY word";
        String[] expected = new String[] {};
        helpTestStringResults(sql, expected);
    }

    public void test_select_word_where_word_like_c_percent() throws Exception {
        createTableWords();
        populateTableWords();

        String sql = "SELECT word FROM words WHERE word LIKE 'c%' ORDER BY word";
        String[] expected = new String[] { "cat", "cot", "cut"};
        helpTestStringResults(sql, expected);
    }

    protected void helpTestStringResults(String sql, String[] expected) throws Exception {
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 0; i < expected.length; i++) {
            assertTrue("Should have a row", _rset.next());
            assertEquals("Should get expected result", expected[i], _rset.getString(1));
        }
        assertTrue("Should have only " + expected.length + " rows", !_rset.next());
        _rset.close();
    }

    public void test_select_count_star_from_foo_where_num_lt_n() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select COUNT(*) from FOO where NUM < ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO + 1; i++) {
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue(_rset.next());
            int count = _rset.getInt(1);
            assertEquals(i, count);
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        pstmt.close();
    }

    public void test_select_sum_num_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select sum(num) from foo where num <= ?");
        int sum = 0;
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            sum += i;
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue(_rset.next());
            int max = _rset.getInt(1);
            assertEquals(sum, max);
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        pstmt.close();
    }

    public void test_select_sum_num_from_empty_table() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select sum(num) from foo");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        _rset.getInt(1);
        assertTrue(_rset.wasNull());
        _rset.close();
        pstmt.close();
    }

    public void test_select_max_num_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select MAX(NUM) from FOO where NUM <= ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue(_rset.next());
            int max = _rset.getInt(1);
            assertEquals(i, max);
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        pstmt.close();
    }

    public void test_select_max_num_from_empty_table() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select MAX(NUM) from FOO");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        _rset.getInt(1);
        assertTrue(_rset.wasNull());
        _rset.close();
        pstmt.close();
    }

    public void test_select_min_num_from_foo() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("select MIN(NUM) from FOO where NUM >= ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            _rset = pstmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue(_rset.next());
            int min = _rset.getInt(1);
            assertEquals(i, min);
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
        }
        pstmt.close();
    }

    public void test_select_min_num_from_foo_with_null() throws Exception {
        createTableFoo();
        {
            Statement stmt = _conn.createStatement();
            stmt.execute("insert into foo values ( null, null, null )");
            stmt.close();
        }
        populateTableFoo();
        {
            Statement stmt = _conn.createStatement();
            stmt.execute("insert into foo values ( null, null, null )");
            stmt.close();
        }
        PreparedStatement pstmt = _conn.prepareStatement("select MIN(NUM) from FOO");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        assertEquals(0, _rset.getInt(1));
        assertTrue(!_rset.wasNull());
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        pstmt.close();
    }

    public void test_select_min_num_from_empty_table() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select MIN(NUM) from FOO");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        _rset.getInt(1);
        assertTrue(_rset.wasNull());
        _rset.close();
        pstmt.close();
    }

    public void test_select_count_num_from_empty_table() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("select COUNT(NUM) from FOO");
        _rset = pstmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        assertEquals(0, _rset.getInt(1));
        assertTrue(!_rset.wasNull());
        _rset.close();
        pstmt.close();
    }

    public void test_select_str_from_foo_order_by_num() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO order by NUM";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_str_from_foo_order_by_num_asc() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO order by NUM asc";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_str_from_foo_order_by_num_desc() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO order by NUM desc";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = NUM_ROWS_IN_FOO - 1; i >= 0; i--) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_str_from_foo_order_by_upper_str_desc() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select STR from FOO order by UPPER(STR) desc";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        for (int i = NUM_ROWS_IN_FOO - 1; i >= 0; i--) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(String.valueOf(i), _rset.getString(1));
            assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_rownum() throws Exception {
        createTableFoo();
        populateTableFoo();
        String sql = "select ROWNUM() from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 1; i < NUM_ROWS_IN_FOO + 1; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_rownum_with_orderby() throws Exception {
        createTableFoo();
        populateTableFoo();
        String sql = "select ROWNUM() from FOO order by NUM desc";
        _rset = _stmt.executeQuery(sql);
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 1; i < NUM_ROWS_IN_FOO + 1; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_str_from_foo_where_rownum_lt_5() throws Exception {
        createTableFoo();
        populateTableFoo();
        String sql = "select ROWNUM(), STR from FOO where ROWNUM() < 5";
        Bag found = new HashBag();
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        for (int i = 1; i < 5; i++) {
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            String str = _rset.getString(2);
            assertNotNull(str);
            assertTrue(!found.contains(str));
            found.add(str);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_where_string_eq_null() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();
        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");
        String sql = "select NUM, STR, NUMTWO from FOO where STR = NULL";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        //  X = null is always false, so expect no rows
        assertTrue(!_rset.next());
    }

    public void test_select_where_int_eq_null() throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();
        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");
        String sql = "select NUM, STR, NUMTWO from FOO where NUMTWO = NULL";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        //  X = null is always false, so expect no rows
        assertTrue(!_rset.next());
    }

    public void test_select_where_int_is_null() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");

        String sql = "select NUM, STR, NUMTWO from FOO where NUMTWO is null";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < 3; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(new Integer(i));
            int num = _rset.getInt(1);
            assertTrue(!_rset.wasNull());
            found.add(new Integer(num));
            assertNull(_rset.getString(2));
            assertTrue(_rset.wasNull());
            assertEquals(0, _rset.getInt(3));
            assertTrue(_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_select_where_int_is_not_null() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");

        String sql = "select NUM, STR, NUMTWO from FOO where NUMTWO is not null";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("ResultSet should contain more rows", _rset.next());
        assertEquals(999, _rset.getInt(1));
        assertTrue(!_rset.wasNull());
        assertEquals("XXX", _rset.getString(2));
        assertTrue(!_rset.wasNull());
        assertEquals(9, _rset.getInt(3));
        assertTrue(!_rset.wasNull());
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_where_not_int_is_null() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");

        String sql = "select NUM, STR, NUMTWO from FOO where not NUMTWO is null";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("ResultSet should contain more rows", _rset.next());
        assertEquals(999, _rset.getInt(1));
        assertTrue(!_rset.wasNull());
        assertEquals("XXX", _rset.getString(2));
        assertTrue(!_rset.wasNull());
        assertEquals(9, _rset.getInt(3));
        assertTrue(!_rset.wasNull());
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_where_string_is_null() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");

        String sql = "select NUM, STR, NUMTWO from FOO where STR is null";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < 3; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(new Integer(i));
            int num = _rset.getInt(1);
            assertTrue(!_rset.wasNull());
            found.add(new Integer(num));
            assertNull(_rset.getString(2));
            assertTrue(_rset.wasNull());
            assertEquals(0, _rset.getInt(3));
            assertTrue(_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_select_where_string_is_not_null() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, NULL, NULL )");
        for (int i = 0; i < 3; i++) {
            pstmt.setInt(1, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 999, 'XXX', 9 )");

        String sql = "select NUM, STR, NUMTWO from FOO where STR is not null";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("ResultSet should contain more rows", _rset.next());
        assertEquals(999, _rset.getInt(1));
        assertTrue(!_rset.wasNull());
        assertEquals("XXX", _rset.getString(2));
        assertTrue(!_rset.wasNull());
        assertEquals(9, _rset.getInt(3));
        assertTrue(!_rset.wasNull());
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
    }

    public void test_select_sequence_nextval() throws Exception {
        createTableFoo(false);
        populateTableFoo();
        createSequenceFooSeq();
        String sql = "SELECT foo_seq.nextval FROM foo";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("ResultSet should contain a row", _rset.next());
        assertEquals(0, _rset.getInt(1));
        _rset.close();
        dropSequenceFooSeq();
    }

    public void test_select_sequence_nextval_from_foo() throws Exception {
        createTableFoo(false);
        populateTableFoo();
        createSequenceFooSeq();
        String sql = "SELECT foo_seq.nextval FROM foo";
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            assertTrue("ResultSet should contain a row", _rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        _rset.close();
        dropSequenceFooSeq();
    }

    public void test_select_star_from_foo_f_bar_b_where_id_eq_num() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        String sql = "select * from FOO f, BAR b where b.ID = f.NUM ";
        PreparedStatement stmt = _conn.prepareStatement(sql);
        _rset = stmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue("Should have rows", _rset.next());

        assertNotNull("Should have a value", _rset.getObject(1));

        stmt.close();
    }

    public void test_select_star_from_foo_where_id_in_list() throws Exception {
        createTableFoo();
        populateTableFoo();

        String sql = "select * from FOO where NUM in (1, 2, 3)";
        PreparedStatement stmt = _conn.prepareStatement(sql);
        _rset = stmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);
        int matches = 0;
        while (_rset.next()) {
            matches++;
        }
        assertEquals("Should have 3 rows selected", 3, matches);

        stmt.close();
    }

    public void test_select_f_star_from_foo_f_bar_b_where_id_eq_num() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        String sql = "select f.* from FOO f, BAR b where b.ID = f.NUM ";
        PreparedStatement stmt = _conn.prepareStatement(sql);
        _rset = stmt.executeQuery();
        assertNotNull("Should have been able to create ResultSet", _rset);

        stmt.close();
    }

    // Test to make sure that when we rollback a connection that the PreparedStatements
    // remain valid.
    public void testPreparedStatementAfterRollback() throws Exception {
        createTableFoo();
        populateTableFoo();

        AxionConnection conn = (AxionConnection) DriverManager.getConnection(getConnectString());

        conn.setAutoCommit(false);

        PreparedStatement stmt = conn.prepareStatement("select NUM, STR from FOO");

        assertNotNull(stmt.executeQuery());

        conn.rollback();

        ResultSet rset2 = stmt.executeQuery();

        //All we are really checking is that we are able to iterate
        //through the result set.
        while (rset2.next()) {
        }
    }

}