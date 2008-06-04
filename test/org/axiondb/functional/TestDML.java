/*
 * $Id: TestDML.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.HashBag;

/**
 * Database Modification Language tests.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 */
public class TestDML extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestDML(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDML.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testTruncateTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        assertResult(NUM_ROWS_IN_FOO, "select count(*) from foo");
        _stmt.execute("truncate table foo");
        assertResult(0, "select count(*) from foo");
        populateTableFoo();
        assertResult(NUM_ROWS_IN_FOO, "select count(*) from foo");

        // now truncate twice, to test truncating an empty table
        _stmt.execute("truncate table foo");
        assertResult(0, "select count(*) from foo");
        _stmt.execute("truncate table foo");
        assertResult(0, "select count(*) from foo");
    }

    public void testInsertViaBatchStatementWithBadStatment() throws Exception {
        createTableFoo();
        Statement stmt = _conn.createStatement();
        stmt.addBatch("insert into FOO (NUM, STR, NUMTWO ) values ( 1, 'xyzzy', null  )");
        stmt.addBatch("insert into FOO (NUM, STR, NUMTWO ) values ( 'this is not a number', 'xyzzy', null  )");
        stmt.addBatch("insert into FOO (NUM, STR, NUMTWO ) values ( 2, 'xyzzy', null  )");
        int[] results = null;
        try {
            stmt.executeBatch();
            fail("Expected BatchUpdateException");
        } catch (BatchUpdateException e) {
            // expected
            results = e.getUpdateCounts();
        }
        stmt.close();

        assertEquals(3, results.length);
        assertEquals(1, results[0]);
        assertEquals(Statement.EXECUTE_FAILED, results[1]);
        assertEquals(1, results[2]);

        _rset = _stmt.executeQuery("select count(NUM) from FOO");
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testInsertViaBatchStatement() throws Exception {
        createTableFoo();

        Statement stmt = _conn.createStatement();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            stmt.addBatch("insert into FOO (NUM, STR, NUMTWO ) values ( " + i + ", 'xyzzy', null  )");
        }

        int[] results = stmt.executeBatch();
        assertEquals(NUM_ROWS_IN_FOO, results.length);

        assertEquals(0, stmt.executeBatch().length);

        stmt.close();

        for (int i = 0; i < results.length; i++) {
            assertEquals(1, results[i]);
        }

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            _rset = _stmt.executeQuery("select NUM, STR from FOO where NUM = " + i);
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals("xyzzy", _rset.getString(2));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    public void testInsertViaBatchPreparedStatement() throws Exception {
        createTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO (NUM, STR, NUMTWO ) values ( ?, 'xyzzy', null  )");

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, i);
            pstmt.addBatch();
        }

        int[] results = pstmt.executeBatch();
        assertEquals(NUM_ROWS_IN_FOO, results.length);

        assertEquals(0, pstmt.executeBatch().length);

        pstmt.close();

        for (int i = 0; i < results.length; i++) {
            assertEquals(1, results[i]);
        }

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            _rset = _stmt.executeQuery("select NUM, STR from FOO where NUM = " + i);
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals("xyzzy", _rset.getString(2));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    public void testCreateIndexOnAlreadyPopulatedTable() throws Exception {
        createTableFoo(false);
        populateTableFoo();
        assertEquals(1, _stmt.executeUpdate("delete from FOO where NUM = 1"));
        assertEquals(1, _stmt.executeUpdate("update FOO set NUM = 1, STR = '1', NUMTWO = 0 where NUM = 2"));
        assertEquals(1, _stmt.executeUpdate("insert into FOO (NUM, STR, NUMTWO ) values ( 2, '2', 1  )"));
        createIndexOnFoo();

        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            _rset = _stmt.executeQuery("select NUM, STR from FOO where NUM = " + i);
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
            assertEquals(String.valueOf(i), _rset.getString(2));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    public void test_delete_from_foo_where_num_gteq_5() throws Exception {
        createTableFoo();
        populateTableFoo();

        assertEquals(NUM_ROWS_IN_FOO - 5, _stmt.executeUpdate("delete from FOO where NUM >= 5"));

        _rset = _stmt.executeQuery("select STR from FOO");
        assertNotNull("Should have been able to create ResultSet", _rset);
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < 5; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(String.valueOf(i));
            String val = _rset.getString(1);
            assertNotNull("Returned String should not be null", val);
            assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
            found.add(val);
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);
    }

    public void test_delete_via_pstmt() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("delete from FOO where NUM >= ? and NUM < ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i += 2) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i + 2);
            assertEquals(2, pstmt.executeUpdate());

            _rset = _stmt.executeQuery("select STR from FOO");
            assertNotNull("Should have been able to create ResultSet", _rset);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            for (int j = i + 2; j < NUM_ROWS_IN_FOO; j++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                expected.add(String.valueOf(j));
                String val = _rset.getString(1);
                assertNotNull("Returned String should not be null", val);
                assertTrue("ResultSet shouldn't think value was null", !_rset.wasNull());
                assertTrue("Shouldn't have seen \"" + val + "\" yet", !found.contains(val));
                found.add(val);
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);

            pstmt.clearParameters();
        }
    }

    public void test_update_via_pstmt() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("update FOO set STR = ? where NUM >= ? and NUM < ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i += 2) {
            pstmt.setString(1, "X");
            pstmt.setInt(2, i);
            pstmt.setInt(3, i + 2);
            assertEquals(2, pstmt.executeUpdate());

            _rset = _stmt.executeQuery("select STR from FOO where STR = 'X'");
            assertNotNull("Should have been able to create ResultSet", _rset);
            for (int j = 0; j < i + 2; j++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                assertNotNull(_rset.getString(1));
                assertEquals("X", _rset.getString(1));
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();

            pstmt.clearParameters();
        }
    }

    public void test_update_key_via_pstmt() throws Exception {
        createTableFoo();
        populateTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement("update FOO set NUM = ? where NUM = ?");
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            pstmt.setInt(1, (10 * i));
            pstmt.setInt(2, i);
            assertEquals(1, pstmt.executeUpdate());

            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull("Should have been able to create ResultSet", _rset);
            Bag expected = new HashBag();
            Bag found = new HashBag();
            for (int j = 0; j <= i; j++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                expected.add(new Integer(j * 10));
                found.add(new Integer(_rset.getInt(1)));
            }
            for (int j = i + 1; j < NUM_ROWS_IN_FOO; j++) {
                assertTrue("ResultSet should contain more rows", _rset.next());
                expected.add(new Integer(j));
                found.add(new Integer(_rset.getInt(1)));
            }
            assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
            _rset.close();
            assertEquals(expected, found);

            pstmt.clearParameters();
        }
    }

    public void testUpdateToColumn() throws Exception {
        createTableFoo();
        populateTableFoo();
        assertEquals(NUM_ROWS_IN_FOO, _stmt.executeUpdate("update FOO set NUMTWO = NUM"));
        _rset = _stmt.executeQuery("select NUMTWO from FOO");
        HashBag expected = new HashBag();
        HashBag found = new HashBag();
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(_rset.next());
            expected.add(new Integer(i));
            found.add(new Integer(_rset.getInt(1)));
            assertTrue(!_rset.wasNull());
        }
        assertTrue(!_rset.next());
        assertEquals(expected, found);
    }

    public void testUpdateToFunction() throws Exception {
        createTableFoo();
        populateTableFoo();
        assertEquals(NUM_ROWS_IN_FOO, _stmt.executeUpdate("update FOO set STR = CONCAT(STR,';',NUM)"));
        _rset = _stmt.executeQuery("select STR from FOO");
        HashBag expected = new HashBag();
        HashBag found = new HashBag();
        for (int i = 0; i < NUM_ROWS_IN_FOO; i++) {
            assertTrue(_rset.next());
            expected.add(String.valueOf(i) + ";" + String.valueOf(i));
            found.add(_rset.getString(1));
            assertTrue(!_rset.wasNull());
        }
        assertTrue(!_rset.next());
        assertEquals(expected, found);
    }

    public void test_insert_null() throws Exception {
        createTableFoo();

        for (int i = 0; i < 10; i++) {
            _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", NULL, NULL )");
        }

        String sql = "select NUM, STR, NUMTWO from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < 10; i++) {
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

    public void test_insert_without_colnames() throws Exception {
        createTableFoo();

        for (int i = 0; i < 3; i++) {
            _stmt.execute("insert into FOO values ( " + i + ", '" + i + "', " + (i / 2) + " )");
        }

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

        for (int i = 0; i < 3; i++) {
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

    public void test_insert_via_stmt() throws Exception {
        createTableFoo();
        // INSERT
        for (int i = 0; i < 3; i++) {
            assertEquals(1, _stmt.executeUpdate("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", '" + i + "', " + (i / 2) + " )"));
        }

        // SELECT
        _rset = _stmt.executeQuery("select STR from FOO");
        assertNotNull("Should have been able to create ResultSet", _rset);
        Bag expected = new HashBag();
        Bag found = new HashBag();
        for (int i = 0; i < 3; i++) {
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

    public void test_insert_via_pstmt() throws Exception {
        createTableFoo();
        // INSERT
        PreparedStatement stmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ? )");
        for (int i = 0; i < 3; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, String.valueOf(i));
            stmt.setInt(3, i / 2);
            assertEquals(1, stmt.executeUpdate());
        }
        stmt.close();

        // SELECT
        _rset = _stmt.executeQuery("select STR from FOO");
        assertNotNull("Should have been able to create ResultSet", _rset);
        Bag expected = new HashBag();
        Bag found = new HashBag();
        for (int i = 0; i < 3; i++) {
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

    public void test_insert_sequence_nextval() throws Exception {
        createTableFoo();
        createSequenceFooSeq();

        PreparedStatement stmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( foo_seq.nextval, null, ? )");
        for (int i = 0; i < 10; i++) {
            stmt.setInt(1, i);
            assertEquals(1, stmt.executeUpdate());
        }
        stmt.close();

        // SELECT
        _rset = _stmt.executeQuery("select NUM, NUMTWO from FOO");
        assertNotNull("Should have been able to create ResultSet", _rset);
        Bag expected = new HashBag();
        Bag found = new HashBag();
        for (int i = 0; i < 10; i++) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            assertEquals(_rset.getInt(1), _rset.getInt(2));

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

    public void test_update_to_sequence_nextval() throws Exception {
        createTableFoo();
        createSequenceFooSeq();
        {
            String sql = "insert into FOO ( NUM, STR, NUMTWO ) values ( ?, 'foo', 999 )";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            stmt.setInt(1, 999);
            assertEquals(1, stmt.executeUpdate());
            stmt.close();
        }
        {
            String sql = "update FOO set NUM = foo_seq.nextval where NUM = 999";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            assertEquals(1, stmt.executeUpdate());
            stmt.close();
        }
        {
            String sql = "select NUM, STR, NUMTWO from FOO where STR = 'foo'";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            ResultSet rset = stmt.executeQuery();
            assertNotNull(rset);
            assertTrue(rset.next());
            assertTrue(999 != rset.getInt(1));
            assertTrue(!rset.wasNull());
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
    }

    public void test_update_multiple_rows_to_sequence_nextval() throws Exception {
        createTableFoo();
        createSequenceFooSeq();
        {
            String sql = "insert into FOO ( NUM, STR, NUMTWO ) values ( ?, 'foo', 999 )";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            stmt.setInt(1, 10);
            assertEquals(1, stmt.executeUpdate());
            stmt.setInt(1, 20);
            assertEquals(1, stmt.executeUpdate());
            stmt.close();
        }
        {
            String sql = "update FOO set NUM = foo_seq.nextval where NUMTWO = 999";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            assertEquals(2, stmt.executeUpdate());
            stmt.close();
        }
        {
            String sql = "select NUM, STR, NUMTWO from FOO where STR = 'foo'";
            PreparedStatement stmt = _conn.prepareStatement(sql);
            ResultSet rset = stmt.executeQuery();
            assertNotNull(rset);
            assertTrue(rset.next());
            int valone = rset.getInt(1);
            assertTrue(!rset.wasNull());
            assertTrue(rset.next());
            int valtwo = rset.getInt(1);
            assertTrue(!rset.wasNull());
            assertTrue(valone != valtwo);
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
    }
    
    public void testFKConstraint1() throws Exception {
        _stmt.execute("create table A (X number(9,0), Y number(9,0))");
        _stmt.execute("create table B (X number(9,0), Y number(9,0))");

        _stmt.execute("ALTER TABLE A ADD CONSTRAINT A_PK PRIMARY KEY (X,Y)");
        _stmt.execute("ALTER TABLE B ADD CONSTRAINT B2A FOREIGN KEY (X,Y) REFERENCES A");

        try {
            _stmt.execute("insert into b values (1,1)");
            fail("Expected SQLException parent row does not exist");
        } catch (SQLException ignore) {
            // keep going
        }
        
        _stmt.execute("insert into a values (1,1)");
        _stmt.execute("insert into a values (1,2)");
        
        _stmt.execute("insert into b values (1,1)");
        _stmt.execute("insert into b values (1,2)");

        try {
            _stmt.execute("delete from a where x = 1");
            fail("Expected SQLException can't delete parent row if child row refering it");
        } catch (SQLException ignore) {
            // keep going
        }
        
        try {
            _stmt.execute("update b set x = 5 where b.y = 1");
            fail("Expected SQLException can't delete parent row if child row refering it");
        } catch (SQLException ignore) {
            // keep going
        }
        
        _stmt.execute("update b set x = null where b.y = 1");
        
    }
    
    public void testFKConstraint2() throws Exception {

        _stmt.execute("create table A (X number(9,0))");
        _stmt.execute("create table B (X number(9,0))");
        _stmt.execute("create table C (Y number(9,0))");

        _stmt.execute("ALTER TABLE A ADD CONSTRAINT A_PK PRIMARY KEY (X)");
        
        // should automatically pick parent table column that matches the name for child column
        _stmt.execute("ALTER TABLE B ADD CONSTRAINT B2A FOREIGN KEY (X) REFERENCES A");
        
        // should automatically pick the PK of parent table column as FK
        _stmt.execute("ALTER TABLE C ADD CONSTRAINT C2A FOREIGN KEY (Y) REFERENCES A");

        try {
            _stmt.execute("ALTER TABLE C ADD CONSTRAINT C2A2 FOREIGN KEY (Y) REFERENCES A(D)");
            fail("Expected SQLException Parent column does not exist");
        } catch (SQLException ignore) {
            // keep going
        }
    }

}