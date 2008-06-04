/*
 * $Id: TestBugs.java,v 1.2 2007/11/29 16:35:37 jawed Exp $
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

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.axiondb.jdbc.AxionConnection;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.2 $ $Date: 2007/11/29 16:35:37 $
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class TestBugs extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestBugs(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestBugs.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected String getConnectString() {
        return "jdbc:axiondb:testdb:testdb";
    }

    protected File getDatabaseDirectory() {
        return new File(new File("."), "testdb");
    }

    //------------------------------------------------------------------- Tests

    public void testSelfJoinBug() throws Exception {
        _stmt.execute("create table ALPHA ( ID int, DESCR varchar(10) )");
        _stmt.execute("insert into ALPHA values ( 1, 'one' )");
        _stmt.execute("insert into ALPHA values ( 2, 'two' )");

        _stmt.execute("create table BETA ( ID int, DESCR varchar(10) )");
        _stmt.execute("insert into BETA values ( 1, 'one' )");
        _stmt.execute("insert into BETA values ( 2, 'two' )");

        // works when joined another
        {
            ResultSet rset = _stmt.executeQuery("select A.id, B.descr from alpha A, beta B where A.id = B.id order by A.id");
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("one", rset.getString(2));
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("two", rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
        }

        // but not with self
        {
            ResultSet rset = _stmt.executeQuery("select A.id, B.descr from alpha A, alpha B where A.id = B.id order by A.id");
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("one", rset.getString(2));
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("two", rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
        }
    }

    // ISSUE #XX
    // See http://axion.tigris.org/servlets/ReadMsg?list=users&msgNo=215
    public void testUniqueBTreeIndexBug1() throws Exception {
        _stmt.execute("CREATE TABLE TEST_TABLE (ID VARCHAR);");
        _stmt.execute("CREATE UNIQUE INDEX TEST_INDEX ON TEST_TABLE (ID);");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('a');");

        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'a';"));
        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'b';"));
    }

    // ISSUE #XX
    // See http://axion.tigris.org/servlets/ReadMsg?list=dev&msgNo=825
    public void testUniqueBTreeIndexBug2() throws Exception {
        _stmt.execute("CREATE TABLE TEST_TABLE (ID VARCHAR);");
        _stmt.execute("CREATE UNIQUE BTREE INDEX TEST_INDEX ON TEST_TABLE (ID);");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");
        assertResult("b", "SELECT id FROM TEST_TABLE where ID='b'");
        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'b';"));
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");
        assertResult("b", "SELECT id FROM TEST_TABLE where ID='b'");

        _stmt.execute("SHUTDOWN");
        _stmt.close();
        _conn.close();
        super.setUp();
        assertResult("b", "SELECT id FROM TEST_TABLE where ID='b'");
    }
    
    // ISSUE #XX
    // See http://axion.tigris.org/servlets/ReadMsg?list=users&msgNo=215
    public void testUniqueArrayIndexBug1() throws Exception {
        _stmt.execute("CREATE TABLE TEST_TABLE (ID VARCHAR);");
        _stmt.execute("CREATE UNIQUE ARRAY INDEX TEST_INDEX ON TEST_TABLE (ID);");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('a');");

        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'a';"));
        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'b';"));
    }

    // ISSUE #XX
    // See http://axion.tigris.org/servlets/ReadMsg?list=dev&msgNo=825
    public void testUniqueArrayIndexBug2() throws Exception {
        _stmt.execute("CREATE TABLE TEST_TABLE (ID VARCHAR);");
        _stmt.execute("CREATE UNIQUE ARRAY INDEX TEST_INDEX ON TEST_TABLE (ID);");
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");
        assertEquals(1, _stmt.executeUpdate("DELETE FROM TEST_TABLE WHERE ID = 'b';"));
        _stmt.execute("INSERT INTO TEST_TABLE VALUES ('b');");

        _stmt.execute("SHUTDOWN");
        _stmt.close();
        _conn.close();
        super.setUp();
        assertResult("b", "SELECT id FROM TEST_TABLE where ID='b'");
    }    

    public void testInWithNullValuesBug() throws Exception {
        _stmt.execute("create table FOO ( id int, parent_id int )");
        _stmt.execute("insert into FOO values ( 1, null )");
        _stmt.execute("insert into FOO values ( 2, 1 )");
        _stmt.execute("insert into FOO values ( 3, 1 )");
        _stmt.execute("insert into FOO values ( 4, 2 )");
        assertResult(1, "select id from FOO where parent_id is null");
        assertResult(4, "select id from FOO where parent_id = 2");
        assertResult(4, "select id from FOO where parent_id in ( 2 )");
    }

    // ISSUE #XX
    // See http://axion.tigris.org/servlets/ReadMsg?list=dev&msgNo=208
    public void test_IssueXX_BooleanBug() throws Exception {
        _stmt.execute("create table BOOL_BUG ( B boolean )");
        PreparedStatement psmt = _conn.prepareStatement("insert into BOOL_BUG values ( ? )");
        psmt.setBoolean(1, true);
        psmt.executeUpdate();
        psmt.close();
        _rset = _stmt.executeQuery("select B from BOOL_BUG");
        assertTrue(_rset.next());
        assertTrue(_rset.getBoolean(1));
    }

    public void test_AggregationBug_CountStar() throws Exception {
        _stmt.execute("create table COUNT_BUG ( \"DATA\"  varchar(10) )");
        _conn.setAutoCommit(false);
        _stmt.execute("insert into COUNT_BUG values ( 'ABC' )");
        _stmt.execute("insert into COUNT_BUG values ( 'XYZ' )");
        _stmt.execute("insert into COUNT_BUG values ( 'XYZ' )");
        _conn.commit();
        PreparedStatement psmt = _conn.prepareStatement("select count(*) from COUNT_BUG where \"DATA\" = ?");
        psmt.setString(1, "xyzzy");
        assertResult(0, psmt.executeQuery());
        psmt.setString(1, "ABC");
        assertResult(1, psmt.executeQuery());
        psmt.setString(1, "XYZ");
        assertResult(2, psmt.executeQuery());
        psmt.setString(1, "xyzzy");
        assertResult(0, psmt.executeQuery());
    }

    public void test_AggregationBug_GroupBy() throws Exception {
        _stmt.execute("create table COUNT_BUG ( \"DATA\" varchar(10) )");
        _conn.setAutoCommit(false);
        _stmt.execute("insert into COUNT_BUG values ( 'ABC' )");
        _stmt.execute("insert into COUNT_BUG values ( 'XYZ' )");
        _stmt.execute("insert into COUNT_BUG values ( 'XYZ' )");
        _conn.commit();
        PreparedStatement psmt = _conn.prepareStatement("select \"DATA\" from COUNT_BUG where \"DATA\" = ? group by \"DATA\"");
        psmt.setString(1, "xyzzy");
        assertNoRows(psmt.executeQuery());
        psmt.setString(1, "ABC");
        assertResult("ABC", psmt.executeQuery());
        psmt.setString(1, "XYZ");
        assertResult("XYZ", psmt.executeQuery());
        psmt.setString(1, "xyzzy");
        assertNoRows(psmt.executeQuery());
    }

    // ISSUE #1
    // See http://axion.tigris.org/issues/show_bug.cgi?id=1
    // Issue #1 was a bug against the old code base, it is no longer valid.

    // ISSUE #2
    // See http://axion.tigris.org/issues/show_bug.cgi?id=2
    // Issue #2 was a bug against the old code base, it is no longer valid.

    // ISSUE #3
    // See http://axion.tigris.org/issues/show_bug.cgi?id=3
    // Issue #3 is invalid, a classpath error on the part of the user.

    // ISSUE #4
    // See http://axion.tigris.org/issues/show_bug.cgi?id=4
    // Issue #4 reports a file missing from the source distribution. Fixed, but not
    // testable here.

    // ISSUE #5
    // See http://axion.tigris.org/issues/show_bug.cgi?id=5
    public void test_Issue5_IndexOnBooleanColumn_default() throws Exception {
        booleanTableTest("");
    }

    // ISSUE #5
    // See http://axion.tigris.org/issues/show_bug.cgi?id=5
    public void test_Issue5_IndexOnBooleanColumn_array() throws Exception {
        booleanTableTest("array");
    }

    // ISSUE #5
    // See http://axion.tigris.org/issues/show_bug.cgi?id=5
    public void test_Issue5_IndexOnBooleanColumn_btree() throws Exception {
        booleanTableTest("btree");
    }

    // ISSUE #5
    // See http://axion.tigris.org/issues/show_bug.cgi?id=5
    private void booleanTableTest(String indextype) throws SQLException {
        _stmt.execute("create table t ( b boolean )");
        _stmt.execute("create " + indextype + " index i on t ( b )");

        assertEquals(1, _stmt.executeUpdate("insert into t values ( true )"));
        _rset = _stmt.executeQuery("select * from t");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getBoolean(1));
        assertTrue(!_rset.next());

        assertEquals(1, _stmt.executeUpdate("insert into t values ( true )"));
        assertEquals(1, _stmt.executeUpdate("insert into t values ( false )"));
        _rset = _stmt.executeQuery("select count(*) from t");
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertTrue(!_rset.next());
    }

    // ISSUE #6
    // See http://axion.tigris.org/issues/show_bug.cgi?id=6
    public void test_Issue6_CantCompareIntAndBigDecimal() throws Exception {
        _stmt.execute("create table x1 ( id int )");
        _stmt.execute("create table x2 ( id number )");
        _stmt.executeUpdate("insert into x1 values ( 1 )");
        _stmt.executeUpdate("insert into x2 values ( 1 )");
        assertOneRow("select * from x1 left outer join x2 on (x1.id = x2.id)");
        assertOneRow("select * from x1, x2 where x1.id = x2.id");
    }

    // ISSUE #7
    // See http://axion.tigris.org/issues/show_bug.cgi?id=7
    public void test_Issue7_ParserAcceptsGibberishAfterStatement() throws Exception {
        _stmt.execute("create table X ( id int )");
        try {
            _stmt.executeQuery("select * from X Y xyzzy");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            _stmt.executeQuery("select * from X where true xyzzy");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    // ISSUE #8
    // See http://axion.tigris.org/issues/show_bug.cgi?id=8
    // TODO: Issue #8 (Insufficient Column Metadata) test me.

    // ISSUE #9
    // See http://axion.tigris.org/issues/show_bug.cgi?id=9
    // See org.axiondb.engine.TestDiskDatabase.testMetaFile

    // ISSUE #10
    // See http://axion.tigris.org/issues/show_bug.cgi?id=10
    public void test_Issue10_ColumnResolutionWithInBug() throws Exception {
        _stmt.execute("create table product ( product_id int, sku varchar(10) )");
        _stmt.executeUpdate("insert into product values ( 1000, 'sku0' )");
        _stmt.execute("create table category_product ( category_id int, product_id int )");
        _stmt.executeUpdate("insert into category_product values ( 100, 1000 )");

        assertOneRow("select * from category_product where category_product.category_id in ( 100 )");

        assertOneRow("select * from category_product, product where category_product.category_id in ( 100 ) and product.product_id = category_product.product_id");
        assertOneRow("select * from category_product, product where product.product_id = category_product.product_id and category_product.category_id in ( 100 )");

        assertOneRow("select * from product, category_product where category_product.category_id = 100 and product.product_id = category_product.product_id");
        assertOneRow("select * from product, category_product where product.product_id = category_product.product_id and category_product.category_id = 100");

        assertOneRow("select * from product, category_product where category_product.category_id in ( 100 ) and product.product_id = category_product.product_id");
        assertOneRow("select * from product, category_product where product.product_id = category_product.product_id and category_product.category_id in ( 100 )");
    }

    // ISSUE #11
    // See http://axion.tigris.org/issues/show_bug.cgi?id=11
    public void test_Issue11_ParensRequiredAroundJoinCondition() throws Exception {
        _stmt.execute("create table X ( id int )");
        _stmt.execute("create table Y ( id int )");
        _stmt.executeUpdate("insert into X values ( 1 )");
        _stmt.executeUpdate("insert into Y values ( 1 )");
        assertOneRow("select * from x, y where x.id = y.id");

        assertOneRow("select * from x left outer join y on ( x.id = y.id )");
        assertOneRow("select * from y left outer join x on ( x.id = y.id )");
        assertOneRow("select * from x right outer join y on ( x.id = y.id )");
        assertOneRow("select * from y right outer join x on ( x.id = y.id )");

        assertOneRow("select * from x left outer join y on (( x.id = y.id ))");
        assertOneRow("select * from x left outer join y on ((( x.id = y.id )))");

        assertException("select * from y right outer join x on ( x.id = y.id ");
        assertException("select * from y right outer join x on x.id = y.id )");

        assertOneRow("select * from x left outer join y on x.id = y.id");
        assertOneRow("select * from y left outer join x on x.id = y.id");
        assertOneRow("select * from x right outer join y on x.id = y.id");
        assertOneRow("select * from y right outer join x on x.id = y.id");
    }

    // ISSUE #12
    // See http://axion.tigris.org/issues/show_bug.cgi?id=12
    // TODO: Issue #12 (System tables should be read only) test me.

    // ISSUE #13
    // See http://axion.tigris.org/issues/show_bug.cgi?id=13
    public void test_Issue13_PlacementOfDefaultClauseInCreateTable() throws Exception {
        _stmt.execute("create table A ( id varchar default 'xyzzy' )");
        _stmt.execute("create table B ( id varchar(100) default 'xyzzy' )");
        _stmt.execute("create table C ( id varchar(100) default 'xyzzy' PRIMARY KEY )");
        _stmt.execute("create table D ( id varchar(100) default 'xyzzy' UNIQUE NOT NULL )");
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_1() throws Exception {
        _stmt.execute("create table foo ( a int, b int, c int )");
        _stmt.executeUpdate("insert into foo values ( 0, 0, 0 )");
        _stmt.executeUpdate("insert into foo values ( 1, 0, 1 )");
        _stmt.executeUpdate("insert into foo values ( 2, 1, 1 )");
        _stmt.executeUpdate("insert into foo values ( 3, 1, 2 )");
        _stmt.executeUpdate("insert into foo values ( 4, 2, 2 )");
        _stmt.executeUpdate("insert into foo values ( 5, 2, 3 )");
        for (int i = 0; i < 6; i++) {
            assertOneRow("select * from foo where b+c=a AND a = " + i);
            assertOneRow("select * from foo where b+c = " + i);
            assertOneRow("select * from foo where b + c = " + i);
            assertResult(i, "select b + c from foo where a = " + i);
            assertResult(i, "select b + c from foo where a = " + i);
            assertResult(0, "select b + c - a from foo where a = " + i);
            assertResult(0, "select b+c-a from foo where a = " + i);
            assertException("select b + c - a = 0 from foo where a = " + i);
            assertException("select b+c-a = 0 from foo where a = " + i);
        }
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_2() throws Exception {
        assertResult(1, "select 1");
        assertResult(-1, "select -1");
        assertResult(2, "select 1+1");
        assertResult(2, "select 1 + 1");
        assertResult(2, "select 1-(-1)");
        assertResult(2, "select 1 - -1");
        assertResult(2, "select 1 --1");
        assertResult(0, "select 1-1");
        assertResult(0, "select 1 - 1");
        assertResult(0, "select 1 + -1");
        assertResult(0, "select 1+-1");
        assertResult(0, "select 1 + (-1)");
        assertResult(0, "select 1 + 0 - 1");
        assertResult(0, "select 1+0-1");
        assertResult(0, "select (1+0) - 1");
        assertResult(0, "select (1+0)-1");
        assertResult(0, "select 1+(0 - 1)");
        assertResult(0, "select 1+(0-1)");
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_3() throws Exception {
        _stmt.execute("create table foo ( a int, b int, c int )");
        _stmt.executeUpdate("insert into foo values ( 3, 1, 2 )");
        assertResult(3, "select a from foo where true");

        ResultSet rset = _stmt.executeQuery("select a from foo where false");
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select a from foo where true or false");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where true and false");
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select a from foo where (true or false) and false");
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select a from foo where true or (false and false)");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where true or false and false");
        assertTrue(rset.next());

        assertException("select true or false");
        assertException("select true and false");
        assertException("select 1=1");
        assertException("select 1>1");
        assertException("select 1<1");
        assertException("select 1<=1");
        assertException("select 1>=1");
        assertException("select 1!=1");
        assertException("select 1<>1");
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_4() throws Exception {
        _stmt.execute("create table foo ( a int, b int, c int )");
        _stmt.executeUpdate("insert into foo values ( 3, 1, 2 )");

        ResultSet rset = _stmt.executeQuery("select a from foo where 1 + 1 = 2");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where 2 + 4 < 6");
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select a from foo where 1 + 1 = 2 or 2 + 4 < 6");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where 1 + 1 = 2 and 2 + 4 < 6");
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select a from foo where 1 + 1 = 2 or (2 + 4 < 6 and 2 + 4 < 6)");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where 1 + 1 = 2 or 2 + 4 < 6 and 2 + 4 < 6");
        assertTrue(rset.next());

        rset = _stmt.executeQuery("select a from foo where (1 + 1 = 2 or 2 + 4 < 6) and 2 + 4 < 6");
        assertFalse(rset.next());
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_5() throws Exception {
        _stmt.execute("create table foo ( a int, b int, c int )");
        _stmt.executeUpdate("insert into foo values ( 3, 1, 2 )");

        assertExceptionOnRead("select a from foo where 1 AND 2");
        assertExceptionOnRead("select a from foo where true AND 0");
        assertExceptionOnRead("select a from foo where true AND 0");

        // since the first part is true, we don't evaluate the second
        ResultSet rset = _stmt.executeQuery("select a from foo where 1+1 > 1 OR 0");
        assertTrue(rset.next());

        // since the first part is true, we do evaluate the second
        assertExceptionOnRead("select a from foo where 1+1 > 1 AND 0");

        // since the first part is false, we evaluate the second
        assertExceptionOnRead("select a from foo where 1+1 > 2 OR 0");
    }

    // ISSUE #14
    // See http://axion.tigris.org/issues/show_bug.cgi?id=14
    public void test_Issue14_ExpressionParsing_6() throws Exception {
        assertResult(3, "select 3");
        assertResult(-3, "select -3");
        assertResult(3.1f, "select 3.1");
        assertResult(-3.1f, "select -3.1");
        assertResult(3.14f, "select 3.14");
        assertResult(-3.14f, "select -3.14");
    }

    // ISSUE #15
    // See http://axion.tigris.org/issues/show_bug.cgi?id=15
    public void test_Issue15_InlineConcatOperator() throws Exception {
        assertResult("ab", "select 'a' || 'b'");
        assertResult("abc", "select 'a' || 'b' || 'c'");
        assertResult("abc", "select 'a'||'b'||'c'");
        assertResult("a||b||c", "select 'a||b' || '||c'");
        assertResult("a||b||c", "select 'a||b'||'||c'");
        assertResult("ab", "select ('a' || 'b')");
        assertResult("ab", "select ('a') || ('b')");
        assertResult("ab", "select 'a' || (('b'))");
        assertResult("abc", "select ('a' || 'b') || 'c'");
        assertResult("abc", "select 'a' || ('b' || 'c')");
        assertResult("abc", "select ('a' || ('b' || 'c'))");
        assertNullResult("select null || null");
        assertNullResult("select 'a' || null");
        assertNullResult("select null || 'a'");
    }

    // ISSUE #16
    // See http://axion.tigris.org/issues/show_bug.cgi?id=16
    public void test_Issue16_MinThrowsNPE() throws Exception {
        _stmt.execute("create table foo ( num int )");
        assertNullResult("select min(num) from foo");
        for (int i = 0; i < 10; i++) {
            assertEquals(1, _stmt.executeUpdate("insert into foo values ( " + i + ")"));
        }
        assertResult(0, "select min(num) from foo");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( null )"));
        assertResult(0, "select min(num) from foo");
        assertResult(0, "select min(num) from foo where num is not null");
    }

    // ISSUE #16
    // See http://axion.tigris.org/issues/show_bug.cgi?id=16
    public void test_Issue16_MaxThrowsNPE() throws Exception {
        _stmt.execute("create table foo ( num int )");
        assertNullResult("select max(num) from foo");
        for (int i = 0; i < 10; i++) {
            assertEquals(1, _stmt.executeUpdate("insert into foo values ( " + i + ")"));
        }
        assertResult(9, "select max(num) from foo");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( null )"));
        assertResult(9, "select max(num) from foo");
        assertResult(9, "select max(num) from foo where num is not null");
    }

    // ISSUE #16
    // See http://axion.tigris.org/issues/show_bug.cgi?id=16
    public void test_Issue16_SumThrowsNPE() throws Exception {
        _stmt.execute("create table foo ( num int )");
        assertNullResult("select sum(num) from foo");
        int sum = 0;
        for (int i = 0; i < 10; i++) {
            sum += i;
            assertEquals(1, _stmt.executeUpdate("insert into foo values ( " + i + ")"));
        }
        assertResult(sum, "select sum(num) from foo");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( null )"));
        assertResult(sum, "select sum(num) from foo");
        assertResult(sum, "select sum(num) from foo where num is not null");
    }

    // ISSUE #17
    // See http://axion.tigris.org/issues/show_bug.cgi?id=17
    public void test_Issue17_FloatingPointScale() throws Exception {
        {
            ResultSet rset = _stmt.executeQuery("select (1.5 + 1.0)");
            assertTrue(rset.next());
            assertEquals(2.5f, rset.getFloat(1), 0.0f);
        }
        {
            ResultSet rset = _stmt.executeQuery("select (1.53333 + 1.0)");
            assertTrue(rset.next());
            assertEquals(2.53333f, rset.getFloat(1), 0.0f);
        }
        {
            ResultSet rset = _stmt.executeQuery("select (1.5 + 1.00)");
            assertTrue(rset.next());
            assertEquals(2.5f, rset.getFloat(1), 0.0f);
        }
        {
            ResultSet rset = _stmt.executeQuery("select (1.50009 + 0.000000000001)");
            assertTrue(rset.next());
            BigDecimal expected = new BigDecimal("1.500090000001");
            BigDecimal found = rset.getBigDecimal(1);
            assertTrue("Expected " + expected + ", found " + found, 0 == expected.compareTo(found));
        }
        {
            ResultSet rset = _stmt.executeQuery("select (1.50009 + 1.000000000001)");
            assertTrue(rset.next());
            BigDecimal expected = new BigDecimal("2.500090000001");
            BigDecimal found = rset.getBigDecimal(1);
            assertTrue("Expected " + expected + ", found " + found, 0 == expected.compareTo(found));
        }
    }

    // ISSUE #18
    // See http://axion.tigris.org/issues/show_bug.cgi?id=18
    public void test_Issue18() throws Exception {
        _stmt.execute("create table employee ( id int, salary int)");
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 1, 200000 )"));
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 2, 100000 )"));

        {
            ResultSet rset = _stmt.executeQuery("select max(salary/4) from employee");
            assertTrue(rset.next());
            assertEquals(200000 / 4, rset.getInt(1));
            assertFalse(rset.next());
            rset.close();
        }
        {
            ResultSet rset = _stmt.executeQuery("select max(salary)/4 from employee");
            assertTrue(rset.next());
            assertEquals(200000 / 4, rset.getInt(1));
            assertFalse(rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select 400000/max(salary/2) from employee");
            assertTrue(rset.next());
            assertEquals(400000 / (200000 / 2), rset.getInt(1));
            assertFalse(rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select 4/max(4/2)");
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertFalse(rset.next());
            rset.close();
        }

    }

    // ISSUE #18
    // Group By should also allow scalar function in select which have literal
    // and column on which we are grouping is used.
    public void test_Issue18_1() throws Exception {
        _stmt.execute("create table employee ( id int, salary int, name varchar(60))");
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 1, 10000, 'A' )"));
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 2, 5000 , 'B' )"));
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 1, 10000, 'A' )"));
        assertEquals(1, _stmt.executeUpdate("insert into employee values ( 2, 5000,  'B' )"));

        {
            ResultSet rset = _stmt.executeQuery("select id, 'Mr. ' || name, sum(salary)  from employee group by id, name order by id ");
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("Mr. A", rset.getString(2));
            assertEquals(20000, rset.getInt(3));
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mr. B", rset.getString(2));
            assertEquals(10000, rset.getInt(3));
            assertFalse(rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select id, 'Mr. ' || name, sum(salary)+5000  from employee group by id, name order by id ");
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("Mr. A", rset.getString(2));
            assertEquals(25000, rset.getInt(3));
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mr. B", rset.getString(2));
            assertEquals(15000, rset.getInt(3));
            assertFalse(rset.next());
            rset.close();
        }
    }

    // ISSUE #21
    // See http://axion.tigris.org/issues/show_bug.cgi?id=21
    public void test_Issue21_OuterJoin() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 1, 'AAA' )"));
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 2, 'BBB' )"));

        ResultSet rset = _stmt.executeQuery("select * from x left join y on x.id = y.id");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("AAA", rset.getString(2));
        assertEquals(0, rset.getInt(3));
        assertTrue(rset.wasNull());
        assertTrue(null == rset.getString(4));
        assertTrue(rset.wasNull());

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("BBB", rset.getString(2));
        assertEquals(0, rset.getInt(3));
        assertTrue(rset.wasNull());
        assertTrue(null == rset.getString(4));
        assertTrue(rset.wasNull());

        assertFalse(rset.next());

        rset.close();
    }

    // ISSUE #23
    // See http://axion.tigris.org/issues/show_bug.cgi?id=23
    public void test_Issue23() throws Exception {
        _stmt.execute("create table emp(id int, name varchar(5), deptcd int, emptype int)");
        assertEquals(1, _stmt.executeUpdate("insert into emp values(1, 'ahi', 1, 3)"));
        assertEquals(1, _stmt.executeUpdate("insert into emp values(2, 'Jon', 2, 4)"));

        _stmt.execute("create table code(code int, codename varchar(8))");
        assertEquals(1, _stmt.executeUpdate("insert into code  values(1, 'RAD')"));
        assertEquals(1, _stmt.executeUpdate("insert into code  values(2, 'HR')"));
        assertEquals(1, _stmt.executeUpdate("insert into code  values(3, 'Perm')"));
        assertEquals(1, _stmt.executeUpdate("insert into code  values(4, 'Contract')"));

        {
            ResultSet rset = _stmt.executeQuery("select emp.id, emp.name, t1.codename, t2.codename from emp, code t1, code t2 where emp.deptcd = t1.code and emp.emptype = t2.code");

            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("ahi", rset.getString(2));
            assertEquals("RAD", rset.getString(3));
            assertEquals("Perm", rset.getString(4));

            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Jon", rset.getString(2));
            assertEquals("HR", rset.getString(3));
            assertEquals("Contract", rset.getString(4));

            assertFalse(rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select emp.id, emp.name, t1.codename, t2.codename from emp inner join code t1 on deptcd = t1.code inner join code t2 on emp.emptype = t2.code");

            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("ahi", rset.getString(2));
            assertEquals("RAD", rset.getString(3));
            assertEquals("Perm", rset.getString(4));

            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Jon", rset.getString(2));
            assertEquals("HR", rset.getString(3));
            assertEquals("Contract", rset.getString(4));

            assertFalse(rset.next());
            rset.close();
        }
    }

    // ISSUE #??
    // Flatfile insert-select (using join) returning incorrect rows: fixed-width test
    public void test_Issue_InsertSelectJoin_FW() throws Exception {
        final String ahi = "asatapathy@seebeyond.com";
        final String jon = "jgiron@seebeyond.com";

        final String compaq = "Compaq Presario 6100 Laptop Computer";
        final String flatscreen = "Dell Flatscreen Monitor A605";
        final String phone = "Samsung A600 Cellular Phone";
        final String latitude = "Dell Latitude D600 Latop Computer";

        File dataDir = new File("testdb");
        Connection fileConn = (AxionConnection) (DriverManager.getConnection("jdbc:axiondb:testdb:" + dataDir.getName()));
        Statement stmt = fileConn.createStatement();

        try {
            stmt.execute("drop table if exists ORDERS");
            stmt.execute("create external table ORDERS (ORDER_ID integer, EMAIL varchar(50)) Organization(loadtype='fixedwidth' trimwhitespace='true')");
            stmt.execute("drop table if exists ORDERDETAILS");
            stmt.execute("create external table ORDERDETAILS (ORDER_ID integer, ITEM varchar(100)) Organization(loadtype='fixedwidth' trimwhitespace='true')");
            stmt.execute("drop table if exists ORDER_TARGET");
            stmt.execute("create external table ORDER_TARGET (ORDER_ID integer, EMAIL varchar(50), ITEM varchar(100)) Organization(loadtype='fixedwidth' trimwhitespace='true')");

            stmt.execute("insert into ORDERS values (1, '" + ahi + "')");
            stmt.execute("insert into ORDERS values (2, '" + jon + "')");

            stmt.execute("insert into ORDERDETAILS values (1, '" + compaq + "')");
            stmt.execute("insert into ORDERDETAILS values (1, '" + flatscreen + "')");
            stmt.execute("insert into ORDERDETAILS values (2, '" + phone + "')");
            stmt.execute("insert into ORDERDETAILS values (2, '" + latitude + "')");

            assertEquals(4, stmt.executeUpdate("insert into ORDER_TARGET select a.order_id, a.email, b.item from ORDERS a "
                + "inner join ORDERDETAILS b on (a.order_id = b.order_id)"));

            ResultSet rs = stmt.executeQuery("select distinct email from ORDER_TARGET where email = '" + ahi + "'");
            assertResult(ahi, rs);

            rs = stmt.executeQuery("select distinct email from ORDER_TARGET where email = '" + jon + "'");
            assertResult(jon, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where email = '" + ahi + "'");
            assertResult(2, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where email = '" + jon + "'");
            assertResult(2, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + compaq + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + flatscreen + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + phone + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + latitude + "'");
            assertResult(1, rs);
        } finally {
            if (stmt != null) {
                stmt.execute("shutdown");
            }

            if (fileConn != null) {
                fileConn.close();
            }

            deleteFile(dataDir);
        }
    }
/*Commented on 29-Nov-2007
    // ISSUE #??
    // Flatfile insert-select (using join) returning incorrect rows: delimited test
    public void test_Issue_InsertSelectJoin_Delim() throws Exception {
        final String ahi = "asatapathy@seebeyond.com";
        final String jon = "jgiron@seebeyond.com";

        final String compaq = "Compaq Presario 6100 Laptop Computer";
        final String flatscreen = "Dell Flatscreen Monitor A605";
        final String phone = "Samsung A600 Cellular Phone";
        final String latitude = "Dell Latitude D600 Latop Computer";

        File dataDir = new File("testdb");
        Connection fileConn = (AxionConnection) (DriverManager.getConnection("jdbc:axiondb:testdb:" + dataDir.getName()));
        Statement stmt = fileConn.createStatement();
        String eol = System.getProperty("line.separator");

        try {
            stmt.execute("drop table if exists ORDERS");
            stmt.execute("create external table ORDERS (ORDER_ID integer, EMAIL varchar(50)) Organization(loadtype='delimited' qualifier='\"' recorddelimiter='"
                + eol + "')");
            stmt.execute("drop table if exists ORDERDETAILS");
            stmt.execute("create external table ORDERDETAILS (ORDER_ID integer, ITEM varchar(100)) Organization(loadtype='delimited' recorddelimiter='"
                + eol + "')");
            stmt.execute("drop table if exists ORDER_TARGET");
            stmt.execute("create external table ORDER_TARGET (ORDER_ID integer, EMAIL varchar(50), ITEM varchar(100)) Organization(loadtype='delimited' recorddelimiter='"
                + eol + "')");

            stmt.execute("insert into ORDERS values (1, '" + ahi + "')");
            stmt.execute("insert into ORDERS values (2, '" + jon + "')");

            stmt.execute("insert into ORDERDETAILS values (1, '" + compaq + "')");
            stmt.execute("insert into ORDERDETAILS values (1, '" + flatscreen + "')");
            stmt.execute("insert into ORDERDETAILS values (2, '" + phone + "')");
            stmt.execute("insert into ORDERDETAILS values (2, '" + latitude + "')");

            assertEquals(4, stmt.executeUpdate("insert into ORDER_TARGET select a.order_id, a.email, b.item from ORDERS a "
                + "inner join ORDERDETAILS b on (a.order_id = b.order_id)"));

            ResultSet rs = stmt.executeQuery("select distinct email from ORDER_TARGET where email = '" + ahi + "'");
            assertResult(ahi, rs);

            rs = stmt.executeQuery("select distinct email from ORDER_TARGET where email = '" + jon + "'");
            assertResult(jon, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where email = '" + ahi + "'");
            assertResult(2, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where email = '" + jon + "'");
            assertResult(2, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + compaq + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + flatscreen + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + phone + "'");
            assertResult(1, rs);

            rs = stmt.executeQuery("select count(*) from ORDER_TARGET where item = '" + latitude + "'");
            assertResult(1, rs);
        } finally {
            if (stmt != null) {
                stmt.execute("shutdown");
            }

            if (fileConn != null) {
                fileConn.close();
            }

            deleteFile(dataDir);
        }
    }*/

    // ISSUE #27
    // See http://axion.tigris.org/issues/show_bug.cgi?id=27
    // Outer Join with where condition does not work
    public void test_Issue_OuterJoin_WithWhereCondition() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(3) )");
        _stmt.execute("create table y ( id int, name varchar(3) )");
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 1, 'AAA' )"));
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 2, 'BBB' )"));
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 3, 'CCC' )"));

        assertEquals(1, _stmt.executeUpdate("insert into y values ( 2, 'XXX' )"));

        ResultSet rset = _stmt.executeQuery("select * from x left outer join y on x.id = y.id where y.id is not null");

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("BBB", rset.getString(2));
        assertEquals(2, rset.getInt(3));
        assertEquals("XXX", rset.getString(4));
        assertFalse(rset.next());

        rset = _stmt.executeQuery("select * from x left outer join y on x.id = y.id where y.id is null");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("AAA", rset.getString(2));
        assertEquals(0, rset.getInt(3));
        assertTrue(rset.wasNull());
        assertTrue(null == rset.getString(4));
        assertTrue(rset.wasNull());

        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        assertEquals("CCC", rset.getString(2));
        assertEquals(0, rset.getInt(3));
        assertTrue(rset.wasNull());
        assertTrue(null == rset.getString(4));
        assertTrue(rset.wasNull());

        assertFalse(rset.next());

        rset.close();
    }

    // [Ahi] To avoid out of Memory at this point we don't
    // allow the transaction go over 5000 rows and as soon as we commit
    // they are again available to RowIterator, since we are holding a live iterator.
    // This leads to an infinite loop. I have put a fix for this by checking
    // whether target is part of sub-query in that I just let it fall thru
    // instead of committing at every 5000 rows which is better than before.

    // But this will still fail with by running out of memory at some point.
    // To avoid OOM , user could choose to use limit option in the subquery.
    // e.g insert into PERSON select * from PERSON limit <n> offset <position>

    // ISSUE #28
    // See http://axion.tigris.org/issues/show_bug.cgi?id=28
    public void testInsertIntoSelectFromBug() throws Exception {
        _stmt.execute("create table PERSON ( NAME varchar(10) )");

        _stmt.execute("insert into PERSON values ( 'Paul' )");
        int expectedRows = 1;
        for (int i = 0; i < 15; i++) {
            _stmt.execute("insert into PERSON select * from PERSON");
            expectedRows *= 2;
            assertResult(expectedRows, "select count(*) from person");
        }
    }

    // ISSUE: ?? Problem with ResultSet.beforeFirst()
    // See http://axion.tigris.org/servlets/ReadMsg?list=users&msgNo=256
    public void test_Issue_ResultSet_BeforeFirst() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(3) )");
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 1, 'AAA' )"));
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 2, 'BBB' )"));
        assertEquals(1, _stmt.executeUpdate("insert into x values ( 3, 'CCC' )"));
        ResultSet rset = _stmt.executeQuery("select * from x");

        // recalculate the size of the current query
        rset.beforeFirst();
        int size = 0;
        while (rset.next()) {
            size++;
        }
        assertEquals(3, size);

        // do it again
        size = 0;
        rset.beforeFirst();
        while (rset.next()) {
            size++;
        }
        assertEquals(3, size);
    }

    // ISSUE: 29 join condition results in ClassCastException
    // See http://axion.tigris.org/issues/show_bug.cgi?id=29
    public void test_Issue29_Outer_Non_Key_Join() throws Exception {
        _stmt.execute("create table a (id int, fname varchar(20));");
        _stmt.execute("create table b (id int, lname varchar(20));");
        assertEquals(1, _stmt.executeUpdate("insert into a values (1, 'Jon');"));
        assertEquals(1, _stmt.executeUpdate("insert into a values (2, 'Arnold');"));
        assertEquals(1, _stmt.executeUpdate("insert into b values (1, 'Giron');"));
        assertEquals(1, _stmt.executeUpdate("insert into b values (2, 'Schwarzenegger');"));
        ResultSet rset = _stmt.executeQuery("select a.id, a.fname, b.lname from a inner join b on (a.id = 2);");

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("Arnold", rset.getString(2));
        assertEquals("Giron", rset.getString(3));

        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        assertEquals("Arnold", rset.getString(2));
        assertEquals("Schwarzenegger", rset.getString(3));

        assertFalse(rset.next());

        rset.close();
    }

    // ISSUE: 31
    // See http://axion.tigris.org/issues/show_bug.cgi?id=31
    public void test_Issue31_LikeEmptyString() throws Exception {
        _stmt.execute("create table foo (id int, val varchar(10))");
        _stmt.execute("insert into foo values (0, null)");
        _stmt.execute("insert into foo values (1, '')");
        _stmt.execute("insert into foo values (2, 'test')");
        ResultSet rset = _stmt.executeQuery("select id, val from foo where val like ''");

        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertEquals("", rset.getString(2));

        assertFalse(rset.next());
        rset.close();

        // Null like clause should evaluate to null per ISO/ANSI SQL standard, which should
        // result in an empty ResultSet.
        rset = _stmt.executeQuery("select id, val from foo where val like null");
        assertFalse(rset.next());

        rset.close();
    }

    //  ISSUE: 35
    // See http://axion.tigris.org/issues/show_bug.cgi?id=35
    public void test_Issue35_LeftAndRightOuterJoin() throws Exception {
        _stmt.execute("create table store_info (store_name varchar(50), sales int);");
        _stmt.execute("create table geography (region_name varchar(50), store_name varchar(50));");
        _stmt.execute("insert into store_info values ('Los Angeles', 1500);");
        _stmt.execute("insert into store_info values ('San Diego', 250);");
        _stmt.execute("insert into store_info values ('Los Angeles', 300);");
        _stmt.execute("insert into store_info values ('Boston', 700);");
        _stmt.execute("insert into geography values ('East', 'Boston' );");
        _stmt.execute("insert into geography values ('East',  'New York' );");
        _stmt.execute("insert into geography values ('West',  'Los Angeles' );");
        _stmt.execute("insert into geography values ('West',  'San Diego' );");

        //left outer join test
        _rset = _stmt.executeQuery("select * from geography a left join store_info b on (a.store_name = b.store_name and a.store_name = 'Boston')");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "East");
        assertEquals(_rset.getString(2), "Boston");
        assertEquals(_rset.getString(3), "Boston");
        assertEquals(_rset.getInt(4), 700);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "East");
        assertEquals(_rset.getString(2), "New York");
        assertEquals(_rset.getString(3), null);
        assertEquals(_rset.getObject(4), null);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "West");
        assertEquals(_rset.getString(2), "Los Angeles");
        assertEquals(_rset.getString(3), null);
        assertEquals(_rset.getObject(4), null);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "West");
        assertEquals(_rset.getString(2), "San Diego");
        assertEquals(_rset.getString(3), null);
        assertEquals(_rset.getObject(4), null);

        assertTrue(!_rset.next());

        //right outer join test
        _rset = _stmt.executeQuery("select * from geography a right join store_info b on (a.store_name = b.store_name and a.store_name = 'Boston')");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), null);
        assertEquals(_rset.getString(2), null);
        assertEquals(_rset.getString(3), "Los Angeles");
        assertEquals(_rset.getInt(4), 1500);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), null);
        assertEquals(_rset.getString(2), null);
        assertEquals(_rset.getString(3), "San Diego");
        assertEquals(_rset.getInt(4), 250);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), null);
        assertEquals(_rset.getString(2), null);
        assertEquals(_rset.getString(3), "Los Angeles");
        assertEquals(_rset.getInt(4), 300);

        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "East");
        assertEquals(_rset.getString(2), "Boston");
        assertEquals(_rset.getString(3), "Boston");
        assertEquals(_rset.getInt(4), 700);

        assertTrue(!_rset.next());
    }

    public void test_IssueXX_JoinTableWithSelf() throws Exception {
        _stmt.execute("create table tree ( id number, label varchar(20), parent_id number )");
        _stmt.execute("insert into tree values ( 1, 'root', null )");
        _stmt.execute("insert into tree values ( 2, 'child', 1 )");
        _stmt.execute("insert into tree values ( 3, 'grand-child', 2 )");
        assertResult(new Object[] { new Integer(1), "root"},
            "select parent.id, parent.label from tree parent, tree child where child.id = 2 and parent.id = child.parent_id");
    }

    // ISSUE: 36 if we have index on the group by column it always skip one row
    // See http://axion.tigris.org/issues/show_bug.cgi?id=36
    public void test_Issue36_UpdateSyntaxIssue() throws Exception {
        _stmt.execute("CREATE TABLE address (address_id  NUMBER NOT NULL,"
            + "address_1 VARCHAR2(10) NOT NULL, city VARCHAR2(10) NOT NULL, state  VARCHAR2(2) NOT NULL)");
        _stmt.execute("insert into address (address_id, address_1, city, state) values (1,'SOMESTREET','CITY','ST')");

        try {
            _conn.prepareStatement("update address set city=?state=?");
            fail("Expected SQL Exception, missing comma in update");
        } catch (SQLException e) {
            //expected
        }

        try {
            _stmt.execute("update address set city='second'state='22'");
            fail("Expected SQL Exception, missing comma in update");
        } catch (SQLException e) {
            //expected
        }
    }

    // ISSUE: 37 if we have index on the group by column it always skip one row
    // See http://axion.tigris.org/issues/show_bug.cgi?id=37
    public void test_Issue37_AggregateWithBtreeIndexCol() throws Exception {
        _stmt.execute("create table x ( xid varchar(5), id int, name varchar(10) )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("create btree index xid_index on x(xid)");

        ResultSet rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(rset);
        assertTrue(rset.next());
        assertEquals("1Amy", rset.getString(1));
        assertEquals(1, rset.getInt(2));

        assertTrue(rset.next());
        assertEquals("2Mike", rset.getString(1));
        assertEquals(2, rset.getInt(2));

        assertTrue(!rset.next());
    }

    // ISSUE: 38 if we have index on the group by column it always skip one row
    // See http://axion.tigris.org/issues/show_bug.cgi?id=38
    public void test_Issue38_MixedCrossProductJoinWithAnsiJoin() throws Exception {
        _stmt.execute("create table PEOPLE (NAME VARCHAR2(15), ID VARCHAR2(4))");
        _stmt.execute("insert into PEOPLE VALUES ('Clark Kent', '0003')");
        _stmt.execute("create table SUPERHEROES (NAME VARCHAR2(15), ID VARCHAR2(4))");
        _stmt.execute("insert into SUPERHEROES VALUES ('Superman', '0003')");
        _stmt.execute("insert into SUPERHEROES VALUES ('Spiderman', '0004')");
        _stmt.execute("create table ADDRESS (TOWN VARCHAR2(15), ID VARCHAR2(4))");
        _stmt.execute("insert into ADDRESS VALUES ('Metropolis', '0003')");
        _stmt.execute("insert into ADDRESS VALUES ('New York', '0004')");

        String sql = "select *  from ADDRESS A, SUPERHEROES S LEFT OUTER JOIN PEOPLE P ON P.ID=S.ID where S.ID=A.ID and A.TOWN='New York'";
        ResultSet rset = _stmt.executeQuery(sql);

        assertNotNull(rset);
        assertTrue(rset.next());
        assertEquals("New York", rset.getString(1));
        assertEquals("0004", rset.getString(2));
        assertEquals("Spiderman", rset.getString(3));
        assertEquals("0004", rset.getString(4));
        assertEquals(null, rset.getString(5));
        assertEquals(null, rset.getString(6));

        assertTrue(!rset.next());

        sql = "select *  from SUPERHEROES S LEFT OUTER JOIN PEOPLE P ON P.ID=S.ID, ADDRESS A where S.ID=A.ID and A.TOWN='New York'";
        rset = _stmt.executeQuery(sql);

        assertNotNull(rset);
        assertTrue(rset.next());
        assertEquals("Spiderman", rset.getString(1));
        assertEquals("0004", rset.getString(2));
        assertEquals(null, rset.getString(3));
        assertEquals(null, rset.getString(4));
        assertEquals("New York", rset.getString(5));
        assertEquals("0004", rset.getString(6));

        assertTrue(!rset.next());

    }

    // ISSUE: 39 Problem with OUTER JOIN and SUBSELECT
    // See http://axion.tigris.org/issues/show_bug.cgi?id=38
    // See http://axion.tigris.org/servlets/ReadMsg?list=users&msgNo=356
    public void test_Issue39_OuterJoinSubSelectBug() throws Exception {
        _stmt.execute("create table answer (id number, testsessionid number, answerscalevalue number, itemno number)");
        _stmt.execute("create table testsession (id number)");
        _stmt.execute("insert into answer (id, testsessionid, answerscalevalue, itemno) values (1,1,5,3)");
        _stmt.execute("insert into testsession values (1)");

        String query = "SELECT testsession.id, i3.answerscalevalue FROM testsession "
            + "left outer join (select id, testsessionid, answerscalevalue, itemno "
            + "from Answer where itemno=1) i3 on TestSession.id = i3.testSessionId";
        ResultSet rset = _stmt.executeQuery(query);
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertFalse(rset.next());
        rset.close();

        query = "SELECT testsession.id, i3.answerscalevalue FROM (select id, testsessionid, answerscalevalue, itemno "
            + "from Answer where itemno=1) i3 right outer join testsession on TestSession.id = i3.testSessionId";
        rset = _stmt.executeQuery(query);
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertFalse(rset.next());
        rset.close();

        _stmt.execute("create table table3 (id number)");

        query = "SELECT testsession.id, i3.answerscalevalue FROM testsession "
            + "left outer join (select id, testsessionid, answerscalevalue, itemno "
            + "from Answer where itemno=1) i3 on TestSession.id = i3.testSessionId left outer join table3 t3 on t3.id = i3.id";
        rset = _stmt.executeQuery(query);
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertFalse(rset.next());
        rset.close();

        query = "SELECT testsession.id, i3.answerscalevalue FROM "
            + "table3 t3 right outer join (select id, testsessionid, answerscalevalue, itemno "
            + "from Answer where itemno=1) i3 right outer join testsession " + "on TestSession.id = i3.testSessionId on t3.id = testsession.id ";
        rset = _stmt.executeQuery(query);
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        assertFalse(rset.next());
        rset.close();
    }

    // ISSUE: 40 Axion allows two columns with the same name
    // See http://axion.tigris.org/issues/show_bug.cgi?id=40
    public void test_Issue40_AlterTableAddDuplicateColumn() throws Exception {
        _stmt.execute("create table x(y varchar)");
        try {
            _stmt.execute("alter table x add y varchar");
            fail("Expected SQL Exception, column already exists");
        } catch (SQLException e) {
            //expected
        }
    }

    // ISSUE: 41 Axion allows two columns with the same name
    // See http://axion.tigris.org/issues/show_bug.cgi?id=41
    public void test_Issue41_AggregateFunctionWithSubSelect() throws Exception {
        _stmt.execute("create table x (x varchar)");
        _stmt.execute("insert into x values ('A')");
        _stmt.execute("insert into x values ('B')");
        _stmt.execute("select count(*) from x");
        _stmt.execute("select 2 / (select count(*) from x) from x");
        _stmt.execute("select count(*) / 2 from x");
        _stmt.execute("select count(*) / (select count(*) from x) from x");
    }

}