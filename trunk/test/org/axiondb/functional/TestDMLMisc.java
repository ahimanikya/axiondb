/*
 * $Id: TestDMLMisc.java,v 1.3 2007/12/13 10:43:05 jawed Exp $
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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.HashBag;

/**
 * Database Modification Language tests.
 * 
 * @version $Revision: 1.3 $ $Date: 2007/12/13 10:43:05 $
 * @author Ritesh Adval
 * @author Ahimanikya Satapathy
 */
public class TestDMLMisc extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestDMLMisc(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDMLMisc.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testInvalidInsert() throws Exception {
        _stmt.execute("create table foo ( id int, val varchar(10))");
        _stmt.executeUpdate("insert into foo (val, id) values ( 'zero', 0 )");
        _stmt.executeUpdate("insert into foo values ( 1, 'one' )");
        _stmt.executeUpdate("insert into foo (id, val) values ( 2, 'two' )");
        try {
            _stmt.executeUpdate("insert into foo (id, val) values ( 3 )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        try {
            _stmt.executeUpdate("insert into foo (id) values ( 3, 'three' )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testSimpleView() throws Exception {
        _stmt.execute("create table foo ( id int, val varchar(10))");
        _stmt.executeUpdate("insert into foo values ( 1, null )");
        _stmt.executeUpdate("insert into foo values ( 2, 'two' )");
        _stmt.executeUpdate("insert into foo values ( 3, null )");
        _stmt.execute("create view bar as select * from foo where val is not null");
        _rset = _stmt.executeQuery("select * from bar");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertFalse(_rset.next());

        try {
            _stmt.execute("create view bar as select * from foo where val is not null");
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }

        _stmt.execute("create view if not exists bar as select * from foo where val is not null");

        _stmt.execute("drop view bar");

        try {
            _stmt.execute("drop view bar");
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        _stmt.execute("drop view if exists bar");
        _rset.close();
    }

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

    public void testDefragTable() throws Exception {
        _stmt.execute("create table foo ( str varchar(10), val int )");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 'one', 1 )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( null, null )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 'three', 3 )"));
        assertEquals(2, _stmt.executeUpdate("delete from foo where str is not null"));
        _stmt.execute("defrag table foo");
    }

    public void testSqlExceptionWhenExistingRowsViolateNewConstraint() throws Exception {
        _stmt.execute("create table foo ( str varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 'one' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( null )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 'three' )"));
        try {
            _stmt.execute("alter table foo add constraint foo_not_null not null ( str )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testInsertWithUnspecifiedColumns() throws Exception {
        _stmt.execute("create table foo ( a integer, b varchar(10), c varchar(10), d varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( a, b, c, d ) values ( 1, '1b', '1c', '1d' )"));
        assertResult(new Object[] { new Integer(1), "1b", "1c", "1d"}, "select a, b, c, d from foo where a = 1");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( a, b, c ) values ( 2, '2b', '2c' )"));
        assertResult(new Object[] { new Integer(2), "2b", "2c", null}, "select a, b, c, d from foo where a = 2");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 3, '3b', '3c', '3d' )"));
        assertResult(new Object[] { new Integer(3), "3b", "3c", "3d"}, "select a, b, c, d from foo where a = 3");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 4, '4b', '4c' )"));
        assertResult(new Object[] { new Integer(4), "4b", "4c", null}, "select a, b, c, d from foo where a = 4");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 5, '5b' )"));
        assertResult(new Object[] { new Integer(5), "5b", null, null}, "select a, b, c, d from foo where a = 5");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 6 )"));
        assertResult(new Object[] { new Integer(6), null, null, null}, "select a, b, c, d from foo where a = 6");
    }

    public void testInsertWithLiteralDefault() throws Exception {
        _stmt.execute("create table foo ( id integer, str varchar(10) default 'xyzzy' )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( 1, 'one' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 2, 'two' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( 3, null )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 4, null )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id ) values ( 5 )"));
        _rset = _stmt.executeQuery("select str from foo order by id");
        assertTrue(_rset.next());
        assertEquals("one", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("two", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("xyzzy", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("xyzzy", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("xyzzy", _rset.getString(1));
        assertTrue(!_rset.next());
    }

    public void testAutonumberColumn() throws Exception {
        _stmt.execute("create sequence foo_id_seq");
        _stmt.execute("create table foo ( id integer default foo_id_seq.nextval, str varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'a' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( null, 'b' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'c' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( null, 'd' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( -17, 'e' )"));
        _rset = _stmt.executeQuery("select id from foo order by str");
        for (int i = 0; i < 4; i++) {
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        assertTrue(_rset.next());
        assertEquals(-17, _rset.getInt(1));
        assertTrue(!_rset.next());
    }

    public void testAutonumberColumn2() throws Exception {
        _stmt.execute("create table foo ( id integer generated always as identity, str varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'a' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'b' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'c' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'd' )"));
        _rset = _stmt.executeQuery("select id from foo order by str");
        for (int i = 0; i < 4; i++) {
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        assertTrue(!_rset.next());
    }

    public void testAutonumberColumn3() throws Exception {
        _stmt.execute("create table foo ( id integer generated by default as identity, str varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'a' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( null, 'b' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'c' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( null, 'd' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( id, str ) values ( -17, 'e' )"));
        _rset = _stmt.executeQuery("select id from foo order by str");
        for (int i = 0; i < 4; i++) {
            assertTrue(_rset.next());
            assertEquals(i, _rset.getInt(1));
        }
        assertTrue(_rset.next());
        assertEquals(-17, _rset.getInt(1));
        assertTrue(!_rset.next());
    }

    public void testInsertWithFunctions() throws Exception {
        _stmt.execute("create table foo ( id integer, str varchar(10), ustr varchar(10), dt date )");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 1, 'xyzzy', upper(str), now() )"));
        _rset = _stmt.executeQuery("select id, str, ustr, dt from foo");
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("xyzzy", _rset.getString(2));
        assertEquals("XYZZY", _rset.getString(3));
        assertNotNull(_rset.getDate(4));
        assertTrue(!_rset.next());
    }

    // NOTE: This test is likely to need to change once we support client/server mode
    //       but for now it is simple and easy to fail on parse errors immediately
    public void testAddBatchFailsImmediatelyOnParseError() throws Exception {
        try {
            _stmt.addBatch("xyzzy");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testCreateTableWithAlwaysGeneratedIdenity() throws Exception {
        try {
            _stmt.execute("create table FOOSEQ( NUM int generated always as identity start with 1 increment by 1 maxvalue 1000 minvalue 1 cycle, STR varchar, NUMTWO bigint generated by default as identity )");
            fail("Expetecd Exception: multiple Identity not allowed");
        } catch (SQLException e) {
            // expected
        }

        _stmt.execute("create table FOOSEQ( NUM int generated always as identity start with 1 increment by 1 maxvalue 1000 minvalue 1 cycle, STR varchar, NUMTWO bigint)");

        try {
            _stmt.execute("insert into FOOSEQ ( NUM, STR, NUMTWO ) values ( NULL , NULL, NULL)");
            fail("Expetecd Exception: can't insert value to generated column");
        } catch (SQLException e) {
            // expected
        }

        for (int i = 0; i < 10; i += 2) {
            _stmt.execute("insert into FOOSEQ ( STR, NUMTWO ) values ( '" + i + "' ," + i + " )");
            _stmt.execute("insert into FOOSEQ ( STR, NUMTWO ) values ( NULL, NULL)");
        }

        String sql = "select NUM, STR, NUMTWO from FOOSEQ";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        for (int i = 0; i < 10; i += 2) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(new Integer(i + 1));
            int num = _rset.getInt(1);
            assertTrue(!_rset.wasNull());
            found.add(new Integer(num));

            assertTrue("ResultSet should contain more rows", _rset.next());

            expected.add(new Integer(i + 2));
            num = _rset.getInt(1);
            assertTrue(!_rset.wasNull());
            found.add(new Integer(num));

        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);

    }

    public void testCreateTableWithGeneratedByDefaultIdenity() throws Exception {

        _stmt.execute("create table FOOSEQ( NUM int, STR varchar, NUMTWO bigint generated by default as identity start with 1 increment by 1 maxvalue 1000 minvalue 1 cycle)");
        for (int i = 0; i < 10; i += 2) {
            _stmt.execute("insert into FOOSEQ ( NUM, STR, NUMTWO ) values ( " + i + ",'" + i + "' ," + i + " )");
            _stmt.execute("insert into FOOSEQ ( NUM, STR, NUMTWO ) values ( NULL , NULL, NULL)");
        }

        String sql = "select NUM, STR, NUMTWO from FOOSEQ";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);

        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();

        int seq = 1;
        for (int i = 0; i < 10; i += 2) {
            assertTrue("ResultSet should contain more rows", _rset.next());
            expected.add(new Long(i));
            long num2 = _rset.getLong(3);
            assertTrue(!_rset.wasNull());
            found.add(new Long(num2));

            assertTrue("ResultSet should contain more rows", _rset.next());

            expected.add(new Long(seq++));
            num2 = _rset.getLong(3);
            assertTrue(!_rset.wasNull());
            found.add(new Long(num2));
        }
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();
        assertEquals(expected, found);

    }

    public void test_insert_when_via_pstmt() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table emp_target ( id int, name varchar(10))");
        _stmt.execute("create btree index emp_target_idx on emp_target ( id )");
        _stmt.execute("create table emp_log ( executionId int, id int, name varchar(10))");

        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");

        // INSERT
        PreparedStatement stmt = _conn.prepareStatement("insert first when s_column1 = 1 then" + " into emp_target (emp_target.id, emp_target.name)"
            + " values (s_column1,  s_column2)" + " else into emp_log (executionId, id, name)" + " values (?, s_column1, s_column2)"
            + " ( SELECT S1.id as s_column1,  S1.name AS s_column2 " + " FROM emp S1)");

        stmt.setInt(1, 1);

        stmt.executeUpdate();
        stmt.close();

        // SELECT on emp_target
        _rset = _stmt.executeQuery("select * from emp_target");

        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();

        //SELECT on emp_log
        _rset = _stmt.executeQuery("select * from emp_log");

        assertNotNull("Should have been able to create ResultSet", _rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals("Teresa", _rset.getString(3));

        assertTrue("ResultSet shouldn't have any more rows", !_rset.next());
        _rset.close();

    }

    /**
     * Tests insert-select when "case-when" used is used more than once in the Select
     * statement. To test Engine handles it without throwing duplicate "CASEWHEN"
     * alias/column name.
     * 
     * @throws Exception
     */
    public void test_insert_select_cases() throws Exception {
        String createT1 = "create table emp_tgt1 (emp_id number(10), emp_name varchar(100))";
        String createS1 = "create table emp_src1 (emp_id number(10), emp_name varchar(100))";
        String createS2 = "create table emp_src2 (emp_id number(10), emp_name varchar(100))";
        String testInsert = "insert into emp_tgt1 (emp_id, emp_name)" + " select " + "   case when (s1.emp_id IS NULL) " + "        then  0 "
            + "        else s1.emp_id " + "   end, " + "   case when (s1.emp_id > 50000) " + "        then s1.emp_id " + "        else s1.emp_name "
            + "   end " + " from " + "  emp_src1 s1 INNER JOIN emp_src2 s2 ON s1.emp_id = s2.emp_id ";

        _stmt.execute(createT1);
        _stmt.execute(createS1);
        _stmt.execute(createS2);

        try {
            _stmt.executeUpdate(testInsert);
        } catch (Throwable ex) {
            fail("Insert-Select exception:" + ex.toString());
        }
    }

    /**
     * Tests insert-select when "count" function used is used more than once in the Select
     * statement. To test Engine handles it without throwing duplicate "CASEWHEN"
     * alias/column name.
     * 
     * @throws Exception
     */
    public void test_insert_select_counts() throws Exception {
        String createT1 = "create table emp_tgt1 (emp_id number(10), emp_name varchar(100))";
        String createS1 = "create table emp_src1 (emp_id number(10), emp_name varchar(100))";
        String testInsert = "insert into emp_tgt1 (emp_id, emp_name)" + " select " + "   count(emp_id), count(emp_name)" + " from " + "  emp_src1  ";

        _stmt.execute(createT1);
        _stmt.execute(createS1);

        try {
            _stmt.executeUpdate(testInsert);
        } catch (Throwable ex) {
            fail("Insert-Select exception:" + ex.toString());
        }
    }

    /**
     * Tests insert-select when "max" function used is used more than once in the Select
     * statement. To test Engine handles it without throwing duplicate "CASEWHEN"
     * alias/column name.
     * 
     * @throws Exception
     */
    public void test_insert_select_maxs() throws Exception {
        String createT1 = "create table emp_tgt1 (emp_id number(10), emp_name varchar(100))";
        String createS1 = "create table emp_src1 (emp_id number(10), emp_name varchar(100))";
        String testInsert = "insert into emp_tgt1 (emp_id, emp_name)" + " select " + "   max(emp_id), max(emp_name)" + " from " + "  emp_src1  ";

        _stmt.execute(createT1);
        _stmt.execute(createS1);

        try {
            _stmt.executeUpdate(testInsert);
        } catch (Throwable ex) {
            fail("Insert-Select exception:" + ex.toString());
        }
    }

    public void testInsertSelectThreeTableJoinWithIsNullWhereCondition1() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) , totalSalary int)");
        _stmt.execute("create btree index emp_target_idx on emp_target ( id )");

        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");

        //insert select
        int count = _stmt.executeUpdate("insert into emp_target(id, name, totalSalary) select s1.id, s1.name, (s2.base_salary + s2.bonus) from emp s1 inner join salary s2 on (s1.id = s2.id) left outer join emp_target s3 on (s2.id = s3.id) where s3.id is null");
        assertEquals(count, 2);

        //again insert select
        count = _stmt.executeUpdate("insert into emp_target(id, name, totalSalary) select s1.id, s1.name, (s2.base_salary + s2.bonus) from emp s1 inner join salary s2 on (s1.id = s2.id) left outer join emp_target s3 on (s2.id = s3.id) where s3.id is null");
        assertEquals(count, 0);

    }

    public void testInsertSelectThreeTableJoinWithIsNullWhereCondition2() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) , totalSalary int)");
        _stmt.execute("create btree index emp_target_idx on emp_target ( id )");

        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");

        //insert select
        int count = _stmt.executeUpdate("insert into emp_target(id, name, totalSalary) select s1.id, s1.name, (s2.base_salary + s2.bonus) from emp s1 left outer join salary s2 on (s1.id = s2.id) left outer join emp_target s3 on (s2.id = s3.id) where s3.id is null");
        assertEquals(count, 3);

        //again insert select
        count = _stmt.executeUpdate("insert into emp_target(id, name, totalSalary) select s1.id, s1.name, (s2.base_salary + s2.bonus) from emp s1 inner join salary s2 on (s1.id = s2.id) left outer join emp_target s3 on (s2.id = s3.id) where s3.id is null");
        assertEquals(count, 0);

    }

    public void testInsertSelectWithGroupBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) , totalSalary int)");

        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");

        //insert select
        int count = _stmt.executeUpdate("insert first when (myid > 0) then " + " into emp_target(id, name, totalSalary) "
            + " values(myid, myname, mysalary) " + " (select s1.id myid, s1.name myname, sum(s2.base_salary + s2.bonus) as mysalary "
            + " from emp s1 inner join salary s2 on (s1.id = s2.id) " + " left outer join emp_target s3 on (s2.id = s3.id) "
            + " where s3.id is null group by s1.id, s1.name)");
        assertEquals(count, 2);

        //again insert select
        count = _stmt.executeUpdate("insert first when (myid > 0) then " + " into emp_target(id, name, totalSalary) "
            + " values(myid, myname, mysalary) " + " (select s1.id myid, s1.name myname, sum(s2.base_salary + s2.bonus) as mysalary "
            + " from emp s1 inner join salary s2 on (s1.id = s2.id) " + " left outer join emp_target s3 on (s2.id = s3.id) "
            + " where s3.id is null group by s1.id, s1.name)");
        assertEquals(count, 0);

    }

    public void testTruncateOnIndexedTable() throws Exception {
        _stmt.execute("create table x ( id int)");
        _stmt.execute("create btree index idx1 on x (id)");
        _stmt.execute("insert into x values ( 1)");
        _stmt.execute("insert into x values ( 2)");

        //select
        _rset = _stmt.executeQuery("select id from x ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table x");

        //select id again
        _rset = _stmt.executeQuery("select id from x ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        //select where x = 1 condition
        _rset = _stmt.executeQuery("select id from x where id = 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //select where x = 2 condition
        _rset = _stmt.executeQuery("select id from x where id = 2");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testAlterTable() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(5))");
        _stmt.execute("insert into x values ( 1, 'aaa')");
        _stmt.execute("insert into x values ( 2, 'bbb')");

        _stmt.execute("alter table x alter column id rename to myid ");

        try {
            _stmt.executeQuery("select id from x ");
            fail("Excepted Exception : column not found");
        } catch (Exception e) {
            // expected
        }

        _stmt.execute("alter table x alter column myid rename to id ");
        _stmt.execute("alter table x drop column id");

        // ALTER ADD COLUMN can be handled.
        _stmt.execute("alter table x add column id int default 1 not null");
        _stmt.execute("insert into x (name) values ('name')");
        assertResult(1, "select id from x where name = 'name'");

        // ALTER TABLE ALTER COLUMN definition.
        _stmt.execute("alter table x drop column id cascade");
        _stmt.execute("alter table x add column id int default 1");
        _stmt.execute("alter table x alter column id set default 5");
        _stmt.execute("insert into x (name) values ('name')");
        assertResult("name", "select name from x where id = 5");

        _stmt.execute("alter table x alter column id drop default");
        _stmt.execute("insert into x (name) values ('name2')");
        _rset = _stmt.executeQuery("select id from x where name = 'name2'");
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        _rset.close();

        // ALTER TABLE <tablename> RENAME TO <newname>
        _stmt.execute("alter table x rename to y ");
        assertResult("name", "select name from y where id = 5");

        _stmt.execute("alter table y drop column id");

        try {
            _stmt.execute("alter table y drop column name");
            fail("Excepted Exception : can't drop last column");
        } catch (Exception e) {
            // expected
        }

    }
// Commented on 29-Nov-2007:uncommented on 13-Dec-2007
    public void testAlterTableOnIndexedTable() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(5))");
        _stmt.execute("create index idx1 on x (id)");
        _stmt.execute("create index idx2 on x (name)");
        _stmt.execute("insert into x values ( 1, 'aaa')");
        _stmt.execute("insert into x values ( 2, 'bbb')");

        _stmt.execute("alter table x alter column id rename to myid cascade");
        _stmt.execute("create index idx1 on x (myid)");

        try {
            _stmt.executeQuery("select id from x ");
            fail("Excepted Exception : column not found");
        } catch (Exception e) {
            // expected
        }

        _stmt.execute("alter table x alter column myid rename to id cascade");
        _stmt.execute("create index idx1 on x (id)");

        // ALTER ADD COLUMN can be handled.
        _stmt.execute("alter table x alter column id set default 3");
        _stmt.execute("insert into x (name) values ('name')");
        assertResult(1, "select id from x where name = 'aaa'");
        assertResult(2, "select id from x where name = 'bbb'");
        assertResult(3, "select id from x where name = 'name'");

        _stmt.execute("alter table x add constraint primary key (id)");

        // ALTER TABLE ALTER COLUMN definition.
        _stmt.execute("alter table x alter column id set default 5");
        _stmt.execute("insert into x (name) values ('name')");
        assertResult(1, "select id from x where name = 'aaa'");
        assertResult(2, "select id from x where name = 'bbb'");
        assertResult("name", "select name from x where id = 3");
        assertResult("name", "select name from x where id = 5");

        try {
            _stmt.execute("create index idx2 on x (name)");
            fail("Excepted Exception : index already exists");
        } catch (Exception e) {
            // expected
        }

        _stmt.execute("alter table x drop primary key");
        _stmt.execute("alter table x alter column id drop default");
        _stmt.execute("insert into x (name) values ('name2')");
        _rset = _stmt.executeQuery("select id from x where name = 'name2'");
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        _rset.close();

        // ALTER TABLE <tablename> RENAME TO <newname>
        _stmt.execute("alter table x rename to y cascade");
        assertResult("name", "select name from y where id = 5");

        _stmt.execute("alter table y drop column id cascade");

        try {
            _stmt.execute("alter table y drop column name cascade");
            fail("Excepted Exception : can't drop last column");
        } catch (Exception e) {
            // expected
        }

        try {
            _stmt.execute("alter table y add column id int default 2 primary key");
            fail("Expected Exception: Could not apply constraint");
        } catch (Exception e) {
            // expected
        }

    }

    public void testAlterTableOnAutonumberColumn() throws Exception {
        _stmt.execute("create table foo ( id integer generated always as identity start with 1, str varchar(10) )");
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'a' )"));
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'b' )"));

        try {
            _stmt.execute("alter table foo alter column id rename to myid ");
            fail("Expected Exception: Can't rename generated columns");
        } catch (Exception e) {
            // expected
        }

        assertResult("a", "select str from foo where id = 1");

        try {
            _stmt.execute("alter table foo alter column id drop default");
            fail("Expected Exception: Can't rename generated columns");
        } catch (Exception e) {
            // expected
        }
        assertEquals(1, _stmt.executeUpdate("insert into foo ( str ) values ( 'c' )"));
        assertResult("c", "select str from foo where id = 3");
    }

    public void testDefalutValuesClauseInInsertCommand() throws Exception {
        _stmt.execute("create table a(id int generated always as identity, name varchar(3) default 'xxx')");

        _stmt.execute("insert into a default values");
        _stmt.execute("insert into a default values");

        //select
        _rset = _stmt.executeQuery("select id, name from a ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0, _rset.getInt(1));
        assertEquals("xxx", _rset.getString(2));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("xxx", _rset.getString(2));
        assertTrue(!_rset.next());
        _rset.close();
    }

}