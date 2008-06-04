/*
 * $Id: TestPrepareStatement.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.engine.BaseDatabase;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.jdbc.AxionConnection;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Ahimanikya Satapathy
 */
public class TestPrepareStatement extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestPrepareStatement(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestPrepareStatement.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        InputStream in = BaseDatabase.class.getClassLoader().getResourceAsStream(
            "org/axiondb/axiondb.properties");
        if (in == null) {
            in = BaseDatabase.class.getClassLoader().getResourceAsStream("axiondb.properties");
        }
        Properties prop = new Properties();
        prop.load(in);
        prop.setProperty("database.commitsize", "2");
        MemoryDatabase db = new MemoryDatabase("testdb", prop);

        _conn = new AxionConnection(db);
        _stmt = _conn.createStatement();
        //super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testBindVariableInFunction() throws Exception {
        PreparedStatement stmt = _conn.prepareStatement("select upper(?)");

        try {
            stmt.executeQuery();
            fail("Expected Exception: unbound variable");
        } catch (Exception e) {
            // expected
        }

        stmt.setString(1, "test");
        assertResult("TEST", stmt.executeQuery());
        stmt.setString(1, "test2");
        assertResult("TEST2", stmt.executeQuery());
        stmt.close();
    }

    public void testBindVariableAsSubSelect() throws Exception {
        createTableFoo();
        populateTableFoo();
        PreparedStatement pstmt = _conn
            .prepareStatement("select NUM, (select UPPER(?)) from FOO where UPPER(STR) = (select UPPER(?))");
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

    public void testMultipleTableInsert() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("create table z ( id int, name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");

        PreparedStatement pstmt = _conn.prepareStatement("insert ALL "
            + " when S.id < ? then into y values(S.id,S.name)"
            + " when S.id > ? then into z values(S.id, S.name) (select * from x) as S");
        pstmt.setInt(1, 2);
        pstmt.setInt(2, 1);
        assertEquals(3, pstmt.executeUpdate());

        ResultSet rset = _stmt.executeQuery("select count(*) from y");
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        rset.close();

        rset = _stmt.executeQuery("select count(*) from z");
        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        rset.close();

        _stmt.execute("truncate table y");
        _stmt.execute("truncate table z");

        rset = _stmt.executeQuery("select count(*) from y");
        assertTrue(rset.next());
        assertEquals(0, rset.getInt(1));
        rset.close();

        rset = _stmt.executeQuery("select count(*) from z");
        assertTrue(rset.next());
        assertEquals(0, rset.getInt(1));
        rset.close();

        rset = _stmt.executeQuery("select count(*) from x");
        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        rset.close();

        pstmt.setInt(1, 3);
        pstmt.setInt(2, 0);
        assertEquals(5, pstmt.executeUpdate());

        rset = _stmt.executeQuery("select count(*) from y");
        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        rset.close();

        rset = _stmt.executeQuery("select count(*) from z");
        assertTrue(rset.next());
        assertEquals(3, rset.getInt(1));
        rset.close();

        pstmt.close();
    }
    
    public void testInsertSelect() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");

        PreparedStatement pstmt = _conn.prepareStatement("insert into y "
            + " select id, (? || name) from x");
        pstmt.setString(1, "Mr.");
        assertEquals(3, pstmt.executeUpdate());

        ResultSet rset = _stmt.executeQuery("select name from y where name='Mr.Amy'");
        assertTrue(rset.next());
        assertEquals("Mr.Amy", rset.getString(1));
        rset.close();

        pstmt.close();
    }

    public void testMultipleTableInsert2() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("create table z ( id int, name varchar(10) )");

        PreparedStatement srcpstmt = _conn.prepareStatement("insert into x values ( ?, ? )");
        srcpstmt.setInt(1, 1);
        srcpstmt.setString(2, "Amy");
        assertEquals(1, srcpstmt.executeUpdate());

        srcpstmt.setInt(1, 2);
        srcpstmt.setString(2, "Mike");
        assertEquals(1, srcpstmt.executeUpdate());

        srcpstmt.setInt(1, 3);
        srcpstmt.setString(2, "Teresa");
        assertEquals(1, srcpstmt.executeUpdate());
        srcpstmt.close();

        PreparedStatement pstmt = _conn.prepareStatement("insert ALL "
            + " when S.id < ? then into y " + " when S.id > ? then into z (select * from x) as S");
        pstmt.setInt(1, 2);
        pstmt.setInt(2, 1);
        assertEquals(3, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rset = _stmt.executeQuery("select count(*) from y");
        assertTrue(rset.next());
        assertEquals(1, rset.getInt(1));
        rset.close();

        rset = _stmt.executeQuery("select count(*) from z");
        assertTrue(rset.next());
        assertEquals(2, rset.getInt(1));
        rset.close();

    }

    public void testBasicSubSelect() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("create table y(id int, name varchar(3))");
        assertEquals(4, stmt.executeUpdate("insert into y select * from x"));

        // exists with sub-select
        PreparedStatement pstmt = _conn.prepareStatement("select * from x where exists "
            + " (select id from x where id = ?)");
        ResultSet rset;

        for (int i = 1; i < 5; i++) {
            pstmt.setInt(1, i);
            rset = pstmt.executeQuery();
            assertTrue(rset.next());
        }

        pstmt.close();

        // in with sub-select
        pstmt = _conn.prepareStatement("select * from x where id in"
            + " (select id from x where id = ?)");
        for (int i = 1; i < 5; i++) {
            pstmt.setInt(1, i);
            rset = pstmt.executeQuery();
            assertTrue(rset.next());
        }

        pstmt.close();

        // A correlated subquery is a subquery that contains a
        // reference to a table that also appears in the outer query

        // scalar sub-select column visibility test
        pstmt = _conn.prepareStatement(" select x.id, (select "
            + " (select s.name from y s where y.id = x.id) "
            + " from y, x where y.id = x.id and y.id = ?) " + " from x");
        pstmt.setInt(1, 2);
        rset = pstmt.executeQuery();
        assertTrue(rset.next());

        try {
            pstmt.setInt(2, 2);
            fail("Expected Exception: Bind variable not found");
        } catch (Exception e) {
            // expected
        }

        pstmt.close();

        pstmt = _conn
            .prepareStatement("SELECT UPPER((SELECT distinct name FROM x where id = ?)) FROM y;");
        pstmt.setInt(1, 2);
        rset = pstmt.executeQuery();
        assertTrue(rset.next());
        pstmt.close();

        // sub-select as FromNode
        // pstmt = _conn.prepareStatement("select * from (select * from x where id not in
        // (select * from x where x.id = ?)) s where s.id = ?");
        // pstmt.setInt(1, 2);
        // pstmt.setInt(2, 2);
        // rset = pstmt.executeQuery();
        // assertTrue(rset.next());
        // pstmt.close();

        rset.close();
        stmt.close();
    }

    public void test_upsert_via_pstmt() throws Exception {
        create_table_x();
        Statement stmt = _conn.createStatement();

        // insert...select...
        stmt.execute("drop table if exists y ");
        stmt.execute("create table y(id int, name varchar(4))");
        assertEquals(2, stmt.executeUpdate("insert into y select * from x where name = 'aaa'"));

        PreparedStatement pstmt = null;
        try {
            pstmt = _conn.prepareStatement("upsert into bogus as D "
                + " using x as S on(S.id = D.id and S.id = ?) "
                + " when matched then update set D.name = '_' || S.name when not matched then "
                + " insert (D.id, D.name) values (S.id, S.name)");
            pstmt.setInt(1, 1);
            pstmt.execute();
            fail("Expected table not found Exception");
        } catch (Exception e) {
            // expected
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
        }

        pstmt = _conn.prepareStatement("upsert into y as D "
            + " using x as S on(S.id = D.id and S.id = ?) "
            + " when matched then update set D.name = '_' || S.name when not matched then "
            + " insert (D.id, D.name) values (S.id, S.name)");

        pstmt.setInt(1, 1);
        pstmt.execute();

        assertEquals(4, pstmt.getUpdateCount());
        pstmt.close();
        
        // inner join in sub-query : shd return zero since y is empty
        stmt.execute("delete from y");
        assertEquals(0, stmt
            .executeUpdate(" merge into y as D using (select x.id, x.name from x,y)"
                + " as S on(s.id = D.id)" + " when matched then update set D.name = S.name"
                + " when not matched then insert (D.id, D.name) values (s.id, s.name)"));

        stmt.executeUpdate("insert into y values(4,'fff')");
        assertEquals(4, stmt
            .executeUpdate(" merge into y as D using (select x.id, x.name from x)"
                + " as S on(s.id = D.id)" + " when matched then update set D.name = S.name"
                + " when not matched then insert (D.id, D.name) values (s.id, s.name)"));
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

        PreparedStatement pstmt = _conn
            .prepareStatement("upsert into y as D using (select id, name from x) as S on(S.id = D.id)"
                + " when matched then update set D.name = S.name when not matched then "
                + " insert (D.id, D.name) values (S.id, S.name) "
                + " exception when S.id < ? then Insert into z");

        pstmt.setInt(1, 3);
        assertEquals(2, pstmt.executeUpdate());

        pstmt.setInt(1, 3);
        assertEquals(0, pstmt.executeUpdate());

        pstmt.close();
        stmt.close();
    }

    public void testUpdateInsertInto() throws Exception {
        createTableFoo();
        populateTableFoo();
        _stmt.execute("create table EXPFOO ( NUM integer, STR varchar2, NUMTWO integer )");

        PreparedStatement pstmt = _conn.prepareStatement("UPDATE FOO SET FOO.NUM = FOO.NUM + ? "
            + " EXCEPTION WHEN FOO.NUM < ? THEN INSERT INTO EXPFOO");

        pstmt.setInt(1, 1);
        pstmt.setInt(2, 4);
        assertEquals(2, pstmt.executeUpdate());
        ResultSet rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(4, rset.getInt(1));

        pstmt.setInt(1, 1);
        pstmt.setInt(2, 2);
        assertEquals(4, pstmt.executeUpdate());
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(6, rset.getInt(1));
        pstmt.close();
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
        assertEquals(1, stmt.executeUpdate("insert into emp values(3,'aaa')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(1,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(2,'bbb')"));
        assertEquals(1, stmt.executeUpdate("insert into tmp values(3,'bbb')"));

        assertEquals(3, stmt.executeUpdate("UPDATE tmp " + "SET tmp.tname = (S.name || 'Test') "
            + "FROM tmp T inner join emp S on T.tid = S.id"));

        assertResult("aaaTest", "select tname from tmp where tid=1");
        assertResult("aaa", "select name from emp where id=1");
    }

    public void testUpdateInsertInto2() throws Exception {
        createTableFoo();
        populateTableFoo();
        _stmt.execute("create table EXPFOO ( NUM integer, STR varchar2, NUMTWO integer )");

        PreparedStatement pstmt = _conn.prepareStatement("UPDATE FOO  SET FOO.NUM = S1.NUM + ? "
            + " FROM FOO S1 WHERE S1.NUM = FOO.NUM EXCEPTION WHEN S1.NUM < ? THEN "
            + " INSERT INTO EXPFOO");

        pstmt.setInt(1, 1);
        pstmt.setInt(2, 4);
        assertEquals(2, pstmt.executeUpdate());
        ResultSet rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(4, rset.getInt(1));

        pstmt.setInt(1, 1);
        pstmt.setInt(2, 2);
        assertEquals(4, pstmt.executeUpdate());
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(6, rset.getInt(1));
        pstmt.close();
    }
}