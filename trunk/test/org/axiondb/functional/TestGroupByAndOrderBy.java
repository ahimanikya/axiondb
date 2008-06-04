/*
 * $Id: TestGroupByAndOrderBy.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test group by and order by.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Ahimanikya Satapathy
 */
public class TestGroupByAndOrderBy extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestGroupByAndOrderBy(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestGroupByAndOrderBy.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests
    public void testOrderByInsideTransaction() throws Exception {
        _conn.setAutoCommit(false);
        _stmt.execute("create table foo ( id int, val clob)");
        _stmt.execute("create unique index foo_pk on foo (id)");
        _stmt.executeUpdate("insert into foo values ( 2, 'two' )");
        _stmt.executeUpdate("insert into foo values ( 3, 'three' )");
        _stmt.executeUpdate("insert into foo values ( 1, 'one' )");

        {
            _rset = _stmt.executeQuery("select id, val, 'literal' from foo order by id");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertEquals("literal", _rset.getString(3));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertEquals("literal", _rset.getString(3));
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertEquals("literal", _rset.getString(3));
            assertFalse(_rset.next());
            _rset.close();
        }

        {
            _rset = _stmt.executeQuery("explain select id, val from foo order by id");
            assertTrue(_rset.next());
            assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Collating"));
            assertFalse(_rset.next());
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select id, val from foo order by id asc");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select id, val from foo order by id desc");
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }

        {
            _rset = _stmt.executeQuery("explain select id, val from foo order by id desc");
            assertTrue(_rset.next());
            assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Collating"));
            assertTrue(_rset.next());
            assertTrue(_rset.getString(1), _rset.getString(1).startsWith("ReverseSorted"));
            assertFalse(_rset.next());
            _rset.close();
        }
        _conn.setAutoCommit(true);
    }

    public void testOrderBy() throws Exception {
        _stmt.execute("create table foo ( id int, val varchar(10))");
        _stmt.execute("create unique index foo_pk on foo (id)");
        _stmt.executeUpdate("insert into foo values ( 2, 'two' )");
        _stmt.executeUpdate("insert into foo values ( 3, 'three' )");
        _stmt.executeUpdate("insert into foo values ( 1, 'one' )");

        {
            _rset = _stmt.executeQuery("select id, val from foo order by id");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select id, val from foo order by id asc");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select id, val from foo order by id desc");
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }
        
        {
            _stmt.executeUpdate("insert into foo values ( 5, 'ONE' )");
            
            _rset = _stmt.executeQuery("select id, val from foo order by upper(val)");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(5, _rset.getInt(1));
            assertEquals("ONE", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(3, _rset.getInt(1));
            assertEquals("three", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertFalse(_rset.next());
            _rset.close();
        }
    }

    
    /**
     * DATE column and java.sql.Date not should consider hours, minutes, seconds and 
     * milli seconds during comparison.
     * 
     * @throws Exception
     */
    public void testSelectDistinctDateColumn() throws Exception {
        _stmt.execute("drop table if exists TableWithDate");        
        _stmt.execute("create table TableWithDate ( empid numeric(10) not null, name varchar(100), created date)");
        _stmt.executeUpdate("insert into TableWithDate (empid, name, created) values (1, 'Johnny Fountain', SYSDATE)");
        _stmt.executeUpdate("insert into TableWithDate (empid, name, created) values (2, 'Johnny F Kennedy', SYSDATE)");
        _stmt.executeUpdate("insert into TableWithDate (empid, name, created) values (3, 'Tiger Woods', SYSDATE)");        
        _stmt.executeUpdate("insert into TableWithDate (empid, name, created) values (4, 'Woody Allen', SYSDATE)");        
        
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        java.sql.Date expDt = new java.sql.Date(cal.getTimeInMillis());
        
        _rset = _stmt.executeQuery("select distinct created from TableWithDate ");
        assertTrue(_rset.next());
        assertEquals(expDt, _rset.getDate(1));
        assertFalse(_rset.next());
        _rset.close();
        _stmt.execute("truncate table TableWithDate");
        _stmt.execute("drop table TableWithDate");        
    }
    
    public void testOrderByWithNull() throws Exception {
        _stmt.execute("create table xyzzy ( id integer )");
        _stmt.executeUpdate("insert into xyzzy values ( null )");
        for (int i = 9; i >= 0; i--) {
            _stmt.executeUpdate("insert into xyzzy values ( " + i + ")");
        }
        _stmt.executeUpdate("insert into xyzzy values ( null )");

        {
            ResultSet rset = _stmt.executeQuery("select * from xyzzy order by id");
            // expect 0 thru 9
            for (int i = 0; i < 10; i++) {
                assertTrue(rset.next());
                assertEquals(i, rset.getInt(1));
                assertTrue(!rset.wasNull());
            }
            // expect 2 nulls
            for (int i = 0; i < 2; i++) {
                assertTrue(rset.next());
                assertEquals(0, rset.getInt(1));
                assertTrue(rset.wasNull());
            }
            // expect no more
            assertTrue(!rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select * from xyzzy order by id asc");
            // expect 0 thru 9
            for (int i = 0; i < 10; i++) {
                assertTrue(rset.next());
                assertEquals(i, rset.getInt(1));
                assertTrue(!rset.wasNull());
            }
            // expect 2 nulls
            for (int i = 0; i < 2; i++) {
                assertTrue(rset.next());
                assertEquals(0, rset.getInt(1));
                assertTrue(rset.wasNull());
            }
            // expect no more
            assertTrue(!rset.next());
            rset.close();
        }

        {
            ResultSet rset = _stmt.executeQuery("select * from xyzzy order by id desc");
            // expect 2 nulls
            for (int i = 0; i < 2; i++) {
                assertTrue(rset.next());
                assertEquals(0, rset.getInt(1));
                assertTrue(rset.wasNull());
            }
            // expect 9 thru 0 nulls
            for (int i = 9; i >= 0; i--) {
                assertTrue(rset.next());
                assertEquals(i, rset.getInt(1));
                assertTrue(!rset.wasNull());
            }
            // expect no more
            assertTrue(!rset.next());
            rset.close();
        }

    }

    public void testGroupWithNoTable() throws Exception {
        _rset = _stmt.executeQuery("select 2 group by 2");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        _rset.close();
    }
    
    public void testAggregateWithArrayInsideTransaction() throws Exception {
        _conn.setAutoCommit(false);
        _stmt.execute("create table x ( xid varchar(5), id int, name varchar(10) )");
        _stmt.execute("create array index xid_index on x(xid)");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid is null ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid = '2Mike' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        _rset = _stmt.executeQuery("select xid, max(id), name from x where name = 'Mike' group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());
    }


    public void testAggregateWithArrayIndexUsingHaving() throws Exception {
        _stmt.execute("create table x ( xid varchar(10), id int, name varchar(10) )");
        _stmt.execute("create array index xid_index on x(xid)");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid is null ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid = '2Mike' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        _rset = _stmt.executeQuery("select xid, max(id), name from x where name = 'Mike' group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id), name from x where xid = '1Amy' group by id, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total from x group by id order by total");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));

        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total, name myname from x group by name having total > 3 order by total");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id), name from x  group by xid, name having max(id) = 2 and name = 'Mike'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

    }

    public void testAggregateWithBTreeIndexUsingHaving() throws Exception {
        _stmt.execute("create table x ( xid varchar(10), id int, name varchar(10) )");
        _stmt.execute("create btree index xid_index on x(xid)");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid is null ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid = '2Mike' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        _rset = _stmt.executeQuery("select xid, max(id), name from x where name = 'Mike' group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id), name from x where xid = '1Amy' group by id, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total, name myname from x group by name order by total");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));

        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total, name myname from x group by name having total > 3");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id), name from x  group by xid, name having name = 'Mike'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

    }

    public void testAggregateWithNoIndexUsingHaving() throws Exception {
        _stmt.execute("create table x ( xid varchar(10), id int, name varchar(10) )");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid is null ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid having xid = '2Mike' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        _rset = _stmt.executeQuery("select xid, max(id), name from x where name = 'Mike' group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id), name from x where xid = '1Amy' group by id, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total, name myname from x group by name order by total");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));

        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id)+sum(id) total, name myname from x group by name having total > 3 order by total");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id), name from x  group by xid, name having name = 'Mike'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

    }

    public void testAggregateWithArrayIndexCol() throws Exception {
        _stmt.execute("create table x ( xid varchar(10), id int, name varchar(10) )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("create array index xid_index on x(xid)");

        _rset = _stmt.executeQuery("select xid, max(id), name from x group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("1Amy", _rset.getString(1));
        assertEquals(1, _rset.getInt(2));
        assertEquals("Amy", _rset.getString(3));

        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id), name from x group by id, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id) from x group by id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("1Amy", _rset.getString(1));
        assertEquals(1, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        assertTrue(!_rset.next());
    }

    public void testAggregateWithBtreeIndexCol() throws Exception {
        _stmt.execute("create table x ( xid varchar(10), id int, name varchar(10) )");
        _stmt.execute("insert into x values ( '1Amy', 1, 'Amy' )");
        _stmt.execute("insert into x values ( '2Mike', 2, 'Mike' )");
        _stmt.execute("create btree index xid_index on x(xid)");

        _rset = _stmt.executeQuery("select xid, max(id), name from x group by xid, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("1Amy", _rset.getString(1));
        assertEquals(1, _rset.getInt(2));
        assertEquals("Amy", _rset.getString(3));

        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals("Mike", _rset.getString(3));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id), name from x group by id, name");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select max(id) from x group by id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));

        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("select xid, max(id) from x group by xid");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("1Amy", _rset.getString(1));
        assertEquals(1, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals("2Mike", _rset.getString(1));
        assertEquals(2, _rset.getInt(2));

        assertTrue(!_rset.next());
    }

    public void testTwoTableInnerJoinWithFilternConditionWithOrderBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into salary values ( 3, 3000, 300 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");

        _stmt.execute("create btree index empidx on emp(id)");
        _stmt.execute("create btree index salaryidx on salary(sid)");

        _rset = _stmt.executeQuery("select s1.id, s1.name, s2.base + s2.bonus from emp s1 inner join salary s2 on s1.id = s2.sid where s1.id > 1 order by s1.id");

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(2200, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(3300, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testTwoTableJoinWithGroupBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");

        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");

        // This query should create a dynamic index on salary and will not be sorted on
        // emp.id, so group by has to sort it to apply group by
        _rset = _stmt.executeQuery("select id, sum(base+bonus) total from emp s1 inner join salary s2 on s1.id = s2.sid group by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();

        _stmt.execute("create btree index empidx on emp(id)");

        // Now emp will be scanned using index on emp.id
        // So group by does not have to sort it to apply group by
        _rset = _stmt.executeQuery("select id, sum(base+bonus) total from emp s1 inner join salary s2 on s1.id = s2.sid group by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();

        // Now emp will be scanned using index on emp.id ,since group by is on sid and
        // emp.id = salary.sid , it will swap the group by column and hence sorting is not
        // required to apply group by
        _rset = _stmt.executeQuery("select sid, sum(base+bonus) total from emp s1 inner join salary s2 on s1.id = s2.sid group by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select sid, sum(base+bonus) total from emp s1 left outer join salary s2 on s1.id = s2.sid group by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertNull(_rset.getObject(2));

        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select id, sum(base+bonus) total from emp s1 right outer join salary s2 on s1.id = s2.sid group by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select id, sum(base+bonus) total from emp s1 right outer join salary s2 on s1.id = s2.sid group by id order by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select id, sum(base+bonus) total from emp s1 right outer join salary s2 on s1.id = s2.sid group by id order by id desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(8800, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2200, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableInnerJoinWithOrderBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");

        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");

        // This query should create a dynamic index on salary and will not be sorted on
        // emp.id, So group by has to sort it to apply group by
        _rset = _stmt.executeQuery("select id, name, base+bonus from emp s1 inner join salary s2 on s1.id = s2.sid order by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 inner join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc

        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 inner join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _stmt.execute("create btree index empidx on emp(id)");

        // Now emp will be scanned using index on emp.id
        // So order by does not have to sort it to apply order by
        _rset = _stmt.executeQuery("select id, name, (base+bonus) total from emp s1 inner join salary s2 on s1.id = s2.sid order by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Now emp will be scanned using index on emp.id ,since order by is on sid and
        // emp.id = salary.sid , it will swap the order by column and hence sorting is not
        // required to apply order by
        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 inner join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc

        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 inner join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();
        
        // non column order by
        _stmt.execute("update emp set name = 'MIKE' where id = 2");
        _stmt.execute("update emp set name = 'amy' where id = 1");
        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 inner join salary s2 on s1.id = s2.sid order by upper(name)");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));
        
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("MIKE", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testTwoTableLeftJoinWithOrderBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");

        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");

        // This will create index on right table dynamically and require explicit sort
        // after join
        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 left outer join salary s2 on s1.id = s2.sid order by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertNull(_rset.getObject(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Will swap sid with id, since right table will be scanned using
        // ChangingIndexedRowIterator and require explicit sort after join
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 left outer join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertEquals("Teresa", _rset.getString(2));
        assertNull(_rset.getObject(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 left outer join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertEquals("Teresa", _rset.getString(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _stmt.execute("create btree index empidx on emp(id)");

        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 left outer join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertEquals("Teresa", _rset.getString(2));
        assertNull(_rset.getObject(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 left outer join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertEquals("Teresa", _rset.getString(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableRightJoinWithOrderBy() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");

        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");

        // This will create index on right table dynamically and require explicit sort
        // after join
        _rset = _stmt.executeQuery("select id, name, (base+bonus) from emp s1 right outer join salary s2 on s1.id = s2.sid order by id");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Will swap sid with id, since right table will be scanned using
        // ChangingIndexedRowIterator and require explicit sort after join
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 right outer join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 right outer join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _stmt.execute("create btree index salaryidx on salary(sid)");

        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 right outer join salary s2 on s1.id = s2.sid order by sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // Test desc
        _rset = _stmt.executeQuery("select sid, name, (base+bonus) from emp s1 right outer join salary s2 on s1.id = s2.sid order by sid desc");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testAmbiguousColumnReferenceVisitorForGroupBy() throws SQLException {
        _stmt.execute("create table a (id int)");
        _stmt.execute("insert into a values (1)");

        // should throw ambiguous column reference exception
        try {
            _rset = _stmt.executeQuery("select id, count(id), id from a group by id");
            fail("Expected exception");
        } catch (SQLException ex) {
            // expected
        }

        try {
            _stmt.executeQuery("select id, (select count(*) from a) id from a group by id");
            fail("Expected exception");
        } catch (SQLException ex) {
            // expected
        }

    }

    public void testBasicHaving() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( eid int, base int, bonus int)");

        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");

        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");
        _stmt.execute("insert into salary values ( 2, 4000, 400 )");

        try {
            _stmt.executeQuery("select s1.id, s1.name, sum(s2.base + s2.bonus) totalSalary "
                + "from emp s1 inner join salary s2 on s1.id = s2.eid group by id, name having s2.base = 4000");
            fail("Expected Invalid Group By Expression error msg");
        } catch (SQLException e) {
            // expected
        }

        _rset = _stmt.executeQuery("select s1.id, s1.name, sum(s2.base + s2.bonus) totalSalary "
            + "from emp s1 inner join salary s2 on s1.id = s2.eid group by id, name having avg(s2.base) = 4000");

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt("id"));
        assertEquals("Mike", _rset.getString("name"));
        assertEquals(13200, _rset.getInt("totalSalary"));
        assertTrue(!_rset.next());
        _rset.close();

    }
}