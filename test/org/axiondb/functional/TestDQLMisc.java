/*
 * $Id: TestDQLMisc.java,v 1.1 2007/11/28 10:01:30 jawed Exp $
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

import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Database Query Language tests.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:30 $
 * @author Amrish Lal
 * @author Chris Johnston
 * @author Ritesh Adval
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */
public class TestDQLMisc extends AbstractFunctionalTest {

    public static Test suite() {
        return new TestSuite(TestDQLMisc.class);
    }

    //------------------------------------------------------------ Conventional

    public TestDQLMisc(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void test_select_str_from_x_inner_y_inner_z() throws Exception {

        try {
            createTableX();
            createTableY();
            createTableZ();
            populateTableX();
            populateTableY();
            populateTableZ();

            String sql = "SELECT * FROM x INNER JOIN y ON(x.a=y.a) INNER JOIN z ON(x.a=z.a)";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            int rslt = 3;
            int count = 0;
            Integer xa_col1 = null;
            Integer xb_col2 = null;
            Integer ya_col3 = null;
            Integer yb_col4 = null;
            Integer za_col5 = null;
            Integer zb_col6 = null;

            while (_rset.next()) {

                xa_col1 = (Integer) _rset.getObject(1);
                xb_col2 = (Integer) _rset.getObject(2);
                ya_col3 = (Integer) _rset.getObject(3);
                yb_col4 = (Integer) _rset.getObject(4);
                za_col5 = (Integer) _rset.getObject(5);
                zb_col6 = (Integer) _rset.getObject(6);

                switch (ya_col3.intValue()) {
                    case 3:
                        assertEquals(xa_col1.intValue(), 3);
                        assertEquals(xb_col2.intValue(), 3);
                        assertEquals(ya_col3.intValue(), 3);
                        assertEquals(yb_col4.intValue(), 30);
                        assertEquals(za_col5.intValue(), 3);
                        assertEquals(zb_col6.intValue(), 300);
                        break;
                    default:
                        assertTrue("Unknown row " + za_col5.intValue(), false);
                        break;
                }
                rslt++;
                count++;
            }
        } finally {
            dropTableX();
            dropTableY();
            dropTableZ();
        }
    }

    public void test_select_str_from_x_inner_y_loj_z() throws Exception {
        int rslt = 3;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;

        try {
            createTableX();
            createTableY();
            createTableZ();
            populateTableX();
            populateTableY();
            populateTableZ();
            String sql = "SELECT * FROM x INNER JOIN y ON(x.a=y.a) LEFT OUTER JOIN z ON(x.a=z.a)";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            while (_rset.next()) {

                xa_col1 = (Integer) _rset.getObject(1);
                xb_col2 = (Integer) _rset.getObject(2);
                ya_col3 = (Integer) _rset.getObject(3);
                yb_col4 = (Integer) _rset.getObject(4);
                za_col5 = (Integer) _rset.getObject(5);
                zb_col6 = (Integer) _rset.getObject(6);

                switch (xa_col1.intValue()) {
                    case 2:
                        assertEquals(xa_col1.intValue(), 2);
                        assertEquals(xb_col2.intValue(), 2);
                        assertEquals(ya_col3.intValue(), 2);
                        assertEquals(yb_col4.intValue(), 20);
                        assertEquals(za_col5, null);
                        assertEquals(zb_col6, null);
                        break;
                    case 3:
                        assertEquals(xa_col1.intValue(), 3);
                        assertEquals(xb_col2.intValue(), 3);
                        assertEquals(ya_col3.intValue(), 3);
                        assertEquals(yb_col4.intValue(), 30);
                        assertEquals(za_col5.intValue(), 3);
                        assertEquals(zb_col6.intValue(), 300);
                        break;
                    default:
                        assertTrue("Unknown row " + za_col5.intValue(), false);
                        break;
                }
                rslt++;
                count++;
            }
        } finally {
            dropTableX();
            dropTableY();
            dropTableZ();
        }
    }

    public void test_select_str_from_x_inner_y_roj_z() throws Exception {
        int rslt = 3;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;

        try {
            createTableX();
            createTableY();
            createTableZ();
            populateTableX();
            populateTableY();
            populateTableZ();
            String sql = "SELECT * FROM x INNER JOIN y ON(x.a=y.a) RIGHT OUTER JOIN z ON(x.a=z.a)";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            while (_rset.next()) {

                xa_col1 = (Integer) _rset.getObject(1);
                xb_col2 = (Integer) _rset.getObject(2);
                ya_col3 = (Integer) _rset.getObject(3);
                yb_col4 = (Integer) _rset.getObject(4);
                za_col5 = (Integer) _rset.getObject(5);
                zb_col6 = (Integer) _rset.getObject(6);

                switch (za_col5.intValue()) {
                    case 3:
                        assertEquals(xa_col1.intValue(), 3);
                        assertEquals(xb_col2.intValue(), 3);
                        assertEquals(ya_col3.intValue(), 3);
                        assertEquals(yb_col4.intValue(), 30);
                        assertEquals(za_col5.intValue(), 3);
                        assertEquals(zb_col6.intValue(), 300);
                        break;
                    case 4:
                        assertEquals(xa_col1, null);
                        assertEquals(xb_col2, null);
                        assertEquals(ya_col3, null);
                        assertEquals(yb_col4, null);
                        assertEquals(za_col5.intValue(), 4);
                        assertEquals(zb_col6.intValue(), 400);
                        break;
                    case 5:
                        assertEquals(xa_col1, null);
                        assertEquals(xb_col2, null);
                        assertEquals(ya_col3, null);
                        assertEquals(yb_col4, null);
                        assertEquals(za_col5.intValue(), 5);
                        assertEquals(zb_col6.intValue(), 500);
                        break;
                    default:
                        assertTrue("Unknown row " + za_col5.intValue(), false);
                        break;
                }
                rslt++;
                count++;
            }
        } finally {
            dropTableX();
            dropTableY();
            dropTableZ();
        }
    }

    public void test_select_str_from_x_loj_y_inner_z() throws Exception {
        int rslt = 3;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;

        try {
            createTableX();
            createTableY();
            createTableZ();
            populateTableX();
            populateTableY();
            populateTableZ();
            String sql = "SELECT * FROM x LEFT OUTER JOIN y ON(x.a=y.a) INNER JOIN z ON(x.a=z.a)";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            while (_rset.next()) {

                xa_col1 = (Integer) _rset.getObject(1);
                xb_col2 = (Integer) _rset.getObject(2);
                ya_col3 = (Integer) _rset.getObject(3);
                yb_col4 = (Integer) _rset.getObject(4);
                za_col5 = (Integer) _rset.getObject(5);
                zb_col6 = (Integer) _rset.getObject(6);

                switch (xa_col1.intValue()) {
                    case 3:
                        assertEquals(xa_col1.intValue(), 3);
                        assertEquals(xb_col2.intValue(), 3);
                        assertEquals(ya_col3.intValue(), 3);
                        assertEquals(yb_col4.intValue(), 30);
                        assertEquals(za_col5.intValue(), 3);
                        assertEquals(zb_col6.intValue(), 300);
                        break;
                    case 4:
                        assertEquals(xa_col1.intValue(), 4);
                        assertEquals(xb_col2.intValue(), 4);
                        assertEquals(ya_col3, null);
                        assertEquals(yb_col4, null);
                        assertEquals(za_col5.intValue(), 4);
                        assertEquals(zb_col6.intValue(), 400);
                        break;
                    default:
                        assertTrue("Unknown row " + za_col5.intValue(), false);
                        break;
                }
                rslt++;
                count++;
            }
        } finally {
            dropTableX();
            dropTableY();
            dropTableZ();
        }
    }

    public void test_select_str_from_x_loj_y_loj_z() throws Exception {
        createTableX();
        createTableY();
        createTableZ();
        populateTableX();
        populateTableY();
        populateTableZ();

        String sql = "SELECT * FROM x LEFT OUTER JOIN y ON(y.a=x.a) LEFT OUTER JOIN Z ON(x.a=z.a)";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        int rslt = 2;
        int count = 0;
        while (_rset.next()) {
            assertEquals("Should have " + rslt + " in row-" + count + " 1st column.", rslt, _rset.getInt(1));
            assertEquals("Should have " + rslt + " in row-" + count + " 2st column.", rslt, _rset.getInt(2));
            if (count == 0 || count == 1) {
                assertEquals("Should have " + rslt + " in row-" + count + " 3rd column.", rslt, _rset.getInt(3));
                assertEquals("Should have " + (rslt * 10) + " in row-" + count + " 4rd column.", rslt * 10, _rset.getInt(4));
            } else {
                assertEquals("Should have NULL in row-" + count + " 3rd column.", null, _rset.getObject(3));
                assertEquals("Should have NULL in row-" + count + " 4rd column.", null, _rset.getObject(4));
            }

            if (count == 0) {
                assertEquals("Should have NULL in row-" + count + " 5th column.", null, _rset.getObject(5));
                assertEquals("Should have NULL in row-" + count + " 6th column.", null, _rset.getObject(6));
            } else {
                assertEquals("Should have " + (rslt) + " in row-" + count + " 5th column.", (rslt), _rset.getInt(5));
                assertEquals("Should have " + (rslt * 100) + " in row-" + count + " 6th column.", (rslt * 100), _rset.getInt(6));
            }

            rslt++;
            count++;
        }
        assertEquals("Number of rows selected should be 3", 3, count);
    }

    public void test_select_str_from_x_loj_y_roj_z() throws Exception {
        createTableX();
        createTableY();
        createTableZ();
        populateTableX();
        populateTableY();
        populateTableZ();

        String sql = "SELECT * FROM x LEFT OUTER JOIN y ON(x.a=y.a) RIGHT OUTER JOIN Z ON(x.a=z.a)";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        int rslt = 2;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;
        while (_rset.next()) {

            xa_col1 = (Integer) _rset.getObject(1);
            xb_col2 = (Integer) _rset.getObject(2);
            ya_col3 = (Integer) _rset.getObject(3);
            yb_col4 = (Integer) _rset.getObject(4);
            za_col5 = (Integer) _rset.getObject(5);
            zb_col6 = (Integer) _rset.getObject(6);

            switch (za_col5.intValue()) {
                case 3:
                    assertEquals(xa_col1.intValue(), 3);
                    assertEquals(xb_col2.intValue(), 3);
                    assertEquals(ya_col3.intValue(), 3);
                    assertEquals(yb_col4.intValue(), 30);
                    assertEquals(za_col5.intValue(), 3);
                    assertEquals(zb_col6.intValue(), 300);
                    break;
                case 4:
                    assertEquals(xa_col1.intValue(), 4);
                    assertEquals(xb_col2.intValue(), 4);
                    assertEquals(ya_col3, null);
                    assertEquals(yb_col4, null);
                    assertEquals(za_col5.intValue(), 4);
                    assertEquals(zb_col6.intValue(), 400);
                    break;
                case 5:
                    assertEquals(xa_col1, null);
                    assertEquals(xb_col2, null);
                    assertEquals(ya_col3, null);
                    assertEquals(yb_col4, null);
                    assertEquals(za_col5.intValue(), 5);
                    assertEquals(zb_col6.intValue(), 500);
                    break;
                default:
                    assertTrue("Unknown row " + za_col5.intValue(), false);
                    break;
            }
            rslt++;
            count++;
        }
        assertEquals("Number of rows selected should be 3", 3, count);
    }

    public void test_select_str_from_x_roj_y_inner_z() throws Exception {

        int rslt = 3;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;

        try {
            createTableX();
            createTableY();
            createTableZ();
            populateTableX();
            populateTableY();
            populateTableZ();

            String sql = "SELECT * FROM x RIGHT OUTER JOIN y ON(x.a=y.a) INNER JOIN z ON(x.a=z.a)";
            _rset = _stmt.executeQuery(sql);
            assertNotNull("Should have been able to create ResultSet", _rset);
            while (_rset.next()) {

                xa_col1 = (Integer) _rset.getObject(1);
                xb_col2 = (Integer) _rset.getObject(2);
                ya_col3 = (Integer) _rset.getObject(3);
                yb_col4 = (Integer) _rset.getObject(4);
                za_col5 = (Integer) _rset.getObject(5);
                zb_col6 = (Integer) _rset.getObject(6);

                switch (ya_col3.intValue()) {
                    case 3:
                        assertEquals(xa_col1.intValue(), 3);
                        assertEquals(xb_col2.intValue(), 3);
                        assertEquals(ya_col3.intValue(), 3);
                        assertEquals(yb_col4.intValue(), 30);
                        assertEquals(za_col5.intValue(), 3);
                        assertEquals(zb_col6.intValue(), 300);
                        break;
                    default:
                        assertTrue("Unknown row " + za_col5.intValue(), false);
                        break;
                }
                rslt++;
                count++;
            }
        } finally {
            dropTableX();
            dropTableY();
            dropTableZ();
        }
    }

    public void test_select_str_from_x_roj_y_loj_z() throws Exception {
        createTableX();
        createTableY();
        createTableZ();
        populateTableX();
        populateTableY();
        populateTableZ();

        String sql = "SELECT * FROM x RIGHT OUTER JOIN y ON(x.a=y.a) LEFT OUTER JOIN z ON(x.a=z.a)";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        int rslt = 3;
        int count = 0;
        Integer xa_col1 = null;
        Integer xb_col2 = null;
        Integer ya_col3 = null;
        Integer yb_col4 = null;
        Integer za_col5 = null;
        Integer zb_col6 = null;
        while (_rset.next()) {

            xa_col1 = (Integer) _rset.getObject(1);
            xb_col2 = (Integer) _rset.getObject(2);
            ya_col3 = (Integer) _rset.getObject(3);
            yb_col4 = (Integer) _rset.getObject(4);
            za_col5 = (Integer) _rset.getObject(5);
            zb_col6 = (Integer) _rset.getObject(6);

            switch (ya_col3.intValue()) {
                case 1:
                    assertEquals(xa_col1, null);
                    assertEquals(xb_col2, null);
                    assertEquals(ya_col3.intValue(), 1);
                    assertEquals(yb_col4.intValue(), 10);
                    assertEquals(za_col5, null);
                    assertEquals(zb_col6, null);
                    break;
                case 2:
                    assertEquals(xa_col1.intValue(), 2);
                    assertEquals(xb_col2.intValue(), 2);
                    assertEquals(ya_col3.intValue(), 2);
                    assertEquals(yb_col4.intValue(), 20);
                    assertEquals(za_col5, null);
                    assertEquals(zb_col6, null);
                    break;
                case 3:
                    assertEquals(xa_col1.intValue(), 3);
                    assertEquals(xb_col2.intValue(), 3);
                    assertEquals(ya_col3.intValue(), 3);
                    assertEquals(yb_col4.intValue(), 30);
                    assertEquals(za_col5.intValue(), 3);
                    assertEquals(zb_col6.intValue(), 300);
                    break;
                default:
                    assertTrue("Unknown row " + za_col5.intValue(), false);
                    break;
            }
            rslt++;
            count++;
        }
    }

    public void test_select_str_from_x_roj_y_roj_z() throws Exception {
        createTableX();
        createTableY();
        createTableZ();
        populateTableX();
        populateTableY();
        populateTableZ();

        String sql = "SELECT * FROM x RIGHT OUTER JOIN y ON(x.a=y.a) RIGHT OUTER JOIN z ON(x.a=z.a)";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet", _rset);
        int rslt = 3;
        int count = 0;
        while (_rset.next()) {
            if (count != 0) {
                assertEquals("Should have NULL in row-" + count + " 1st column.", null, _rset.getObject(1));
                assertEquals("Should have NULL in row-" + count + " 2nd column.", null, _rset.getObject(2));
                assertEquals("Should have NULL in row-" + count + " 3rd column.", null, _rset.getObject(3));
                assertEquals("Should have NULL in row-" + count + " 4rd column.", null, _rset.getObject(4));
            } else {
                assertEquals("Should have " + rslt + " in row-" + count + " 1st column.", rslt, _rset.getInt(1));
                assertEquals("Should have " + rslt + " in row-" + count + " 2nd column.", rslt, _rset.getInt(2));
                assertEquals("Should have " + rslt + " in row-" + count + " 3rd column.", rslt, _rset.getInt(3));
                assertEquals("Should have " + (rslt * 10) + " in row-" + count + " 4th column.", (rslt * 10), _rset.getInt(4));
            }
            assertEquals("Should have " + (rslt) + " in row-" + count + " 5th column.", (rslt), _rset.getInt(5));
            assertEquals("Should have " + (rslt * 100) + " in row-" + count + " 6th column.", (rslt * 100), _rset.getInt(6));
            rslt++;
            count++;
        }
    }

    public void testAliasAsColumnName() throws SQLException {
        _stmt.execute("create table test ( test_id int, name varchar(20) )");
        _stmt.execute("insert into test ( test_id, name) values (1, 'SOME_TEST')");
        _rset = _stmt.executeQuery("select test_id as my_id, name as my_name from test");

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt("my_id"));
        assertEquals(1, _rset.getInt("test_id"));
        assertEquals("SOME_TEST", _rset.getString("my_name"));
        assertEquals("SOME_TEST", _rset.getString("name"));
        _rset.close();

    }

    public void testAnsiInnerJoinWithIndexBug1() throws Exception {
        _stmt.execute("create table a ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index aone  on a ( one )");
        _stmt.execute("create table b ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index bone  on b ( one )");
        _stmt.execute("insert into a values ( 'one', 'two' )");
        _stmt.execute("insert into b values ( 'one', 'two' )");

        _rset = _stmt.executeQuery("select * from A inner join B on A.ONE = B.ONE");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testAnsiInnerJoinWithIndexBug2() throws Exception {
        _stmt.execute("create table a ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index aone on a ( one )");
        _stmt.execute("create table b ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index bone on b ( one )");
        _stmt.execute("create table c ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index cone on c ( one )");
        _stmt.execute("create table d ( one varchar(10), two varchar(10) )");
        _stmt.execute("create index done on d ( one )");
        _stmt.execute("insert into a values ( 'one', 'two' )");
        _stmt.execute("insert into b values ( 'one', 'two' )");
        _stmt.execute("insert into c values ( 'one', 'two' )");
        _stmt.execute("insert into d values ( 'one', 'two' )");

        _rset = _stmt.executeQuery("select * from A inner join B on A.ONE = B.ONE inner join C on B.ONE = C.ONE inner join D on C.ONE = D.ONE");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();
    }

    // s (a, b, c), t (d, e, f)
    // select * from s where exists (select * from t s where s.c = a)
    // will not find a match for s.c, which is the expected ANSI behavior.
    public void testColumnBindingInCorrelatedQuery() throws Exception {
        _stmt.execute("create table s ( a int, b int, c int)");
        _stmt.execute("create table t ( d int, e int, f int)");

        try {
            _stmt.execute("select * from s where exists (select * from t s where s.c = a)");
            // FIXME: fail("Expected column not found exception");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testFourTableJoinWithRightAsNestedTableView() throws Exception {
        _stmt.execute("create table a ( id int, sid int )");
        _stmt.execute("create table b ( id int, sid int )");
        _stmt.execute("create table c ( id int, sid int )");
        _stmt.execute("create table d ( id int, sid int )");

        _stmt.execute("insert into a values ( 1, 51 )");
        _stmt.execute("insert into a values ( 2, 52 )");

        _stmt.execute("insert into b values ( 1, 53 )");
        _stmt.execute("insert into b values ( 2, 54 )");

        _stmt.execute("insert into c values ( 1, 55 )");

        _stmt.execute("insert into d values ( 1, 56 )");

        //inner - (inner - inner) join expects one row
        _rset = _stmt.executeQuery("select * from a s1 inner join b s2" + " inner join c s3 inner join d s4 on (s3.id= s4.id)"
            + " on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);
        assertTrue(!_rset.next());
        _rset.close();

        //inner -(left - inner) join expects two rows
        _rset = _stmt.executeQuery("select * from a s1 inner join b s2" + " left outer join c s3 inner join d s4"
            + " on (s3.id= s4.id) on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertEquals(_rset.getInt(2), 52);
        assertEquals(_rset.getInt(3), 2);
        assertEquals(_rset.getInt(4), 54);
        assertNull(_rset.getObject(5));
        assertNull(_rset.getObject(6));
        assertNull(_rset.getObject(7));
        assertNull(_rset.getObject(8));
        _rset.close();

        //inner -(right - inner) join expects one row
        _rset = _stmt.executeQuery("select * from a s1 inner join b s2" + " right outer join c s3 inner join d s4"
            + " on (s3.id= s4.id) on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);
        assertTrue(!_rset.next());
        _rset.close();

        //left -(inner - inner) join expects two row
        _rset = _stmt.executeQuery("select * from a s1 left outer join b" + " s2 inner join c s3 inner join d s4 on (s3.id= s4.id)"
            + " on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);
        assertTrue(_rset.next());

        assertEquals(_rset.getInt(1), 2);
        assertEquals(_rset.getInt(2), 52);
        assertNull(_rset.getObject(3));
        assertNull(_rset.getObject(4));
        assertNull(_rset.getObject(5));
        assertNull(_rset.getObject(6));
        assertNull(_rset.getObject(7));
        assertNull(_rset.getObject(8));
        assertTrue(!_rset.next());
        _rset.close();

        //left -(left - inner) join expects two row
        _rset = _stmt.executeQuery("select * from a s1 left outer join b s2" + " left outer join c s3 inner join d s4"
            + " on (s3.id= s4.id) on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertEquals(_rset.getInt(2), 52);
        assertEquals(_rset.getInt(3), 2);
        assertEquals(_rset.getInt(4), 54);
        assertNull(_rset.getObject(5));
        assertNull(_rset.getObject(6));
        assertNull(_rset.getObject(7));
        assertNull(_rset.getObject(8));
        _rset.close();

        //right -(left - inner) join expects two row
        _rset = _stmt.executeQuery("select * from a s1 right outer join b s2" + " left outer join c s3 inner join d s4"
            + " on (s3.id= s4.id) on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 51);
        assertEquals(_rset.getInt(3), 1);
        assertEquals(_rset.getInt(4), 53);
        assertEquals(_rset.getInt(5), 1);
        assertEquals(_rset.getInt(6), 55);
        assertEquals(_rset.getInt(7), 1);
        assertEquals(_rset.getInt(8), 56);

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertEquals(_rset.getInt(2), 52);
        assertEquals(_rset.getInt(3), 2);
        assertEquals(_rset.getInt(4), 54);
        assertNull(_rset.getObject(5));
        assertNull(_rset.getObject(6));
        assertNull(_rset.getObject(7));
        assertNull(_rset.getObject(8));
        _rset.close();

    }

    public void testIsNullUsingIntBtreeIndex() throws Exception {
        _stmt.execute("create table null_test ( id int, name varchar(10) )");
        _stmt.execute("create btree index int_idx on null_test (id)");
        _stmt.execute("insert into null_test values ( 1, 'Amy' )");
        _stmt.execute("insert into null_test values ( NULL, 'Mike' )");
        _stmt.execute("insert into null_test values ( 3, 'Teresa' )");
        _stmt.execute("insert into null_test values ( NULL, 'James' )");

        //is not null test
        _rset = _stmt.executeQuery("select id, name from null_test where id is not null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(1));
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(1));
        assertTrue(!_rset.next());
        _rset.close();

        //is null test
        _rset = _stmt.executeQuery("select id, name from null_test where id is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("explain select id, name from null_test where id is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod(LazyRow("));
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("explain select id, name from null_test where id = 2 and id is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod(LazyRow("));
        assertTrue(_rset.next());        
        assertEquals(_rset.getString(1), "Filtering(ISNULL((NULL_TEST).ID))");        
        assertTrue(!_rset.next());
        _rset.close();

        //test deleting one null row to test index deletion and test above again
        int updated = _stmt.executeUpdate("delete from null_test where id is null and name like 'Mike'");
        assertNotNull(_rset);
        assertEquals(updated, 1);

        //is not null test
        _rset = _stmt.executeQuery("select id, name from null_test where id is not null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(1));
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(1));
        assertTrue(!_rset.next());
        _rset.close();

        //is null test
        _rset = _stmt.executeQuery("select id, name from null_test where id is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        //name should be 'James'
        assertEquals(_rset.getObject(2), "James");
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testIsNullUsingStringBtreeIndex() throws Exception {
        _stmt.execute("create table null_test ( id int, name varchar(10) )");
        _stmt.execute("insert into null_test values ( 1, 'Amy' )");
        _stmt.execute("insert into null_test values ( 2, NULL )");
        _stmt.execute("insert into null_test values ( 3, 'Teresa' )");
        _stmt.execute("insert into null_test values ( 4, NULL )");

        _rset = _stmt.executeQuery("explain select id, name from null_test where id > 2 and name is null and name <> 'bogus'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "Unmod(MemoryTable(NULL_TEST))");
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Filtering"));
        assertTrue(!_rset.next());
        _rset.close();
        
        _stmt.execute("create btree index str_idx on null_test (name)");
        
        //is not null test
        _rset = _stmt.executeQuery("select id, name from null_test where name is not null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(2));
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(2));
        assertTrue(!_rset.next());
        _rset.close();

        //is null test
        _rset = _stmt.executeQuery("select id, name from null_test where name is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(2));
        assertTrue(_rset.next());
        assertNull(_rset.getObject(2));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("explain select id, name from null_test where name is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod(LazyRow("));
        assertTrue(!_rset.next());
        _rset.close();

        //test deleting one null row to test index deletion and test above again
        int updated = _stmt.executeUpdate("delete from null_test where name is null and id = 2");
        assertNotNull(_rset);
        assertEquals(updated, 1);

        // testing again (is not null test)
        _rset = _stmt.executeQuery("select id, name from null_test where name is not null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(2));
        assertTrue(_rset.next());
        assertNotNull(_rset.getObject(2));
        assertTrue(!_rset.next());
        _rset.close();

        //(testing again) is null test
        _rset = _stmt.executeQuery("select id, name from null_test where name is null");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        //id should be 4
        assertEquals(_rset.getInt(1), 4);
        assertNull(_rset.getObject(2));
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testLeftOuterJoinHavingRightTableColumnIndexed() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create btree index salary_idx on salary ( id )");
        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");

        //inner join
        _rset = _stmt.executeQuery("select * from emp inner join salary on emp.id = salary.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join
        _rset = _stmt.executeQuery("select * from emp left outer join salary on emp.id = salary.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        // make sure for left outer join right table column values are null for non
        // matching key from emp
        _rset = _stmt.executeQuery("select base_salary from emp left outer join salary on emp.id = salary.id where emp.id = 3");
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));

    }

    public void testLikeWithFilePath() throws Exception {
        _stmt.execute("create table table1 (field1 varchar(40))");
        _stmt.execute("insert into table1 values ('C:\\documents\\java\\docs\\index.html')");
        _rset = _stmt.executeQuery("SELECT * FROM table1 WHERE field1 LIKE 'C:\\documents\\java\\docs%' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("SELECT * FROM table1 WHERE field1 LIKE 'C:\\documents\\java\\docs\\index.html' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
    }

    public void testLikeWithFilePathAndEscapeChar() throws Exception {
        _stmt.execute("create table like_test (field varchar(40))");
        _stmt.execute("insert into like_test values ('C:\\documents\\java\\docs\\index.html')");
        _stmt.execute("insert into like_test values ('C:\\documents\\java\\docs\\')");
        _stmt.execute("insert into like_test values ('C:\\documents\\java\\docs.html')");
        //test $
        _stmt.execute("insert into like_test values ('C:\\documents\\java\\docs\\index.html$')");
        //test %
        _stmt.execute("insert into like_test values ('C:\\documents\\java\\docs\\%')");

        //insert all special chars
        //insert []
        _stmt.execute("insert into like_test values ('C:\\documents[aa]\\java\\docs\\')");
        //insert {}
        _stmt.execute("insert into like_test values ('C:\\documents{aa}\\java\\docs\\')");
        //insert |
        _stmt.execute("insert into like_test values ('C:\\|documents{aa}|\\java\\%docs\\')");
        //insert ^
        _stmt.execute("insert into like_test values ('C:\\^documents^\\java\\docs\\')");
        //insert +
        _stmt.execute("insert into like_test values ('C:\\+documents+\\java\\docs\\')");
        //test _
        _stmt.execute("insert into like_test values ('C:\\_documents_\\java\\docs\\')");

        //test (
        _stmt.execute("insert into like_test values ('C:\\java\\doc(2\\%')");
        //test ()
        _stmt.execute("insert into like_test values ('C:\\java\\doc(2)\\%')");

        //
        _stmt.execute("insert into like_test values ('aaabb')");

        _stmt.execute("insert into like_test values ('aaa%bb%cc')");

        _stmt.execute("insert into like_test values ('aaa%dd%cc')");

        _stmt.execute("insert into like_test values ('aaa%bb%dd')");

        _stmt.execute("insert into like_test values ('aaa$bb$cc')");

        _stmt.execute("insert into like_test values ('aaa$bb%cc')");

        _stmt.execute("insert into like_test values ('1122')");

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs%' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());

        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html' ESCAPE '' ");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22019): invalid escape character");            
        } catch (SQLException expected) {
            if (!"22019".equals(expected.getSQLState())) {
                fail("Expected SQLException(22019): invalid escape character");
            }
        }

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\%%' ESCAPE '%' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\$%' ESCAPE '$' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\|%' ESCAPE '|' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\|%' ESCAPE 's' ");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected invalid escaping");
        } catch (Exception e) {
            // expected
        }

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\0%' ESCAPE '0' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$$' ESCAPE '$' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\index.html$", _rset.getString(1));
        assertTrue(!_rset.next());

        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$' ESCAPE '$' ");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$a' ESCAPE '$' ");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html1$' ESCAPE '1' ");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        //more than one escape char
        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$' ESCAPE '11' ");
            fail("Expected SQLException");
        } catch (SQLException expected) {
            if (!"22019".equals(expected.getSQLState())) {
                fail("Expected SQLException(22019) - invalid escape character");
            }
        }

        //no row expected
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$_' ESCAPE '$' ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents\\java\\docs\\index.html$' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents\\java\\docs\\index.html$", _rset.getString(1));
        assertTrue(!_rset.next());

        //test []
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents[aa]\\java\\docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents[aa]\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test {}
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\documents{aa}\\java\\docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\documents{aa}\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test (
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\java\\doc(2\\%' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\java\\doc(2\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        //test ( with escape
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\java\\doc(2%$%' ESCAPE '$' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\java\\doc(2\\%", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("C:\\java\\doc(2)\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        //test ()
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\java\\doc(2)\\%' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\java\\doc(2)\\%", _rset.getString(1));
        assertTrue(!_rset.next());

        //test |
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\|documents{aa}|\\java\\%docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\|documents{aa}|\\java\\%docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test escape with pipe % {}
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\||documents{aa}||\\java\\|%docs\\' ESCAPE '|'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\|documents{aa}|\\java\\%docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test ^
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\^documents^\\java\\docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\^documents^\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test +
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\+documents+\\java\\docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\+documents+\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test _ it should match two rows
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\_documents_\\java\\docs\\' ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\^documents^\\java\\docs\\", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("C:\\+documents+\\java\\docs\\", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("C:\\_documents_\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test _ with escape
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'C:\\|_documents|_\\java\\docs\\' ESCAPE '|'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("C:\\_documents_\\java\\docs\\", _rset.getString(1));
        assertTrue(!_rset.next());

        //test multiple character match using escape
        _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'aaaaaabb' ESCAPE 'a'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("aaabb", _rset.getString(1));
        assertTrue(!_rset.next());

        //test missing right number of escape characters missing one 'a;
        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'aaaaabb' ESCAPE 'a'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        //test extra right number of escape characters extra one 'a;
        try {
            _rset = _stmt.executeQuery("SELECT * FROM like_test WHERE field LIKE 'aaaaaaabb' ESCAPE 'a'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        //test multiple %
        _rset = _stmt.executeQuery("select * from like_test where field like 'aaa%%bb%cc'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("aaa%bb%cc", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("aaa$bb$cc", _rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("aaa$bb%cc", _rset.getString(1));
        assertTrue(!_rset.next());

        //test multiple % with escape (each escape characters should be followed by
        //either escape character it self or % or _
        try {
            _rset = _stmt.executeQuery("select * from like_test where field like 'aaa%%bb%cc' escape '%'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        try {
            _rset = _stmt.executeQuery("select * from like_test where field like 'aaa%bb%%cc' escape '%'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        _rset = _stmt.executeQuery("select * from like_test where field like 'aaa$%bb%cc' escape '$'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("aaa%bb%cc", _rset.getString(1));
        assertTrue(!_rset.next());

        //should not return any row
        _rset = _stmt.executeQuery("select * from like_test where field like '1122' escape '1'");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        // 1122 with 1 as escape then correct pattern should be 111122 not 11122
        try {
            _rset = _stmt.executeQuery("select * from like_test where field like '11122' escape '1'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        //1122 with 1 as escape then correct pattern should be 111122 not 1111122
        try {
            _rset = _stmt.executeQuery("select * from like_test where field like '1111122' escape '1'");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            fail("Expected SQLException(22025) - invalid escape sequence");
        } catch (SQLException expected) {
            if (!"22025".equals(expected.getSQLState())) {
                fail("Expected SQLException(22025) - invalid escape sequence");
            }
        }

        _rset = _stmt.executeQuery("select * from like_test where field like '111122' escape '1'");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals("1122", _rset.getString(1));
        assertTrue(!_rset.next());
    }

    public void testQueryOptimizerProcessWhereTree() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("create table z ( id int, name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");
        _stmt.execute("insert into y values ( 2, 'James' )");
        _stmt.execute("insert into z values ( 5, NULL )");
        _stmt.execute("insert into x values ( 5, NULL )");

        //inner join
        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 3 > y.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 3 > x.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id <= 2");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 1 <= y.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select * from x inner join y on x.id > y.id and x.id = y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select * from y inner join x on x.id > y.id and x.id = y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select * from z inner join y on z.id > y.id and z.id < y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 1 != y.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where x.id IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id where z.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(5, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id where x.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(5, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

    }
    
    
    public void testQueryOptimizerProcessWhereTreeWithBTreeIndex() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("create table z ( id int, name varchar(10) )");
        
        _stmt.execute("create btree index xidx on x(id)");
        _stmt.execute("create btree index yidx on y(id)");
        
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");
        _stmt.execute("insert into y values ( 2, 'James' )");
        _stmt.execute("insert into z values ( 5, NULL )");
        _stmt.execute("insert into x values ( 5, NULL )");

        //inner join
        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 3 > y.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 3 > x.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id <= 2");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 1 <= y.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id > y.id and x.id = y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select * from y inner join x on x.id > y.id and x.id = y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("select * from y inner join z on z.id > y.id and z.id < y.id where y.id != 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where 1 != y.id ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where y.id IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id where x.id IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _stmt.execute("create btree index zidx on z(id)");
        
        
        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id where z.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(5, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id where x.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(5, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();
        

        _stmt.execute("insert into z values ( 2, 'Mike' )");
        _stmt.execute("create btree index xname on x(name)");
        _stmt.execute("create btree index zname on z(name)");
        
        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id where x.name IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();
        

    }

    public void testResolveColumnWithTableAlias() throws SQLException {
        _stmt.execute("create table testsession (id number)");
        _stmt.execute("insert into testsession values (1)");

        // should throw column not found exception : fixed it already enbale this
        try {
            _stmt.executeQuery("SELECT i2.id, i3.answerscalevalue FROM testsession i2 left outer join testsession i3 on i2.id= i3.testSessionId");
            fail("Expected exception");
        } catch (SQLException ex) {
            // expected
        }
        _rset = _stmt.executeQuery("SELECT i2.id, i3.id FROM testsession i2 left outer join testsession i3 on i2.id= i3.id");
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt("id"));
        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testRightOuterJoinIndexonLeft() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create index emp_idx on emp ( id )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) )");
        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");
        _stmt.execute("insert into salary values ( 3, 3000, 300 )");
        _stmt.execute("insert into emp_target values ( 2, 'Teresa' )");
        _stmt.execute("insert into emp_target values ( 3, 'Teresa' )");

        // right outer - right outer join expects one row
        _rset = _stmt.executeQuery("select s1.id, s2.id from emp" + " s1 right outer join salary s2 on (s1.id = s2.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(1, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertTrue(!_rset.next());

        _rset.close();

        _rset = _stmt.executeQuery("select s1.id s1id, s2.id s2id from"
            + " (select emp.id, emp.name from emp inner join salary on emp.id = salary.id) s1" + " right outer join emp_target s2 on s1.id = s2.id");

        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));

        assertTrue(!_rset.next());
        _rset.close();

        // right outer - right outer join expects one row
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp"
            + " s1 inner join salary s2 on(s1.id = s2.id) right outer join emp_target s3 " + " on (s1.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        // right outer - right outer join expects one row
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1 right outer join salary s2 inner join emp_target s3 "
            + " on(s2.id = s3.id) on (s1.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select count(*) from emp s1 right outer join salary s2 inner join emp_target s3 "
            + " on(s2.id = s3.id) on (s2.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(6, _rset.getInt(1));
        assertTrue(!_rset.next());
        _rset.close();
        
        _rset = _stmt.executeQuery("explain select count(*) from emp s1 right outer join salary s2 inner join emp_target s3 "
            + " on(s2.id = s3.id) on (s2.id = s3.id)");
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("ChangingIndexed"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("IndexNestedLoop"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Nested"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Grouped"));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1 right outer join salary s2 inner join emp_target s3 "
            + " on(s2.id = s3.id) on (s2.id = s3.id)");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testRightOuterJoinIndexonLeft2() throws Exception {
        _stmt.execute("create table emp ( id int)");
        _stmt.execute("create table salary ( id int)");
        _stmt.execute("create index emp_idx on emp ( id )");
        _stmt.execute("create table emp_target ( id int)");
        _stmt.execute("insert into emp values ( 1)");
        _stmt.execute("insert into emp values ( 2)");
        _stmt.execute("insert into emp values ( 3)");
        _stmt.execute("insert into salary values ( 1)");
        _stmt.execute("insert into salary values ( 2)");
        _stmt.execute("insert into salary values ( 3)");
        _stmt.execute("insert into emp_target values ( 2)");
        _stmt.execute("insert into emp_target values ( 3)");

        // right outer - right outer join expects one row
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp" + " s1 right outer join salary s2 inner join emp_target s3 "
            + " on(s2.id = s3.id) on (s3.id = s1.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(2, _rset.getInt(2));
        assertEquals(2, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        assertEquals(3, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testThreeTableJoinHavingTwoTableColumnIndexed() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create btree index salary_idx on salary ( id )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) )");
        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");
        _stmt.execute("insert into emp_target values ( 3, 'Teresa' )");

        //inner join
        _rset = _stmt.executeQuery("select * from emp s1 inner join salary" + " s2 on (s1.id = s2.id) inner join emp_target s3"
            + " on (s2.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join expect no rows as we compare s2.id with s3.id (s2.id will be
        // null)
        _rset = _stmt.executeQuery("select * from emp s1 left outer join salary" + " s2 on s1.id = s2.id inner join emp_target s3"
            + " on(s2.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join expect one rows as we compare s1.id with s3.id
        _rset = _stmt.executeQuery("select * from emp s1 left outer join salary s2" + " on s1.id = s2.id inner join emp_target s3"
            + " on(s1.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());

    }

    public void testThreeTableJoinWithComparisonConditionAppliedAtJoinAndAfterJoin() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");
        //empty table
        _stmt.execute("create table salary_src ( sid int, base int, bonus int)");
        _stmt.execute("create table manager ( id int, isManager varchar(30))");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 4, 4000, 400 )");
        _stmt.execute("insert into manager values ( 2, 'Yes' )");

        //(1)(a)
        //test that comparision function is appled at join level
        //s1.id < s3.id and s1.id = s2.sid are applied together at join level
        //and we get a nested join
        _rset = _stmt.executeQuery("explain select * from emp s1 inner join salary s2 on (s1.id = s2.sid) inner join manager s3 on(s1.id < s3.id and s1.id = s2.sid)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Changing"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("IndexNestedLoop"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Nested"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Filtering"));
        assertTrue(!_rset.next());
        _rset.close();

        //(1)(b)
        //expects one row
        _rset = _stmt.executeQuery("select * from emp s1 inner join salary s2 on (s1.id = s2.sid) inner join manager s3 on(s1.id < s3.id and s1.id = s2.sid)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1, _rset.getInt(3));
        assertEquals(1000, _rset.getInt(4));
        assertEquals(100, _rset.getInt(5));
        assertEquals(2, _rset.getInt(6));
        assertEquals("Yes", _rset.getString(7));

        assertTrue(!_rset.next());
        _rset.close();

        //(2)(a)
        //test that comparision function is appled after join
        //s1.id < s3.id is applied after join
        //and we get a nested join
        _rset = _stmt.executeQuery("explain select * from emp s1 inner join salary s2 on (s1.id = s2.sid) inner join manager s3 on(s1.id < s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Changing"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("IndexNestedLoop"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1).startsWith("Nested"));
        assertTrue(!_rset.next());
        _rset.close();

        //(2)(b)
        //expects one row same as (1)(b) only difference is in the query planner strategy
        // see the diff between (1)(a) and (2)(a)
        _rset = _stmt.executeQuery("select * from emp s1 inner join salary s2 on (s1.id = s2.sid) inner join manager s3 on(s1.id < s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(1, _rset.getInt(3));
        assertEquals(1000, _rset.getInt(4));
        assertEquals(100, _rset.getInt(5));
        assertEquals(2, _rset.getInt(6));
        assertEquals("Yes", _rset.getString(7));

        assertTrue(!_rset.next());
        _rset.close();

        //test two table left outer join
        _rset = _stmt.executeQuery("select * from emp s1 left outer join manager s3 on (s1.id < s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));

        assertTrue(!_rset.next());
        _rset.close();

        //test two table left outer join with or condition
        _rset = _stmt.executeQuery("select * from emp s1 left outer join manager s3 on (s1.id < s3.id or s1.id = s3.id)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));

        assertTrue(!_rset.next());
        _rset.close();

        //test two table left outer join with and condition
        _rset = _stmt.executeQuery("select * from emp s1 left outer join manager s3 on (s1.id < s3.id and s1.id = 1)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));

        assertTrue(!_rset.next());
        _rset.close();

        //test three table join
        _rset = _stmt.executeQuery("select * from emp s1 left outer join manager s3 on (s1.id < s3.id) left outer join salary_src s2 on (s1.id = s2.sid)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(!_rset.next());
        _rset.close();

        //test three table join with and condition
        _rset = _stmt.executeQuery("select * from emp s1 left outer join manager s3 on (s1.id < s3.id and s1.id = 1) left outer join salary_src s2 on (s1.id = s2.sid)");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(2, _rset.getInt(3));
        assertEquals("Yes", _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(null, _rset.getObject(3));
        assertEquals(null, _rset.getString(4));
        assertEquals(null, _rset.getObject(5));
        assertEquals(null, _rset.getString(6));
        assertEquals(null, _rset.getObject(7));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testThreeTableJoinWithExplainCommand() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create table emp_target ( id int, salary int, name varchar(10) )");

        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");

        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");
        _stmt.execute("insert into salary values ( 3, 3000, 300 )");

        _stmt.execute("insert into emp_target values ( 3, 3300, 'Teresa' )");

        _rset = _stmt.executeQuery("select s1.id, s1.name, s2.base_salary+s2.bonus from emp_target t1 right outer join emp s1 inner join salary s2 "
            + " on(s1.id = s2.id) on (s1.id = t1.id) where t1.id is null");

        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals(_rset.getString(2), "Amy");
        assertEquals(_rset.getInt(3), 1100);

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(_rset.getString(2), "Mike");
        assertEquals(2200, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("explain select s1.id, s1.name, s2.base_salary+s2.bonus from emp_target t1 right outer join emp s1 inner join salary s2 "
            + " on(s1.id = s2.id) on (s1.id = t1.id) where t1.id is null");

        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("ChangingIndex"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("ChangingIndex"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Unmod"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("IndexNestedLoop"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("IndexNestedLoop"));
        assertTrue(_rset.next());
        assertTrue(_rset.getString(1), _rset.getString(1).startsWith("Filtering"));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testThreeTableJoinWithRightTableViewHavingTwoTableColumnIndexed() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10) )");
        _stmt.execute("create table salary ( id int, base_salary int, bonus int )");
        _stmt.execute("create btree index salary_idx on salary ( id )");
        _stmt.execute("create table emp_target ( id int, name varchar(10) )");
        _stmt.execute("insert into emp values ( 1, 'Amy' )");
        _stmt.execute("insert into emp values ( 2, 'Mike' )");
        _stmt.execute("insert into emp values ( 3, 'Teresa' )");
        _stmt.execute("insert into salary values ( 1, 1000, 100 )");
        _stmt.execute("insert into salary values ( 2, 2000, 200 )");
        _stmt.execute("insert into emp_target values ( 3, 'Teresa' )");

        //inner-inner join we expect no rows
        _rset = _stmt.executeQuery("select s1.id s1id, s2.id s2id, s3.id s3id from emp s1" + " inner join salary s2 inner join emp_target s3"
            + " on (s2.id = s3.id) on (s1.id = s2.id)");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        // left outer- inner join, expect three rows as we compare s1.id with s3.id (s3.id
        // will always be null) left outer condition s1.id = s3.id
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1" + " left outer join salary s2 inner join emp_target s3"
            + " on(s2.id = s3.id) on s1.id = s3.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 3);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(!_rset.next());
        _rset.close();

        // left outer- join inner, expect three rows as we compare s1.id with s2.id (s2.id
        // will always be null) left outer condition s1.id = s2.id
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1 left" + " outer join salary s2 inner join emp_target s3"
            + " on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 3);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(!_rset.next());
        _rset.close();

        // nested inner join between table s2 and s3 but join condition is s1.id = s3.id,
        // s1 is not in this join 's scope should throw exception.
        try {
            _rset = _stmt.executeQuery("select * from emp s1 left outer join salary s2 "
                + " inner join emp_target s3 on(s1.id = s3.id) on s1.id = s2.id");
            fail("Expected SQLException: out of scope column");
        } catch (SQLException e) {
            // expected
        }

        try {
            _rset = _stmt.executeQuery("select s1.id sid, s2.id, s3.id from emp s1 left outer join salary s2 "
                + " inner join emp_target s3 on(sid = s3.id) on s1.id = s2.id");
            fail("Expected SQLException: sid is not in scope");
        } catch (SQLException e) {
            // expected
        }

        try {
            _rset = _stmt.executeQuery("select s1.id sid, s2.id, s3.id from emp s1 left outer join salary s2 "
                + " inner join emp_target s3 on(s2.id = s3.id) on s1.id = s2.id where max(sid) = 3");
            fail("Expected SQLException : group function not allowed here");
        } catch (SQLException e) {
            // expected
        }

        try {
            _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id + 2," + " (s2.id + 2 + s1.id) as newid "
                + " from emp s1 left outer join salary s2 " + " inner join emp_target s3 on(newid = s3.id) on s1.id = s2.id");
            fail("Expected SQLException: newid is using out of scope column");
        } catch (SQLException e) {
            // expected
        }

        //left outer - right outer join expects three rows
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1 " + "left outer join salary s2 right join emp_target s3"
            + " on(s2.id = s3.id) on s1.id = s3.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertNull(_rset.getObject(2));
        assertNull(_rset.getObject(3));

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 3);
        assertNull(_rset.getObject(2));
        assertEquals(_rset.getInt(1), 3);

        assertTrue(!_rset.next());
        _rset.close();

        // right outer - inner outer join expects no rows.
        // right outer condition s1.id = s3.id
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp" + " s1 right outer join salary s2 inner join emp_target s3"
            + " on(s2.id = s3.id) on s1.id = s3.id");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        // right outer - inner outer join expects no rows.
        // right outer condition s1.id = s2.id
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1" + " right outer join salary s2 inner join emp_target"
            + " s3 on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        // right outer - right outer join expects one row
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp" + " s1 right outer join salary s2 right join emp_target s3"
            + " on(s2.id = s3.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertNull(_rset.getObject(2));
        assertEquals(_rset.getInt(3), 3);

        assertTrue(!_rset.next());
        _rset.close();

        //insert one more row in emp_target
        _stmt.execute("insert into emp_target values ( 1, 'Jennifer' )");

        //right outer - right outer join expects two row
        _rset = _stmt.executeQuery("select s1.id, s2.id, s3.id from emp s1 right" + " outer join salary s2 right join emp_target s3"
            + " on(s3.id = s2.id) on s1.id = s2.id");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertNull(_rset.getObject(2));
        assertEquals(_rset.getInt(3), 3);

        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 1);
        assertEquals(_rset.getInt(2), 1);
        assertEquals(_rset.getInt(3), 1);

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableAnsiJoinWithCompositeCondition() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("create table z ( id int, name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");
        _stmt.execute("insert into y values ( 2, 'James' )");
        _stmt.execute("insert into z values ( 2, NULL )");

        //inner join
        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id and y.id <= 2");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id and y.id != 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id and y.id IS NOT NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id and z.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("select * from x inner join z on x.id = z.id and z.name IS NULL");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join
        _rset = _stmt.executeQuery("select * from x left outer join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join make sure for left outer join right table
        //column values are null for non matching key from emp
        _rset = _stmt.executeQuery("select y.id from x left outer join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableAnsiJoinWithCompositeOnAndWhereConditions() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10) )");
        _stmt.execute("create table y ( id int, name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy' )");
        _stmt.execute("insert into x values ( 2, 'Mike' )");
        _stmt.execute("insert into x values ( 3, 'Teresa' )");
        _stmt.execute("insert into y values ( 2, 'James' )");

        //inner join
        _rset = _stmt.executeQuery("select * from x inner join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join
        _rset = _stmt.executeQuery("select * from x left outer join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //left outer join make sure for left outer join right table
        //column values are null for non matching key from emp
        _rset = _stmt.executeQuery("select y.id from x left outer join y on x.id = y.id and y.id > 1");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableInnerJoinWithComparisionCondition() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 4, 4000, 400 )");

        _rset = _stmt.executeQuery("select * from emp s1 inner join salary s2 on s1.id < s2.sid");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testTwoTableInnerJoinWithCompositeOnConditions() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10), dept int )");
        _stmt.execute("create table y ( id int, name varchar(10), dept int)");
        _stmt.execute("insert into x values ( 1, 'Amy', 101)");
        _stmt.execute("insert into x values ( 2, 'Mike', 102 )");

        _stmt.execute("insert into y values ( 1, 'Amy', 101)");
        _stmt.execute("insert into y values ( 2, 'James', 102 )");

        _rset = _stmt.executeQuery("select x.id, x.name, y.name from x inner join y on y.id = x.id and x.name = y. name and y.dept = x.dept");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableJoinWithComparisionWhereCondition() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into salary values ( 1, 100, 100)");
        _stmt.execute("insert into salary values ( 4, 4000, 400 )");

        _rset = _stmt.executeQuery("select * from emp s1, salary s2 where s1.id < s2.sid");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(4, _rset.getInt(3));
        assertEquals(4000, _rset.getInt(4));
        assertEquals(400, _rset.getInt(5));

        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableLeftOuterJoinWithCompositeOnAndWhereConditions() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10), dept int )");
        _stmt.execute("create table y ( id int, name varchar(10), dept int)");
        _stmt.execute("insert into x values ( 1, 'Amy', 101)");
        _stmt.execute("insert into x values ( 2, 'Mike', 102 )");
        _stmt.execute("insert into x values ( 3, 'Teresa', 103 )");
        _stmt.execute("insert into y values ( 2, 'James', 102 )");
        _stmt.execute("insert into y values ( 3, 'Mathew', 101 )");

        //left outer join
        _rset = _stmt.executeQuery("select x.id, x.name, y.name from x left outer join y on x.id = y.id where x.id > 1 and x.dept = y.dept");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getInt(1), 2);
        assertEquals(_rset.getString(2), "Mike");
        assertEquals(_rset.getString(3), "James");
        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testTwoTableRightOuterJoinWithCompositeOnAndWhereConditions() throws Exception {
        _stmt.execute("create table x ( id int, name varchar(10), dept int )");
        _stmt.execute("create table y ( id int, name varchar(10), dept int)");
        _stmt.execute("insert into x values ( 1, 'Amy', 101)");
        _stmt.execute("insert into x values ( 2, 'Mike', 102 )");
        _stmt.execute("insert into x values ( 3, 'Teresa', 103 )");
        _stmt.execute("insert into y values ( 2, 'James', 102 )");
        _stmt.execute("insert into y values ( 3, 'Mathew', 101 )");

        //left outer join
        _rset = _stmt.executeQuery("select x.id, x.name, y.name, 2, 'test' from x right outer join y on y.id = x.id where y.id > 2 and y.dept = x.dept");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    public void testTwoTableRightOuterJoinWithNonEqualJoin() throws Exception {
        _stmt.execute("create table emp ( id int, name varchar(10))");
        _stmt.execute("create table salary ( sid int, base int, bonus int)");
        _stmt.execute("insert into emp values ( 3, 'Teresa')");
        _stmt.execute("insert into emp values ( 4, 'xxx')");
        _stmt.execute("insert into emp values ( 1, 'Amy')");
        _stmt.execute("insert into emp values ( 2, 'Mike')");

        _stmt.execute("insert into salary values ( 1, 1000, 100)");
        _stmt.execute("insert into salary values ( 4, 4000, 400 )");

        _rset = _stmt.executeQuery("select id, name, base+bonus from emp s1 right outer join salary s2 on s1.id < s2.sid");
        assertNotNull(_rset);

        assertTrue(_rset.next());
        assertNull(_rset.getObject(1));
        assertNull(_rset.getObject(2));
        assertEquals(1100, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals("Teresa", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        assertEquals("Amy", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals("Mike", _rset.getString(2));
        assertEquals(4400, _rset.getInt(3));

        assertTrue(!_rset.next());
        _rset.close();
    }

    public void testMultiStatementInsertWithWhenClauseEvaluatingToNull() throws Exception {
        _stmt.execute("create table emp (id int, name varchar(25), manager_id int)");
        _stmt.execute("create table subordinates (id int, manager_id int)");
        _stmt.execute("create table leaders (id int, name varchar(25))");
        
        _stmt.execute("insert into emp values (1, 'Tom President', null)");
        _stmt.execute("insert into emp values (2, 'Suzy Secretary', 1)");
        _stmt.execute("insert into emp values (3, 'Dick Manager', 1)");
        _stmt.execute("insert into emp values (4, 'Harry Peon', 3)");
        
        _stmt.execute("insert first when matches(col4, '[A-Za-z ]*') then "
            + "into subordinates (id, manager_id) values (col1, col3) "
            + "else into leaders (id, name) values (col1, col2) "
            + "(select s1.id as col1, s1.name as col2, s1.manager_id as col3, s2.name as col4 "
            + "from emp s1 left outer join emp s2 on (s1.manager_id = s2.id))");
        
        _rset = _stmt.executeQuery("select id from leaders");
        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt(1));
        
        _rset = _stmt.executeQuery("select id, manager_id from subordinates order by id");
        
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt(1));
        assertEquals(1, _rset.getInt(2));
        
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt(1));
        assertEquals(1, _rset.getInt(2));
        
        assertTrue(_rset.next());
        assertEquals(4, _rset.getInt(1));
        assertEquals(3, _rset.getInt(2));
        
        assertFalse(_rset.next());
    }
}