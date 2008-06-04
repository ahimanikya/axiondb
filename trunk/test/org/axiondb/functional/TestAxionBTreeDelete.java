/*
 * $Id: TestAxionBTreeDelete.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Charles Ye
 * @author Ahimanikya Satapathy
 */
public class TestAxionBTreeDelete extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestAxionBTreeDelete(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestAxionBTreeDelete.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected String getConnectString() {
        // disk table
        // return "jdbc:axiondb:diskdb:testdb";
        // memory table
        return "jdbc:axiondb:memdb";
    }

    protected File getDatabaseDirectory() {
        return new File(new File("."), "testdb");
    }

    //------------------------------------------------------------------- Tests
    public void test001_IntBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create btree index id_index on x(id)");

        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");

        _stmt.executeUpdate("delete from x where name='Amy1'");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        while (rset.next()) {
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }
        assertTrue(!rset.next());
    }

    public void test002_IntBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create unique btree index id_index on x(id)");

        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");

        _stmt.execute("delete from x where name='Amy1'");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        while (rset.next()) {
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }
        assertTrue(!rset.next());
    }

    public void test003_IntBTree() throws Exception {
        _stmt.execute("create table x ( id integer, xid varchar(10), name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");
        _stmt.execute("create unique btree index id_index on x(id)");

        _stmt.executeUpdate("delete from x where id <= 1");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x");

        for (int i = 1; i <= 1; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());
    }

    public void test004_IntBTree() throws Exception {
        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 1, 'Amy4', 'Amy4' )");
        _stmt.execute("insert into x values ( 1, 'Amy5', 'Amy5' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");
        _stmt.execute("insert into x values ( 2, 'Mike2', 'Mike2' )");
        _stmt.execute("insert into x values ( 2, 'Mike3', 'Mike3' )");
        _stmt.execute("insert into x values ( 2, 'Mike4', 'Mike4' )");
        _stmt.execute("insert into x values ( 2, 'Mike5', 'Mike5' )");
        _stmt.execute("create btree index id_index on x(id)");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x");
        assertNotNull(rset);
        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("Amy" + i, rset.getString(3));
        }

        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());

        _stmt.executeUpdate("delete from x where id = 1");

        rset = _stmt.executeQuery("select id, xid, name from x");

        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());
    }

    public void test005_IntBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid integer, name varchar(10) )");
        _stmt.execute("create btree index id_index on x(id)");

        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 1," + i + ", 'Amy1' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 1," + i + ", 'Amy2' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 2," + i + ", 'Mike1' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 2," + i + ", 'Mike2' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 3," + i + ", 'Mike3' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 3," + i + ", 'Mike4' )");
        }

        //long start = System.currentTimeMillis();
        _stmt.executeUpdate("delete from x where name='Amy1'");
        //long end = System.currentTimeMillis();
        //System.out.println("delete operation time =" + (end - start));

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        int count = 0;
        while (rset.next()) {
            count++;
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where id >= 2");

        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike1".equals(rset.getString(3)) || "Mike2".equals(rset.getString(3)) || "Mike3".equals(rset.getString(3))
                || "Mike4".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 4);
        assertTrue(!rset.next());

        _stmt.executeUpdate("delete from x where name='Mike1'");
        _stmt.executeUpdate("delete from x where name='Mike2'");
        rset = _stmt.executeQuery("select id, xid, name from x where id = 2");
        assertTrue(!rset.next());

        _stmt.executeUpdate("delete from x where name='Mike3'");
        _stmt.executeUpdate("delete from x where name='Mike4'");
        rset = _stmt.executeQuery("select id, xid, name from x where id = 3");
        assertTrue(!rset.next());

        _stmt.executeUpdate("delete from x where name='Amy2'");
        rset = _stmt.executeQuery("select id, xid, name from x where id = 1");
        assertTrue(!rset.next());
    }

    public void test006_IntBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid integer, name varchar(10) )");
        _stmt.execute("create btree index id_index on x(id)");

        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( 1," + i + ", 'Amy1' )");
            _stmt.execute("insert into x values ( 2," + i + ", 'Mike1' )");
            _stmt.execute("insert into x values ( 1," + i + ", 'Amy2' )");
            _stmt.execute("insert into x values ( 2," + i + ", 'Mike2' )");
            _stmt.execute("insert into x values ( 3," + i + ", 'Mike3' )");
            _stmt.execute("insert into x values ( 3," + i + ", 'Mike4' )");
        }

        //long start = System.currentTimeMillis();
        _stmt.executeUpdate("delete from x where name='Amy1'");
        //long end = System.currentTimeMillis();
        //System.out.println("delete operation time =" + (end - start));

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        int count = 0;
        while (rset.next()) {
            count++;
            assertTrue(!"Amy1".equals(rset.getString(3)));

        }
        assertTrue(count == MAX_LOOP);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where id = 2");
        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike1".equals(rset.getString(3)) || "Mike2".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 2);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where id = 3");
        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike3".equals(rset.getString(3)) || "Mike4".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 2);
        assertTrue(!rset.next());
    }

    public void test008_IntBtree() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("create table mytable ( intcol integer, strcol varchar(10), extra integer )");
        stmt.execute("create btree index intindex on mytable (intcol)");
        stmt.execute("create btree index strindex on mytable (strcol)");
        for (int i = 0; i < 10; i++) {
            stmt.execute("insert into mytable values ( 1, 'key', " + i + " )");
        }

        assertNRows(10, "select intcol, strcol, extra from mytable");
        assertNRows(10, "select intcol, strcol, extra from mytable where strcol = 'key'");
        assertNRows(10, "select intcol, strcol, extra from mytable where intcol = 1");

        assertEquals(1, stmt.executeUpdate("delete from mytable where extra = 0"));

        assertNRows(9, "select intcol, strcol, extra from mytable");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }

        assertNRows(9, "select intcol, strcol, extra from mytable where strcol = 'key'");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where strcol = 'key'");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        assertNRows(9, "select intcol, strcol, extra from mytable where intcol = 1");

        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where intcol = 1");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        stmt.close();
    }

    public void test001_StringBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create btree index xid_index on x(xid)");

        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");

        _stmt.executeUpdate("delete from x where name='Amy1'");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        while (rset.next()) {
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }
        assertTrue(!rset.next());
    }

    public void test002_StringBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create unique btree index xid_index on x(xid)");

        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");

        _stmt.execute("delete from x where name='Amy1'");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        while (rset.next()) {
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }
        assertTrue(!rset.next());
    }

    public void test003_StringBTree() throws Exception {
        _stmt.execute("create table x ( id integer, xid varchar(10), name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");
        _stmt.execute("create unique btree index xid_index on x(xid)");

        _stmt.executeUpdate("delete from x where id <= 1");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x");

        for (int i = 1; i <= 1; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());
    }

    public void test004_StringBTree() throws Exception {
        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("insert into x values ( 1, 'Amy1', 'Amy1' )");
        _stmt.execute("insert into x values ( 1, 'Amy2', 'Amy2' )");
        _stmt.execute("insert into x values ( 1, 'Amy3', 'Amy3' )");
        _stmt.execute("insert into x values ( 1, 'Amy4', 'Amy4' )");
        _stmt.execute("insert into x values ( 1, 'Amy5', 'Amy5' )");
        _stmt.execute("insert into x values ( 2, 'Mike1', 'Mike1' )");
        _stmt.execute("insert into x values ( 2, 'Mike2', 'Mike2' )");
        _stmt.execute("insert into x values ( 2, 'Mike3', 'Mike3' )");
        _stmt.execute("insert into x values ( 2, 'Mike4', 'Mike4' )");
        _stmt.execute("insert into x values ( 2, 'Mike5', 'Mike5' )");
        _stmt.execute("create btree index xid_index on x(xid)");

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x");
        assertNotNull(rset);
        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(1, rset.getInt(1));
            assertEquals("Amy" + i, rset.getString(3));
        }

        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());

        _stmt.executeUpdate("delete from x where id = 1");

        rset = _stmt.executeQuery("select id, xid, name from x");

        for (int i = 1; i <= 5; i++) {
            assertTrue(rset.next());
            assertEquals(2, rset.getInt(1));
            assertEquals("Mike" + i, rset.getString(3));
        }
        assertTrue(!rset.next());
    }

    public void test005_StringBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create btree index xid_index on x(xid)");

        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( " + i + ", 'Amy', 'Amy1' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( " + i + ", 'Amy', 'Amy2' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( " + i + ", 'Mike', 'Mike1' )");
        }
        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( " + i + ", 'Mike', 'Mike2' )");
        }

        //long start = System.currentTimeMillis();
        _stmt.executeUpdate("delete from x where name='Amy1'");
        //long end = System.currentTimeMillis();
        //System.out.println("delete operation time =" + (end - start));

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where xid = 'Amy'");

        int count = 0;
        while (rset.next()) {
            count++;
            assertTrue(!"Amy1".equals(rset.getString(3)));
        }

        assertTrue(count == MAX_LOOP);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where xid = 'Mike' ");

        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike1".equals(rset.getString(3)) || "Mike2".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 2);
        assertTrue(!rset.next());
    }

    public void test008_StringBTree() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("create table mytable ( intcol integer, strcol varchar(10), extra integer )");
        stmt.execute("create btree index intindex on mytable (intcol)");
        stmt.execute("create btree index strindex on mytable (strcol)");
        for (int i = 0; i < 10; i++) {
            stmt.execute("insert into mytable values ( 1, 'key', " + i + " )");
        }

        assertNRows(10, "select intcol, strcol, extra from mytable");
        assertNRows(10, "select intcol, strcol, extra from mytable where strcol = 'key'");
        assertNRows(10, "select intcol, strcol, extra from mytable where intcol = 1");

        assertEquals(1, stmt.executeUpdate("delete from mytable where extra = 0"));

        assertNRows(9, "select intcol, strcol, extra from mytable");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }

        assertNRows(9, "select intcol, strcol, extra from mytable where strcol = 'key'");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where strcol = 'key'");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        assertNRows(9, "select intcol, strcol, extra from mytable where intcol = 1");

        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where intcol = 1");
            while (rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        stmt.close();
    }

    public void test006_StringBTree() throws Exception {

        _stmt.execute("create table x ( id int, xid varchar(10), name varchar(10) )");
        _stmt.execute("create btree index xid_index on x(xid)");

        for (int i = 0; i < MAX_LOOP; i++) {
            _stmt.execute("insert into x values ( " + 1 + ", 'Amy1', 'Amy1' )");
            _stmt.execute("insert into x values ( " + 2 + ", 'Mike1', 'Mike1' )");
            _stmt.execute("insert into x values ( " + 1 + ", 'Amy2', 'Amy2' )");
            _stmt.execute("insert into x values ( " + 2 + ", 'Mike2', 'Mike2' )");
            _stmt.execute("insert into x values ( " + 3 + ", 'Mike3', 'Mike3' )");
            _stmt.execute("insert into x values ( " + 3 + ", 'Mike4', 'Mike4' )");
        }

        //long start = System.currentTimeMillis();
        _stmt.executeUpdate("delete from x where name='Amy1'");
        //long end = System.currentTimeMillis();
        //System.out.println("delete operation time =" + (end - start));

        ResultSet rset = _stmt.executeQuery("select id, xid, name from x where id = 1");

        int count = 0;
        while (rset.next()) {
            count++;
            assertTrue(!"Amy1".equals(rset.getString(3)));

        }
        assertTrue(count == MAX_LOOP);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where id = 2");
        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike1".equals(rset.getString(3)) || "Mike2".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 2);
        assertTrue(!rset.next());

        rset = _stmt.executeQuery("select id, xid, name from x where id = 3");
        count = 0;
        while (rset.next()) {
            count++;
            assertTrue("Mike3".equals(rset.getString(3)) || "Mike4".equals(rset.getString(3)));
        }
        assertTrue(count == MAX_LOOP * 2);
        assertTrue(!rset.next());
    }

    private static int MAX_LOOP = 100; //1000; // test loop
}