/*
 * $Id: TestIndexSpecials.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This class is designed two have only two tests Do not add more.
 * 
 * @author Ritesh adval
 */
public class TestIndexSpecials extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestIndexSpecials(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestIndexSpecials.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        try { if(_rset!= null) _rset.close(); } catch(Exception t) {}
        try { if(_stmt!= null) _stmt.close(); } catch(Exception t) {}
        try { if(_conn!= null) _conn.close(); } catch(Exception t) {}
        _rset = null;
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection(getConnectString());
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
        //delete data base dir after we execute two test cases
        if (!oneExecuted) {
            deleteFile(getDatabaseDirectory());
        }
    }

    protected String getConnectString() {
        return "jdbc:axiondb:testdb:testdb";
    }

    protected File getDatabaseDirectory() {
        return new File(new File("."), "testdb");
    }

    public void testTruncateOnIndexedTable1() throws Exception {
        if (!oneExecuted) {
            truncateOnIndexedTable1();
            oneExecuted = true;
        } else {
            truncateOnIndexedTable2();
            oneExecuted = false;
        }
    }

    public void testTruncateOnIndexedTable2() throws Exception {
        if (!oneExecuted) {
            truncateOnIndexedTable1();
            oneExecuted = true;
        } else {
            truncateOnIndexedTable2();
            oneExecuted = false;
        }
    }

    private void truncateOnIndexedTable1() throws Exception {
        //************(1)int btree index****************
        createXTable();
        //select
        _rset = _stmt.executeQuery("select id from x ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table x");

        selectXAfterTruncate();

        //**************(2)int array index****************
        createYTable();
        //select
        _rset = _stmt.executeQuery("select id from y ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table y");

        selectYAfterTruncate();

        //*****************(3) string btree index *******************
        createZTable();
        //select
        _rset = _stmt.executeQuery("select id from z ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table z");

        selectZAfterTruncate();

        //*****************(4) string btree index *******************
        createWTable();
        //select
        _rset = _stmt.executeQuery("select id from w ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table w");

        selectWAfterTruncate();

    }

    private void truncateOnIndexedTable2() throws Exception {
        //***************(1) int btree index******************
        //select id again
        selectXAfterTruncate();

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

        selectXAfterTruncate();

        //***************(2)int array index*****************
        //select id again
        selectYAfterTruncate();

        _stmt.execute("insert into y values ( 1)");
        _stmt.execute("insert into y values ( 2)");

        //select
        _rset = _stmt.executeQuery("select id from y ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table y");

        selectYAfterTruncate();

        //*********************(3) string btree index *****************
        //select id again
        selectZAfterTruncate();

        _stmt.execute("insert into z values ( '1')");
        _stmt.execute("insert into z values ( '2')");

        //select
        _rset = _stmt.executeQuery("select id from z ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table z");

        selectZAfterTruncate();

        //*********************(4) string array index *****************
        //select id again
        selectWAfterTruncate();

        _stmt.execute("insert into w values ( '1')");
        _stmt.execute("insert into w values ( '2')");

        //select
        _rset = _stmt.executeQuery("select id from w ");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertTrue(!_rset.next());
        _rset.close();

        //truncate
        _stmt.execute("truncate table w");

        selectWAfterTruncate();
    }

    private void createXTable() throws Exception {
        _stmt.execute("create table x ( id int)");
        _stmt.execute("create btree index idx_x on x (id)");
        _stmt.execute("insert into x values ( 1)");
        _stmt.execute("insert into x values ( 2)");

    }

    private void createYTable() throws Exception {
        _stmt.execute("create table y ( id int)");
        _stmt.execute("create index idx_y on y (id)");
        _stmt.execute("insert into y values ( 1)");
        _stmt.execute("insert into y values ( 2)");
    }

    private void createZTable() throws Exception {
        _stmt.execute("create table z ( id varchar)");
        _stmt.execute("create btree index idx_z on z (id)");
        _stmt.execute("insert into z values ( '1')");
        _stmt.execute("insert into z values ( '2')");

    }

    private void createWTable() throws Exception {
        _stmt.execute("create table w ( id varchar)");
        _stmt.execute("create index idx_w on w (id)");
        _stmt.execute("insert into w values ( '1')");
        _stmt.execute("insert into w values ( '2')");

    }

    private void selectXAfterTruncate() throws Exception {
        //      select id again
        _rset = _stmt.executeQuery("select id from x ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        //select where id = 1 condition
        _rset = _stmt.executeQuery("select id from x where id = 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //select where id = 2 condition
        _rset = _stmt.executeQuery("select id from x where id = 2");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    private void selectYAfterTruncate() throws Exception {
        //      select id again
        _rset = _stmt.executeQuery("select id from y ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        //select where id = 1 condition
        _rset = _stmt.executeQuery("select id from y where id = 1");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //select where id = 2 condition
        _rset = _stmt.executeQuery("select id from y where id = 2");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    private void selectZAfterTruncate() throws Exception {
        //      select id again
        _rset = _stmt.executeQuery("select id from z ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        //select where id = 1 condition
        _rset = _stmt.executeQuery("select id from z where id like '1' ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //select where id = 2 condition
        _rset = _stmt.executeQuery("select id from z where id like '2' ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    private void selectWAfterTruncate() throws Exception {
        //      select id again
        _rset = _stmt.executeQuery("select id from w ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());

        //select where id = 1 condition
        _rset = _stmt.executeQuery("select id from w where id like '1' ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

        //select where id = 2 condition
        _rset = _stmt.executeQuery("select id from w where id like '2' ");
        assertNotNull(_rset);
        assertTrue(!_rset.next());
        _rset.close();

    }

    private static boolean oneExecuted = false;
}