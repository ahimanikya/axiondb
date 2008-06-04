/*
 * $Id: TestFunctional.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

package org.axiondb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Chuck Burdick
 */
public class TestFunctional extends AbstractDbdirTest {
    private static final Log _log = LogFactory.getLog(TestFunctional.class);
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;

    public TestFunctional(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = {TestFunctional.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestFunctional.class);
    }

    public void setUp() throws SQLException, ClassNotFoundException {
        Class.forName("org.axiondb.jdbc.AxionDriver");
        _conn = DriverManager.getConnection("jdbc:axiondb:memdb");
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        cleanJdbc();
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:memdb");
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
        super.tearDown();
    }

    public void testCreate() {
        _log.debug("Starting testCreate()");
        assertNotNull("Should have a valid connection", _conn);
        assertNotNull("Should have a valid statement", _stmt);
    }

    public void testCreateTable() throws SQLException {
        _log.debug("Starting testCreateTable()");
        String sql = "CREATE TABLE foo ( test VARCHAR )";
        assertTrue("Should not generate result set", !_stmt.execute(sql));
    }

    public void testInsertNoTable() {
        _log.debug("Starting testInsertNoTable()");
        try {
            String sql = "INSERT INTO foo ( test ) VALUES ( 'this' )";
            _stmt.execute(sql);
            fail("Should throw SQLException because no tables defined");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsertWrongTable() throws SQLException {
        _log.debug("Starting testInsertWrongTable()");
        testCreateTable();
        try {
            String sql = "INSERT INTO bar ( test ) VALUES ( 'this' )";
            _stmt.execute(sql);
            fail("Should throw SQLException because no tables defined");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsert() throws SQLException {
        _log.debug("Starting testInsert()");
        testCreateTable();
        String sql = "INSERT INTO foo ( test ) VALUES ( 'this' );";

        sql = "SELECT test FROM foo";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should get results", _rset);
        while(_rset.next()) {
            assertEquals("Should have result", "this", _rset.getString(1));
        }
    }

    // UTILITY METHODS

    private void cleanJdbc() {
        try { _rset.close(); } catch(Exception t) { }
        try { _stmt.close(); } catch(Exception t) { }
        try { _conn.close(); } catch(Exception t) { }
        _rset = null;
        _stmt = null;
        _conn = null;
    }
}
