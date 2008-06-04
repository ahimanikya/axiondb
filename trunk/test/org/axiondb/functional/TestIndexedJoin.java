/*
 * $Id: TestIndexedJoin.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

package org.axiondb.functional;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 */
public class TestIndexedJoin extends TestCase {
    protected Connection _conn = null;
    protected ResultSet _rset = null;
    protected Statement _stmt = null;

    //------------------------------------------------------------ Conventional

    public TestIndexedJoin(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestIndexedJoin.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws SQLException, ClassNotFoundException {
        Class.forName("org.axiondb.jdbc.AxionDriver");
        _conn = DriverManager.getConnection("jdbc:axiondb:memdb");
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        try { _rset.close(); } catch(Exception t) { }
        try { _stmt.close(); } catch(Exception t) { }
        try { _conn.close(); } catch(Exception t) { }
        _rset = null;
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:memdb");
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
    }

    //------------------------------------------------------------------- Tests

    public void testSelectOneRow() throws Exception {
        createTableFoo();
        populateTableFoo();
        createTableBar();
        populateTableBar();

        PreparedStatement stmt = _conn.prepareStatement("select FOO.B from FOO, BAR where FOO.A = BAR.A and BAR.B = ?");
        for(int i=0;i<10;i++) {
            stmt.setInt(1,i*2);
            _rset = stmt.executeQuery();
            assertNotNull("Should have been able to create ResultSet",_rset);
            assertTrue(_rset.next());
            assertEquals((i*2),_rset.getInt(1));
            assertTrue(!_rset.next());
            _rset.close();
        }
    }

    //-------------------------------------------------------------------- Util

    private void createTableFoo() throws Exception {
        _stmt.execute("create table FOO ( A integer, B integer )");
        _stmt.execute("create index FOO_NDX on FOO ( A )");
    }

    private void populateTableFoo() throws Exception {
        for(int i=0;i<10;i++) {
            _stmt.execute("insert into FOO ( A, B ) values ( " + i + "," + (2*i) + ")");
        }
    }

    private void createTableBar() throws Exception {
        _stmt.execute("create table BAR ( A integer, B integer )");
        _stmt.execute("create index BAR_NDX on BAR ( B )");
    }

    private void populateTableBar() throws Exception {
        for(int i=0;i<10;i++) {
            _stmt.execute("insert into BAR ( A, B ) values ( " + i + "," + (2*i) + ")");
        }
    }

}
