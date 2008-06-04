/*
 * $Id: TestPersistentDatabase.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.FileUtil;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Rodney Waldhoff
 */
public class TestPersistentDatabase extends TestCase {
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;

    //------------------------------------------------------------ Conventional

    public TestPersistentDatabase(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestPersistentDatabase.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private File _dbDir = null;
    private String _connectString = "jdbc:axiondb:diskdb:testdb";

    public void setUp() throws Exception {
        Class.forName("org.axiondb.jdbc.AxionDriver");
        _dbDir = new File(".", "testdb");
        openJDBC();
    }

    public void tearDown() throws Exception {
        closeJDBC();
        FileUtil.delete(_dbDir);
        _dbDir = null;
    }

    //------------------------------------------------------------------- Tests

    public void testConstraintsPersist() throws Exception {
        _stmt.execute("create table foo ( id integer, str varchar(10) )");
        _stmt.execute("alter table foo add constraint foo_not_null not null ( str )");
        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 1, 'one' )"));
        try {
            _stmt.executeUpdate("insert into foo values ( 2, null )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        {
            _rset = _stmt.executeQuery("select * from FOO");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(!_rset.next());
        }

        closeJDBC();
        openJDBC();

        assertEquals(1, _stmt.executeUpdate("insert into foo values ( 2, 'two' )"));
        try {
            _stmt.executeUpdate("insert into foo values ( 3, null )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }

        {
            _rset = _stmt.executeQuery("select * from FOO order by id");
            assertTrue(_rset.next());
            assertEquals(1, _rset.getInt(1));
            assertEquals("one", _rset.getString(2));
            assertTrue(_rset.next());
            assertEquals(2, _rset.getInt(1));
            assertEquals("two", _rset.getString(2));
            assertTrue(!_rset.next());
        }

    }

    public void testConnected() throws Exception {
        assertNotNull(_conn);
        assertNotNull(_stmt);
    }

    public void testCreateTable() throws Exception {
        createTableFoo();
        File tabledir = new File(_dbDir, "FOO");
        assertTrue(tabledir.exists());
        File metafile = new File(tabledir, "FOO.META");
        assertTrue(metafile.exists());
    }

    public void testCreateBadTableType() throws Exception {
        try {
            _stmt.execute("create bogus table FOO ( NUM integer, STR varchar2, NUMTWO integer )");
            fail("Expected Exception : Bad table type");
        } catch (Exception e) {
            // expected
        }
    }

    public void testCreatePopulateTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        File tabledir = new File(_dbDir, "FOO");
        assertTrue(tabledir.exists());
        File metafile = new File(tabledir, "FOO.META");
        assertTrue(metafile.exists());
        File datafile = new File(tabledir, "FOO.DATA");
        assertTrue(datafile.toString(), datafile.exists());
        _stmt.execute("shutdown");
        assertTrue(datafile.length() > 8);
    }

    public void testCreateClosePopulateTable() throws Exception {
        createTableFoo();
        closeJDBC();
        openJDBC();
        populateTableFoo();
        File tabledir = new File(_dbDir, "FOO");
        assertTrue(tabledir.exists());
        File metafile = new File(tabledir, "FOO.META");
        assertTrue(metafile.exists());
        File datafile = new File(tabledir, "FOO.DATA");
        assertTrue(datafile.exists());
        _stmt.execute("shutdown");
        assertTrue(datafile.length() > 8);
    }

    public void testCreatePopulateSelectTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        _rset = _stmt.executeQuery("select NUM from FOO");
        assertNotNull(_rset);
        int expectedSum = 0;
        int actualSum = 0;
        for (int i = 0; i < 10; i++) {
            expectedSum += i;
            assertTrue(_rset.next());
            actualSum += _rset.getInt(1);
            assertTrue(!_rset.wasNull());
        }
        assertTrue(!_rset.next());
        _rset.close();
        assertEquals(expectedSum, actualSum);
    }

    public void testCreatePopulateCloseSelectTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        closeJDBC();
        openJDBC();
        _rset = _stmt.executeQuery("select NUM from FOO");
        assertNotNull(_rset);
        int expectedSum = 0;
        int actualSum = 0;
        for (int i = 0; i < 10; i++) {
            expectedSum += i;
            assertTrue(_rset.next());
            actualSum += _rset.getInt(1);
            assertTrue(!_rset.wasNull());
        }
        assertTrue(!_rset.next());
        _rset.close();
        assertEquals(expectedSum, actualSum);
    }

    public void testCreateClosePopulateCloseSelectTable() throws Exception {
        createTableFoo();
        closeJDBC();
        openJDBC();
        populateTableFoo();
        closeJDBC();
        openJDBC();
        _rset = _stmt.executeQuery("select NUM from FOO");
        assertNotNull(_rset);
        int expectedSum = 0;
        int actualSum = 0;
        for (int i = 0; i < 10; i++) {
            expectedSum += i;
            assertTrue(_rset.next());
            actualSum += _rset.getInt(1);
            assertTrue(!_rset.wasNull());
        }
        assertTrue(!_rset.next());
        _rset.close();
        assertEquals(expectedSum, actualSum);
    }

    public void testCreateClosePopulateCloseSelectCloseSelectTable() throws Exception {
        createTableFoo();
        closeJDBC();
        openJDBC();
        populateTableFoo();
        closeJDBC();
        openJDBC();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
        closeJDBC();
        openJDBC();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
    }

    public void testCreatePopulateSelectDeleteCloseSelectTable() throws Exception {
        createTableFoo();
        populateTableFoo();

        // select
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }

        // delete
        assertEquals(5, _stmt.executeUpdate("delete from FOO where NUM >= 5"));
        assertEquals(1, _stmt.executeUpdate("delete from FOO where NUM = 3"));

        // close
        closeJDBC();
        openJDBC();

        // select
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 3; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            for (int i = 4; i < 5; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
    }

    public void testCreateRemountPopulateRemountSelectRemountDeleteRemountSelectTable()
            throws Exception {
        createTableFoo();
        PreparedStatement pstmt = _conn.prepareStatement("remount ?");
        pstmt.setString(1, _dbDir.getCanonicalPath());
        pstmt.execute();
        populateTableFoo();
        pstmt.setString(1, _dbDir.getCanonicalPath());
        pstmt.execute();
        // select
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
        pstmt.setString(1, _dbDir.getCanonicalPath());
        pstmt.execute();

        // delete
        assertEquals(5, _stmt.executeUpdate("delete from FOO where NUM >= 5"));
        assertEquals(1, _stmt.executeUpdate("delete from FOO where NUM = 3"));

        pstmt.setString(1, _dbDir.getCanonicalPath());
        pstmt.execute();

        // select
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 3; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            for (int i = 4; i < 5; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
        pstmt.close();
    }

    public void testRemount() throws Exception {
        // create a table and populate it
        createTableFoo();
        populateTableFoo();

        // select
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }

        // now move those files
        //File newdbdir = new File(".", "dupdb");
        File newdbdir = new File(".", "testdb");
        //copyFiles(_dbDir, newdbdir);

        // remount
        PreparedStatement pstmt = _conn.prepareStatement("remount ?");
        pstmt.setString(1, newdbdir.getCanonicalPath());
        pstmt.execute();

        // select again
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }

        // remount again
        pstmt.setString(1, _dbDir.getCanonicalPath());
        pstmt.execute();

        //deleteFile(newdbdir);

        // select again
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }

        closeJDBC();

    }

    public void testCheckFileState() throws Exception {
        // create a table and populate it
        createTableFoo();
        populateTableFoo();
        _rset = _stmt.executeQuery("checkfilestate");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        _rset.close();
        closeJDBC();
    }

    public void testCheckFileState2() throws Exception {
        // create a table and populate it
        createTableFoo();
        populateTableFoo();
        assertTrue(_stmt.execute("checkfilestate"));
        _rset = _stmt.getResultSet();
        assertNotNull(_rset);
        assertTrue(_rset.next());
        _rset.close();
        closeJDBC();
    }

    public void testCreatePopulateSelectUpdateSelectTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
        assertEquals(1, _stmt.executeUpdate("update FOO set NUM = 10 where NUM = 0"));
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 1; i <= 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
    }

    public void testCreatePopulateSelectUpdateCloseSelectTable() throws Exception {
        createTableFoo();
        populateTableFoo();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
        assertEquals(1, _stmt.executeUpdate("update FOO set NUM = 10 where NUM = 0"));
        closeJDBC();
        openJDBC();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 1; i <= 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }
    }

    public void testCreatePopulateSelectUpdateInsertInto() throws Exception {
        createTableFoo();
        populateTableFoo();
        createExceptionTableFoo();
        {
            _rset = _stmt.executeQuery("select NUM from FOO");
            assertNotNull(_rset);
            int expectedSum = 0;
            int actualSum = 0;
            for (int i = 0; i < 10; i++) {
                expectedSum += i;
                assertTrue(_rset.next());
                actualSum += _rset.getInt(1);
                assertTrue(!_rset.wasNull());
            }
            assertTrue(!_rset.next());
            _rset.close();
            assertEquals(expectedSum, actualSum);
        }

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO  SET FOO.NUM = S1.NUM+1 "
            + " FROM FOO S1 WHERE S1.NUM = FOO.NUM EXCEPTION WHEN S1.NUM < 5 THEN "
            + " INSERT INTO EXPFOO"));
        ResultSet rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(5, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO  SET FOO.NUM = S1.NUM+1 "
            + " FROM FOO S1 WHERE S1.NUM = FOO.NUM EXCEPTION WHEN S1.NUM < 5 THEN "
            + " INSERT INTO EXPFOO VALUES(S1.NUM, S1.STR, S1.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(10, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = S1.NUM+1 "
            + " FROM FOO S1 WHERE S1.NUM = FOO.NUM EXCEPTION WHEN S1.NUM < 5 THEN "
            + " INSERT INTO EXPFOO(NUM,STR,NUMTWO) VALUES(S1.NUM, S1.STR, S1.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(15, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = S1.NUM+1 "
            + " FROM FOO S1 WHERE S1.NUM = FOO.NUM EXCEPTION WHEN S1.NUM < 5 THEN "
            + " INSERT INTO EXPFOO T (T.NUM, T.STR, T.NUMTWO) "
            + " VALUES(S1.NUM, S1.STR, S1.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(20, rset.getInt(1));

    }

    public void testCreatePopulateSelectUpdateInsertInto2() throws Exception {
        createTableFoo();
        populateTableFoo();
        createExceptionTableFoo();

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = FOO.NUM+1 "
            + " EXCEPTION WHEN FOO.NUM < 5 THEN INSERT INTO EXPFOO"));
        ResultSet rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(5, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = FOO.NUM+1 "
            + " EXCEPTION WHEN FOO.NUM < 5 THEN "
            + " INSERT INTO EXPFOO VALUES(FOO.NUM, FOO.STR, FOO.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(10, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = FOO.NUM+1 "
            + " EXCEPTION WHEN FOO.NUM < 5 THEN "
            + " INSERT INTO EXPFOO(NUM,STR,NUMTWO) VALUES(FOO.NUM, FOO.STR, FOO.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(15, rset.getInt(1));

        assertEquals(5, _stmt.executeUpdate(" UPDATE FOO SET FOO.NUM = FOO.NUM+1 "
            + " EXCEPTION WHEN FOO.NUM < 5 THEN "
            + " INSERT INTO EXPFOO T (T.NUM, T.STR, T.NUMTWO) "
            + " VALUES(FOO.NUM, FOO.STR, FOO.NUMTWO)"));
        rset = _stmt.executeQuery("select count(*) from expfoo");
        rset.next();
        assertEquals(20, rset.getInt(1));
    }
    
    public void testUpdateExceptionWhenClause() throws Exception {
        createTableFoo();
        populateTableFoo();
        createExceptionTableFoo();

        PreparedStatement pstmt = _conn.prepareStatement(" UPDATE FOO SET FOO.NUM = FOO.NUM+1 "
            + " EXCEPTION WHEN FOO.NUM < ? THEN "
            + " INSERT INTO EXPFOO T (T.NUM, T.STR, T.NUMTWO) "
            + " VALUES(FOO.NUM, FOO.STR, FOO.NUMTWO)");
        pstmt.setInt(1, 5);
        
        assertEquals(5, pstmt.executeUpdate());
        pstmt.close();
    }

    public void testMultipleConnectionsBetweenShutdowns() throws Exception {
        closeJDBC(); // close out the instance scope connection

        // create a table
        {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = DriverManager.getConnection(_connectString);
                stmt = conn.createStatement();
                stmt.execute("create table BAR ( \"VALUE\" varchar(10) )");
            } finally {
                stmt.close();
                conn.close();
            }
        }

        // insert one row at a time
        for (int i = 0; i < 10; i++) {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = DriverManager.getConnection(_connectString);
                stmt = conn.createStatement();
                stmt.executeUpdate("insert into BAR values ( '" + i + "')");
            } finally {
                stmt.close();
                conn.close();
            }
            // and select it back
            ResultSet rset = null;
            try {
                conn = DriverManager.getConnection(_connectString);
                stmt = conn.createStatement();
                rset = stmt.executeQuery("select count(*) from BAR");
                assertTrue(rset.next());
                assertEquals(i + 1, rset.getInt(1));
                assertTrue(!rset.next());
            } finally {
                rset.close();
                stmt.close();
                conn.close();
            }
        }

        // now shutdown
        {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = DriverManager.getConnection(_connectString);
                stmt = conn.createStatement();
                stmt.execute("shutdown");
            } finally {
                stmt.close();
                conn.close();
            }
        }

        // reopen and select again
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rset = null;
            try {
                conn = DriverManager.getConnection(_connectString);
                stmt = conn.createStatement();
                rset = stmt.executeQuery("select count(*) from BAR");
                assertTrue(rset.next());
                assertEquals(10, rset.getInt(1));
                assertTrue(!rset.next());
            } finally {
                rset.close();
                stmt.close();
                conn.close();
            }
        }

    }

    //-------------------------------------------------------------------- Util

    private void openJDBC() throws Exception {
        _conn = DriverManager.getConnection(_connectString);
        _stmt = _conn.createStatement();
    }

    private void closeJDBC() throws Exception {
        try { if(_rset!= null) _rset.close(); } catch(Exception t) {}
        try { if(_stmt!= null) _stmt.close(); } catch(Exception t) {}
        try { if(_conn!= null) _conn.close(); } catch(Exception t) {}
        _rset = null;
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection(_connectString);
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
    }

    private void createTableFoo() throws Exception {
        _stmt.execute("create table FOO ( NUM integer, STR varchar2, NUMTWO integer )");
        _stmt.execute("create index FOONDX on FOO ( NUM )");
    }

    private void createExceptionTableFoo() throws Exception {
        _stmt.execute("create table EXPFOO ( NUM integer, STR varchar2, NUMTWO integer )");
    }

    private void populateTableFoo() throws Exception {
        for (int i = 0; i < 10; i++) {
            _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", '" + i + "', "
                + (i / 2) + ")");
        }
    }

    private void copyFiles(File from, File to) throws IOException {
        if (from.isDirectory()) {
            copyDirectory(from, to);
        } else {
            copyFile(from, to);
        }
    }

    private void copyFile(File from, File to) throws IOException {
        AxionFileSystem fs = new AxionFileSystem();
        InputStream in = fs.openDataInputSteam(from);
        OutputStream out = fs.createDataOutputSteam(to);
        for (int b = in.read(); b != -1; b = in.read()) {
            out.write(b);
        }
        in.close();
        out.close();
    }

    private void copyDirectory(File from, File to) throws IOException {
        to.mkdirs();
        String[] files = from.list();
        for (int i = 0; i < files.length; i++) {
            File secondfrom = new File(from, files[i]);
            File secondto = new File(to, files[i]);
            copyFiles(secondfrom, secondto);
        }
    }

}