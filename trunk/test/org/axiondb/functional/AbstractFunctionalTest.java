/*
 * $Id: AbstractFunctionalTest.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import org.axiondb.io.FileUtil;
import org.axiondb.jdbc.AxionConnection;


/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Amrish Lal
 * @author Girish Patil
 */
public abstract class AbstractFunctionalTest extends TestCase {

    //------------------------------------------------------------ Conventional

    public AbstractFunctionalTest(String testName) {
        super(testName);
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch(Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        _conn = (AxionConnection)(DriverManager.getConnection(getConnectString()));
        _stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
        deleteFile(getDatabaseDirectory());
    }

    protected boolean deleteFile(File file) throws Exception {
        return FileUtil.delete(file);
    }

    //-------------------------------------------------------------------- Util


    protected void assertNRows(int n, String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = _stmt.executeQuery(query);
            assertNRows(n,rset);
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected void assertNRows(int n, ResultSet rset) throws SQLException {
        for(int i=0;i<n;i++) {
            assertTrue("Expected " + (i+1) + "th row.",rset.next());
        }
        assertTrue("Did not expect " + (n+1) + "th row.",!rset.next());
    }

    protected void assertOneRow(String query) throws SQLException {
        assertNRows(1,query);
    }

    protected void assertNoRows(String query) throws SQLException {
        assertNRows(0,query);
    }

    protected void assertNoRows(ResultSet rset) throws SQLException {
        assertNRows(0,rset);
    }

    protected void assertException(String query) {
        try {
            _stmt.execute(query);
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
    }

    protected void assertExceptionOnRead(String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = _stmt.executeQuery(query);
            rset.next();
            Object obj = rset.getObject(1);
            fail("Expected Exception, found " + obj);
        } catch(Exception e) {
            // expected
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected void assertObjectResult(Object expected, ResultSet rset) throws SQLException {
        try {
            assertTrue("Expected a row here.",rset.next());
            assertEquals(expected,rset.getObject(1));
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            rset.close();
        }
    }


    protected void assertObjectResult(Object expected, String query) throws SQLException {
        assertObjectResult(expected,_stmt.executeQuery(query));
    }


    protected void assertResult(Object[] expected, ResultSet rset) throws SQLException {
        try {
            assertTrue("Expected a row here.",rset.next());
            for(int i=0;i < expected.length; i++) {
                if(expected[i] == null) {
                    assertNull(rset.getString(i+1));
                    assertTrue(rset.wasNull());
                } else if(expected[i] instanceof Integer) {
                    assertEquals( ((Number)expected[i]).intValue(), rset.getInt(i + 1));
                    assertFalse(rset.wasNull());
                } else if(expected[i] instanceof String) {
                    assertEquals(expected[i], rset.getString(i + 1));
                    assertFalse(rset.wasNull());
                } else {
                    fail("Unexpected type " + expected.getClass().getName() + " in expected array.  Please add a new case to the assertResult method.");
                }
            }
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            rset.close();
        }
    }

    protected void assertResult(Object[] expected, String query) throws SQLException {
        assertResult(expected,_stmt.executeQuery(query));
    }

    protected void assertResult(String expected, ResultSet rset) throws SQLException {
        try {
            assertTrue("Expected a row here.",rset.next());
            assertEquals(expected,rset.getString(1));
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            rset.close();
        }
    }


    protected void assertResult(String expected, String query) throws SQLException {
        assertResult(expected,_stmt.executeQuery(query));
    }

    protected void assertResult(int expected, ResultSet rset) throws SQLException {
        try {
            assertTrue("Expected a row here.",rset.next());
            assertEquals(expected,rset.getInt(1));
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected void assertResult(int expected, String query) throws SQLException {
        assertResult(expected,_stmt.executeQuery(query));
    }

    protected void assertResult(float expected, String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = _stmt.executeQuery(query);
            assertTrue("Expected a row here.",rset.next());
            assertEquals(expected,rset.getFloat(1),0.0001f);
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected void assertNullResult(String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = _stmt.executeQuery(query);
            assertTrue("Expected a row here.",rset.next());
            assertNull(rset.getObject(1));
            assertTrue(rset.wasNull());
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected void assertResult(boolean expected, String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = _stmt.executeQuery(query);
            assertTrue("Expected a row here.",rset.next());
            assertEquals(expected,rset.getBoolean(1));
            assertTrue("Expected no more rows here.",!rset.next());
        } finally {
            if(null != rset) {
                rset.close();
            }
        }
    }

    protected String getConnectString() {
        return "jdbc:axiondb:memdb";
    }

    protected File getDatabaseDirectory() {
        return null;
    }

    protected void createTableFoo(boolean includeIndex) throws Exception {
        _stmt.execute("create table FOO ( NUM integer, STR varchar2(255), NUMTWO integer )");
        if(includeIndex) {
            createIndexOnFoo();
        }
    }

    protected void createTableFoo() throws Exception {
        createTableFoo(true);
    }

    protected void createIndexOnFoo() throws Exception {
    }

    protected void createAndPopulateDual() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("create table DUAL ( DUMMY varchar(10) )");
        stmt.executeUpdate("insert into DUAL values ( 'X' )");
        stmt.close();
    }

    protected void populateTableFoo() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ?)");
        for(int i=0;i<NUM_ROWS_IN_FOO;i++) {
            pstmt.setInt(1,i);
            pstmt.setString(2,String.valueOf(i));
            pstmt.setInt(3,(i/2));
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableBar() throws Exception {
        _stmt.execute("create table BAR ( ID integer, DESCR varchar(10), DESCR2 varchar(10) )");
    }

    protected void populateTableBar() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("insert into BAR ( ID, DESCR, DESCR2 ) values ( ?, ?, ?)");
        for(int i=0;i<NUM_ROWS_IN_BAR;i++) {
            pstmt.setInt(1,i);
            pstmt.setString(2,String.valueOf(i));
            pstmt.setString(3,"Descr"+String.valueOf(i));
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableX() throws Exception {
        _stmt.execute("CREATE TABLE x(a INTEGER, b INTEGER)");
    }

    protected void dropTableX() throws Exception {
        _stmt.execute("DROP TABLE x");
    }

    protected void populateTableX() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO x VALUES(?,?)");
        int count = 0;
        int strt = 2;
        while (count < NUM_ROWS_IN_X) {
            pstmt.setInt(1,strt);
            pstmt.setInt(2,strt);
            strt++;
            count++;
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableY() throws Exception {
        _stmt.execute("CREATE TABLE Y(a INTEGER, b INTEGER)");
    }

    protected void dropTableY() throws Exception {
        _stmt.execute("DROP TABLE y");
    }

    protected void populateTableY() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO y VALUES(?,?)");
        int count = 0;
        int strt = 1;
        while (count < NUM_ROWS_IN_Y) {
            pstmt.setInt(1,strt);
            pstmt.setInt(2,(strt * 10));
            strt++;
            count++;
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableZ() throws Exception {
        _stmt.execute("CREATE TABLE Z(a INTEGER, b INTEGER)");
    }

    protected void dropTableZ() throws Exception {
        _stmt.execute("DROP TABLE z");
    }

    protected void populateTableZ() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO z VALUES(?,?)");
        int count = 0;
        int strt = 3;
        while (count < NUM_ROWS_IN_Z) {
            pstmt.setInt(1,strt);
            pstmt.setInt(2,(strt * 100));
            strt++;
            count++;
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableStates() throws Exception {
        _stmt.execute("CREATE TABLE states ( state varchar(20) )");
    }

    protected void populateTableStates() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO states VALUES(?)");
        String[] states = new String[] { "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "viginia", "washington", "west virginia", "wisconsin", "wyoming" };

        for (int i = 0; i < 50; i++) {
            pstmt.setString(1, states[i]);
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createTableWords() throws Exception {
        _stmt.execute("CREATE TABLE words ( word varchar(10) )");
    }

    protected void populateTableWords() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("INSERT INTO words VALUES(?)");
        String[] words = new String[] { "bat", "bait", "bet", "bent", "bit", "bot", "bolt", "but", "bunt", "big", "bing", "bog", "borg",
                                        "cat", "cot", "cut",
                                        "dang", "dog", "dig", "drag",
                                        "fat", "falt", "fig", "fist", "fit", "fog", "frog" };

        for (int i = 0; i < words.length; i++) {
            pstmt.setString(1, words[i]);
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    protected void createSequenceFooSeq() throws Exception {
        _stmt.execute("create sequence foo_seq");
    }

    protected void dropSequenceFooSeq() throws Exception {
        _stmt.execute("drop sequence foo_seq");
    }

    protected void executeSiliently(Statement stmt, String query){
        try {
            stmt.execute(query);
        } catch (Exception ex){
            // do nothing
        }
    }

    protected void closeDBResources(Connection conn, Statement stmnt, ResultSet rs){
        if (rs != null){
            try {
                rs.close();
            } catch (Exception ex){
                //
            }
        }

        if (stmnt != null){
            try {
                stmnt.close();
            } catch (Exception ex){
                //
            }
        }

        if (conn != null){
            try {
                conn.close();
            } catch (Exception ex){
                //
            }
        }

    }

    protected static final int NUM_ROWS_IN_FOO = 6; // assumed to be even in some tests
    protected static final int NUM_ROWS_IN_BAR = 6;
    protected static final int NUM_ROWS_IN_X = 3;
    protected static final int NUM_ROWS_IN_Y = 3;
    protected static final int NUM_ROWS_IN_Z = 3;
    protected AxionConnection _conn = null;
    protected ResultSet _rset = null;
    protected Statement _stmt = null;
}
