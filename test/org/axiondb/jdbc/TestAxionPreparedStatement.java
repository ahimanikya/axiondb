/*
 * $Id: TestAxionPreparedStatement.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

package org.axiondb.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Chuck Burdick
 */
public class TestAxionPreparedStatement extends TestAxionStatement {
   public TestAxionPreparedStatement(String testName) {
      super(testName);
   }

   public static Test suite() {
      return new TestSuite(TestAxionPreparedStatement.class);
   }

    public void setUp() throws Exception {
        super.setUp();
        _queryStmt = getConnection().prepareStatement(QUERY);
        _insertStmt = getConnection().prepareStatement(INSERT);
    }

    public void tearDown() throws Exception {
        try { _queryStmt.close(); } catch (Exception e) {}
        _queryStmt = null;
        try { _insertStmt.close(); } catch (Exception e) {}
        _insertStmt = null;
        super.tearDown();
    }

    protected Statement getQueryStatement() {
        return _queryStmt;
    }

    protected Statement getInsertStatement() {
        return _insertStmt;
    }

    protected ResultSet executeQuery() throws SQLException {
        return _queryStmt.executeQuery();
    }

    protected boolean executeQueryViaExecute() throws SQLException {
        return _queryStmt.execute();
    }

    protected boolean executeInsertViaExecute() throws SQLException {
        return _insertStmt.execute();
    }

    public void testGetResultSetMetaData() throws Exception {
        ResultSet rset = executeQuery();
        assertNotNull(((PreparedStatement)getQueryStatement()).getMetaData());
        rset.close();
    }

    public void testCantCallStatementExecuteMethods() throws Exception {
        try { 
            _queryStmt.execute("delete from foo");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try { 
            _queryStmt.executeQuery("select * from foo");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try { 
            _queryStmt.executeUpdate("delete from foo");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try { 
            _queryStmt.addBatch("delete from foo");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testSetEscapeProcessing() throws Exception {
        // "Since prepared statements have usually been parsed prior to making this call,
        // disabling escape processing for prepared statements will have no effect"
        _queryStmt.setEscapeProcessing(true);
        _queryStmt.setEscapeProcessing(false);
    }

    public void testGetResultSetMetaDataFailure() throws Exception {
        executeInsertViaExecute();
        try {
            ((PreparedStatement)getInsertStatement()).getMetaData();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testExecuteStatementWithNoResultSetFailure() throws Exception {
        _queryStmt = getConnection().prepareStatement(INSERT);
        try {
            executeQuery();
            fail("Expected SQLException; associated statement does not return a ResultSet.");
        } catch (SQLException e) {
            // expected
        }
    }
    
    private PreparedStatement _queryStmt = null;
    private PreparedStatement _insertStmt = null;
}
