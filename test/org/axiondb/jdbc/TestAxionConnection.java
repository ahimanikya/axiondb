/*
 * $Id: TestAxionConnection.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

package org.axiondb.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestAxionConnection extends AxionTestCaseSupport {

    public TestAxionConnection(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestAxionConnection.class);
    }

    public void testGetURL() throws Exception {
        assertEquals(CONNECT_STRING,getAxionConnection().getURL());
    }

    // "Calling the method close on a Connection object that is already closed is a no-op"
    public void testDoubleClose() throws Exception {
        Connection conn = getConnection();
        assertTrue(!conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }

    public void testCommitAndRollbackWithoutTransaction() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        try {
            conn.commit();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            conn.rollback();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        conn.close();
    }

    public void testAutoCommitTrueByDefault() throws Exception {
        assertTrue(getConnection().getAutoCommit());
    }

    public void testGetCatalog() throws Exception {
        assertEquals("",getConnection().getCatalog());
    }

    public void testGetMetaData() throws Exception {
        assertNotNull(getConnection().getMetaData());
    }

    public void testPrepareCall() throws Exception {
        try {
            getConnection().prepareCall("call xyzzy");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            getConnection().prepareCall("call xyzzy",1,1);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testPrepareStatement() throws Exception {
        try {
            getConnection().prepareStatement("select * from foo where id = ?",1,1);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testCreateStatement() throws Exception {
        // Per JDBC spec, createStatement() returns a forward-only, read-only Statement.        
        Statement stmt = getConnection().createStatement();
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());        
        
        // Axion supports forward-only and scroll-sensitive ResultSets.
        assertExpectedStatementType(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertExpectedStatementType(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertExpectedStatementType(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        assertExpectedStatementType(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        
        // Unsupported but known ResultSet types should return a "best-guess" implementation and
        // generate "an SQLWarning on the Connection object that is creating the statement." [sic]
        {
            Connection conn = getConnection();
            conn.clearWarnings();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            SQLWarning warning = conn.getWarnings();
            assertNotNull(warning);
            assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stmt.getResultSetType());
        }
        
        {
            Connection conn = getConnection();
            conn.clearWarnings();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            assertNotNull(conn.getWarnings());
            assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stmt.getResultSetType());
        }
        
        // Bogus type and concurrency values should throw a SQLException - unsure whether this
        // too should instead return a fallback Statement type. 
        try {
            getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, -12324);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        
        try {
            getConnection().createStatement(-12324, ResultSet.CONCUR_UPDATABLE);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        
        try {
            getConnection().createStatement(-12324, -12324);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    /**
     * @param stmt
     * @param type_forward_only
     * @param concur_read_only
     */
    private void assertExpectedStatementType(int stmtType, int stmtConcur) 
            throws Exception {
        Statement stmt = getConnection().createStatement(stmtType, stmtConcur);
        assertNotNull(stmt);
        assertEquals(stmtType, stmt.getResultSetType());
        assertEquals(stmtConcur, stmt.getResultSetConcurrency());
    }

    public void testSetCatalog() throws Exception {
        getConnection().setCatalog("");
    }
    
    public void testSetReadOnly() throws Exception {
        getConnection().setReadOnly(false);
    }
    
    public void testSetTypeMap() throws Exception {
        getConnection().setTypeMap(Collections.EMPTY_MAP);
    }
    
    public void testGetWarnings() throws Exception {
        Connection conn = getConnection();
        assertNull(conn.getWarnings());
        conn.clearWarnings();
        assertNull(conn.getWarnings());
        conn.close();
        try {
            conn.getWarnings();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testIsReadOnly() throws Exception {
        assertTrue(! getConnection().isReadOnly());
    }

    public void testNativeSQL() throws Exception {
        String sql = "select * from foo";
        assertEquals(sql,getConnection().nativeSQL(sql));
    }

    public void testGetTypeMap() throws Exception {
        assertNotNull(getConnection().getTypeMap());
        assertTrue(getConnection().getTypeMap().isEmpty());
    }

    public void testSetAutoCommit() throws Exception {
        Connection conn = getConnection();
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertTrue(!conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
    }

    public void testDefaultTransactionIsolation() throws Exception {
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, getConnection().getTransactionIsolation());
    }

    public void testSetTransactionIsolation() throws Exception {
        Connection conn = getConnection();
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        try {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
}
