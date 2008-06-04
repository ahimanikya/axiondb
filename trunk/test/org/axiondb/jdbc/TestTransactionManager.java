/*
 * $Id: TestTransactionManager.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

package org.axiondb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.Database;
import org.axiondb.Table;
import org.axiondb.Transaction;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.jdbc.AxionConnection;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rodney Waldhoff
 */
public class TestTransactionManager extends TestCase {

    //------------------------------------------------------------ Conventional


    public TestTransactionManager(String testName) {
        super(testName);
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch(Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    public static Test suite() {
        return new TestSuite(TestTransactionManager.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private Database _database = null;
    private String _connectString = "jdbc:axiondb:memdb";

    public void setUp() throws Exception {
        super.setUp();
        Connection conn = DriverManager.getConnection(_connectString);
        _database = ((AxionConnection)(conn)).getDatabase();
        Statement stmt = conn.createStatement();
        stmt.execute("create table FOO ( NUM integer, STR varchar2 )");
        stmt.close();
        PreparedStatement pstmt = conn.prepareStatement("insert into FOO values ( ?, ? )");
        for(int i=0;i<10;i++) {
            pstmt.setInt(1,i);
            pstmt.setString(2,String.valueOf(i));
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _database.shutdown();
        _database = null;
    }

    //------------------------------------------------------------------- Tests

    public void testNoOpCommit() throws Exception {
        Transaction t = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,t.getState());
        _database.getTransactionManager().commitTransaction(t);
        assertEquals(Transaction.STATE_APPLIED,t.getState());
    }

    public void testNoOpRollback() throws Exception {
        Transaction t = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,t.getState());
        _database.getTransactionManager().abortTransaction(t);
        assertEquals(Transaction.STATE_ABORTED,t.getState());
    }

    public void testDontApplyWhenThereExistsOpenTransaction() throws Exception {
        Transaction t = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,t.getState());

        Transaction u = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,u.getState());

        Table foo = t.getTable("FOO");
        SimpleRow row = new SimpleRow(2);
        row.set(0,new Integer(17));
        row.set(1,"seventeen");        
        foo.addRow(row);
        
        _database.getTransactionManager().commitTransaction(t);
        assertEquals(Transaction.STATE_COMMITTED,t.getState());

        _database.getTransactionManager().commitTransaction(u);
        assertEquals(Transaction.STATE_APPLIED,t.getState());
        assertEquals(Transaction.STATE_APPLIED,u.getState());
    }

    public void testOpenOnTransaction() throws Exception {
        Transaction t = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,t.getState());

        Transaction u = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,u.getState());

        Table foo = t.getTable("FOO");
        SimpleRow row = new SimpleRow(2);
        row.set(0,new Integer(17));
        row.set(1,"seventeen");        
        foo.addRow(row);
        
        _database.getTransactionManager().commitTransaction(t);
        assertEquals(Transaction.STATE_COMMITTED,t.getState());

        Transaction v = _database.getTransactionManager().createTransaction();
        assertEquals(Transaction.STATE_OPEN,v.getState());
        assertEquals(t,v.getOpenOnTransaction());

        _database.getTransactionManager().commitTransaction(u);
        _database.getTransactionManager().commitTransaction(v);
        assertEquals(Transaction.STATE_APPLIED,t.getState());
        assertEquals(Transaction.STATE_APPLIED,u.getState());
        assertEquals(Transaction.STATE_APPLIED,v.getState());
    }
}
