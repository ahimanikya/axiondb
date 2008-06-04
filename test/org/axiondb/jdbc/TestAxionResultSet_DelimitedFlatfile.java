/*
 * $Id: TestAxionResultSet_DelimitedFlatfile.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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
package org.axiondb.jdbc;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.axiondb.io.FileUtil;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 *
 * @author Jonathan Giron
 * @version $Revision: 1.1 $
 */
public class TestAxionResultSet_DelimitedFlatfile extends TestAxionResultSet {

    private static final String DATABASE_NAME = "JDBC_DelimitedFFDB";

    /**
     * Constructor for TestAxionResultSet_DelimitedFlatfile.
     * @param arg0
     */
    public TestAxionResultSet_DelimitedFlatfile(String testName) {
        super(testName);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(TestAxionResultSet_DelimitedFlatfile.class);
    }    

    protected void doCleanup() throws Exception {
        if (_rset != null) {
            _rset.close();
        }
        
        if (_stmt != null) {
            _stmt.execute("drop table foo");
            _stmt.close();
        }
        
        if (_conn != null) {
            _conn.close();
        }
        
        FileUtil.delete(getDatabaseDirectory());
    }
    
    protected String getDatabaseName() {
        return DATABASE_NAME;
    }
    
    protected File getDatabaseDirectory() {
        return new File(".", DATABASE_NAME);
    }
    
    /**
     * Creates basic (scrollable, read-only) table for use in general ResultSet testing.
     * 
     * @throws Exception if error occurs during table or ResultSet construction
     */
    protected void createBasicTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try {
            _stmt.executeUpdate("drop table foo");
        } catch (SQLException ignore) {
            // ignore - table doesn't exist.
        }
        
        _stmt.executeUpdate("create external table foo (test varchar(10), num int) organization ("
            + "loadtype='delimited' filename='basicfoo.txt')");
        _stmt.execute("truncate table foo");
        
        _conn.setAutoCommit(false);
        for (int i = 0; i < IMAX; i++) {
            for (int j = 0; j < JMAX; j++) {
                _stmt.executeUpdate("insert into foo values ('" + String.valueOf((char) (65 + i)) 
                    + "', " + j + ")");
            }
        }
        _conn.commit();
        _conn.setAutoCommit(true);
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        _stmt.executeQuery("select * from foo");
        _rset = _stmt.getCurrentResultSet();
    }

    /**
     * Creates empty table for use in testing position logic against a degenerate AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction 
     */
    protected void createEmptyTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create external table foo (id int) organization (loadtype='delimited' filename='emptyfoo.txt')");
        _stmt.execute("truncate table foo");
        
        _rset = _stmt.executeQuery("select * from foo");
    }
    
    /**
     * Creates empty table for use in testing position logic against a degenerate AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction 
     */
    protected void createOneRowTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create external table foo (id int) organization (loadtype='delimited' filename='onerowfoo.txt')");
        _stmt.execute("truncate table foo");
        
        _stmt.execute("insert into foo values (1)");

        _rset = _stmt.executeQuery("select * from foo");
    }
    
    /**
     * Constructs a table for use in testing updating methods in AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction
     */
    protected void createUpdateTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        String createString = "create external table foo (id int, str_10 varchar(10), dt date, "
            + "tm time, ts timestamp, bool boolean) organization (loadtype='delimited' filename='updatefoo.txt')";
        _stmt.execute(createString);
        _stmt.execute("truncate table foo");

        _stmt.execute("insert into foo values (1, 'This is 1', '2005-01-01', '12:34:56', '2005-03-31 23:56:00.0', false)");
        _stmt.execute("insert into foo values (2, 'This is 2', '2005-02-02', '23:45:00', '2005-04-01 00:00:00.0', false)");
        _stmt.execute("insert into foo values (3, 'This is 3', '2005-03-03', '06:30:30', '2005-04-02 01:23:45.6', false)");
        _stmt.execute("insert into foo values (4, 'This is 4', '2005-04-04', '07:45:45', '2005-04-03 02:34:32.1', false)");

        _rset = _stmt.executeQuery("select * from foo");
    }
  
}
