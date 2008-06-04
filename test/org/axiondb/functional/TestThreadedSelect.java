/*
 * $Id: TestThreadedSelect.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 */
public class TestThreadedSelect extends TestCase {
    private static final int NUMBER_OF_TEST_THREADS = 5;
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;

    //------------------------------------------------------------ Conventional

    public TestThreadedSelect(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestThreadedSelect.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        Class.forName("org.axiondb.jdbc.AxionDriver");
        _conn = DriverManager.getConnection("jdbc:axiondb:diskdb:testdb2");
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        try { if(_rset!= null)_rset.close(); } catch(Exception t) { }
        try { if(_stmt!= null)_stmt.close(); } catch(Exception t) { }
        try { if(_conn!= null)_conn.close(); } catch(Exception t) { }
        _rset = null;
        _stmt = null;
        _conn = null;
        
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:diskdb:testdb2");
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
        
        deleteFile(new File("testdb2"));
    }

    private boolean deleteFile(File file) throws Exception {
        if(file.exists()) {
            if(file.isDirectory()) {
                File[] files = file.listFiles();
                for(int i = 0; i < files.length; i++) {
                    deleteFile(files[i]);
                }
            }
            return file.delete();
            }
        return true;
    }

    //------------------------------------------------------------------- Tests

    public void testWithoutIndex() throws Exception {
        createTableFoo();
        populateTableFoo();
        runTests();
    }

    public void testWithIndex() throws Exception {
        createTableFoo();
        createIndexOnFoo();
        populateTableFoo();
        runTests();
    }
    
    public void testWithoutIndexForFlatFile() throws Exception {
        createDelimitedTableFoo();
        populateTableFoo();
        runTests();
    }

    public void testWithIndexForFlatFile() throws Exception {
        createDelimitedTableFoo();
        createIndexOnFoo();
        populateTableFoo();
        runTests();
    }

    //----------------------------------------------------------- Inner Classes

    private void runTests() throws Exception {
        TestThread[] tests = new TestThread[NUMBER_OF_TEST_THREADS];
        Thread[] threads = new Thread[tests.length];
        for(int i=0;i<tests.length;i++) {
            tests[i] = new TestThread();
            threads[i] = new Thread(tests[i]);
        }
        for(int i=0;i<tests.length;i++) {
            threads[i].start();
        }
        for(int i=0;i<tests.length;i++) {
            while(!tests[i].done) {
                try {
                    Thread.sleep(100L);
                } catch(Exception e) {
                }
            }
            assertTrue(tests[i].reason,tests[i].success);
        }
    }

    class TestThread implements Runnable {       
        public boolean done = false;
        public boolean success = false;
        public String reason = null;
        
        public void run() {
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rset = null;

            try {
                conn = DriverManager.getConnection("jdbc:axiondb:diskdb:testdb2");
                stmt = conn.prepareStatement("select NUM, STR from FOO");
                for(int i=0;i<20;i++) {
                    rset = stmt.executeQuery();
                    for(int j=0;j<30;j++) {
                        if(!rset.next()) {
                            success = false;
                            reason = "Expected 30 rows, only found " + j + " [" + i +"," + j +"]";
                            break;
                        }
                        if(null == rset.getString(2)) {
                            success = false;
                            reason = "Expected non null column found null at " + j + " [" + i +"," + j +"]";
                            break;
                        }
                    }
                    if(rset.next()) {
                        reason = "Expected no more rows [" + i +"]";
                        success = false;
                        break;
                    }
                    rset.close();
                }
                success = true;
            } catch(Exception e) {
                reason = e.toString();
                e.printStackTrace();
                success = false;
            } finally {
                try { rset.close(); } catch(Exception t) { }
                try { stmt.close(); } catch(Exception t) { }
                try { conn.close(); } catch(Exception t) { }
                done = true;
            }
        }
    }

    //-------------------------------------------------------------------- Util

    private void createTableFoo() throws Exception {
        _stmt.execute("create table FOO ( NUM integer, STR varchar2(2), NUMTWO integer )");
    }
    
    private void createDelimitedTableFoo() throws Exception {
        String eol = System.getProperty("line.separator");
        _stmt.execute("create external table FOO ( NUM integer, STR varchar2(60), " +
                " NUMTWO integer ) organization(loadtype='delimited' recorddelimiter='" + eol +"')");
    }

    private void createIndexOnFoo() throws Exception {
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
    }

    private void populateTableFoo() throws Exception {
        for(int i=0;i<30;i++) {
            _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", '" + i + "', " + (i/2) + ")");
        }
    }

}
