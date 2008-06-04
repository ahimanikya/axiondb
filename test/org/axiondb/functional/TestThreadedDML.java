/*
 * $Id: TestThreadedDML.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

package org.axiondb.functional;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.io.FileUtil;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 */
public class TestThreadedDML extends TestCase {
    private static final int NUM_TEST_THREADS = 5;
    private Connection _conn = null;
    private Statement _stmt = null;

    protected String getConnectString() {
        return "jdbc:axiondb:memdb";
    }

    protected File getDatabaseDirectory() {
        return null;
    }

    //------------------------------------------------------------ Conventional

    public TestThreadedDML(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestThreadedDML.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        Class.forName("org.axiondb.jdbc.AxionDriver");
        _conn = DriverManager.getConnection(getConnectString());
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        try {
            _stmt.execute("shutdown");
        } catch (Throwable e) {
            //e.printStackTrace();
        }
            
        try {
            _stmt.close();
            _conn.close();
        } catch (Exception t) {
            t.printStackTrace();
        }
        _stmt = null;
        _conn = null;

        FileUtil.delete(getDatabaseDirectory());
    }

    //------------------------------------------------------------------- Tests

    public void testWithAutocommit() throws Exception {
        createTableFoo();
        populateTableFoo();
        runTests(true);
    }

    public void testWithNoAutocommit() throws Exception {
        createTableFoo();
        populateTableFoo();
        runTests(false);
    }

    public void testIndexedWithAutocommit() throws Exception {
        createTableFoo();
        createIndexOnFoo();
        populateTableFoo();
        runTests(true);
    }

    public void testIndexedWithNoAutocommit() throws Exception {
        createTableFoo();
        createIndexOnFoo();
        populateTableFoo();
        runTests(false);
    }

    //----------------------------------------------------------- Inner Classes

    private void runTests(boolean autocommit) throws Exception {
        TestThread[] tests = new TestThread[NUM_TEST_THREADS];
        Thread[] threads = new Thread[NUM_TEST_THREADS];
        for (int i = 0; i < NUM_TEST_THREADS; i++) {
            tests[i] = new TestThread(autocommit);
            threads[i] = new Thread(tests[i]);
        }
        for (int i = 0; i < NUM_TEST_THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NUM_TEST_THREADS; i++) {
            while (!tests[i].done) {
                try {
                    Thread.sleep(100L);
                } catch (Exception e) {
                }
            }
            if (!tests[i].success) {
                throw tests[i].exception;
            }
        }
        Thread.sleep(100L);
    }

    class TestThread implements Runnable {
        private Random _random = new Random();
        private boolean _autocommit;
        public boolean done = false;
        public boolean success = true;
        public Exception exception = null;

        public TestThread(boolean autocommit) {
            _autocommit = autocommit;
        }

        private Exception select(Connection conn) {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            try {
                stmt = conn.prepareStatement("select NUM, STR from FOO where NUM > ? and NUM < ?");
                stmt.setInt(1, _random.nextInt(10));
                stmt.setInt(2, _random.nextInt(100) + 10);
                rset = stmt.executeQuery();
                while (rset.next()) {
                    int num = rset.getInt(1);
                    String str = rset.getString(2);
                    if (!str.equals(String.valueOf(num))) {
                        return new RuntimeException(str + " != " + num);
                    }
                }
                return null;
            } catch (Exception e) {
                return e;
            } finally {
                try {
                    if(rset!= null) rset.close();
                } catch (Exception t) {
                }
                try {
                    if(stmt!= null) stmt.close();
                } catch (Exception t) {
                }
            }
        }

        private Exception insert(Connection conn) {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ? )");
                for (int i = 0; i < 5; i++) {
                    int num = _random.nextInt(100);
                    stmt.setInt(1, num);
                    stmt.setString(2, String.valueOf(num));
                    stmt.setInt(3, num / 2);
                    if (1 != stmt.executeUpdate()) {
                        return new RuntimeException("Expected 1");
                    }
                }
                return null;
            } catch (Exception e) {
                if (e instanceof SQLException && ((SQLException) e).getErrorCode() == 51000) {
                    // ignored
                    return null;
                }
                return e;
            } finally {
                try {
                    stmt.close();
                } catch (Exception t) {
                }
            }
        }

        private Exception update(Connection conn) {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("update FOO set NUM = ?, STR = ?, NUMTWO = ? where NUM = ? or NUMTWO = ?");
                for (int i = 0; i < 5; i++) {
                    int num = _random.nextInt(100);
                    stmt.setInt(1, num);
                    stmt.setString(2, String.valueOf(num));
                    stmt.setInt(3, num / 2);
                    stmt.setInt(4, _random.nextInt(100));
                    stmt.setInt(5, _random.nextInt(100));
                    stmt.executeUpdate();
                }
                return null;
            } catch (Exception e) {
                if (e instanceof SQLException && ((SQLException) e).getErrorCode() == 51000) {
                    // ignored
                    return null;
                }
                return e;
            } finally {
                try {
                    stmt.close();
                } catch (Exception t) {
                }
            }
        }

        private Exception delete(Connection conn) {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("delete from FOO where NUM between ? and ?");
                for (int i = 0; i < 5; i++) {
                    int num = _random.nextInt(100);
                    stmt.setInt(1, num);
                    stmt.setInt(2, num + _random.nextInt(10));
                    stmt.executeUpdate();
                }
                return null;
            } catch (Exception e) {
                if (e instanceof SQLException && ((SQLException) e).getErrorCode() == 51000) {
                    // ignored
                    return null;
                }
                return e;
            } finally {
                try {
                    stmt.close();
                } catch (Exception t) {
                }
            }
        }

        public void run() {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(getConnectString());
                conn.setAutoCommit(_autocommit);
                for (int i = 0; i < 50; i++) {
                    switch (_random.nextInt(4)) {
                        case 0:
                            exception = select(conn);
                            break;
                        case 1:
                            exception = insert(conn);
                            break;
                        case 2:
                            exception = update(conn);
                            break;
                        case 3:
                            exception = delete(conn);
                            break;
                    }
                    if (null != exception) {
                        success = false;
                        break;
                    }
                }
            } catch (Exception e) {
                exception = e;
                success = false;
            } finally {
                if (!_autocommit) {
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                    }
                }
                try {
                    conn.close();
                } catch (Exception t) {
                }
                done = true;
            }
        }
    }

    //-------------------------------------------------------------------- Util

    private void createTableFoo() throws Exception {
        _stmt.execute("create table FOO ( NUM integer, STR varchar2(2), NUMTWO integer )");
    }

    private void createIndexOnFoo() throws Exception {
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
    }

    private void populateTableFoo() throws Exception {
        for (int i = 0; i < 30; i++) {
            _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", '" + i + "', " + (i / 2) + ")");
        }
    }

}