/*
 * $Id: TestInsertIntoSeveralLargeIndices.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 */
public class TestInsertIntoSeveralLargeIndices extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestInsertIntoSeveralLargeIndices(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInsertIntoSeveralLargeIndices.class);
    }

    public static void main(String[] args) throws Exception {
        TestInsertIntoSeveralLargeIndices test = new TestInsertIntoSeveralLargeIndices("via main");
        int rowsToInsert = DEFAULT_TOTAL_INSERTS;
        int commitEvery = DEFAULT_COMMIT_EVERY;
        boolean createStringIndices = true;
        switch (args.length) {
            case 0:
                break;
            case 3:
                createStringIndices = "true".equals(args[2]);
            case 2:
                commitEvery = Integer.parseInt(args[1]);
            case 1:
                try {
                    rowsToInsert = Integer.parseInt(args[0]);
                } catch(NumberFormatException e) {
                    printHelp();
                    return;
                }
                break;
            default:
                printHelp();
                return;
        }
        test.createSchema(createStringIndices);
        test.populate(rowsToInsert, commitEvery);
    }
    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        createSchema(true);
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected String getConnectString() {
        return "jdbc:axiondb:testdb:testdb";
    }

    protected File getDatabaseDirectory() {
        return new File("testdb");
    }

    //------------------------------------------------------------------- Tests

    public void testPopulate() throws Exception {
        populate(DEFAULT_TOTAL_INSERTS, DEFAULT_COMMIT_EVERY);
    }

    //-------------------------------------------------------------- 

    private void createSchema(boolean createStringIndices) throws Exception {
        Connection conn = DriverManager.getConnection(getConnectString());
        Statement stmt = conn.createStatement();
        stmt.execute("create sequence mysequence");
        stmt.execute(
            "create table mytable ( id integer, columna varchar(10), columnb varchar(10), columnc varchar(10), columnd varchar(10) )");
        stmt.execute("create btree index indexid on mytable ( id )");
        if (createStringIndices) {
            stmt.execute("create btree index indexa on mytable ( columna )");
            stmt.execute("create btree index indexb on mytable ( columnb )");
            stmt.execute("create btree index indexc on mytable ( columnc )");
            stmt.execute("create btree index indexd on mytable ( columnd )");
        }
        stmt.close();
        conn.close();
    }

    private void populate(int totalInserts, int commitEvery) throws SQLException {
        long init = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        PreparedStatement pstmt =
            conn.prepareStatement("insert into mytable values ( mysequence.nextval, ?, ?, ?, ? )");
        long last = System.currentTimeMillis();
        for (int i = 0; i < totalInserts; i++) {
            if (i != 0 && i % commitEvery == 0) {
                long commitstart = System.currentTimeMillis();
                conn.commit();
                long now = System.currentTimeMillis();
                printTimes(i, init, last, commitstart, now);
                last = now;
            }
            pstmt.setString(1, createRandomString());
            pstmt.setString(2, createRandomString());
            pstmt.setString(3, createRandomString());
            pstmt.setString(4, createRandomString());
            pstmt.executeUpdate();
        }
        long commitstart = System.currentTimeMillis();
        conn.commit();
        long now = System.currentTimeMillis();
        printTimes(totalInserts, init, last, commitstart, now);
        pstmt.close();
        conn.close();
        System.out.println("Total time: " + (System.currentTimeMillis() - init));
    }

    private static String createRandomString() {
        char[] chars = new char[RANDOM.nextInt(64)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = CHARS[RANDOM.nextInt(CHARS.length)];
        }
        return new String(chars);
    }

    private static void printTimes(
        int totalRowCount,
        long startTime,
        long timeOfLastCommit,
        long timeOfCommitStart,
        long now) {
        System.out.println(
            totalRowCount
                + " records inserted. [total time: "
                + (now - startTime)
                + ", time: "
                + (now - timeOfLastCommit)
                + "; free: "
                + Runtime.getRuntime().freeMemory()
                + "]. Commit time: "
                + (100L * (now - timeOfCommitStart) / (now - timeOfLastCommit))
                + "%");
    }

    private static void printHelp() {
        System.out.println("args: [<number of rows to insert> [<number of rows between commits> [<create string indices>]]]");
        System.out.println("defaults: " + DEFAULT_TOTAL_INSERTS + " " + DEFAULT_COMMIT_EVERY + " true");
    }
    //--------------------------------------------------------------
    private static final Random RANDOM = new Random();

    private static final char[] CHARS;
    static {
        StringBuffer buf = new StringBuffer();
        for (char c = 'a'; c <= 'z'; c++) {
            buf.append(c);
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            buf.append(c);
        }
        buf.append(" ~!@#$%^&*()-+=");
        for (char c = '0'; c <= '9'; c++) {
            buf.append(c);
        }
        CHARS = buf.toString().toCharArray();
    }

    private static final int DEFAULT_TOTAL_INSERTS = 200000;
    private static final int DEFAULT_COMMIT_EVERY = 5000;
}
