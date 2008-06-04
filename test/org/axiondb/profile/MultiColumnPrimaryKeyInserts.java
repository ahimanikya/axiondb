/*
 * $Id: MultiColumnPrimaryKeyInserts.java,v 1.1 2007/11/28 10:01:38 jawed Exp $
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
package org.axiondb.profile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


/**
 * Derived from Chas Emerick's 25-Jan-2005 post to the user's list.
 * @author Chas Emerick
 * @author Rodney Waldhoff
 */
public class MultiColumnPrimaryKeyInserts {
    private static Random _random = new Random();

    static {
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        String connectString = "jdbc:axiondb:test:/tmp/testdb" +_random.nextLong();        
        int numLoops = 10000;
        int reportEvery = 1000;
        boolean commitEveryBatch = true;
        boolean useConstraint = false;
        
        if(args.length > 0 && "--help".equals(args[0])) {
            System.out.println("args: <total # of inserts> <batch size> <commit after every batch> <use constraint> <connect string>");
            System.out.println("defaults: " + numLoops + " "  + reportEvery + " " + commitEveryBatch + " " + useConstraint + " " + connectString);
            return;
        }

        switch(args.length) {
            case 5:
                connectString = args[4];
            case 4:
                useConstraint = Boolean.valueOf(args[3]).booleanValue();
            case 3:
                commitEveryBatch = Boolean.valueOf(args[2]).booleanValue();
            case 2:
                reportEvery = Integer.parseInt(args[1]);
            case 1:
                numLoops = Integer.parseInt(args[0]);
                break;
        }

        System.out.println("Total number of inserts: " + numLoops);
        System.out.println("Reporting every: " + reportEvery);
        System.out.println("Commit after each batch? " + commitEveryBatch);
        System.out.println("Apply constraint? " + useConstraint);
        System.out.println("Connect String: " + connectString);

        System.out.println("Running...");
        System.out.println("");
        
        Connection conn = DriverManager.getConnection(connectString);
        conn.setAutoCommit(false);
        run(conn,numLoops,reportEvery,commitEveryBatch,useConstraint);
        conn.close();
    }

    private static void run(Connection conn, int loops, int reportEvery, boolean commitEveryBatch, boolean useConstraint) throws SQLException {
        String data = String.valueOf(_random.nextLong()) + String.valueOf(_random.nextLong()) + String.valueOf(_random.nextLong()) ;
        {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table data_test (a int, b int, c varchar(1000) " + (useConstraint ? ", constraint pk primary key (a, b)" : "") + ")");
            stmt.close();
            conn.commit();
        }
        PreparedStatement pstmt = conn.prepareStatement("insert into data_test (a, b, c) values (?, ?, ?)");
        long start = System.currentTimeMillis();
        long time = start;
        for(int i = 1; i <= loops; i++) {
            pstmt.setInt(1, i/10);
            pstmt.setInt(2, i%10);
            pstmt.setString(3,data);
            pstmt.executeUpdate();
            if (i % reportEvery == 0) {
                if(commitEveryBatch) { conn.commit(); }
                long stop = System.currentTimeMillis();
                System.out.println("Batch of " + reportEvery + " inserts took " + (stop - time) + " ms [" + (((double)stop-time)/reportEvery) + " ms/insert]");
                time = System.currentTimeMillis();
            }
        }
        conn.commit();
        long stop = System.currentTimeMillis();
        System.out.println("Total of " + loops + " inserts took " + (stop - start) + " ms [" + (((double)stop-start)/loops) + " ms/insert]");
    }
    
}
