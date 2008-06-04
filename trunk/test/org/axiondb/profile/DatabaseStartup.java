/*
 * $Id: DatabaseStartup.java,v 1.1 2007/11/28 10:01:38 jawed Exp $
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


/**
 * @author Rodney Waldhoff
 */
public class DatabaseStartup {
    static {
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        String connectString = "jdbc:axiondb:test:testdb";        
        int numLoops = 10;
        boolean insert = false;

        switch(args.length) {
            case 3:
                insert = Boolean.valueOf(args[2]).booleanValue();
            case 2:
                connectString = args[1];
            case 1:
                numLoops = Integer.parseInt(args[0]);
                break;
        }

        System.out.println("Connect String: " + connectString);
        System.out.println("Loops: " + numLoops);

        System.out.println("Running...");
        System.out.println("");

        if(insert) {
            insertRows(numLoops, connectString);
        } else {
            loadDatabase(numLoops,connectString);
        }
        
    }

    private static void loadDatabase(int numLoops, String connectString) throws SQLException {
        long start = System.currentTimeMillis();
        for(int i=0;i<numLoops;i++) {
            long loopstart = System.currentTimeMillis();
            Connection conn = DriverManager.getConnection(connectString);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("select 1");
            rset.next();
            rset.close();
            stmt.execute("shutdown");            
            conn.close();            
            long loopstop = System.currentTimeMillis();
            System.out.println("Elapsed Time: " + (loopstop-loopstart) + " ms");
        }
        long stop = System.currentTimeMillis();
        System.out.println("Total Time: " + (stop-start) + " ms");
        System.out.println("Average: " + ((stop-start)/numLoops) + " ms");
    }

    private static final Random _random = new Random();
        
    private static void insertRows(int numRows, String connectString) throws SQLException {
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(connectString);
        {
            Statement stmt = conn.createStatement();
            stmt.execute("create table foo ( id int primary key, other int, name varchar(1000) )");
            stmt.close();
        }
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement("insert into foo values ( ?, ?, ? )");
        for(int i=0;i<numRows;i++) {
            pstmt.setInt(1,i);
            pstmt.setInt(2,_random.nextInt());
            pstmt.setString(3,randomString());
            pstmt.execute();
            if(i%1000 == 0) { conn.commit(); }
        }
        pstmt.close();
        conn.commit();
        conn.close();            
        long stop = System.currentTimeMillis();
        System.out.println("Total Time: " + (stop-start) + " ms");
    }
    
    private static final String randomString() {
        switch(_random.nextInt(10)) {
            case 0:
                return "Mauris auctor. Etiam dignissim. Aliquam ipsum quam, varius quis, vestibulum at, elementum nec, arcu. Fusce ipsum dui, dictum vehicula, iaculis non, tempor at, tortor. Nam dignissim. Sed placerat justo vitae ligula. Morbi ac libero. Nam varius pede ac sapien. Ut scelerisque, mi.";
            case 1:
                return "X";
            case 2: 
                return "Nam varius pede ac sapien. Ut scelerisque, mi.";
            case 3: 
                return "Fusce ipsum dui, dictum vehicula, iaculis non, tempor at, tortor.";
            case 4: 
                return "Dignissim, Etiam ";
            case 5: 
                return "Morbi ac libero.";
            case 6: 
                return "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Proin dictum ultricies mauris. Curabitur nec lorem. Duis ultrices semper lacus. Quisque.";
            case 7: 
                return "012-345-678-901-234-567-890-123-456-789-012-345-678-90";
            case 8: 
                return LONG;
            case 9: 
                return VERY_LONG;
            default:
                return "elephant";
                
        }
    }
    
    
    private static String LONG;
    private static String VERY_LONG;
    static {
        StringBuffer bufLong = new StringBuffer();
        StringBuffer bufVery = new StringBuffer();
        for(int i=0;i<255;i++) {
            bufLong.append("x");
            bufVery.append(i);
        }
        LONG = bufLong.toString();
        VERY_LONG = bufVery.toString();
    }
}
