/*
 * $Id: ExternalTableInsertSelect.java,v 1.1 2007/11/28 10:01:38 jawed Exp $
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
import java.sql.Statement;

/**
 */
public class ExternalTableInsertSelect {

    static {
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        {
            long start = System.currentTimeMillis();
            testDirectInsert();
            long stop = System.currentTimeMillis();
            System.out.println("Total time for testDirectInsert: " + (stop - start));
        }
        
        {
            long start = System.currentTimeMillis();
            testInsertSelectUsingExternalDbTable();
            long stop = System.currentTimeMillis();
            System.out.println("Total time for testInsertSelectUsingExternalDbTable: " + (stop - start));
        }
    }
    
    private static void testInsertSelectUsingExternalDbTable() throws Exception{
        String connectString = "jdbc:axiondb:test";
        String createdblink = "CREATE DATABASE LINK FLATFILEDB ( DRIVER='org.axiondb.jdbc.AxionDriver' URL='jdbc:axiondb:testdb:C:\\otdFLATFILEDB_1119047797165' USERNAME='sa' PASSWORD='sa')";
        String truncateRemoteTarget = "TRUNCATE TABLE T1_PQ_EMP_DET";
        String createRemoteTarget = "CREATE EXTERNAL TABLE IF NOT EXISTS T1_PQ_EMP_DET (EMP_ID numeric(10) NULL, FNAME varchar(100) NULL, LNAME varchar(100) NULL, NAME varchar(100) NULL, COMPANY varchar(100) NULL, SSN varchar(100) NULL, ADDRESS1 varchar(100) NULL, ADDRESS2 varchar(100) NULL, ADDRESS3 varchar(100) NULL, POSTAL_CODE varchar(100) NULL, LAST_UPDATED varchar(100) NULL, STR_TS varchar(100) NULL, NUM_MISC varchar(10) NULL, NUM_YEAR varchar(10) NULL, NUM_MONTH varchar(10) NULL, NUM_DAY_OF_MONTH varchar(10) NULL, NUM_HOUR varchar(10) NULL, NUM_MIN varchar(10) NULL, NUM_SEC varchar(10) NULL, NUM_SHORT_ZIP varchar(10) NULL ) ORGANIZATION ( LOADTYPE='REMOTE' VENDOR='AXION' DBLINK='FLATFILEDB' REMOTETABLE='PQ_EMP_DET' )";
        String createRemoteSource = "CREATE EXTERNAL TABLE IF NOT EXISTS S1_PQ_EMPLOYEE ( EMP_ID numeric(10) NULL, FNAME varchar(100) NULL, LNAME varchar(100) NULL, NAME_LF varchar(100) NULL, NAME_FL varchar(100) NULL, COMPANY varchar(100) NULL, SSN varchar(100) NULL, LAST_UPD varchar(100) NULL, STR_TS_ISO varchar(100) NULL, STR_TS_UK varchar(100) NULL, STR_TS_US varchar(100) NULL ) ORGANIZATION ( LOADTYPE='REMOTE' VENDOR='AXION' DBLINK='FLATFILEDB' REMOTETABLE='PQ_EMPLOYEE' )";
        String insertSelect = "INSERT INTO T1_PQ_EMP_DET ( EMP_ID, FNAME, LNAME, COMPANY, SSN, LAST_UPDATED ) SELECT S1.EMP_ID AS s_column1, S1.FNAME AS s_column2, S1.LNAME AS s_column3, S1.COMPANY AS s_column4, S1.SSN AS s_column5, S1.LAST_UPD AS s_column6 FROM S1_PQ_EMPLOYEE S1";

        System.out.println("Started");
        setup();

        Connection conn = DriverManager.getConnection(connectString);
        conn.setAutoCommit(false);
        
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createdblink);
        
        stmt.executeUpdate(createRemoteSource);
        stmt.executeUpdate(createRemoteTarget);
        stmt.executeUpdate(truncateRemoteTarget);
        
        stmt.executeUpdate(insertSelect);
        
        System.out.println("Done");
        
        conn.commit();
        conn.close();
        
        System.out.println("Done Commit");
        
    }
    
    private static void testDirectInsert() throws Exception{
        String connectString = "jdbc:axiondb:testdb:C:\\otdFLATFILEDB_1119047797165";
        String insertSelect = "INSERT INTO PQ_EMP_DET ( EMP_ID, FNAME, LNAME, COMPANY, SSN, LAST_UPDATED ) SELECT S1.EMP_ID AS s_column1, S1.FNAME AS s_column2, S1.LNAME AS s_column3, S1.COMPANY AS s_column4, S1.SSN AS s_column5, S1.LAST_UPD AS s_column6 FROM PQ_EMPLOYEE S1";

        System.out.println("Started");

        setup();
        
        Connection conn = DriverManager.getConnection(connectString);
        conn.setAutoCommit(false);
        
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(insertSelect);
        
        System.out.println("Done");
        
        conn.commit();
        conn.close();
        
        System.out.println("Done Commit");
    }
    
    private static void setup() throws Exception{
        String connectString = "jdbc:axiondb:testdb:C:\\otdFLATFILEDB_1119047797165";
        String dropTarget = "DROP TABLE IF EXISTS PQ_EMP_DET";
        String truncateTarget = "TRUNCATE TABLE PQ_EMP_DET";
        String createTarget = "CREATE EXTERNAL TABLE IF NOT EXISTS PQ_EMP_DET (EMP_ID numeric(10) NULL, FNAME varchar(100) NULL, LNAME varchar(100) NULL, NAME varchar(100) NULL, COMPANY varchar(100) NULL, SSN varchar(100) NULL, ADDRESS1 varchar(100) NULL, ADDRESS2 varchar(100) NULL, ADDRESS3 varchar(100) NULL, POSTAL_CODE varchar(100) NULL, LAST_UPDATED varchar(100) NULL, STR_TS varchar(100) NULL, NUM_MISC varchar(10) NULL, NUM_YEAR varchar(10) NULL, NUM_MONTH varchar(10) NULL, NUM_DAY_OF_MONTH varchar(10) NULL, NUM_HOUR varchar(10) NULL, NUM_MIN varchar(10) NULL, NUM_SEC varchar(10) NULL, NUM_SHORT_ZIP varchar(10) NULL ) ORGANIZATION ( LOADTYPE='delimited' )";
      
        String dropSource = "DROP TABLE IF EXISTS PQ_EMPLOYEE";
        String createSource = "CREATE EXTERNAL TABLE IF NOT EXISTS PQ_EMPLOYEE ( EMP_ID numeric(10) NULL, FNAME varchar(100) NULL, LNAME varchar(100) NULL, NAME_LF varchar(100) NULL, NAME_FL varchar(100) NULL, COMPANY varchar(100) NULL, SSN varchar(100) NULL, LAST_UPD varchar(100) NULL, STR_TS_ISO varchar(100) NULL, STR_TS_UK varchar(100) NULL, STR_TS_US varchar(100) NULL ) ORGANIZATION ( LOADTYPE='delimited')";

        System.out.println("In SetUp...");

        Connection conn = DriverManager.getConnection(connectString);
        conn.setAutoCommit(false);
        
        Statement stmt = conn.createStatement();
        
        stmt.executeUpdate(dropSource);
        stmt.executeUpdate(createSource);
        
        stmt.executeUpdate(dropTarget);
        stmt.executeUpdate(createTarget);
        stmt.executeUpdate(truncateTarget);

        conn.commit();
        conn.close();
        System.out.println("Done SetUp...");
    }

}
