/*
 * $Id: TestBinaryStream.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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
package org.axiondb.functional;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.sql.*;

/**
 * Test the usage of varbinary datatypes.
 * 
 * @author Steven Harris
 */
public class TestBinaryStream extends TestCase {

    // Change this to suite your specific needs
    public static final String DB_URI = "jdbc:axiondb:memdb";

    // SQL text
    public static final String CREATE_TABLE_SQL = "CREATE TABLE MyTable "
        + "( pKey varchar(100) NOT NULL, uValue varchar(100) NOT NULL, vBin varbinary(100) NOT NULL, Primary key (pKey), Unique (uValue) )";

    public static final String INSERT_VALUES_SQL = "INSERT INTO MyTable (pKey, uValue, vBin) VALUES (?, ?, ?)";

    public static final String READ_BINVAL_SQL = "SELECT vBin FROM MyTable";

    public static final String DROP_TABLE_SQL = "DROP TABLE MyTable";

    // instances shared by all tests
    private Connection con;
    private Statement stmt;

    /**
     * Constructor requiring the name of the test to be run.
     * 
     * @param testName the name of the test to be run
     */
    public TestBinaryStream(String testName) {
        super(testName);
    }

    /**
     * Default suite() method discovers all tests.
     */
    public static Test suite() {
        return new TestSuite(TestBinaryStream.class);
    }

    /**
     * Fixture set up here.
     * 
     * @throws Exception Some DB error
     */
    protected void setUp() throws Exception {
        // grab a connection to the database
        Class.forName("org.axiondb.jdbc.AxionDriver");
        con = DriverManager.getConnection(DB_URI);
        stmt = con.createStatement();
    }

    /**
     * Clean up resources.
     * 
     * @throws Exception Some DB error
     */
    protected void tearDown() throws Exception {
        String sqlText = DROP_TABLE_SQL;
        stmt.executeUpdate(sqlText);
        con.close();
    }

    /**
     * setBinaryStream succeeds but getting the same value as a binary stream causes an
     * unexpected exception.
     */
    public void testGetBinaryStream() throws Exception {
        String sqlText = CREATE_TABLE_SQL;
        stmt.executeUpdate(sqlText);

        // insert a record
        String text = "This is some text";
        PreparedStatement ps = con.prepareStatement(INSERT_VALUES_SQL);
        ps.setString(1, "Primary Key #1");
        ps.setString(2, "Unique Value #1");
        byte[] bArray = text.getBytes();
        ByteArrayInputStream bais = new ByteArrayInputStream(bArray);
        ps.setBinaryStream(3, bais, bArray.length);
        ps.executeUpdate();
        //con.commit();

        // read back the binary value
        ResultSet rs = stmt.executeQuery(READ_BINVAL_SQL);
        if (rs.next() == true) {
            try {
                rs.getBinaryStream("vBin");
                rs.getObject("vBin");
            } catch (SQLException e) {
                String message;
                message = "Unexpected exception when getting binary stream\n";
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                message += "Details : " + sw.toString();
                fail(message);
            }
        }
    }

}