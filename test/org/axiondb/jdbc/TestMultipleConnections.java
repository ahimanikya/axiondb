/*
 * $Id: TestMultipleConnections.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.HashBag;
import org.axiondb.Database;
import org.axiondb.jdbc.AxionConnection;


/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rodney Waldhoff
 */
public class TestMultipleConnections extends TestCase {
    public TestMultipleConnections(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMultipleConnections.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private String _name = "testdb";
    private String _connectString = "jdbc:axiondb:diskdb:testdb";

    public void setUp() throws Exception {
        Class.forName("org.axiondb.jdbc.AxionDriver");
    }

    public void tearDown() throws Exception {
        {
            Connection conn = DriverManager.getConnection(_connectString);
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
        deleteFile(new File(_name));
    }

    private boolean deleteFile(File file) throws Exception {
        if(file.exists()) {
            if(file.isDirectory()) {
                File[] files = file.listFiles();
                for(int i = 0; i < files.length; i++) {
                    deleteFile(files[i]);
                }
            }
            if(!file.delete()) {
                return false;
            }
            return true;
        }
        return true;
    }

    //------------------------------------------------------------------- Tests

    public void testSameDatabase() throws Exception {
        AxionConnection conn1 = (AxionConnection)(DriverManager.getConnection(_connectString));
        AxionConnection conn2 = (AxionConnection)(DriverManager.getConnection(_connectString));
        assertSame(conn1.getDatabase(),conn2.getDatabase());
    }

    public void testSameDatabase2() throws Exception {
        AxionConnection conn1 = (AxionConnection)(DriverManager.getConnection(_connectString));
        Database db = conn1.getDatabase();
        conn1.close();
        AxionConnection conn2 = (AxionConnection)(DriverManager.getConnection(_connectString));
        assertSame(db,conn2.getDatabase());
    }

    public void testCreateOneSelectAnother() throws Exception {
        Connection conn = DriverManager.getConnection(_connectString);
        Connection conn2 = DriverManager.getConnection(_connectString);
        
        Statement stmt = conn.createStatement();
        createTableFoo(stmt);
        populateTableFoo(stmt);
        stmt.close();

        stmt = conn2.createStatement();
        selectFromFoo(stmt);
        stmt.close();
        conn.close();
        conn2.close();
    }

    //-------------------------------------------------------------------- Util

    private void createTableFoo(Statement stmt) throws Exception {
        stmt.execute("create table FOO ( NUM integer, STR varchar2(25), NUMTWO integer )");
        createIndexOnFoo(stmt);
    }

    private void createIndexOnFoo(Statement stmt) throws Exception {
        stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
    }

    private void populateTableFoo(Statement stmt) throws Exception {
        for(int i=0;i<30;i++) {
            stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( " + i + ", '" + i + "', " + (i/2) + ")");
        }
    }

    private void selectFromFoo(Statement stmt) throws Exception {
        ResultSet rset = stmt.executeQuery("select STR from FOO");
        assertNotNull("Should have been able to create ResultSet",rset);

        Bag expected = new HashBag();
        Bag found = new HashBag();

        for(int i=0;i<30;i++) {
            assertTrue("ResultSet should contain more rows",rset.next());
            expected.add(String.valueOf(i));
            String val = rset.getString(1);
            assertNotNull("Returned String should not be null",val);
            assertTrue("ResultSet shouldn't think value was null",!rset.wasNull());
            assertTrue("Shouldn't have seen \"" + val + "\" yet",!found.contains(val));
            found.add(val);
        }
        assertTrue("ResultSet shouldn't have any more rows",!rset.next());
        rset.close();
        assertEquals(expected,found);
    }

}
