/*
 * $Id: TestForElmar.java,v 1.1 2007/11/28 10:01:30 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:30 $
 * @author Rodney Waldhoff
 */
public class TestForElmar extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestForElmar(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestForElmar.class);
    }

    protected File getDatabaseDirectory() {
        return new File("testdb2");
    }    
    
    protected String getConnectString() {
        return "jdbc:axiondb:testdb2:testdb2";
    }
    
    //------------------------------------------------------------------- Tests

    public void testElmarsExample() throws Exception {
        // first, create the database and table
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            stmt.execute("create table foo ( id integer )");
            stmt.execute("insert into foo values ( 0 )");
            stmt.close();
            conn.close();            
        }
        
        // now following Elmar's example:

        // 1) for each table, I create a database connection, execute a query to see if
        // the table exists and close the connection again. [But not the statement]
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("select count(*) from foo");
            assertTrue(rset.next());
            assertEquals(1,rset.getInt(1));
            rset.close();
            // stmt.close(); // note statement not closed
            conn.close();            
        }

        // 2) The application seems to perform fine, no exceptions or other complaints,
        // even when saving to the database and issuing a checkpoint.
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            stmt.execute("insert into foo values ( 1 )");
            stmt.close();
            conn.close();            
        }
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            stmt.execute("insert into foo values ( 2 )");
            // stmt.close(); // note statement not closed
            conn.close();            
        }
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            stmt.execute("insert into foo values ( 3 )");
            stmt.close();
            conn.close();            
        }

        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("select count(*) from foo");
            assertTrue(rset.next());
            assertEquals(4,rset.getInt(1));
            rset.close();
            stmt.close();
            conn.close();            
        }
        
        // 3) Upon shutdown I receive the exception shown above and none of the data
        // produced during the session actually made it to Disk.
        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();            
        }

        {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb2");
            assertNotNull(conn);
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("select count(*) from foo");
            assertTrue(rset.next());
            assertEquals(4,rset.getInt(1));
            rset.close();
            stmt.close();
            conn.close();            
        }
    }

}
