/*
 * $Id: TestTransactions.java,v 1.2 2007/12/13 10:43:52 jawed Exp $
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.HashBag;

/**
 * @version $Revision: 1.2 $ $Date: 2007/12/13 10:43:52 $
 * @author Rodney Waldhoff
 */
public class TestTransactions extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestTransactions(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestTransactions.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testBug() throws Exception {
        createTableFoo();
        populateTableFoo();
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);
        {
            PreparedStatement stmt = null;
            try {
                stmt = cone.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ? )");
                for(int i=0;i<1;i++) {
                    int num = _random.nextInt(100);
                    stmt.setInt(1,num);
                    stmt.setString(2,String.valueOf(num));
                    stmt.setInt(3,num/2);
                    assertEquals(1,stmt.executeUpdate());
                }
            } finally {
                try { stmt.close(); } catch(Exception t) { }         
            }
        }
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            try {
                stmt = cone.prepareStatement("select NUM, STR from FOO where NUM > ? and NUM < ?");
                stmt.setInt(1,_random.nextInt(10));
                stmt.setInt(2,_random.nextInt(100)+10);
                rset = stmt.executeQuery();
                while(rset.next()) {
                    int num = rset.getInt(1);
                    String str = rset.getString(2);
                    assertEquals(String.valueOf(num),str);
                }
            } finally {
                try { rset.close(); } catch(Exception t) { }
                try { stmt.close(); } catch(Exception t) { }
            }
        }
        cone.close();
    }

    public void testDeleteUpdatedRowBug() throws Exception {
        createTableFoo();
        populateTableFoo();
        
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        // update a row
        {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("update FOO set NUM = ?, STR = ?, NUMTWO = ? where NUM = ?");
                stmt.setInt(1,1000);
                stmt.setString(2,String.valueOf(1000));
                stmt.setInt(3,1000/2);
                stmt.setInt(4,2);
                assertEquals(1,stmt.executeUpdate());
            } finally {
                try { stmt.close(); } catch(Exception t) { }
            }
        }
        // now delete the updated row
        {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("delete from FOO where NUM between ? and ?");
                stmt.setInt(1,999);
                stmt.setInt(2,1001);
                assertEquals(1,stmt.executeUpdate());
            } finally {
                try { stmt.close(); } catch(Exception t) { }
            }
        }
        conn.commit();
        conn.close();
    }

    public void testInsertIsolation() throws Exception {
        createTableFoo();
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
        populateTableFoo();
        
        // create a transactional connection
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);

        // insert into it       
        {
            PreparedStatement stmt = cone.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ? )");                
            for(int i=NUM_ROWS_IN_FOO+10;i<NUM_ROWS_IN_FOO+20;i++) {
                stmt.setInt(1,i);
                stmt.setString(2,String.valueOf(i));
                stmt.setInt(3,i/2);
                assertEquals(1,stmt.executeUpdate());
            }
            stmt.close();
        }
        // we should see those rows in c1       
        {
            PreparedStatement stmt = cone.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            
            HashBag expected = new HashBag();
            HashBag found = new HashBag();
            for(int i=NUM_ROWS_IN_FOO+10;i<NUM_ROWS_IN_FOO+20;i++) {                    
                assertTrue(rset.next());
                String value = rset.getString(2);
                assertNotNull(value);
                assertTrue(!found.contains(value));
                expected.add(String.valueOf(i));
                found.add(value);
            }
            assertTrue(!rset.next());
            assertEquals(expected,found);
            rset.close();
            stmt.close();
        }
        
        // but not in a new connection, because they haven't been committed yet
        Connection ctwo = DriverManager.getConnection(getConnectString());
        ctwo.setAutoCommit(false);
        {
            PreparedStatement stmt = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // but if we commit c1
        cone.commit();
        
        // we still see the rows in c1
        {
            PreparedStatement stmt = cone.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            
            HashBag expected = new HashBag();
            HashBag found = new HashBag();
            for(int i=NUM_ROWS_IN_FOO+10;i<NUM_ROWS_IN_FOO+20;i++) {                    
                assertTrue(rset.next());
                String value = rset.getString(2);
                assertNotNull(value);
                assertTrue(!found.contains(value));
                expected.add(String.valueOf(i));
                found.add(value);
            }
            assertTrue(!rset.next());
            assertEquals(expected,found);
            rset.close();
            stmt.close();
        }

        // and we see the rows in a newly created connection 
        Connection cthree = DriverManager.getConnection(getConnectString());
        cthree.setAutoCommit(false);
        {
            PreparedStatement stmt = cthree.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            
            HashBag expected = new HashBag();
            HashBag found = new HashBag();
            for(int i=NUM_ROWS_IN_FOO+10;i<NUM_ROWS_IN_FOO+20;i++) {                    
                assertTrue(rset.next());
                String value = rset.getString(2);
                assertNotNull(value);
                assertTrue(!found.contains(value));
                expected.add(String.valueOf(i));
                found.add(value);
            }
            assertTrue(!rset.next());
            assertEquals(expected,found);
            rset.close();
            stmt.close();
        }
        cthree.close();
        
        // but not in c2, since it was opened before c1 was committed
        {
            PreparedStatement stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // now commit c2:
        ctwo.commit();
        // and we should see the inserted rows
        {
            PreparedStatement stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM >= ? and NUM < ?");
            stmt.setInt(1,NUM_ROWS_IN_FOO+10);
            stmt.setInt(2,NUM_ROWS_IN_FOO+20);
            ResultSet rset = stmt.executeQuery();
            
            HashBag expected = new HashBag();
            HashBag found = new HashBag();
            for(int i=NUM_ROWS_IN_FOO+10;i<NUM_ROWS_IN_FOO+20;i++) {                    
                assertTrue(rset.next());
                String value = rset.getString(2);
                assertNotNull(value);
                assertTrue(!found.contains(value));
                expected.add(String.valueOf(i));
                found.add(value);
            }
            assertTrue(!rset.next());
            assertEquals(expected,found);
            rset.close();
            stmt.close();
        }
        ctwo.close();        
        cone.close();
    }

    public void testUpdateIsolation() throws Exception {
        createTableFoo();
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
        populateTableFoo();
        
        // create a transactional connection
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);

        // update using it       
        {
            PreparedStatement stmt = null;
            stmt = cone.prepareStatement("update FOO set NUM = ? where NUM = ?");                
            stmt.setInt(1,1000);
            stmt.setInt(2,2);
            assertEquals(1,stmt.executeUpdate());
            stmt.close();
        }
        
        // we should see the update in c1       
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cone.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // but not in a new connection, because they haven't been committed yet
        Connection ctwo = DriverManager.getConnection(getConnectString());
        ctwo.setAutoCommit(false);

        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        
        // but if we commit c1
        cone.commit();
        
        // we still see the update in c1
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cone.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // and in a newly created connection 
        Connection cthree = DriverManager.getConnection(getConnectString());
        cthree.setAutoCommit(false);
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cthree.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        cthree.close();
        
        // but not in c2, since it was opened before c1 was committed
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        
        // now commit c2:
        ctwo.commit();

        // and we should see the updated row
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,1000);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        ctwo.close();        
        cone.close();
    }

    public void testDeleteIsolation() throws Exception {
        createTableFoo();
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
        populateTableFoo();
        
        // create a transactional connection
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);

        // update using it       
        {
            PreparedStatement stmt = null;
            stmt = cone.prepareStatement("delete from FOO where NUM = ?");                
            stmt.setInt(1,2);
            assertEquals(1,stmt.executeUpdate());
            stmt.close();
        }
        
        // we should not see the row in c1
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cone.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // but we should still see it in a new connection, because they c1 hasn't been committed yet
        Connection ctwo = DriverManager.getConnection(getConnectString());
        ctwo.setAutoCommit(false);

        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // but if we commit c1
        cone.commit();
        
        // we still see delete in c1
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cone.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        // and in a newly created connection 
        Connection cthree = DriverManager.getConnection(getConnectString());
        cthree.setAutoCommit(false);
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = cthree.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        cthree.close();
        
        // but not in c2, since it was opened before c1 was committed
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(rset.next());
            assertEquals("2",rset.getString(2));
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        
        // now commit c2:
        ctwo.commit();

        // and we should see the deletion
        {
            PreparedStatement stmt = null;
            ResultSet rset = null;
            stmt = ctwo.prepareStatement("select NUM, STR from FOO where NUM = ?");
            stmt.setInt(1,2);
            rset = stmt.executeQuery();
            assertTrue(!rset.next());
            rset.close();
            stmt.close();
        }
        
        ctwo.close();        
        cone.close();
    }
//Commented on 13-Dec-2007
//    public void testInterleavedWithAutoCommit() throws Exception {
//        createTableFoo();
//        populateTableFoo();
//        
//        Connection cone = DriverManager.getConnection(getConnectString());
//        Connection ctwo = DriverManager.getConnection(getConnectString());
//        interleaved(cone,ctwo);
//        ctwo.close();
//        cone.close();
//    }

    public void testInterleavedWithoutAutoCommit() throws Exception {
        createTableFoo();
        populateTableFoo();
        
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);
        Connection ctwo = DriverManager.getConnection(getConnectString());
        ctwo.setAutoCommit(false);
        interleaved(cone,ctwo);
        ctwo.commit();
        ctwo.close();
        try {
            cone.commit();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try { cone.close(); } catch(Exception e) {}
    }
//    Commented on 13-Dec-2007
//    public void testIndexedInterleavedWithAutoCommit() throws Exception {
//        createTableFoo();
//        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
//        populateTableFoo();        
//        
//        Connection cone = DriverManager.getConnection(getConnectString());
//        Connection ctwo = DriverManager.getConnection(getConnectString());
//        interleaved(cone,ctwo);
//        ctwo.close();
//        cone.close();
//    }

    public void testIndexedInterleavedWithoutAutoCommit() throws Exception {
        createTableFoo();
        _stmt.execute("create index FOO_NUM_NDX on FOO ( NUM )");
        populateTableFoo();
        
        Connection cone = DriverManager.getConnection(getConnectString());
        cone.setAutoCommit(false);
        Connection ctwo = DriverManager.getConnection(getConnectString());
        ctwo.setAutoCommit(false);
        interleaved(cone,ctwo);
        ctwo.close();
        cone.close();
    }
    
    private void interleaved(Connection cone, Connection ctwo) throws Exception {
        select(cone);
        select(ctwo);
        insert(cone);
        insert(ctwo);
        select(cone);
        select(ctwo);
        update(cone);
        update(ctwo);
        select(cone);
        select(ctwo);
        delete(cone);
        delete(ctwo);
        select(cone);
        select(ctwo);
    }
    
    private void select(Connection conn) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            stmt = conn.prepareStatement("select NUM, STR from FOO where NUM > ? and NUM < ?");
            stmt.setInt(1,_random.nextInt(10));
            stmt.setInt(2,_random.nextInt(100)+10);
            rset = stmt.executeQuery();
            while(rset.next()) {
                int num = rset.getInt(1);
                String str = rset.getString(2);
                if(! str.equals(String.valueOf(num))) {
                    throw new RuntimeException(str + " != " + num);
                }
            }
        } finally {
            try { rset.close(); } catch(Exception t) { }
            try { stmt.close(); } catch(Exception t) { }
        }
    }
    
    private void insert(Connection conn) throws Exception {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("insert into FOO ( NUM, STR, NUMTWO ) values ( ?, ?, ? )");
            for(int i=0;i<5;i++) {
                int num = _random.nextInt(100);
                stmt.setInt(1,num);
                stmt.setString(2,String.valueOf(num));
                stmt.setInt(3,num/2);
                if(1 != stmt.executeUpdate()) {
                    throw new RuntimeException("Expected 1");
                }
            }
        } finally {
            try { stmt.close(); } catch(Exception t) { }
        }
    }
    
    private void update(Connection conn) throws Exception {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("update FOO set NUM = ?, STR = ?, NUMTWO = ? where NUM = ? or NUMTWO = ?");
            for(int i=0;i<5;i++) {
                int num = _random.nextInt(100);
                stmt.setInt(1,num);
                stmt.setString(2,String.valueOf(num));
                stmt.setInt(3,num/2);
                stmt.setInt(4,_random.nextInt(100));
                stmt.setInt(5,_random.nextInt(100));
                stmt.executeUpdate();
            }
        } finally {
            try { stmt.close(); } catch(Exception t) { }
        }
    }
    
    private void delete(Connection conn) throws Exception {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("delete from FOO where NUM between ? and ?");
            for(int i=0;i<5;i++) {
                int num = _random.nextInt(100);
                stmt.setInt(1,num);
                stmt.setInt(2,num+_random.nextInt(10));
                stmt.executeUpdate();
            }
        } finally {
            try { stmt.close(); } catch(Exception t) { }
        }
    }
    
    private Random _random = new Random();
}
