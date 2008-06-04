/*
 * $Id: TestTransactionalLobs.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.types.ByteArrayBlob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 */
public class TestTransactionalLobs extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestTransactionalLobs(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestTransactionalLobs.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testSimpleClobInsert() throws Exception {
        String body = "This is a simple test.";
        createClobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        
        assertEquals(0,selectClobCount(conn));
        insertClob(conn,0,body);
        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));
        conn.commit();
        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));
        
        conn.close();
    }

    public void testSimpleBlobInsert() throws Exception {
        String body = "This is a simple test.";
        createBlobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        
        assertEquals(0,selectBlobCount(conn));
        insertBlob(conn,0,body.getBytes());
        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));
        conn.commit();
        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));
        
        conn.close();
    }

    public void testClobInsertRollback() throws Exception {
        String body = "This is a simple test.";
        createClobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        
        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));

        insertClob(conn,0,body);

        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));
        
        conn.rollback();

        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));
        
        conn.close();
    }

    public void testBlobInsertRollback() throws Exception {
        String body = "This is a simple test.";
        createBlobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);
        
        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));

        insertBlob(conn,0,body.getBytes());

        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));
        
        conn.rollback();

        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));
        
        conn.close();
    }

    public void testClobInsertIsolation() throws Exception {
        String body = "This is a simple test.";
        createClobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));
        
        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));      
        
        insertClob(conn,0,body);
        
        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));

        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));      

        conn2.commit();        

        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));

        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));      
        
        conn.commit();

        assertEquals(1,selectClobCount(conn));
        assertEquals(body,selectClob(conn,0));

        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));                     
        
        conn.close();

        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));
        
        conn2.commit();                     

        assertEquals(1,selectClobCount(conn2));
        assertEquals(body,selectClob(conn2,0));

        conn2.close();
    }

    public void testClobUpdateIsolation() throws Exception {
        String inserted = "This is a simple test.";
        createClobTable(_conn);
        insertClob(_conn,0,inserted);        
        assertEquals(1,selectClobCount(_conn));
        assertEquals(inserted,selectClob(_conn,0));

        assertEquals(1,selectClobCount(_conn));
        assertEquals(inserted,selectClob(_conn,0));
        
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(1,selectClobCount(conn));
        assertEquals(inserted,selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
                
        String updated = "This is the updated contents.";
        
        updateClob(conn,0,updated);

        assertEquals(1,selectClobCount(conn));
        assertEquals(updated,selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
        
        conn2.commit();

        assertEquals(1,selectClobCount(conn));
        assertEquals(updated,selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
        
        conn.commit();

        assertEquals(1,selectClobCount(conn));
        assertEquals(updated,selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));

        conn.close();        

        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));

        conn2.commit();

        assertEquals(1,selectClobCount(conn2));
        assertEquals(updated,selectClob(conn2,0));

        conn2.close();
    }

    public void testClobDeleteIsolation() throws Exception {
        String inserted = "This is a simple test.";
        createClobTable(_conn);
        insertClob(_conn,0,inserted);        
        assertEquals(1,selectClobCount(_conn));
        assertEquals(inserted,selectClob(_conn,0));

        assertEquals(1,selectClobCount(_conn));
        assertEquals(inserted,selectClob(_conn,0));
        
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(1,selectClobCount(conn));
        assertEquals(inserted,selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
                        
        deleteClob(conn,0);

        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
        
        conn2.commit();

        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));
        
        conn.commit();

        assertEquals(0,selectClobCount(conn));
        assertNull(selectClob(conn,0));
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));

        conn.close();        
        
        assertEquals(1,selectClobCount(conn2));
        assertEquals(inserted,selectClob(conn2,0));

        conn2.commit();

        assertEquals(0,selectClobCount(conn2));
        assertNull(selectClob(conn2,0));

        conn2.close();
    }

    public void testBlobInsertIsolation() throws Exception {
        String body = "This is a simple test.";
        createBlobTable(_conn);

        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));
        
        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));      
        
        insertBlob(conn,0,body.getBytes());
        
        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));

        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));      

        conn2.commit();        

        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));

        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));      
        
        conn.commit();

        assertEquals(1,selectBlobCount(conn));
        assertEquals(body,new String(selectBlob(conn,0)));

        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));                     
        
        conn.close();

        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));
        
        conn2.commit();                     

        assertEquals(1,selectBlobCount(conn2));
        assertEquals(body,new String(selectBlob(conn2,0)));

        conn2.close();
    }

    public void testBlobUpdateIsolation() throws Exception {
        String inserted = "This is a simple test.";
        createBlobTable(_conn);
        insertBlob(_conn,0,inserted.getBytes());        
        assertEquals(1,selectBlobCount(_conn));
        assertEquals(inserted,new String(selectBlob(_conn,0)));

        assertEquals(1,selectBlobCount(_conn));
        assertEquals(inserted,new String(selectBlob(_conn,0)));
        
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(1,selectBlobCount(conn));
        assertEquals(inserted,new String(selectBlob(conn,0)));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
                
        String updated = "This is the updated contents.";
        
        updateBlob(conn,0,updated.getBytes());

        assertEquals(1,selectBlobCount(conn));
        assertEquals(updated,new String(selectBlob(conn,0)));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
        
        conn2.commit();

        assertEquals(1,selectBlobCount(conn));
        assertEquals(updated,new String(selectBlob(conn,0)));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
        
        conn.commit();

        assertEquals(1,selectBlobCount(conn));
        assertEquals(updated,new String(selectBlob(conn,0)));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));

        conn.close();        

        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));

        conn2.commit();

        assertEquals(1,selectBlobCount(conn2));
        assertEquals(updated,new String(selectBlob(conn2,0)));

        conn2.close();
    }

    public void testBlobDeleteIsolation() throws Exception {
        String inserted = "This is a simple test.";
        createBlobTable(_conn);
        insertBlob(_conn,0,inserted.getBytes());        
        assertEquals(1,selectBlobCount(_conn));
        assertEquals(inserted,new String(selectBlob(_conn,0)));

        assertEquals(1,selectBlobCount(_conn));
        assertEquals(inserted,new String(selectBlob(_conn,0)));
        
        Connection conn = DriverManager.getConnection(getConnectString());
        conn.setAutoCommit(false);

        Connection conn2 = DriverManager.getConnection(getConnectString());
        conn2.setAutoCommit(false);
        
        assertEquals(1,selectBlobCount(conn));
        assertEquals(inserted,new String(selectBlob(conn,0)));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
                        
        deleteBlob(conn,0);

        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
        
        conn2.commit();

        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));
        
        conn.commit();

        assertEquals(0,selectBlobCount(conn));
        assertNull(selectBlob(conn,0));
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));

        conn.close();        
        
        assertEquals(1,selectBlobCount(conn2));
        assertEquals(inserted,new String(selectBlob(conn2,0)));

        conn2.commit();

        assertEquals(0,selectBlobCount(conn2));
        assertNull(selectBlob(conn2,0));

        conn2.close();
    }

    //------------------------------------------------------------------- Utils

    private void createClobTable(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("create table CLOB_TBL ( ID integer, BODY compressedclob )");
        stmt.close();
    }

    private void createBlobTable(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("create table BLOB_TBL ( ID integer, BODY compressedblob )");
        stmt.close();
    }

    private void insertClob(Connection conn, int id, String body) throws Exception {
        Statement stmt = conn.createStatement();
        assertEquals(1,stmt.executeUpdate("insert into CLOB_TBL values ( " + id + ", '" + body + "' )"));
/*
        // TODO: this style of clob writing doesn't work transactionally right now, but the inline style does
        assertEquals(1,stmt.executeUpdate("insert into CLOB_TBL values ( " + id + ", 'newlob()' )"));
        ResultSet rset = stmt.executeQuery("select BODY from CLOB_TBL where ID = " + id);
        assertTrue(rset.next());
        Clob clob = rset.getClob(1);
        Writer out = clob.setCharacterStream(0);
        out.write(body);
        out.close();
        rset.close();
*/        
        stmt.close();
    }

    private void insertBlob(Connection conn, int id, byte[] body) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("insert into BLOB_TBL values ( ?, ? )");
        stmt.setInt(1,id);
        stmt.setBlob(2, new ByteArrayBlob(body));
        assertEquals(1,stmt.executeUpdate());
        stmt.close();
    }

    private void updateClob(Connection conn, int id, String body) throws Exception {
        Statement stmt = conn.createStatement();        
        assertEquals(1,stmt.executeUpdate("update CLOB_TBL set BODY = '" + body + "' where ID = " + id));
/*        
        // TODO: this style of clob writing doesn't work transactionally right now, but the inline style does
        ResultSet rset = stmt.executeQuery("select BODY from CLOB_TBL where ID = " + id);
        assertTrue(rset.next());
        Clob clob = rset.getClob(1);
        Writer out = clob.setCharacterStream(0);
        out.write(body);
        out.close();
        rset.close();
*/        
        stmt.close();
    }

    private void updateBlob(Connection conn, int id, byte[] body) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("update BLOB_TBL set BODY = ? where ID = ?");
        stmt.setBlob(1, new ByteArrayBlob(body));
        stmt.setInt(2,id);
        assertEquals(1,stmt.executeUpdate());
        stmt.close();
    }

    private void deleteClob(Connection conn, int id) throws Exception {
        Statement stmt = conn.createStatement();
        assertEquals(1,stmt.executeUpdate("delete from CLOB_TBL where ID = " + id));
        stmt.close();
    }

    private void deleteBlob(Connection conn, int id) throws Exception {
        Statement stmt = conn.createStatement();
        assertEquals(1,stmt.executeUpdate("delete from BLOB_TBL where ID = " + id));
        stmt.close();
    }

    private String selectClob(Connection conn, int id) throws Exception {
        Statement stmt = conn.createStatement();
        String result = null;
        ResultSet rset = stmt.executeQuery("select BODY from CLOB_TBL where ID = " + id);
        if (rset.next()) {
            result = rset.getString(1);
        }
        rset.close();
        stmt.close();
        return result;
    }

    private byte[] selectBlob(Connection conn, int id) throws Exception {
        Statement stmt = conn.createStatement();
        byte[] result = null;
        ResultSet rset = stmt.executeQuery("select BODY from BLOB_TBL where ID = " + id);
        if (rset.next()) {
            ByteArrayOutputStream read = new ByteArrayOutputStream();
            Blob blob = rset.getBlob(1);
            if(null != blob) {
                InputStream in = blob.getBinaryStream();
                for(int b = in.read(); b != -1; b = in.read()) {
                    read.write((byte)b);
                }
                in.close();
                result = read.toByteArray();
            }
        }
        rset.close();
        stmt.close();
        return result;
    }

    private int selectClobCount(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        int result = -1;
        ResultSet rset = stmt.executeQuery("select count(*) from CLOB_TBL");
        assertTrue(rset.next());
        result = rset.getInt(1);
        rset.close();
        stmt.close();
        return result;
    }

    private int selectBlobCount(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        int result = -1;
        ResultSet rset = stmt.executeQuery("select count(*) from BLOB_TBL");
        assertTrue(rset.next());
        result = rset.getInt(1);
        rset.close();
        stmt.close();
        return result;
    }


}
