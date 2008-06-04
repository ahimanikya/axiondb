/*
 * $Id: AbstractBlobTest.java,v 1.2 2007/12/11 16:22:56 jawed Exp $
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.TestCase;

import org.axiondb.jdbc.AxionBlob;
import org.axiondb.util.Base64;

/**
 * @version $Revision: 1.2 $ $Date: 2007/12/11 16:22:56 $
 * @author Rodney Waldhoff 
 */
public abstract class AbstractBlobTest extends TestCase {
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;
    private String _axiondbLobdir = null;

    public AbstractBlobTest(String s) {
        super(s);
    }

    public abstract String getConnectString();

    public void setUp() throws Exception {
        _axiondbLobdir = System.getProperty("axiondb.lobdir");
        System.setProperty("axiondb.lobdir","testdb");

        Class.forName("org.axiondb.jdbc.AxionDriver");

        _conn = DriverManager.getConnection(getConnectString());
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        try { _rset.close(); } catch(Exception t) { }
        try { _stmt.close(); } catch(Exception t) { }
        try { _conn.close(); } catch(Exception t) { }
        _rset = null;
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection(getConnectString());
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
        deleteFile(new File("testdb"));
        if(null == _axiondbLobdir) {
            System.getProperties().remove("axiondb.lobdir");
        } else {
            System.setProperty("axiondb.lobdir",_axiondbLobdir);
        }
    }

    protected boolean deleteFile(File file) throws Exception {
        if(file.exists()) {
            if(file.isDirectory()) {
                File[] files = file.listFiles();
                for(int i = 0; i < files.length; i++) {
                    deleteFile(files[i]);
                }
            }
            return file.delete();
        }
        return true;
    }

    private void createBlobTable(boolean compressed) throws Exception {
        if(compressed) {
            _stmt.execute("create table FOO ( ID integer, BODY compressedblob )");
        } else {
            _stmt.execute("create table FOO ( ID integer, BODY blob )");
        }
    }

    public void testSetBlobAsBinaryStream() throws Exception {
        updateAndSelectBlobAsBinaryStream(false);
    }

    public void testSetCompressedBlobAsBinaryStream() throws Exception {
        updateAndSelectBlobAsBinaryStream(true);
    }

    public void testSetBlobAsByteArray() throws Exception {
        updateAndSelectBlobAsByteArray(false);
    }

    public void testSetCompressedBlobAsByteArray() throws Exception {
        updateAndSelectBlobAsByteArray(true);
    }

    public void testSetBlobViaBase64() throws Exception {
        updateAndSelectBlobViaBase64Encoding(false);
    }

    public void testSetCompressedBlobViaBase64() throws Exception {
        updateAndSelectBlobViaBase64Encoding(true);
    }

    public void testInsertEmptyBlob() throws Exception {
        createBlobTable(false);
        insertEmptyBlob();
    }

    public void testInsertEmptyCompressedBlob() throws Exception {
        createBlobTable(true);
        insertEmptyBlob();
    }

    public void testGetBinaryStream() throws Exception {
        createBlobTable(false);
        _stmt.execute("insert into FOO ( ID, BODY ) values ( 0, BASE64DECODE('VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQ=') )");
        _rset = _stmt.executeQuery("select ID, BODY from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNotNull(_rset.getBinaryStream(2));
        assertNotNull(_rset.getBinaryStream("body"));
    }

    public void testGetBinaryStreamWhenNull() throws Exception {
        createBlobTable(false);
        _stmt.execute("insert into FOO ( ID, BODY ) values ( 0, NULL )");
        _rset = _stmt.executeQuery("select ID, BODY from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNull(_rset.getBinaryStream(2));
        assertNull(_rset.getBinaryStream("body"));
    }

    private void insertEmptyBlob() throws Exception {
        _stmt.execute("insert into FOO ( ID, BODY ) values ( 0, 'newlob()' )");
        _rset = _stmt.executeQuery("select ID, BODY from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNotNull(_rset.getBlob(2));
        assertNotNull(_rset.getBlob("body"));
        assertTrue(_rset.getBlob(2) instanceof AxionBlob);
    }

    public void testWriteToEmptyBlob() throws Exception {
        createBlobTable(false);
        writeToEmptyBlob();
    }

    public void testWriteToEmptyCompressedBlob() throws Exception {
        createBlobTable(true);
        writeToEmptyBlob();
    }

    private void writeToEmptyBlob() throws Exception {
        {
            _stmt.execute("insert into FOO ( ID, BODY ) values ( 0, 'newlob()' )");
        }
        {
            _rset = _stmt.executeQuery("select ID, BODY from FOO");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            assertEquals(0,_rset.getInt(1));
            assertNotNull(_rset.getBlob(2));
            assertTrue(_rset.getBlob(2) instanceof AxionBlob);
            AxionBlob blob = (AxionBlob)(_rset.getBlob(2));
            OutputStream out = blob.setBinaryStream(0L);
            for(int i=0;i<10;i++) {
                out.write("The quick brown fox jumped over the lazy dogs. ".getBytes());
            }
            out.close();
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select ID, BODY from FOO");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            assertEquals(0,_rset.getInt(1));
            assertNotNull(_rset.getBlob(2));
            assertTrue(_rset.getBlob(2) instanceof AxionBlob);
            AxionBlob blob = (AxionBlob)(_rset.getBlob(2));
            InputStream in = blob.getBinaryStream();
            ByteArrayOutputStream read = new ByteArrayOutputStream();
            for(int c=in.read();c!=-1;c=in.read()) {
                read.write(c);
            }
            in.close();
            _rset.close();
            StringBuffer expected = new StringBuffer();
            for(int i=0;i<10;i++) {
                expected.append("The quick brown fox jumped over the lazy dogs. ");
            }
            String actual = new String(read.toByteArray());
            assertEquals(expected.toString(),actual);
        }
    }

    public void testWriteToNonEmptyBlob() throws Exception {
        createBlobTable(false);
        writeToNonEmptyBlob();
    }

    public void testWriteToNonEmptyCompressedBlob() throws Exception {
        createBlobTable(true);
        writeToNonEmptyBlob();
    }

    private void writeToNonEmptyBlob() throws Exception {
        writeToEmptyBlob();
        {
            _rset = _stmt.executeQuery("select ID, BODY from FOO");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            assertEquals(0,_rset.getInt(1));
            assertNotNull(_rset.getClob(2));
            assertTrue(_rset.getBlob(2) instanceof AxionBlob);
            AxionBlob blob = (AxionBlob)(_rset.getBlob(2));
            blob.truncate(0);
            OutputStream out = blob.setBinaryStream(0L);
            out.write("It was the best of times, it was the worst of times.".getBytes());
            out.close();
            _rset.close();
        }
        {
            _rset = _stmt.executeQuery("select ID, BODY from FOO");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            assertEquals(0,_rset.getInt(1));
            assertNotNull(_rset.getBlob(2));
            assertTrue(_rset.getBlob(2) instanceof AxionBlob);
            AxionBlob blob = (AxionBlob)(_rset.getBlob(2));
            InputStream in = blob.getBinaryStream();
            ByteArrayOutputStream read = new ByteArrayOutputStream();
            for(int c=in.read();c!=-1;c=in.read()) {
                read.write(c);
            }
            in.close();
            _rset.close();
            String expected = "It was the best of times, it was the worst of times.";
            String actual = new String(read.toByteArray());
            assertEquals(expected,actual);
        }
    }

    private void insertAndSelectBlobAsBinaryStream(boolean compressed) throws Exception {
        byte[] body = "This is a blob, inserted as if it were a byte array field".getBytes();
        createBlobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ? )");
        pstmt.setBinaryStream(1,new ByteArrayInputStream(body),body.length);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(new String(Base64.encodeBase64(body)),readBlobAsBase64String(0));
    }

    private void updateAndSelectBlobAsBinaryStream(boolean compressed) throws Exception {
        insertAndSelectBlobAsBinaryStream(compressed);
        byte[] body = "This is the updated data".getBytes();
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ? where ID = 0");
        pstmt.setBinaryStream(1,new ByteArrayInputStream(body),body.length);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(new String(Base64.encodeBase64(body)),readBlobAsBase64String(0));
    }

    private void insertAndSelectBlobAsByteArray(boolean compressed) throws Exception {
        byte[] body = "This is a blob, inserted as if it were a byte array field".getBytes();
        createBlobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ? )");
        pstmt.setObject(1,body);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(new String(Base64.encodeBase64(body)),readBlobAsBase64String(0));
    }

    private void updateAndSelectBlobAsByteArray(boolean compressed) throws Exception {
        insertAndSelectBlobAsByteArray(compressed);
        byte[] body = "This is the updated data".getBytes();
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ? where ID = 0");
        pstmt.setObject(1,body);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(new String(Base64.encodeBase64(body)),readBlobAsBase64String(0));
    }

    private void insertAndSelectBlobViaBase64Encoding(boolean compressed) throws Exception {
        byte[] body = "This is a blob, inserted as if it were a byte array field".getBytes();
        String encoded = new String(Base64.encodeBase64(body));
        createBlobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, base64decode(?) )");
        pstmt.setString(1,encoded);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(encoded,readBlobAsBase64String(0));
    }

    private void updateAndSelectBlobViaBase64Encoding(boolean compressed) throws Exception {
        insertAndSelectBlobViaBase64Encoding(compressed);
        byte[] body = "This is the updated data".getBytes();
        String encoded = new String(Base64.encodeBase64(body));
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = BASE64DECODE(?) where ID = 0");
        pstmt.setString(1,encoded);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();                
        compareByteArrays(body,readBlobAsStream(0));
        assertEquals(encoded,readBlobAsBase64String(0));
    }

    private byte[] readBlobAsStream(int id) throws Exception {
        _rset = _stmt.executeQuery("select BODY from FOO where ID = " + id);
        assertTrue(_rset.next());
        ByteArrayOutputStream read = new ByteArrayOutputStream();
        Blob blob = _rset.getBlob(1);
        InputStream in = blob.getBinaryStream();
        for(int b = in.read(); b != -1; b = in.read()) {
            read.write((byte)b);
        }
        in.close();
        _rset.close();
        return read.toByteArray();
    }

    private String readBlobAsBase64String(int id) throws Exception {
        _rset = _stmt.executeQuery("select BASE64ENCODE(BODY) from FOO where ID = " + id);
        assertTrue(_rset.next());
        String value = _rset.getString(1);
        _rset.close();
        return value;
    }

    private void compareByteArrays(byte[] expected, byte[] actual) {
        assertEquals("array length",expected.length,actual.length);
        for(int i=0;i<expected.length;i++) {
            assertEquals("["+i+"]",expected[i],actual[i]);
        }
    }
}
