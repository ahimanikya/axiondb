/*
 * $Id: AbstractClobTest.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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
import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.TestCase;

import org.axiondb.jdbc.AxionClob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Rodney Waldhoff 
 */
public abstract class AbstractClobTest extends TestCase {
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;
    private String _axiondbLobdir = null;

    public AbstractClobTest(String s) {
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

    private void createClobTable(boolean compressed) throws Exception {
        if(compressed) {
            _stmt.execute("create table FOO ( ID integer, BODY compressedclob, BODY2 compressedclob )");
        } else {
            _stmt.execute("create table FOO ( ID integer, BODY clob, BODY2 clob )");
        }
    }

    public void testInsertEmptyClob() throws Exception {
        createClobTable(false);
        insertEmptyClob();
    }

    public void testInsertEmptyCompressedClob() throws Exception {
        createClobTable(true);
        insertEmptyClob();
    }

    private void insertEmptyClob() throws Exception {
        _stmt.execute("insert into FOO ( ID, BODY, BODY2 ) values ( 0, 'newlob()', 'newlob()' )");
        _rset = _stmt.executeQuery("select ID, BODY, BODY2 from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNotNull(_rset.getClob(2));
        assertNotNull(_rset.getClob("body"));
        assertNotNull(_rset.getClob(3));
        assertTrue(_rset.getClob(2) instanceof AxionClob);
        assertTrue(_rset.getClob(3) instanceof AxionClob);
    }

    public void testWriteToEmptyClob() throws Exception {
        createClobTable(false);
        writeToEmptyClob();
    }

    public void testInsertAndSelectClobAsString() throws Exception {
        insertAndSelectClobAsString(false);
    }

    public void testUpdateAndSelectClobAsString() throws Exception {
        updateAndSelectClobAsString(false);
    }

    public void testInsertAndSelectCompressedClobAsString() throws Exception {
        insertAndSelectClobAsString(true);
    }

    public void testUpdateAndSelectCompressedClobAsString() throws Exception {
        updateAndSelectClobAsString(true);
    }

    public void testInsertAndSelectClobAsStringViaPstmt() throws Exception {
        insertAndSelectClobAsStringViaPstmt(false);
    }

    public void testUpdateAndSelectClobAsStringViaPstmt() throws Exception {
        updateAndSelectClobAsStringViaPstmt(false);
    }

    public void testInsertAndSelectCompressedClobAsStringViaPstmt() throws Exception {
        insertAndSelectClobAsStringViaPstmt(true);
    }

    public void testUpdateAndSelectCompressedClobAsStringViaPstmt() throws Exception {
        updateAndSelectClobAsStringViaPstmt(true);
    }

    public void testSetClobAsAsciiStream() throws Exception {
        updateAndSelectClobAsAsciiStream(false);
    }

    public void testSetCompressedClobAsAsciiStream() throws Exception {
        updateAndSelectClobAsAsciiStream(true);
    }

    public void testSetClobAsCharacterStream() throws Exception {
        updateAndSelectClobAsCharacterStream(false);
    }

    public void testSetCompressedClobAsCharacterStream() throws Exception {
        updateAndSelectClobAsCharacterStream(true);
    }

    public void testSetClobAsUnicodeStream() throws Exception {
        updateAndSelectClobAsUnicodeStream(false);
    }

    public void testSetCompressedClobAsUnicodeStream() throws Exception {
        updateAndSelectClobAsUnicodeStream(true);
    }

    public void testWriteToEmptyCompressedClob() throws Exception {
        createClobTable(true);
        writeToEmptyClob();
    }

    public void testGetAsciiStream() throws Exception {
        createClobTable(false);
        _stmt.execute("insert into FOO ( ID, BODY, BODY2 ) values ( 0, 'The quick brown fox jumped over the lazy dogs.', NULL )");
        _rset = _stmt.executeQuery("select ID, BODY, BODY2 from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNotNull(_rset.getAsciiStream(2));
        assertNotNull(_rset.getCharacterStream(2));
        assertNotNull(_rset.getAsciiStream("body"));
        assertNotNull(_rset.getCharacterStream("body"));

        assertNull(_rset.getAsciiStream(3));
        assertNull(_rset.getAsciiStream("body2"));
        assertNull(_rset.getCharacterStream(3));
        assertNull(_rset.getCharacterStream("body2"));
    }

    private void writeToEmptyClob() throws Exception {
        String bodyline = "The quick brown fox jumped over the lazy dogs. ";
        String bodyline2 = "And then the dogs ran away. ";

        _stmt.execute("insert into FOO ( ID, BODY, BODY2 ) values ( 0, 'newlob()', 'newlob()' )");

        _rset = _stmt.executeQuery("select ID, BODY, BODY2 from FOO");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(0,_rset.getInt(1));
        assertNotNull(_rset.getClob(2));
        assertNotNull(_rset.getClob(3));
        assertTrue(_rset.getClob(2) instanceof AxionClob);
        assertTrue(_rset.getClob(3) instanceof AxionClob);
        AxionClob clob = (AxionClob)(_rset.getClob(2));
        Writer out = clob.setCharacterStream(0L);
        for(int i=0;i<10;i++) {
            out.write(bodyline);
        }
        out.close();
        
        clob = (AxionClob)(_rset.getClob(3));        
        out = clob.setCharacterStream(0L);
        for(int i=0;i<10;i++) {
            out.write(bodyline2);
        }
        out.close();
        _rset.close();

        StringBuffer expected = new StringBuffer();
        for(int i=0;i<10;i++) {
            expected.append(bodyline);
        }
        assertEquals(expected.toString(),readClobAsStream(0, 1));
        assertEquals(expected.toString(),readClobAsVarchar(0, 1));

        expected = new StringBuffer();
        for(int i=0;i<10;i++) {
            expected.append(bodyline2);
        }
        assertEquals(expected.toString(),readClobAsStream(0, 2));
        assertEquals(expected.toString(),readClobAsVarchar(0, 2));
    }

    public void testWriteToNonEmptyClob() throws Exception {
        createClobTable(false);
        writeToNonEmptyClob();
    }

    public void testWriteToNonEmptyCompressedClob() throws Exception {
        createClobTable(true);
        writeToNonEmptyClob();
    }

    private void writeToNonEmptyClob() throws Exception {
        String body = "It was the best of times, it was the worst of times.";
        String body2 = "Or was it?";
        writeToEmptyClob();
        {
            _rset = _stmt.executeQuery("select ID, BODY, BODY2 from FOO");
            assertNotNull(_rset);
            assertTrue(_rset.next());
            assertEquals(0,_rset.getInt(1));
            assertNotNull(_rset.getClob(2));
            assertTrue(_rset.getClob(2) instanceof AxionClob);
            AxionClob clob = (AxionClob)(_rset.getClob(2));
            clob.truncate(0);
            Writer out = clob.setCharacterStream(0L);
            out.write(body);
            out.close();
            
            assertNotNull(_rset.getClob(3));
            assertTrue(_rset.getClob(3) instanceof AxionClob);
            clob = (AxionClob)(_rset.getClob(3));
            clob.truncate(0);
            out = clob.setCharacterStream(0L);
            out.write(body2);
            out.close();

            _rset.close();
        }
        assertEquals(body,readClobAsStream(0, 1));
        assertEquals(body,readClobAsVarchar(0, 1));

        assertEquals(body2,readClobAsStream(0, 2));
        assertEquals(body2,readClobAsVarchar(0, 2));
    }

    private void insertAndSelectClobAsString(boolean compressed) throws Exception {
        String body = "This is a clob, inserted as if it were a VARCHAR field";
        String body2 = "And this is as well";
        createClobTable(compressed);
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( 0, '" + body + "', '" + body2 + "')"));
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void updateAndSelectClobAsString(boolean compressed) throws Exception {
        insertAndSelectClobAsString(compressed);
        String body = "This is the updated clob.";
        String body2 = "Me too.";
        assertEquals(1,_stmt.executeUpdate("update FOO set BODY = '" + body + "', BODY2 = '" + body2 + "' where ID = 0"));
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void insertAndSelectClobAsStringViaPstmt(boolean compressed) throws Exception {
        String body = "This is a clob, inserted as if it were a VARCHAR field";
        String body2 = "Yadda, yadda, yadda";
        createClobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ?, ? )");
        pstmt.setString(1,body);
        pstmt.setString(2,body2);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void updateAndSelectClobAsStringViaPstmt(boolean compressed) throws Exception {
        insertAndSelectClobAsStringViaPstmt(compressed);
        String body = "This is the updated clob.";
        String body2 = "yup, updated clob here too.";
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ?, BODY2 = ? where ID = 0");
        pstmt.setString(1,body);
        pstmt.setString(2,body2);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void insertAndSelectClobAsAsciiStream(boolean compressed) throws Exception {
        String body = "This is a clob, inserted as if it were a VARCHAR field";
        String body2 = "clob, clob, clob";
        createClobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ?, ? )");
        pstmt.setAsciiStream(1,new ByteArrayInputStream(body.getBytes("ASCII")),body.length());
        pstmt.setAsciiStream(2,new ByteArrayInputStream(body2.getBytes("ASCII")),body2.length());
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void updateAndSelectClobAsAsciiStream(boolean compressed) throws Exception {
        insertAndSelectClobAsAsciiStream(compressed);
        String body = "This is the updated clob.";
        String body2 = "Ditto";
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ?, BODY2 = ? where ID = 0");
        pstmt.setAsciiStream(1,new ByteArrayInputStream(body.getBytes("ASCII")),body.length());
        pstmt.setAsciiStream(2,new ByteArrayInputStream(body2.getBytes("ASCII")),body2.length());
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void insertAndSelectClobAsUnicodeStream(boolean compressed) throws Exception {
        String body = "This is a clob, inserted as if it were a VARCHAR field";
        String body2 = "blah, blah";
        createClobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ?, ? )");
        pstmt.setUnicodeStream(1,new ByteArrayInputStream(body.getBytes("UnicodeBig")),body.length()*2);
        pstmt.setUnicodeStream(2,new ByteArrayInputStream(body2.getBytes("UnicodeBig")),body2.length()*2);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void updateAndSelectClobAsUnicodeStream(boolean compressed) throws Exception {
        insertAndSelectClobAsUnicodeStream(compressed);
        String body = "This is the updated clob.";
        String body2 = "Ditto.";
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ?, BODY2 = ? where ID = 0");
        pstmt.setUnicodeStream(1,new ByteArrayInputStream(body.getBytes("UnicodeBig")),body.length()*2);
        pstmt.setUnicodeStream(2,new ByteArrayInputStream(body2.getBytes("UnicodeBig")),body2.length()*2);
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void insertAndSelectClobAsCharacterStream(boolean compressed) throws Exception {
        String body = "This is a clob, inserted as if it were a VARCHAR field";
        String body2 = "Ditto.";
        createClobTable(compressed);
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOO values ( 0, ?, ? )");
        pstmt.setCharacterStream(1,new StringReader(body),body.length());
        pstmt.setCharacterStream(2,new StringReader(body2),body2.length());
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private void updateAndSelectClobAsCharacterStream(boolean compressed) throws Exception {
        insertAndSelectClobAsCharacterStream(compressed);
        String body = "This is the updated clob.";
        String body2 = "This is the updated clob as well.";
        PreparedStatement pstmt = _conn.prepareStatement("update FOO set BODY = ?, BODY2 = ? where ID = 0");
        pstmt.setCharacterStream(1,new StringReader(body),body.length());
        pstmt.setCharacterStream(2,new StringReader(body2),body2.length());
        assertEquals(1,pstmt.executeUpdate());
        pstmt.close();
        
        assertEquals(body,readClobAsVarchar(0, 1));
        assertEquals(body,readClobAsStream(0, 1));

        assertEquals(body2,readClobAsVarchar(0, 2));
        assertEquals(body2,readClobAsStream(0, 2));
    }

    private String readClobAsVarchar(int id, int index) throws Exception {
        _rset = _stmt.executeQuery("select BODY, BODY2 from FOO where ID = " + id);
        assertTrue(_rset.next());
        String value = _rset.getString(index);
        _rset.close();
        return value;
    }

    private String readClobAsStream(int id, int index) throws Exception {
        _rset = _stmt.executeQuery("select BODY, BODY2 from FOO where ID = " + id);
        assertTrue(_rset.next());
        StringBuffer buf = new StringBuffer();
        AxionClob clob = (AxionClob)(_rset.getClob(index));
        Reader in = clob.getCharacterStream();
        for(int c=in.read();c!=-1;c=in.read()) {
            buf.append((char)c);
        }
        in.close();
        _rset.close();
        return buf.toString();
    }
}
