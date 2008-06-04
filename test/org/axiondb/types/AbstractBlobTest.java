/*
 * $Id: AbstractBlobTest.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

package org.axiondb.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.axiondb.jdbc.AxionBlob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public abstract class AbstractBlobTest extends TestCase {

    public AbstractBlobTest(String testName) {
        super(testName);
    }

    protected byte[] _bytes = "The quick brown fox jumped over the lazy dogs.".getBytes();
    
    protected abstract AxionBlob getBlob() throws Exception;

    public void testGetBinaryStream() throws Exception {
        Blob blob = getBlob();
        InputStream in = blob.getBinaryStream();
        for(int i=0;i < _bytes.length;i++) {
            assertEquals(_bytes[i],in.read());
        }
        assertEquals(-1,in.read());
        assertEquals(-1,in.read());
        in.close();
    }

    public void testGetBytes() throws Exception {
        Blob blob = getBlob();
        byte[] found = blob.getBytes(0L,_bytes.length);
        assertNotNull(found);
        for(int i=0;i < _bytes.length;i++) {
            assertEquals(_bytes[i],found[i]);
        }
    }

    public void testGetBytes2() throws Exception {
        Blob blob = getBlob();
        byte[] found = blob.getBytes(3,_bytes.length-3);
        assertNotNull(found);
        for(int i=3;i < _bytes.length-3;i++) {
            assertEquals(_bytes[i],found[i-3]);
        }
    }

    public void testGetBytes3() throws Exception {
        Blob blob = getBlob();
        try {
            blob.getBytes(-1L,2);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            blob.getBytes(0,-1);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testGetBytes4() throws Exception {
        Blob blob = getBlob();
        byte[] found = blob.getBytes(0,0);        
        assertNotNull(found);
        assertEquals(0,found.length);
    }

    public void testTruncate() throws Exception  {
        AxionBlob blob = getBlob();
        blob.truncate(("The quick".length()));
        InputStream in = blob.getBinaryStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("The quick",buf.toString());
        in.close();
    }

    public void testSetBinaryStream() throws Exception {
        AxionBlob blob = getBlob();
        OutputStream out = blob.setBinaryStream(0L);

        out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.".getBytes("US-ASCII"));
        out.close();

        InputStream in = blob.getBinaryStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
        in.close();
    }

    public void testSetBinaryStream2() throws Exception {
        AxionBlob blob = getBlob();
        {
            OutputStream out = blob.setBinaryStream(0L);

            out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.".getBytes("US-ASCII"));
            out.close();
        
            InputStream in = blob.getBinaryStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
            in.close();
        }
        blob.truncate(0);
        {
            OutputStream out = blob.setBinaryStream(0L);

            out.write("Every good boy does fine.".getBytes("US-ASCII"));
            out.close();
        
            InputStream in = blob.getBinaryStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("Every good boy does fine.",buf.toString());
            in.close();
        }
    }
}
