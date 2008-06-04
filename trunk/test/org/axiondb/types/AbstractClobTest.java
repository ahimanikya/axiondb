/*
 * $Id: AbstractClobTest.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.axiondb.jdbc.AxionClob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public abstract class AbstractClobTest extends TestCase {

    public AbstractClobTest(String testName) {
        super(testName);
    }

    protected String _text = "The quick brown fox jumped over the lazy dogs.";
    
    protected abstract AxionClob getClob() throws Exception;

    public void testGetAsciiStream() throws Exception {
        Clob clob = getClob();
        InputStream in = clob.getAsciiStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals(_text,buf.toString());
        in.close();
    }

    public void testGetCharacterStream() throws Exception  {
        Clob clob = getClob();
        Reader in = clob.getCharacterStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals(_text,buf.toString());
        in.close();
    }

    public void testTruncate() throws Exception  {
        AxionClob clob = getClob();
        clob.truncate(("The quick".length()));
        Reader in = clob.getCharacterStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("The quick",buf.toString());
        in.close();
    }

    public void testGetSubstring() throws Exception {
        AxionClob clob = getClob();       
        String substring = null;
        try {
            substring = clob.getSubString(("The quick".length()), " brown fox jumped".length());
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        assertEquals(" brown fox jumped",substring);
    }

    public void testPositionOfString() throws Exception {
        AxionClob clob = getClob();       
        String substring = " brown fox jumped";
        long pos = -1;
        try {
            pos = clob.position(substring,3L);
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        assertEquals(_text.indexOf(substring,3),(int)pos);
    }

    public void testPositionOfClob() throws Exception {
        AxionClob clob = getClob();       
        String substring = " brown fox jumped";
        long pos = -1;
        try {
            pos = clob.position(new StringClob(substring),3L);
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        assertEquals(_text.indexOf(substring,3),(int)pos);
    }

    public void testPositionOfNullString() throws Exception {
        AxionClob clob = getClob();       
        try {
            clob.position((String)null,0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testPositionOfNullClob() throws Exception {
        AxionClob clob = getClob();       
        try {
            clob.position((Clob)null,0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testPositionOfStringNotFound() throws Exception {
        AxionClob clob = getClob();       
        String substring = " bROWn fox jumped";
        long pos = -1;
        try {
            pos = clob.position(substring,3L);
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        assertEquals((int)pos,_text.indexOf(substring,3));
    }

    public void testSetAsciiStream() throws Exception {
        AxionClob clob = getClob();
        OutputStream out = null;
        try {
            out = clob.setAsciiStream(0L);
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.".getBytes("US-ASCII"));
        out.close();

        InputStream in = clob.getAsciiStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
        in.close();
    }

    public void testSetAsciiStream2() throws Exception {
        AxionClob clob = getClob();
        {
            OutputStream out = null;
            try {
                out = clob.setAsciiStream(0L);
            } catch(SQLException e) {
                return; // exit on unsupported
            }
            out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.".getBytes("US-ASCII"));
            out.close();
    
            InputStream in = clob.getAsciiStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
            in.close();
        }
        clob.truncate(0);
        {
            OutputStream out = null;
            try {
                out = clob.setAsciiStream(0L);
            } catch(SQLException e) {
                return; // exit on unsupported
            }
            out.write("Every good boy does fine.".getBytes("US-ASCII"));
            out.close();
    
            InputStream in = clob.getAsciiStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("Every good boy does fine.",buf.toString());
            in.close();
        }
    }

    public void testSetCharacterStream() throws Exception {
        AxionClob clob = getClob();
        Writer out = null;
        try {
            out = clob.setCharacterStream(0L);
        } catch(SQLException e) {
            return; // exit on unsupported
        }
        out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.");
        out.close();

        InputStream in = clob.getAsciiStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
        in.close();
    }

    public void testSetCharacterStream2() throws Exception {
        AxionClob clob = getClob();
        {
            Writer out = null;
            try {
                out = clob.setCharacterStream(0L);
            } catch(SQLException e) {
                return; // exit on unsupported
            }
            out.write("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.");
            out.close();
    
            InputStream in = clob.getAsciiStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("The quick yellow dogs crept under the lazy foxes. Every good boy does fine.",buf.toString());
            in.close();
        }
        clob.truncate(0);
        {
            Writer out = null;
            try {
                out = clob.setCharacterStream(0L);
            } catch(SQLException e) {
                return; // exit on unsupported
            }
            out.write("Every good boy does fine.");
            out.close();
    
            InputStream in = clob.getAsciiStream();
            StringBuffer buf = new StringBuffer();
            for(int c=in.read();c != -1;c = in.read()) {
                buf.append((char)c);
            }
            assertEquals("Every good boy does fine.",buf.toString());
            in.close();
        }
    }
}
