
/*
 * $Id: TestCompressedFileClob.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.zip.GZIPOutputStream;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionClob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 */
public class TestCompressedFileClob extends AbstractClobTest {

    public TestCompressedFileClob(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestCompressedFileClob.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestCompressedFileClob.class);
    }

    private File _file = null;
    
    public void setUp() throws Exception {
        super.setUp();
        _file = new File("testfileclob.temp");
        if(_file.exists()) {
            _file.delete();
        }
        _file.deleteOnExit();
        OutputStream out = new GZIPOutputStream(new FileOutputStream(_file));
        out.write(_text.getBytes("US-ASCII"));
        out.close();
    }

    public void tearDown() {
        try {
            _file.delete();
        } catch(Exception e) {
            // ignored
        }
        _file = null;
    }

    public void testTruncate() throws Exception  {
        AxionClob clob = getClob();
        try {
            clob.truncate(("The quick".length()));
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }

    protected AxionClob getClob() throws Exception {
        return new ClobSource(new CompressedLobSource(new FileLobSource(_file)));
    }
}
