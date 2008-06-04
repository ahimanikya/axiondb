/*
 * $Id: TestCLOBType.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
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
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionClob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author James Burke
 * @author Rodney Waldhoff
 */
public class TestCLOBType extends TestCase {

    public TestCLOBType(String name) {
        super(name);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestCLOBType.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestCLOBType.class);
    }

    private LOBType _clobtype = null;
    private String _origLobDirProp = null;

    public void setUp() throws Exception {
        super.setUp();
        _origLobDirProp = System.getProperty("axoindb.lobdir");       
        System.setProperty("axiondb.lobdir","testlobdir");
        File file = new File("testlobdir");
        file.deleteOnExit();
        file.mkdirs();
        _clobtype = new CLOBType();
        _clobtype.setLobDir(file);
    }

    public void tearDown() throws Exception {
        deleteFile((new File("testlobdir")));
        if(null == _origLobDirProp) {
            System.getProperties().remove("axiondb.lobdir");            
        } else {
            System.setProperty("axiondb.lobdir",_origLobDirProp);
        }
        super.tearDown();
    }

    private boolean deleteFile(File file) throws Exception {
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

    public void testSupportsSuccessor() throws Exception {
        assertTrue(!_clobtype.supportsSuccessor());
    }

    public void testAcceptsNewClob() throws Exception {
        assertTrue(_clobtype.accepts("newlob()"));
    }

    public void testCreateNewClob() throws Exception {
        Clob clob = _clobtype.toClob("newlob()");
        assertNotNull(clob);
        AxionClob aclob = (AxionClob)(clob);

        Writer out = aclob.setCharacterStream(0L);
        out.write("Four score and seven years ago.");
        out.close();

        Reader in = aclob.getCharacterStream();
        StringBuffer buf = new StringBuffer();
        for(int c=in.read();c != -1;c = in.read()) {
            buf.append((char)c);
        }
        assertEquals("Four score and seven years ago.",buf.toString());
        in.close();
    }
}
