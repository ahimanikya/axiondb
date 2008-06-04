/*
 * $Id: TestBLOBType.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionBlob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author James Burke
 * @author Rodney Waldhoff
 */
public class TestBLOBType extends TestCase {

    public TestBLOBType(String name) {
        super(name);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestBLOBType.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestBLOBType.class);
    }

    private LOBType _blobtype = null;
    private String _dataAsString = null;
    private byte[] _data = null;
    private String _origLobDirProp = null;

    public void setUp() throws Exception {
        super.setUp();
        _origLobDirProp = System.getProperty("axoindb.lobdir");       
        System.setProperty("axiondb.lobdir","testlobdir");
        File file = new File("testlobdir");
        file.deleteOnExit();
        file.mkdirs();
        _blobtype = new BLOBType();
        _blobtype.setLobDir(file);
        _dataAsString = "Four score and seven years ago.";
        _data = _dataAsString.getBytes();
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
        assertTrue(!_blobtype.supportsSuccessor());
    }

    public void testAcceptsNewLob() throws Exception {
        assertTrue(_blobtype.accepts("newlob()"));
    }

    public void testCreateNewBlob() throws Exception {
        Blob blob = _blobtype.toBlob("newlob()");
        assertNotNull(blob);
        AxionBlob ablob = (AxionBlob)(blob);

        OutputStream out = ablob.setBinaryStream(0L);
        out.write(_data);
        out.close();

        InputStream in = ablob.getBinaryStream();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for(int i = in.read();i != -1;i = in.read()) {
            buf.write(i);
        }
        in.close();
        assertEquals(_dataAsString,new String(buf.toByteArray()));
    }
}
