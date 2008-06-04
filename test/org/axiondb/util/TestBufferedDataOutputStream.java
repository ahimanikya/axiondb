/*
 * $Id: TestBufferedDataOutputStream.java,v 1.1 2007/11/28 10:01:51 jawed Exp $
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

package org.axiondb.util;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:51 $
 * @author Rodney Waldhoff
 */
public class TestBufferedDataOutputStream extends TestCase {

    //------------------------------------------------------------ Conventional
        
    public TestBufferedDataOutputStream(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestBufferedDataOutputStream.class);
    }

    //--------------------------------------------------------------- Lifecycle
        
    private File _file = null;
    private BufferedDataOutputStream _out;
    
    public void setUp() throws Exception {
        _file = new File("doos.test");
    }

    public void tearDown() throws Exception {
        try { _out.close(); } catch(Exception e) { }
        _file.delete();
    }

    //------------------------------------------------------------------- Tests        

    public void testWrite() throws Exception {
        _out = new AxionFileSystem().openBufferedDOSAppend(_file, 20);
        _out.writeInt(1);
        _out.writeUTF("This is a literal string.");
        _out.writeInt(Integer.MIN_VALUE);
        _out.write(3);
        _out.writeBoolean(true);
        _out.writeByte(5);
        _out.writeChar('c');
        _out.writeLong(7);
        _out.writeShort(9);
        _out.flush();
        _out.close();

        BufferedDataInputStream in = new AxionFileSystem().openBufferedDIS(_file);
        assertEquals(1,in.readInt());
        assertEquals("This is a literal string.",in.readUTF());
        assertEquals(Integer.MIN_VALUE,in.readInt());
        assertEquals(3,in.read());
        assertEquals(true,in.readBoolean());
        assertEquals(5,in.readByte());
        assertEquals('c',in.readChar());
        assertEquals(7,in.readLong());
        assertEquals(9,in.readShort());
        in.close();
    }

}
