/*
 * $Id: TestBufferedRandomAccessReadWrite.java,v 1.1 2007/11/28 10:01:51 jawed Exp $
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:51 $
 * @author Rodney Waldhoff
 */
public class TestBufferedRandomAccessReadWrite extends TestCase {

    //-----------------------------------------------------b------- Conventional

    public TestBufferedRandomAccessReadWrite(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestBufferedRandomAccessReadWrite.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private Random _random = new Random();
    private AxionFileSystem FS = new AxionFileSystem();

    //------------------------------------------------------------------- Tests

    public void testRead() throws Exception {
        byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        FileOutputStream out = new FileOutputStream("test.file");
        out.write(bytes);
        out.close();

        
        BufferedDataInputStream in = new BufferedDataInputStream(FS.open(new File("test.file")), 12);
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], (byte) in.read());
        }
        assertEquals(-1, in.read());
        assertEquals(-1, in.read());
        in.close();
        File toDelete = new File("test.file");
        toDelete.delete();
    }

    public void testReadIntoBigBuffer() throws Exception {

        byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        FileOutputStream out = new FileOutputStream("test.file");
        out.write(bytes);
        out.close();


        BufferedDataInputStream in = FS.openBufferedDIS(new File("test.file"));
        byte[] data = new byte[256];
        assertEquals(128, in.read(data));
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], data[i]);
        }
        in.close();
        File toDelete = new File("test.file");
        toDelete.delete();
    }

    public void testSeek() throws Exception {
        byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        FileOutputStream out = new FileOutputStream("test.file");
        out.write(bytes);
        out.close();

        BufferedDataInputStream in = new BufferedDataInputStream(FS.open(new File("test.file")), 12);
        for (int i = 0; i < bytes.length; i++) {
            in.seek(i);
            assertEquals("" + i, bytes[i], (byte) in.read());
        }
        for (int i = 0; i < bytes.length; i++) {
            in.seek(bytes.length - i - 1);
            assertEquals(bytes[bytes.length - i - 1], (byte) in.read());
        }
        in.close();
        File toDelete = new File("test.file");
        toDelete.delete();
    }

    public void testShortBufferedRead() throws Exception {
        for (int i = 0; i < 10; i++) {
            byte[] bytes = new byte[i];
            _random.nextBytes(bytes);
            FileOutputStream out = new FileOutputStream("test.file");
            out.write(bytes);
            out.close();

            BufferedDataInputStream in = FS.openBufferedDIS(new File("test.file"));
            byte[] bytesin = new byte[1];
            for (int j = 0; j < i; j++) {
                in.read(bytesin);
                assertEquals("i=" + i + ";j=" + j, bytes[j], bytesin[0]);
            }
            in.close();
            File toDelete = new File("test.file");
            toDelete.delete();
        }
    }

    public void testLongBufferedRead() throws Exception {
        for (int i = 2040; i < 2100; i++) {
            byte[] bytes = new byte[i];
            _random.nextBytes(bytes);
            FileOutputStream out = new FileOutputStream("test.file");
            out.write(bytes);
            out.close();

            BufferedDataInputStream in = FS.openBufferedDIS(new File("test.file"));
            byte[] bytesin = new byte[1];
            for (int j = 0; j < i; j++) {
                in.read(bytesin);
                assertEquals("i=" + i + ";j=" + j, bytes[j], bytesin[0]);
            }
            in.close();
            File toDelete = new File("test.file");
            toDelete.delete();
        }
    }

    public void testLongBufferedRead2() throws Exception {
        for (int i = 2040; i < 2100; i++) {
            byte[] bytes = new byte[i];
            _random.nextBytes(bytes);
            FileOutputStream out = new FileOutputStream("test.file");
            out.write(bytes);
            out.close();

            BufferedDataInputStream in = FS.openBufferedDIS(new File("test.file"));
            byte[] bytesin = new byte[i];
            in.read(bytesin);
            for (int j = 0; j < i; j++) {
                assertEquals(bytes[j], bytesin[j]);
            }
            in.close();
            File toDelete = new File("test.file");
            toDelete.delete();
        }
    }

    public void testWriteAndRead() throws Exception {
        BufferedDataOutputStream writefile = FS.openBufferedDOSAppend(new File("test.file"), 20);
        // create a long string
        String text = null;
        {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < 10; i++) {
                buf.append("The quick brown fox jumped over the lazy dogs. ");
            }
            text = buf.toString();
        }

        long offarray[] = new long[10];
        CharacterVaryingType varcharType = new CharacterVaryingType(1000);
        IntegerType intType = new IntegerType();
        for (int i = 0; i < 10; i++) {
            offarray[i] = writefile.getPos();
            intType.write(new Integer(i), writefile);
            varcharType.write(text, writefile);
        }
        writefile.close();
        
        BufferedDataInputStream readfile = FS.openBufferedDIS(new File("test.file"));
        for (int i = 0; i < 10; i++) {
            try {
                readfile.seek(offarray[i]);
                assertEquals(new Integer(i), intType.read(readfile));
                assertEquals(text, varcharType.read(readfile));
            } catch (IOException e) {
                throw new AxionException("IOException  while reading at position " + offarray[i] + " in data file " + readfile, e);
            }
        }
        
        readfile.close();
        File toDelete = new File("test.file");
        toDelete.delete();
    }
}