/*
 * $Id: TestEncoder.java,v 1.1 2007/11/28 10:01:51 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:51 $
 * @author Rahul Dwivedi
 * @author Rodney Waldhoff
 */
public class TestEncoder extends TestCase {

    //------------------------------------------------------------ Conventional
        
    public TestEncoder(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestEncoder.class);
    }

    //--------------------------------------------------------------- Lifecycle

    //------------------------------------------------------------------- Tests        

    public void testOldMain() throws Exception {
        // this is the old main method of AsciiEbcdicEncoder, pretty much blindly converted to assertions
        byte[] b = "-a".getBytes();
        byte[] b1 = " a".getBytes();
        AsciiEbcdicEncoder.convertAsciiToEbcdic(b);
        AsciiEbcdicEncoder.convertAsciiToEbcdic(b1);
        assertEquals(96,b[0]);
        assertEquals(-127,b[1]);
        assertEquals(64,b1[0]);
        assertEquals(-127,b1[1]);
    }

    public void testEncodeDecodeInt() throws Exception {
        for(int i='a';i<'z';i++) {
            assertEquals(i,AsciiEbcdicEncoder.EBCDICToASCII(AsciiEbcdicEncoder.ASCIIToEBCDIC(i)));
        }
    }

    public void testEncodeDecodeArray() throws Exception {
        String alpha = "abcdefghijklmnopqrstuvwxyz";
        byte[] ascii = alpha.getBytes("US-ASCII");
        AsciiEbcdicEncoder.convertAsciiToEbcdic(ascii);
        AsciiEbcdicEncoder.convertEbcdicToAscii(ascii);
        assertEquals(alpha,new String(ascii,"US-ASCII"));
    }
}
