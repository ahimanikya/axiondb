/*
 * $Id: TestByteArrayBlob.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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

import java.sql.Blob;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionBlob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 */
public class TestByteArrayBlob extends AbstractBlobTest {

    public TestByteArrayBlob(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestByteArrayBlob.class);
    }

    protected AxionBlob getBlob() throws Exception {
        return new ByteArrayBlob(_bytes);
    }

    public void testConstructorOnNull() throws Exception  {
        try {
            new ByteArrayBlob(null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
    }
    
    public void testTruncate() throws Exception  {
        AxionBlob blob = getBlob();
        try {
            blob.truncate(0);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testSetBytes() throws Exception  {
        AxionBlob blob = getBlob();
        try {
            blob.setBytes(0,null,0,0);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testSetBytes2() throws Exception  {
        AxionBlob blob = getBlob();
        try {
            blob.setBytes(0,null);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testPosition() throws Exception  {
        AxionBlob blob = getBlob();
        try {
            blob.position((Blob)null,0);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testPosition2() throws Exception  {
        AxionBlob blob = getBlob();
        try {
            blob.position((byte[])null,0);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testSetBinaryStream() throws Exception {
        AxionBlob blob = getBlob();
        try {
            blob.setBinaryStream(0L);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testSetBinaryStream2() throws Exception {
        testSetBinaryStream();
    }
    
}
