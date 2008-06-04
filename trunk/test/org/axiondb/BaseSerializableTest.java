/*
 * $Id: BaseSerializableTest.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
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

package org.axiondb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import junit.framework.TestCase;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Chuck Burdick
 * @author Rod Waldhoff
 */
public abstract class BaseSerializableTest extends TestCase {

    //------------------------------------------------------------ Conventional

    protected BaseSerializableTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Framework

    protected abstract Serializable makeSerializable();

    protected InputStream getCanonicalSerializedFormStream() {
        return getClass().getResourceAsStream(getCanonicalSerializedFormName());
    }
    
    protected String getCanonicalSerializedFormName() {
        return makeSerializable().getClass().getName() + ".ser";
    }
    
    public static Object cloneViaSerialization(Object obj)
        throws IOException, ClassNotFoundException {
        return deserialize(serialize(obj));
    }

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(buffer);
        out.writeObject(obj);
        out.flush();
        return buffer.toByteArray();
    }

    public static Object deserialize(byte[] bytes)
        throws IOException, ClassNotFoundException {
        return (new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject();
    }
    
    //--------------------------------------------------------------- Lifecycle

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public final void testSimpleSerializeDeserialize() throws Exception {
        assertNotNull(cloneViaSerialization(makeSerializable()));
    }
    
    public final void testCanonicalForm() throws Exception {
        /*
        // you can use this to generate the file the first time
        {
            java.io.File f = new java.io.File(getCanonicalSerializedFormName());
            ObjectOutputStream out = new ObjectOutputStream(new java.io.FileOutputStream(f));
            out.writeObject(makeSerializable());
            out.close();
        }
        */
        ObjectInputStream in= null;
        Object obj = null;
        try {
            in = new ObjectInputStream(getCanonicalSerializedFormStream());
            obj = in.readObject();
        } finally {
            try { in.close(); } catch(Exception e) { }
        }
        assertNotNull(this.getClass().getName(),obj);
        assertEquals(this.getClass().getName(),makeSerializable().getClass(),obj.getClass());
    }

}
