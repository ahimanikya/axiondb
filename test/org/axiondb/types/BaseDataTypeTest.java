/*
 * $Id: BaseDataTypeTest.java,v 1.3 2008/02/21 13:00:28 jawed Exp $
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.axiondb.BaseSerializableTest;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:28 $
 * @author Rodney Waldhoff
 */
public abstract class BaseDataTypeTest extends BaseSerializableTest {

    //------------------------------------------------------------ Conventional

    public BaseDataTypeTest(String testName) {
        super(testName);
    }

    //---------------------------------------------------------------- Framework
    
    protected abstract DataType getDataType();
    
    protected final Serializable makeSerializable() {
        return getDataType();
    }

    protected void compareEm(Object[] values) throws Exception {
        for(int i=0;i<values.length;i++) {
            for(int j=0;j<values.length;j++) {
                int result = getDataType().compare(values[i],values[j]);
                if(i < j) {
                    assertTrue(values[i] + " should be less than " + values[j] + " found " + result,result < 0);
                } else if(i == j) {
                    assertTrue(values[i] + " should be equal to " + values[j] + " found " + result,result == 0);
                } else {
                    assertTrue(values[i] + " should be greater than " + values[j] + " found " + result,result > 0);
                }
            }
        }
    }

    //------------------------------------------------------------------- Tests

    public final void testBaseToString() throws Exception {
        assertNotNull(getDataType().toString());
        assertNull(getDataType().toString(null));
    }

    public final void testAcceptsNull() throws Exception {
        assertTrue("Should accept null",getDataType().accepts(null));
    }

    public final void testConvertNullToNull() throws Exception {
        assertTrue(null == getDataType().convert(null));
    }

    public final void testHasComparator() throws Exception {
        DataType type = getDataType();
        if(type instanceof BaseDataType) {
            assertNotNull(((BaseDataType)type).getComparator());
        }
    }

    public final void testWriteReadNull() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        getDataType().write(null,new DataOutputStream(buf));
        Object read = getDataType().read(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));        
        assertNull(read);
    }

    public final void testWriteReadSeveralNulls() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        getDataType().write(null,new DataOutputStream(buf));
        getDataType().write(null,new DataOutputStream(buf));
        getDataType().write(null,new DataOutputStream(buf));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()));
        assertNull(getDataType().read(in));
        assertNull(getDataType().read(in));
        assertNull(getDataType().read(in));
    }
    
    public final void testPreferredValueClassNameIsNotNull() throws Exception {
        assertNotNull(getDataType().getPreferredValueClassName());
    }

    public void testGetPrecision() throws Exception {
        assertEquals(0,getDataType().getPrecision());
    }

    protected void assertInvalidWriteFails(Object badValue) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            getDataType().write(badValue, new DataOutputStream(bos));
            fail("Expected IOException due to invalid write data");
        } catch (IOException expected) {
            // expected
        }
    }
}

