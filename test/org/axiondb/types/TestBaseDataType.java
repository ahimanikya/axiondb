/*
 * $Id: TestBaseDataType.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.DatabaseMetaData;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public class TestBaseDataType extends BaseDataTypeTest {

    //------------------------------------------------------------ Conventional

    public TestBaseDataType(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBaseDataType.class);
        return suite;
    }


    //--------------------------------------------------------------- Framework
    
    protected DataType getDataType() {
        return getBaseDataType();
    }

    protected BaseDataType getBaseDataType() {
        return new MockBaseDataTypeImpl();
    }
    
    //------------------------------------------------------------------- Tests
    
    public void testGetPreferredValueClassName() {
        assertEquals("java.lang.Object",getDataType().getPreferredValueClassName());
    }
    
    public void testSupportsSuccessor() {
        assertTrue(!getDataType().supportsSuccessor());
    }

    public void testSuccessorIsUnsupported() {
        try {
            getDataType().successor(null);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }

    public void testNumberToNumber() throws Exception {
        assertEquals(new Integer(1),getBaseDataType().toNumber(new Integer(1)));
    }

    public void testNullToBigDecimal() throws Exception {
        assertNull(getDataType().toBigDecimal(null));
    }

    public void testNullToBigInteger() throws Exception {
        assertNull(getDataType().toBigInteger(null));
    }

    public void testToByte() throws Exception {
        assertEquals((byte)3,getDataType().toByte(new Byte((byte)3)));
    }

    public void testToDate() throws Exception {
        try {
            getDataType().toDate(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToDouble() throws Exception {
        assertEquals(3.14d,getDataType().toDouble(new Double(3.14d)),0.0d);
    }

    public void testToFloat() throws Exception {
        assertEquals(3.14f,getDataType().toFloat(new Double(3.14f)),0.0f);
    }

    public void testToInt() throws Exception {
        assertEquals(3,getDataType().toInt(new Integer(3)));
    }

    public void testToLong() throws Exception {
        assertEquals(3L,getDataType().toLong(new Long(3L)));
    }

    public void testNullToString() throws Exception {
        assertNull(getDataType().toString(null));
    }

    public void testNonNullToString() throws Exception {
        assertEquals("xyzzy",getDataType().toString("xyzzy"));
    }

    public void testToTime() throws Exception {
        try {
            getDataType().toTime(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToTimestamp() throws Exception {
        try {
            getDataType().toTimestamp(null);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testToClob() throws Exception {
        assertNotNull(getDataType().toClob("xyzzy"));
    }

    public void testToBlob() throws Exception {
        assertNull(getDataType().toBlob(null));
        byte[] xyzzy = "xyzzy".getBytes();
        byte[] xyzzy1 = getDataType().toBlob(xyzzy).getBytes(0, xyzzy.length);
        for(int i = 0; i< xyzzy.length; i++) {
            assertEquals(xyzzy[i], xyzzy1[i]);
        }
    }

    public void testGetLiteralPrefix() throws Exception {
        assertNull(getDataType().getLiteralPrefix());
    }

    public void testGetLiteralSuffix() throws Exception {
        assertNull(getDataType().getLiteralSuffix());
    }

    public void testGetNullableCode() throws Exception {
        assertEquals(DatabaseMetaData.typeNullable,getDataType().getNullableCode());
    }

    public void testIsCaseSensitive() throws Exception {
        assertTrue(!getDataType().isCaseSensitive());
    }
}

class MockBaseDataTypeImpl extends BaseDataType implements Serializable {
    public boolean accepts(Object value) {
        return true;
    }

    public Object convert(Object value) {
        return value;
    }

    public int getJdbcType() {
        return 0;
    }

    public DataType makeNewInstance() {
        return new MockBaseDataTypeImpl();
    }

    public Object read(DataInput in) throws IOException {
        if(in.readBoolean()) {
            return new Integer(1);
        }
        return null;
    }

    public void write(Object value, DataOutput out) throws IOException {
        if(null == value) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
        }
    }
}
