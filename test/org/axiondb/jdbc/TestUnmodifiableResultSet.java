/*
 * $Id: TestUnmodifiableResultSet.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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
package org.axiondb.jdbc;

import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.types.LOBType;

/**
 *
 * @author Jonathan Giron
 * @version $Revision: 1.1 $
 */
public class TestUnmodifiableResultSet extends AxionTestCaseSupport {
    /**
     * @param testName
     */
    public TestUnmodifiableResultSet(String testName) {
        super(testName);
    }
    
    public static Test suite() {
        return new TestSuite(TestUnmodifiableResultSet.class);
    }
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    public void setUp() throws Exception {
        super.setUp();
        
        Statement stmt = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.execute("create table foo (id int, name varchar(50))");
        stmt.execute("insert into foo values (1, 'my first line')");
        stmt.execute("insert into foo values (2, 'my second line')");
        
        _unmodRs = stmt.executeQuery("select * from foo");
    }    

    public void testCancelRowUpdates() {
        try {
            _unmodRs.cancelRowUpdates();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testDeleteRow() {
        try {
            _unmodRs.deleteRow();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testInsertRow() {
        try {
            _unmodRs.insertRow();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testMoveToCurrentRow() {
        try {
            _unmodRs.moveToCurrentRow();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testMoveToInsertRow() {
        try {
            _unmodRs.moveToInsertRow();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testUpdateRow() {
        try {
            _unmodRs.updateRow();
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateNull(int)
     */
    public void testUpdateNullint() {
        try {
            _unmodRs.updateNull(1);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateByte(int, byte)
     */
    public void testUpdateByteintbyte() {
        try {
            _unmodRs.updateByte(1, (byte) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateDouble(int, double)
     */
    public void testUpdateDoubleintdouble() {
        try {
            _unmodRs.updateDouble(1, 0.0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateFloat(int, float)
     */
    public void testUpdateFloatintfloat() {
        try {
            _unmodRs.updateFloat(1, 0.0f);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateInt(int, int)
     */
    public void testUpdateIntintint() {
        try {
            _unmodRs.updateInt(1, 1);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateLong(int, long)
     */
    public void testUpdateLongintlong() {
        try {
            _unmodRs.updateLong(1, 0L);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateShort(int, short)
     */
    public void testUpdateShortintshort() {
        try {
            _unmodRs.updateShort(1, (short) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBoolean(int, boolean)
     */
    public void testUpdateBooleanintboolean() {
        try {
            _unmodRs.updateBoolean(1, false);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBytes(int, byte[])
     */
    public void testUpdateBytesintbyteArray() {
        try {
            _unmodRs.updateBytes(1, new byte[0]);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateAsciiStream(int, InputStream, int)
     */
    public void testUpdateAsciiStreamintInputStreamint() {
        try {
            _unmodRs.updateAsciiStream(1, System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBinaryStream(int, InputStream, int)
     */
    public void testUpdateBinaryStreamintInputStreamint() {
        try {
            _unmodRs.updateBinaryStream(1, System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateCharacterStream(int, Reader, int)
     */
    public void testUpdateCharacterStreamintReaderint() {
        try {
            _unmodRs.updateCharacterStream(1, new InputStreamReader(System.in), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateObject(int, Object)
     */
    public void testUpdateObjectintObject() {
        try {
            _unmodRs.updateObject(1, new Object());
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

    }

    /*
     * Class under test for void updateObject(int, Object, int)
     */
    public void testUpdateObjectintObjectint() {
        try {
            _unmodRs.updateObject(1, new Object(), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateString(int, String)
     */
    public void testUpdateStringintString() {
        try {
            _unmodRs.updateString(1, "");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateNull(String)
     */
    public void testUpdateNullString() {
        try {
            _unmodRs.updateNull("id");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateByte(String, byte)
     */
    public void testUpdateByteStringbyte() {
        try {
            _unmodRs.updateByte("id", (byte) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateDouble(String, double)
     */
    public void testUpdateDoubleStringdouble() {
        try {
            _unmodRs.updateDouble("id", 0.0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateFloat(String, float)
     */
    public void testUpdateFloatStringfloat() {
        try {
            _unmodRs.updateFloat("id", 0.0f);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateInt(String, int)
     */
    public void testUpdateIntStringint() {
        try {
            _unmodRs.updateInt("id", 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateLong(String, long)
     */
    public void testUpdateLongStringlong() {
        try {
            _unmodRs.updateLong("id", 0L);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateShort(String, short)
     */
    public void testUpdateShortStringshort() {
        try {
            _unmodRs.updateShort("id", (short) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBoolean(String, boolean)
     */
    public void testUpdateBooleanStringboolean() {
        try {
            _unmodRs.updateBoolean("id", false);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBytes(String, byte[])
     */
    public void testUpdateBytesStringbyteArray() {
        try {
            _unmodRs.updateBytes("id", new byte[0]);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBigDecimal(int, BigDecimal)
     */
    public void testUpdateBigDecimalintBigDecimal() {
        try {
            _unmodRs.updateBigDecimal("id", BigDecimal.valueOf(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateArray(int, Array)
     */
    public void testUpdateArrayintArray() {
        try {
            _unmodRs.updateArray(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBlob(int, Blob)
     */
    public void testUpdateBlobintBlob() throws AxionException {
        try {
            _unmodRs.updateBlob(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateClob(int, Clob)
     */
    public void testUpdateClobintClob() throws AxionException {
        try {
            _unmodRs.updateClob("id", _lobType.toClob("foo"));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateDate(int, Date)
     */
    public void testUpdateDateintDate() {
        try {
            _unmodRs.updateDate(1, new Date(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateRef(int, Ref)
     */
    public void testUpdateRefintRef() {
        try {
            _unmodRs.updateRef(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateTime(int, Time)
     */
    public void testUpdateTimeintTime() {
        try {
            _unmodRs.updateTime(1, new Time(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateTimestamp(int, Timestamp)
     */
    public void testUpdateTimestampintTimestamp() {
        try {
            _unmodRs.updateTimestamp(1, new Timestamp(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateAsciiStream(String, InputStream, int)
     */
    public void testUpdateAsciiStreamStringInputStreamint() {
        try {
            _unmodRs.updateAsciiStream("foo", System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBinaryStream(String, InputStream, int)
     */
    public void testUpdateBinaryStreamStringInputStreamint() {
        try {
            _unmodRs.updateBinaryStream("foo", System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateCharacterStream(String, Reader, int)
     */
    public void testUpdateCharacterStreamStringReaderint() {
        try {
            _unmodRs.updateCharacterStream("foo", new InputStreamReader(System.in), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateObject(String, Object)
     */
    public void testUpdateObjectStringObject() {
        try {
            _unmodRs.updateObject("foo", new Object());
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateObject(String, Object, int)
     */
    public void testUpdateObjectStringObjectint() {
        try {
            _unmodRs.updateObject("foo", new Object(), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateString(String, String)
     */
    public void testUpdateStringStringString() {
        try {
            _unmodRs.updateString("foo", "");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBigDecimal(String, BigDecimal)
     */
    public void testUpdateBigDecimalStringBigDecimal() {
        try {
            _unmodRs.updateBigDecimal("foo", BigDecimal.valueOf(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateArray(String, Array)
     */
    public void testUpdateArrayStringArray() {
        try {
            _unmodRs.updateArray("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateBlob(String, Blob)
     */
    public void testUpdateBlobStringBlob() throws AxionException {
        try {
            _unmodRs.updateBlob("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    /*
     * Class under test for void updateClob(String, Clob)
     */
    public void testUpdateClobStringClob() throws AxionException {
        try {
            _unmodRs.updateClob("foo", _lobType.toClob("foo"));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }    }

    /*
     * Class under test for void updateDate(String, Date)
     */
    public void testUpdateDateStringDate() {
        try {
            _unmodRs.updateDate("foo", new Date(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }    }

    /*
     * Class under test for void updateRef(String, Ref)
     */
    public void testUpdateRefStringRef() {
        try {
            _unmodRs.updateRef("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }    }

    /*
     * Class under test for void updateTime(String, Time)
     */
    public void testUpdateTimeStringTime() {
        try {
            _unmodRs.updateTime("foo", new Time(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }    }

    /*
     * Class under test for void updateTimestamp(String, Timestamp)
     */
    public void testUpdateTimestampStringTimestamp() {
        try {
            _unmodRs.updateTimestamp("foo", new Timestamp(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    private LOBType _lobType = new LOBType();
    private ResultSet _unmodRs;
}
