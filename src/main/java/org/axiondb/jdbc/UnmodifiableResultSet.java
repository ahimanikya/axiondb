/*
 * 
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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
//import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
//import java.sql.RowId;
import java.sql.SQLException;
//import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
//import org.axiondb.jdbc.AxionRowId;

/**
 * @author Jonathan Giron
 * @version 
 */
public final class UnmodifiableResultSet extends BaseAxionResultSetDecorator {

    public static ResultSet decorate(ResultSet that) {
        return (null == that) ? null : (that instanceof UnmodifiableResultSet) ? that : new UnmodifiableResultSet(that);
    }

    /**
     * @param rs ResultSet that need to be decorated
     */
    public UnmodifiableResultSet(ResultSet rs) {
        super(rs);
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void deleteRow() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void insertRow() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNull(String columnName) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateRow() throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateString(String columnName, String x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        throw new SQLException("Read-only ResultSet - not supported.");
    }

    /*public RowId getRowId(int columnIndex) throws SQLException {
        return new AxionRowId(getRow());
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return new AxionRowId(getRow());
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }*/

    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    /*public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public NClob getNClob(int columnIndex) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public NClob getNClob(String columnLabel) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }*/

    public String getNString(int columnIndex) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public String getNString(String columnLabel) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
     throw new SQLException("Read-only ResultSet - not supported.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
         throw new SQLException("Read-only ResultSet - not supported.");
    }
}
