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
//import java.sql.NClob;
import java.sql.ResultSet;
//import java.sql.RowId;
import java.sql.SQLException;
//import java.sql.SQLXML;

/**
 *
 * @author Jonathan Giron
 * @version 
 */
public final class ForwardOnlyResultSet extends BaseAxionResultSetDecorator {
    
    /**
     * @param rs ResultSet that needs to be decorated
     */
    public ForwardOnlyResultSet(ResultSet rs) {
        super(rs);
    }
    
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public void afterLast() throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public void beforeFirst() throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public boolean first() throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public boolean last() throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public boolean previous() throws SQLException {
        throw new SQLException("Forward-only ResultSet - not supported.");
    }
    
    public boolean relative(int rows) throws SQLException {
        if (rows < 0) {
            throw new SQLException("Forward-only ResultSet - not supported.");
        }
        
        return super.relative(rows);
    }
    
    public void setFetchDirection(int direction) throws SQLException {
        if (ResultSet.FETCH_FORWARD == direction) {
            super.setFetchDirection(direction);
        } else {
            throw new SQLException("Forward-only ResultSet - not supported.");
        }
    }
    
    /*public RowId getRowId(int columnIndex) throws SQLException {
        return new AxionRowId(getRow());
    }
    
    public RowId getRowId(String columnLabel) throws SQLException {
        return new AxionRowId(getRow());
    }
    
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    /*public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }*/
    
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
}

