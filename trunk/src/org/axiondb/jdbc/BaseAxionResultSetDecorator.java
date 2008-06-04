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
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Decorator for AxionResultSet to disable update and other
 * 
 * @author Jonathan Giron
 * @version 
 */
public abstract class BaseAxionResultSetDecorator implements ResultSet {

    protected BaseAxionResultSetDecorator(ResultSet rs) {
        _rs = rs;
    }

    public boolean absolute(int row) throws SQLException {
        return _rs.absolute(row);
    }

    public void afterLast() throws SQLException {
        _rs.afterLast();
    }

    public void beforeFirst() throws SQLException {
        _rs.beforeFirst();
    }

    public void cancelRowUpdates() throws SQLException {
        _rs.cancelRowUpdates();
    }

    public void clearWarnings() throws SQLException {
        _rs.clearWarnings();
    }

    public void close() throws SQLException {
        _rs.close();
    }
    
    /*public boolean isClosed() throws SQLException {
        return _rs.isClosed();
    }*/

    public void deleteRow() throws SQLException {
        _rs.deleteRow();
    }

    public int findColumn(String columnName) throws SQLException {
        return _rs.findColumn(columnName);
    }

    public boolean first() throws SQLException {
        return _rs.first();
    }

    public Array getArray(int i) throws SQLException {
        return _rs.getArray(i);
    }

    public Array getArray(String colName) throws SQLException {
        return _rs.getArray(colName);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return _rs.getAsciiStream(columnIndex);
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        return _rs.getAsciiStream(columnName);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return _rs.getBigDecimal(columnIndex);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return _rs.getBigDecimal(columnIndex, scale);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return _rs.getBigDecimal(columnName);
    }

    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return _rs.getBigDecimal(columnName, scale);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return _rs.getBinaryStream(columnIndex);
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        return _rs.getBinaryStream(columnName);
    }

    public Blob getBlob(int i) throws SQLException {
        return _rs.getBlob(i);
    }

    public Blob getBlob(String colName) throws SQLException {
        return _rs.getBlob(colName);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return _rs.getBoolean(columnIndex);
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return _rs.getBoolean(columnName);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return _rs.getByte(columnIndex);
    }

    public byte getByte(String columnName) throws SQLException {
        return _rs.getByte(columnName);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return _rs.getBytes(columnIndex);
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return _rs.getBytes(columnName);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return _rs.getCharacterStream(columnIndex);
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        return _rs.getCharacterStream(columnName);
    }

    public Clob getClob(int i) throws SQLException {
        return _rs.getClob(i);
    }

    public Clob getClob(String colName) throws SQLException {
        return _rs.getClob(colName);
    }

    public int getConcurrency() throws SQLException {
        return _rs.getConcurrency();
    }

    public String getCursorName() throws SQLException {
        return _rs.getCursorName();
    }

    public Date getDate(int columnIndex) throws SQLException {
        return _rs.getDate(columnIndex);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return _rs.getDate(columnIndex, cal);
    }

    public Date getDate(String columnName) throws SQLException {
        return _rs.getDate(columnName);
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return _rs.getDate(columnName, cal);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return _rs.getDouble(columnIndex);
    }

    public double getDouble(String columnName) throws SQLException {
        return _rs.getDouble(columnName);
    }

    public int getFetchDirection() throws SQLException {
        return _rs.getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        return _rs.getFetchSize();
    }

    public float getFloat(int columnIndex) throws SQLException {
        return _rs.getFloat(columnIndex);
    }

    public float getFloat(String columnName) throws SQLException {
        return _rs.getFloat(columnName);
    }

    public int getInt(int columnIndex) throws SQLException {
        return _rs.getInt(columnIndex);
    }

    public int getInt(String columnName) throws SQLException {
        return _rs.getInt(columnName);
    }

    public long getLong(int columnIndex) throws SQLException {
        return _rs.getLong(columnIndex);
    }

    public long getLong(String columnName) throws SQLException {
        return _rs.getLong(columnName);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return _rs.getMetaData();
    }

    public Object getObject(int columnIndex) throws SQLException {
        return _rs.getObject(columnIndex);
    }

    public Object getObject(int i, Map map) throws SQLException {
        return _rs.getObject(i, map);
    }

    public Object getObject(String columnName) throws SQLException {
        return _rs.getObject(columnName);
    }

    public Object getObject(String colName, Map map) throws SQLException {
        return _rs.getObject(colName, map);
    }

    public Ref getRef(int i) throws SQLException {
        return _rs.getRef(i);
    }

    public Ref getRef(String colName) throws SQLException {
        return _rs.getRef(colName);
    }

    public int getRow() throws SQLException {
        return _rs.getRow();
    }

    public short getShort(int columnIndex) throws SQLException {
        return _rs.getShort(columnIndex);
    }

    public short getShort(String columnName) throws SQLException {
        return _rs.getShort(columnName);
    }

    public Statement getStatement() throws SQLException {
        return _rs.getStatement();
    }

    public String getString(int columnIndex) throws SQLException {
        return _rs.getString(columnIndex);
    }

    public String getString(String columnName) throws SQLException {
        return _rs.getString(columnName);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return _rs.getTime(columnIndex);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return _rs.getTime(columnIndex, cal);
    }

    public Time getTime(String columnName) throws SQLException {
        return _rs.getTime(columnName);
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return _rs.getTime(columnName, cal);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return _rs.getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return _rs.getTimestamp(columnIndex, cal);
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return _rs.getTimestamp(columnName);
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return _rs.getTimestamp(columnName, cal);
    }

    public int getType() throws SQLException {
        return _rs.getType();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return _rs.getUnicodeStream(columnIndex);
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return _rs.getUnicodeStream(columnName);
    }

    public URL getURL(int columnIndex) throws SQLException {
        return _rs.getURL(columnIndex);
    }

    public URL getURL(String columnName) throws SQLException {
        return _rs.getURL(columnName);
    }

    public SQLWarning getWarnings() throws SQLException {
        return _rs.getWarnings();
    }

    public void insertRow() throws SQLException {
        _rs.insertRow();
    }

    public boolean isAfterLast() throws SQLException {
        return _rs.isAfterLast();
    }

    public boolean isBeforeFirst() throws SQLException {
        return _rs.isBeforeFirst();
    }

    public boolean isFirst() throws SQLException {
        return _rs.isFirst();
    }

    public boolean isLast() throws SQLException {
        return _rs.isLast();
    }

    public boolean last() throws SQLException {
        return _rs.last();
    }

    public void moveToCurrentRow() throws SQLException {
        _rs.moveToCurrentRow();
    }

    public void moveToInsertRow() throws SQLException {
        _rs.moveToInsertRow();
    }

    public boolean next() throws SQLException {
        return _rs.next();
    }

    public boolean previous() throws SQLException {
        return _rs.previous();
    }

    public void refreshRow() throws SQLException {
        _rs.refreshRow();
    }

    public boolean relative(int rows) throws SQLException {
        return _rs.relative(rows);
    }

    public boolean rowDeleted() throws SQLException {
        return _rs.rowDeleted();
    }

    public boolean rowInserted() throws SQLException {
        return _rs.rowInserted();
    }

    public boolean rowUpdated() throws SQLException {
        return _rs.rowUpdated();
    }

    public void setFetchDirection(int direction) throws SQLException {
        _rs.setFetchDirection(direction);
    }

    public void setFetchSize(int rows) throws SQLException {
        _rs.setFetchSize(rows);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        _rs.updateArray(columnIndex, x);
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        _rs.updateArray(columnName, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        _rs.updateAsciiStream(columnIndex, x, length);
    }

    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        _rs.updateAsciiStream(columnName, x, length);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        _rs.updateBigDecimal(columnIndex, x);
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        _rs.updateBigDecimal(columnName, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        _rs.updateBinaryStream(columnIndex, x, length);
    }

    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        _rs.updateBinaryStream(columnName, x, length);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        _rs.updateBlob(columnIndex, x);
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        _rs.updateBlob(columnName, x);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        _rs.updateBoolean(columnIndex, x);
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        _rs.updateBoolean(columnName, x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        _rs.updateByte(columnIndex, x);
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        _rs.updateByte(columnName, x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        _rs.updateBytes(columnIndex, x);
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        _rs.updateBytes(columnName, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        _rs.updateCharacterStream(columnIndex, x, length);
    }

    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        _rs.updateCharacterStream(columnName, reader, length);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        _rs.updateClob(columnIndex, x);
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        _rs.updateClob(columnName, x);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        _rs.updateDate(columnIndex, x);
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        _rs.updateDate(columnName, x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        _rs.updateDouble(columnIndex, x);
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        _rs.updateDouble(columnName, x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        _rs.updateFloat(columnIndex, x);
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        _rs.updateFloat(columnName, x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        _rs.updateInt(columnIndex, x);
    }

    public void updateInt(String columnName, int x) throws SQLException {
        _rs.updateInt(columnName, x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        _rs.updateLong(columnIndex, x);
    }

    public void updateLong(String columnName, long x) throws SQLException {
        _rs.updateLong(columnName, x);
    }

    public void updateNull(int columnIndex) throws SQLException {
        _rs.updateNull(columnIndex);
    }

    public void updateNull(String columnName) throws SQLException {
        _rs.updateNull(columnName);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        _rs.updateObject(columnIndex, x);
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        _rs.updateObject(columnIndex, x, scale);
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        _rs.updateObject(columnName, x);
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        _rs.updateObject(columnName, x, scale);
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        _rs.updateRef(columnIndex, x);
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        _rs.updateRef(columnName, x);
    }

    public void updateRow() throws SQLException {
        _rs.updateRow();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        _rs.updateShort(columnIndex, x);
    }

    public void updateShort(String columnName, short x) throws SQLException {
        _rs.updateShort(columnName, x);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        _rs.updateString(columnIndex, x);
    }

    public void updateString(String columnName, String x) throws SQLException {
        _rs.updateString(columnName, x);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        _rs.updateTime(columnIndex, x);
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        _rs.updateTime(columnName, x);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        _rs.updateTimestamp(columnIndex, x);
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        _rs.updateTimestamp(columnName, x);
    }

    public boolean wasNull() throws SQLException {
        return _rs.wasNull();
    }
    
    private ResultSet _rs;
}
