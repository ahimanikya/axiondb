/*
 * $Id: AxionResultSet.java,v 1.8 2007/04/18 08:19:06 nilesh_apte Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
//import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
//import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
//import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.ConstraintViolationException;
import org.axiondb.DataType;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowDecoratorIterator;
import org.axiondb.Selectable;
import org.axiondb.Transaction;
import org.axiondb.TransactionManager;
import org.axiondb.engine.rowiterators.EmptyRowIterator;
import org.axiondb.engine.rowiterators.RowIteratorRowDecoratorIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.util.ExceptionConverter;

/**
 * A {@link java.sql.ResultSet}implementation.
 *
 * @version $Revision: 1.8 $ $Date: 2007/04/18 08:19:06 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class AxionResultSet implements ResultSet {
    
    private static final int DIR_FORWARD = 1;
    private static final int DIR_REVERSE = -1;
    private static final int DIR_UNKNOWN = 0;
    
    /** What {@link #getBigDecimal}returns when the corresponding value is NULL. */
    private static final BigDecimal NULL_BIGDECIMAL = null;
    
    /** What {@link #getBlob}returns when the corresponding value is NULL. */
    private static final Blob NULL_BLOB = null;
    
    /** What {@link #getBoolean}returns when the corresponding value is NULL. */
    private static final boolean NULL_BOOLEAN = false;
    
    /** What {@link #getByte}returns when the corresponding value is NULL. */
    private static final byte NULL_BYTE = (byte) 0;
    
    /** What {@link #getBytes}returns when the corresponding value is NULL. */
    private static final byte[] NULL_BYTES = null;
    
    /** What {@link #getClob}returns when the corresponding value is NULL. */
    private static final Clob NULL_CLOB = null;
    
    /** What {@link #getDate}returns when the corresponding value is NULL. */
    private static final Date NULL_DATE = null;
    
    /** What {@link #getDouble}returns when the corresponding value is NULL. */
    private static final double NULL_DOUBLE = 0d;
    
    /** What {@link #getFloat}returns when the corresponding value is NULL. */
    private static final float NULL_FLOAT = 0;
    
    /** What {@link #getInt}returns when the corresponding value is NULL. */
    private static final int NULL_INT = 0;
    
    /** What {@link #getLong}returns when the corresponding value is NULL. */
    private static final long NULL_LONG = 0L;
    
    /** What {@link #getCharacterStream}returns when the corresponding value is NULL. */
    private static final Reader NULL_READER = null;
    
    /** What {@link #getShort}returns when the corresponding value is NULL. */
    private static final short NULL_SHORT = (short) 0;
    
    /** What {@link #getBinaryStream}returns when the corresponding value is NULL. */
    private static final InputStream NULL_STREAM = null;
    
    /** What {@link #getString}returns when the corresponding value is NULL. */
    private static final String NULL_STRING = null;
    
    /** What {@link #getTime}returns when the corresponding value is NULL. */
    private static final Time NULL_TIME = null;
    
    /** What {@link #getTimestamp}returns when the corresponding value is NULL. */
    private static final Timestamp NULL_TIMESTAMP = null;
    
    /** What {@link #getURL}returns when the corresponding value is NULL. */
    private static final URL NULL_URL = null;
    
    private static final int USE_DEFAULT_SCALE = Integer.MIN_VALUE;
    
    public static ResultSet createEmptyResultSet(Statement stmt) {
        return new AxionResultSet(new RowIteratorRowDecoratorIterator(EmptyRowIterator.INSTANCE, new RowDecorator(Collections.EMPTY_MAP)),
                new Selectable[0], stmt);
    }
    
    //-------------------------------------------------------------- Attributes
    
    /** @deprecated use {@link #AxionResultSet(RowDecoratorIterator,Selectable,Statement)  */
    public AxionResultSet(RowDecoratorIterator rows, Selectable[] selected) {
        this(rows, selected, null);
    }
    
    public AxionResultSet(RowDecoratorIterator rows, Selectable[] selected, Statement stmt) {
        _rows = rows;
        _selected = selected;
        _meta = new AxionResultSetMetaData(selected);
        setStatement(stmt);
    }
    
    protected AxionResultSet() {
    }
    
    //------------------------------------------------------- ResultSet Methods
    
    public boolean absolute(int row) throws SQLException {
        assertOpen();
        assertScrollable();
        
        if (row < 0) {
            if (row == -1) {
                return last();
            }
            
            afterLast();
            return relative(row);
        } else {
            beforeFirst();
            return relative(row);
        }
    }
    
    public void afterLast() throws SQLException {
        last();
        next();
    }
    
    public void beforeFirst() throws SQLException {
        assertOpen();
        assertScrollable();
        clearInsertOrUpdateIfAny();
        
        try {
            _rows.reset();
            _currentRowIndex = 0; // TODO: this should probably be an attribute of the RowDecoratorIterator
            _afterLast = false;
            _lastDir = DIR_FORWARD;
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        } catch (RuntimeException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public void cancelRowUpdates() throws SQLException {
        assertOpen();
        assertUpdateable();
        assertNotInInsertMode();
        clearUpdateRow();
    }
    
    public void clearWarnings() throws SQLException {
        _warning = null;
    }
    
    public void close() throws SQLException {
        if (null != _transactionManager && null != _transaction) {
            try {
                _transactionManager.commitTransaction(_transaction);
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            }
            _transactionManager = null;
            _transaction = null;
        }
        _closed = true;
        _selected = null;
        _currentRow = null;
        _insertUpdateRow = null;
        _warning = null;
        _updateLock = null;
    }
    
    public void deleteRow() throws SQLException {
        assertOpen();
        
        synchronized (_updateLock) {
            assertUpdateable();
            assertNotInInsertMode();
            assertCurrentRow();
            
            try {
                boolean lastRowDeleted = isLast();
                
                _rows.getIterator().remove();
                clearUpdateRow();
                
                if (lastRowDeleted) {
                    _afterLast = true;
                } else {
                    _currentRowIndex--;
                }
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            } catch (UnsupportedOperationException e) {
                throw new SQLException("Cannot delete row: table/view is not updateable.");
            }
        }
    }
    
    public int findColumn(String colName) throws SQLException {
        assertOpen();
        return getResultSetIndexForColumnName(colName);
    }
    
    public boolean first() throws SQLException {
        beforeFirst();
        return next();
    }
    
    public Array getArray(int i) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Array getArray(String colName) throws SQLException {
        return getArray(getResultSetIndexForColumnName(colName));
    }
    
    public InputStream getAsciiStream(int i) throws SQLException {
        Clob clob = getClob(i);
        if (null == clob) {
            return NULL_STREAM;
        }
        return clob.getAsciiStream();
    }
    
    public InputStream getAsciiStream(String colName) throws SQLException {
        return getAsciiStream(getResultSetIndexForColumnName(colName));
    }
    
    public BigDecimal getBigDecimal(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BIGDECIMAL;
        }
        
        try {
            return getDataType(i).toBigDecimal(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    /** @deprecated See {@link java.sql.ResultSet#getBigDecimal(int,int)} */
    public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BIGDECIMAL;
        }
        try {
            BigInteger bigint = getDataType(i).toBigInteger(value);
            if (null == bigint) {
                return NULL_BIGDECIMAL;
            }
            return new BigDecimal(bigint, scale);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public BigDecimal getBigDecimal(String colName) throws SQLException {
        return getBigDecimal(getResultSetIndexForColumnName(colName));
    }
    
    /** @deprecated See {@link java.sql.ResultSet#getBigDecimal(java.lang.String,int)} */
    public BigDecimal getBigDecimal(String colName, int scale) throws SQLException {
        return getBigDecimal(getResultSetIndexForColumnName(colName), scale);
    }
    
    public InputStream getBinaryStream(int i) throws SQLException {
        Blob blob = getBlob(i);
        if (null == blob) {
            return NULL_STREAM;
        }
        return blob.getBinaryStream();
    }
    
    public InputStream getBinaryStream(String colName) throws SQLException {
        return getBinaryStream(getResultSetIndexForColumnName(colName));
    }
    
    public Blob getBlob(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BLOB;
        }
        try {
            return getDataType(i).toBlob(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public Blob getBlob(String colName) throws SQLException {
        return getBlob(getResultSetIndexForColumnName(colName));
    }
    
    public boolean getBoolean(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BOOLEAN;
        }
        try {
            return getDataType(i).toBoolean(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public boolean getBoolean(String colName) throws SQLException {
        return getBoolean(getResultSetIndexForColumnName(colName));
    }
    
    public byte getByte(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BYTE;
        }
        try {
            return getDataType(i).toByte(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public byte getByte(String colName) throws SQLException {
        return getByte(getResultSetIndexForColumnName(colName));
    }
    
    public byte[] getBytes(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_BYTES;
        }
        try {
            return getDataType(i).toByteArray(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public byte[] getBytes(String colName) throws SQLException {
        return getBytes(getResultSetIndexForColumnName(colName));
    }
    
    public Reader getCharacterStream(int i) throws SQLException {
        Clob clob = getClob(i);
        if (null == clob) {
            return NULL_READER;
        }
        return clob.getCharacterStream();
    }
    
    public Reader getCharacterStream(String colName) throws SQLException {
        return getCharacterStream(getResultSetIndexForColumnName(colName));
    }
    
    public Clob getClob(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_CLOB;
        }
        try {
            return getDataType(i).toClob(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public Clob getClob(String colName) throws SQLException {
        return getClob(getResultSetIndexForColumnName(colName));
    }
    
    public int getConcurrency() throws SQLException {
        return (_stmt != null) ? _stmt.getResultSetConcurrency() : ResultSet.CONCUR_READ_ONLY;
    }
    
    public String getCursorName() throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Date getDate(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_DATE;
        }
        try {
            return getDataType(i).toDate(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public Date getDate(int i, Calendar cal) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Date getDate(String colName) throws SQLException {
        return getDate(getResultSetIndexForColumnName(colName));
    }
    
    public Date getDate(String colName, Calendar cal) throws SQLException {
        return getDate(getResultSetIndexForColumnName(colName), cal);
    }
    
    public double getDouble(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_DOUBLE;
        }
        try {
            return getDataType(i).toDouble(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public double getDouble(String colName) throws SQLException {
        return getDouble(getResultSetIndexForColumnName(colName));
    }
    
    public int getFetchDirection() throws SQLException {
        return FETCH_UNKNOWN;
    }
    
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    public float getFloat(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_FLOAT;
        }
        try {
            return getDataType(i).toFloat(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public float getFloat(String colName) throws SQLException {
        return getFloat(getResultSetIndexForColumnName(colName));
    }
    
    public int getInt(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_INT;
        }
        
        try {
            return getDataType(i).toInt(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public int getInt(String colName) throws SQLException {
        return getInt(getResultSetIndexForColumnName(colName));
    }
    
    public long getLong(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_LONG;
        }
        try {
            return getDataType(i).toLong(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public long getLong(String colName) throws SQLException {
        return getLong(getResultSetIndexForColumnName(colName));
    }
    
    public ResultSetMetaData getMetaData() throws SQLException {
        return _meta;
    }
    
    public Object getObject(int i) throws SQLException {
        return getValue(i);
    }
    
    public Object getObject(int i, Map map) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Object getObject(String colName) throws SQLException {
        return getObject(getResultSetIndexForColumnName(colName));
    }
    
    public Object getObject(String colName, Map map) throws SQLException {
        return getObject(getResultSetIndexForColumnName(colName), map);
    }
    
    public Ref getRef(int i) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Ref getRef(String colName) throws SQLException {
        return getRef(getResultSetIndexForColumnName(colName));
    }
    
    public int getRow() throws SQLException {
        try {
            return _currentRow.getRowIndex() + 1;
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public short getShort(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_SHORT;
        }
        try {
            return getDataType(i).toShort(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public short getShort(String colName) throws SQLException {
        return getShort(getResultSetIndexForColumnName(colName));
    }
    
    public Statement getStatement() throws SQLException {
        return _stmt;
    }
    
    public String getString(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_STRING;
        }
        try {
            return getDataType(i).toString(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public String getString(String colName) throws SQLException {
        return getString(getResultSetIndexForColumnName(colName));
    }
    
    public Time getTime(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_TIME;
        }
        try {
            return getDataType(i).toTime(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public Time getTime(int i, Calendar cal) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Time getTime(String colName) throws SQLException {
        return getTime(getResultSetIndexForColumnName(colName));
    }
    
    public Time getTime(String colName, Calendar cal) throws SQLException {
        return getTime(getResultSetIndexForColumnName(colName), cal);
    }
    
    public Timestamp getTimestamp(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_TIMESTAMP;
        }
        try {
            return getDataType(i).toTimestamp(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public Timestamp getTimestamp(int i, Calendar cal) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public Timestamp getTimestamp(String colName) throws SQLException {
        return getTimestamp(getResultSetIndexForColumnName(colName));
    }
    
    public Timestamp getTimestamp(String colName, Calendar cal) throws SQLException {
        return getTimestamp(getResultSetIndexForColumnName(colName), cal);
    }
    
    public int getType() throws SQLException {
        return (_stmt != null) ? _stmt.getResultSetType() : ResultSet.TYPE_FORWARD_ONLY;
    }
    
    /** @deprecated See {@link java.sql.ResultSet#getUnicodeStream} */
    public InputStream getUnicodeStream(int i) throws SQLException {
        String val = getString(i);
        if (null == val) {
            return NULL_STREAM;
        }
        try {
            return new ByteArrayInputStream(val.getBytes("UnicodeBig"));
        } catch (UnsupportedEncodingException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    /** @deprecated See {@link java.sql.ResultSet#getUnicodeStream} */
    public InputStream getUnicodeStream(String colName) throws SQLException {
        return getUnicodeStream(getResultSetIndexForColumnName(colName));
    }
    
    public URL getURL(int i) throws SQLException {
        Object value = getValue(i);
        if (null == value) {
            return NULL_URL;
        }
        try {
            return getDataType(i).toURL(value);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public URL getURL(String colName) throws SQLException {
        return getURL(getResultSetIndexForColumnName(colName));
    }
    
    public SQLWarning getWarnings() throws SQLException {
        return _warning;
    }
    
    public void insertRow() throws SQLException {
        assertOpen();
        
        synchronized (_updateLock) {
            assertUpdateable();
            assertInInsertMode();
            assertUpdateRowExists();
            assertCurrentRow();
            
            try {
                // Add a copy of the _insertUpdateRow to allow it to be reused in subsequent
                // inserts.
                _rows.getIterator().add(new SimpleRow(_insertUpdateRow));
            } catch (AxionException e) {
                handleExceptionOnInsertUpdate(e, "Cannot insert row.");
            } catch (UnsupportedOperationException e) {
                throw new SQLException("Cannot insert row - table/view is not updateable.");
            }
        }
    }
    
    public boolean isAfterLast() throws SQLException {
        return !(_rows.getIterator().isEmpty()) && _afterLast;
    }
    
    public boolean isBeforeFirst() throws SQLException {
        return !(_rows.getIterator().isEmpty()) && 0 == _currentRowIndex;
    }
    
    public boolean isFirst() throws SQLException {
        return !(_rows.getIterator().isEmpty()) && 1 == _currentRowIndex;
    }
    
    public boolean isLast() throws SQLException {
        assertOpen();
        
        boolean isLastRow = false;
        if (!(_rows.getIterator().isEmpty()) && !_afterLast && _currentRowIndex != 0) {
            isLastRow = !_rows.hasNext();
        }
        return isLastRow;
    }
    
    public boolean last() throws SQLException {
        assertOpen();
        assertScrollable();
        
        _lastDir = DIR_FORWARD;
        clearInsertOrUpdateIfAny();
        
        RowDecorator row = null;
        try {
            row = _rows.last();
            if (row != null) {
                _currentRow = row;
                _currentRowIndex = _currentRow.getRowIndex() + 1;
                return true;
            }
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
        return false;
    }
    
    public void moveToCurrentRow() throws SQLException {
        assertOpen();
        assertUpdateable();
        clearInsertRow();
    }
    
    public void moveToInsertRow() throws SQLException {
        assertOpen();
        assertUpdateable();
        createInsertRow();
    }
    
    public boolean next() throws SQLException {
        assertOpen();
        clearInsertOrUpdateIfAny();
        
        if (_maxRows > 0 && _currentRowIndex >= _maxRows) {
            _afterLast = true;
            return false;
        }
        
        boolean result = false;
        try {
            result = _rows.hasNext();
        } catch (RuntimeException re) {
            throw ExceptionConverter.convert(re);
        }
        
        if (result) {
            try {
                if (DIR_REVERSE == _lastDir) {
                    _rows.next();
                }
                _lastDir = DIR_FORWARD;
                
                if(_rows.hasNext()) {
                    _afterLast = false;
                    _currentRow = _rows.next();
                    _currentRowIndex = _currentRow.getRowIndex() + 1;
                } else {
                    _currentRowIndex++;
                    _afterLast = true;
                }
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            }
        } else {
            if (!_afterLast) {
                _currentRowIndex++;
            }
            _afterLast = true;
        }
        
        return result;
    }
    
    public boolean previous() throws SQLException {
        assertOpen();
        assertScrollable();
        clearInsertOrUpdateIfAny();
        
        if (_currentRowIndex <= 0) {
            return false;
        }
        
        boolean result = false;
        try {
            result = _rows.hasPrevious();
        } catch (RuntimeException re) {
            throw ExceptionConverter.convert(re);
        }
        
        if (result) {
            try {
                if (_afterLast && _rows.hasCurrent()) {
                    _currentRow = _rows.current();
                    _currentRowIndex = _currentRow.getRowIndex() + 1;
                } else if (isFirst()) {
                    beforeFirst();
                    _afterLast = false;
                    return false;
                } else {
                    if (!_afterLast && DIR_FORWARD == _lastDir) {
                        _rows.previous();
                    }
                    _lastDir = DIR_REVERSE;
                    _currentRow = _rows.previous();
                    _currentRowIndex = _currentRow.getRowIndex() + 1;
                }
                
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            }
        } else {
            beforeFirst();
        }
        
        _afterLast = false;
        return result;
    }
    
    public void refreshRow() throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public boolean relative(int rows) throws SQLException {
        assertOpen();
        assertScrollable();
        
        if (rows < 0) {
            return previous(-1 * rows);
        }
        return next(rows);
    }
    
    public boolean rowDeleted() throws SQLException {
        return false;
    }
    
    public boolean rowInserted() throws SQLException {
        return false;
    }
    
    public boolean rowUpdated() throws SQLException {
        return false;
    }
    
    public void setFetchDirection(int direction) throws SQLException {
        // fetchDirection is just a hint
    }
    
    public void setFetchSize(int size) throws SQLException {
        // fetch size is just a hint
    }
    
    public void setMaxRows(int max) {
        _maxRows = max;
    }
    
    public void setTransaction(TransactionManager manager, Transaction transaction) {
        _transactionManager = manager;
        _transaction = transaction;
    }
    
    /** Currently unsupported. */
    public void updateArray(int arg0, Array arg1) throws SQLException {
        throw new SQLException("updateArray is currently not supported");
    }
    
    public void updateArray(String colName, Array arg1) throws SQLException {
        updateArray(getResultSetIndexForColumnName(colName), arg1);
    }
    
    public void updateAsciiStream(int i, InputStream in, int length) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public void updateAsciiStream(String colName, InputStream in, int length) throws SQLException {
        updateAsciiStream(getResultSetIndexForColumnName(colName), in, length);
    }
    
    public void updateBigDecimal(int i, BigDecimal value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateBigDecimal(String colName, BigDecimal value) throws SQLException {
        updateBigDecimal(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateBinaryStream(int i, InputStream value, int length) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public void updateBinaryStream(String colName, InputStream value, int length)
    throws SQLException {
        updateBinaryStream(getResultSetIndexForColumnName(colName), value, length);
    }
    
    /** Currently unsupported. */
    public void updateBlob(int arg0, Blob arg1) throws SQLException {
        throw new SQLException("updateBlob is currently not supported");
    }
    
    public void updateBlob(String colName, Blob arg1) throws SQLException {
        updateBlob(getResultSetIndexForColumnName(colName), arg1);
    }
    
    public void updateBoolean(int i, boolean value) throws SQLException {
        setValue(i, Boolean.valueOf(value), USE_DEFAULT_SCALE);
    }
    
    public void updateBoolean(String colName, boolean value) throws SQLException {
        updateBoolean(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateByte(int i, byte value) throws SQLException {
        setValue(i, new Byte(value), USE_DEFAULT_SCALE);
    }
    
    public void updateByte(String colName, byte value) throws SQLException {
        updateByte(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateBytes(int i, byte[] value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateBytes(String colName, byte[] value) throws SQLException {
        updateBytes(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateCharacterStream(int i, Reader value, int length) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public void updateCharacterStream(String colName, Reader value, int length) throws SQLException {
        updateCharacterStream(getResultSetIndexForColumnName(colName), value, length);
    }
    
    /** Currently unsupported. */
    public void updateClob(int arg0, Clob arg1) throws SQLException {
        throw new SQLException("updateClob is currently not supported");
    }
    
    public void updateClob(String colName, Clob arg1) throws SQLException {
        updateClob(getResultSetIndexForColumnName(colName), arg1);
    }
    
    public void updateDate(int i, Date value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateDate(String colName, Date value) throws SQLException {
        updateDate(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateDouble(int i, double value) throws SQLException {
        setValue(i, new Double(value), USE_DEFAULT_SCALE);
    }
    
    public void updateDouble(String colName, double value) throws SQLException {
        updateDouble(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateFloat(int i, float value) throws SQLException {
        setValue(i, new Float(value), USE_DEFAULT_SCALE);
    }
    
    public void updateFloat(String colName, float value) throws SQLException {
        updateFloat(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateInt(int i, int value) throws SQLException {
        setValue(i, new Integer(value), USE_DEFAULT_SCALE);
    }
    
    public void updateInt(String colName, int value) throws SQLException {
        updateInt(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateLong(int i, long value) throws SQLException {
        setValue(i, new Long(value), USE_DEFAULT_SCALE);
    }
    
    public void updateLong(String colName, long value) throws SQLException {
        updateLong(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateNull(int i) throws SQLException {
        setValue(i, null, USE_DEFAULT_SCALE);
    }
    
    public void updateNull(String colName) throws SQLException {
        updateNull(getResultSetIndexForColumnName(colName));
    }
    
    public void updateObject(int i, Object value) throws SQLException {
        updateObject(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateObject(int i, Object value, int scale) throws SQLException {
        setValue(i, value, scale);
    }
    
    public void updateObject(String colName, Object value) throws SQLException {
        updateObject(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateObject(String colName, Object value, int scale) throws SQLException {
        updateObject(getResultSetIndexForColumnName(colName), value, scale);
    }
    
    /** Currently unsupported. */
    public void updateRef(int arg0, Ref arg1) throws SQLException {
        throw new SQLException("updateRef is currently not supported");
    }
    
    public void updateRef(String colName, Ref arg1) throws SQLException {
        updateRef(getResultSetIndexForColumnName(colName), arg1);
    }
    
    public void updateRow() throws SQLException {
        assertOpen();
        
        synchronized (_updateLock) {
            assertUpdateable();
            assertNotInInsertMode();
            assertUpdateRowExists();
            assertCurrentRow();
            
            try {
                _rows.getIterator().set(_insertUpdateRow);
                _currentRow = _rows.current();
            } catch (AxionException e) {
                handleExceptionOnInsertUpdate(e, "Cannot update row.");
            } catch (UnsupportedOperationException e) {
                throw new SQLException("Cannot update row: table/view is not updateable.");
            }
            
            _insertUpdateRow = null;
        }
    }
    
    public void updateShort(int i, short value) throws SQLException {
        setValue(i, new Short(value), USE_DEFAULT_SCALE);
    }
    
    public void updateShort(String colName, short value) throws SQLException {
        updateShort(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateString(int i, String value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateString(String colName, String value) throws SQLException {
        updateString(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateTime(int i, Time value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateTime(String colName, Time value) throws SQLException {
        updateTime(getResultSetIndexForColumnName(colName), value);
    }
    
    public void updateTimestamp(int i, Timestamp value) throws SQLException {
        setValue(i, value, USE_DEFAULT_SCALE);
    }
    
    public void updateTimestamp(String colName, Timestamp value) throws SQLException {
        updateTimestamp(getResultSetIndexForColumnName(colName), value);
    }
    
    public boolean wasNull() throws SQLException {
        return _wasNull;
    }
    
    protected void setStatement(Statement stmt) {
        _stmt = stmt;
    }
    
    /** Throw a {@link SQLException}if there is no {@link #_currentRow}. */
    private final void assertCurrentRow() throws SQLException {
        if (null == _currentRow) {
            throw new SQLException("No current row");
        }
    }
    
    private final void assertInInsertMode() throws SQLException {
        if (!_insertMode) {
            throw new SQLException("Not in insert mode - operation failed.");
        }
    }
    
    private final void assertNotInInsertMode() throws SQLException {
        if (_insertMode) {
            throw new SQLException("In insert mode - operation failed.");
        }
    }
    
    private final void assertOpen() throws SQLException {
        if (_closed) {
            throw new SQLException("Already closed");
        }
    }
    
    private final void assertScrollable() throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY == getType()) {
            throw new SQLException("Invalid cursor movement request - ResultSet is forward-only.");
        }
    }
    
    private final void assertUpdateable() throws SQLException {
        if (ResultSet.CONCUR_UPDATABLE != getConcurrency()) {
            throw new SQLException("Not an updateable ResultSet - operation failed.");
        }
    }
    
    private final void assertUpdateRowExists() throws SQLException {
        if (null == _insertUpdateRow) {
            throw new SQLException("No update row");
        }
    }
    
    private void clearInsertOrUpdateIfAny() throws SQLException {
        if (_insertUpdateRow != null) {
            if (_insertMode) {
                moveToCurrentRow();
            } else {
                cancelRowUpdates();
            }
        }
    }
    
    private void clearInsertRow() {
        synchronized (_updateLock) {
            clearUpdateRow();
            _insertMode = false;
        }
    }
    
    private void clearUpdateRow() {
        synchronized (_updateLock) {
            _insertUpdateRow = null;
        }
    }
    
    private Row createInsertRow() throws SQLException {
        synchronized (_updateLock) {
            _insertMode = true;
            _insertUpdateRow = new SimpleRow(_meta.getColumnCount());
        }
        return _insertUpdateRow;
    }
    
    /**
     * Obtain the DataType for the given 1-based (ResultSet) index
     */
    private DataType getDataType(int num) throws SQLException {
        Selectable sel = _selected[num - 1];
        return sel.getDataType();
    }
    
    private Row getOrCreateUpdateRow() throws SQLException {
        assertCurrentRow();
        
        synchronized (_updateLock) {
            if (null == _insertUpdateRow) {
                _insertUpdateRow = new SimpleRow(_currentRow.getRow());
            }
            return _insertUpdateRow;
        }
    }
    
    //------------------------------------------------------------ Private Util
    
    /**
     * Get the 1-based ResultSet index for the specified column name.
     */
    private int getResultSetIndexForColumnName(String columnname) throws SQLException {
        ColumnIdentifier id = null;
        columnname = columnname.toUpperCase();
        for (int i = 0; i < _selected.length; i++) {
            if (_selected[i] instanceof ColumnIdentifier) {
                id = (ColumnIdentifier) (_selected[i]);
                if (columnname.equals(id.getName()) || columnname.equals(id.getAlias())) {
                    return (i + 1);
                }
            }
        }
        throw new SQLException("No column named " + columnname + " found.");
    }
    
    /**
     * Obtain the value from the current row for the given 1-based (ResultSet) index, and
     * convert it according to the corresponding {@link DataType}
     */
    private Object getValue(int num) throws SQLException {
        assertOpen();
        Object val = null;
        
        if (_insertMode) {
            val = _insertUpdateRow.get(num - 1);
        } else {
            assertCurrentRow();
            
            Selectable sel = _selected[num - 1];
            try {
                val = sel.evaluate(_currentRow);
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            }
        }
        
        _wasNull = (null == val);
        return val;
    }
    
    
    private void handleExceptionOnInsertUpdate(AxionException e, String defaultMessage)
    throws SQLException {
        if (!"99999".equals(e.getSQLState())) {
            if (e instanceof ConstraintViolationException) {
                if ("NOT NULL".equals(((ConstraintViolationException) e).getConstraintType())) {
                    throw new SQLException(e.getMessage(), "22004");
                }
            }
            throw new SQLException(e.getMessage(), e.getSQLState());
        } else {
            throw new SQLException(defaultMessage);
        }
    }
    
    /** move forward n rows, i.e., call next() n times */
    private boolean next(int n) throws SQLException {
        boolean result = (null != _currentRow);
        for (int i = 0; i < n; i++) {
            result = next();
        }
        return result;
    }
    
    /** move backward n rows, i.e., call previous() n times */
    private boolean previous(int n) throws SQLException {
        boolean result = (null != _currentRow);
        for (int i = 0; i < n; i++) {
            result = previous();
        }
        return result;
    }
    
    /**
     * Sets value of the given row in the update row cache for the given 1-based (ResultSet) index,
     * converting it as appropriate to the corresponding {@link DataType}
     *
     * @param num (1-based) column index
     * @param value new column value
     * @param scale for {@link Types#DECIMAL} or {@link Types#NUMERIC} types, this is the number of
     * digits after the decimal point.  For all other types this value will be ignored.
     *
     * @throws SQLException
     */
    private void setValue(int num, Object value, int scale) throws SQLException {
        assertUpdateable();
        
        DataType type = getDataType(num);
        synchronized (_updateLock) {
            Row row = (_insertMode) ? _insertUpdateRow : getOrCreateUpdateRow();
            try {
                row.set(num - 1, type.convert(value));
            } catch (UnsupportedOperationException e) {
                ExceptionConverter.convert(e);
            } catch (AxionException e) {
                ExceptionConverter.convert(e);
            }
        }
    }
    
    /*public RowId getRowId(int columnIndex) throws SQLException {
        return new AxionRowId(_currentRow.getRow().getIdentifier());
    }
    
    public RowId getRowId(String columnLabel) throws SQLException {
        return new AxionRowId(_currentRow.getRow().getIdentifier());
    }
    
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
    }
    
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
    }
    
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    public boolean isClosed() throws SQLException {
        return _closed;
    }
    
    public void updateNString(int columnIndex, String nString) throws SQLException {
    }
    
    public void updateNString(String columnLabel, String nString) throws SQLException {
    }
    
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    }
    
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    }
    
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    }
    
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    }
    */
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    }
    
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    }
    
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    }
    
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    }
    
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    }
    
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    }
    
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    }
    
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    }
    
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    }
    
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    }
    
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    }
    
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    }
    
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    }
    
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    }
    
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    }
    
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }
    
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    }
    
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    }
    
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    }
    
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    }
    
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    }
    
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }
    
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    }
    
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    }
    
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
    }
    
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
    }
    
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    }
    
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    }
    
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported");
    }
    
    protected RowDecoratorIterator _rows = null;
    private boolean _afterLast = false;
    private boolean _closed = false;
    private RowDecorator _currentRow = null;
    private int _currentRowIndex = 0;
    private boolean _insertMode = false;
    private Row _insertUpdateRow = null;
    private int _lastDir = DIR_UNKNOWN;
    private int _maxRows = 0;
    private ResultSetMetaData _meta = null;
    private Selectable[] _selected = null;
    private Statement _stmt = null;
    
    private Transaction _transaction = null;
    private TransactionManager _transactionManager = null;
    private Object _updateLock = new Object();
    private SQLWarning _warning = null;
    
    /** Whether the last value returned was NULL. */
    private boolean _wasNull = false;
}
