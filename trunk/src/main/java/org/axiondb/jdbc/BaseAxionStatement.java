/*
 * 
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.parser.AxionSqlParser;
import org.axiondb.util.ExceptionConverter;

/**
 * Abstract base {@link Statement}implementation.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */
public abstract class BaseAxionStatement implements Statement {

    protected BaseAxionStatement(AxionConnection conn) throws SQLException {
        _conn = conn;
        _parser = new AxionSqlParser();
    }

    // ------------------------------------------------------------------------

    public void close() throws SQLException {
        // Per JDBC API subsequent calls to close() on an already-closed Statement should
        // be silently ignored.
        if (!_closed) {
            closeCurrentResultSet();
            clearConnection();

            clearBatchContext();
            _batchContext = null;
            _closed = true;
        }
    }

    public Connection getConnection() throws SQLException {
        return _conn;
    }

    public int getMaxRows() throws SQLException {
        return _maxRows;
    }

    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("MaxRows should be non-negative");
        }
        _maxRows = max;
    }

    // ------------------------------------------------------------------------

    protected final void assertOpen() throws SQLException {
        if (_closed) {
            throw new SQLException("Already closed.");
        }
    }

    protected final void clearConnection() {
        _conn = null;
    }
    
    public boolean isClosed() {
        return _closed;
    }

    protected void clearCurrentResult() throws SQLException {
        clearCurrentUpdateCount();
        closeCurrentResultSet();
    }

    protected int clearCurrentUpdateCount() {
        int count = getCurrentUpdateCount();
        setCurrentUpdateCount(-1);
        return count;
    }

    protected void closeCurrentResultSet() throws SQLException {
        try {
            if (null != _rset) {
                _rset.close();
            }
        } finally {
            _rset = null;
        }
    }

    protected final AxionConnection getAxionConnection() throws SQLException {
        return (AxionConnection) (getConnection());
    }

    protected final AxionResultSet getCurrentResultSet() {
        return _rset;
    }

    protected final int getCurrentUpdateCount() {
        return _updateCount;
    }

    protected final Database getDatabase() throws AxionException {
        return _conn.getCurrentTransaction();
    }

    protected final boolean hasCurrentResultSet() {
        return (null != _rset);
    }
    
    public AxionCommand parseCommand(String sql) throws SQLException {
        try {
            return _parser.parse(sql);
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        } catch (RuntimeException e) {
            throw ExceptionConverter.convert(e);
        }
    }

    protected void setCurrentResult(boolean isrset, AxionCommand cmd) {
        if (isrset) {
            setCurrentResultSet((AxionResultSet) cmd.getResultSet());
        } else {
            setCurrentUpdateCount(cmd.getEffectedRowCount());
        }
    }

    /**
     * @param rset the non- <code>null</code> instance to set current {@link ResultSet}
     *        to
     * @see #clearCurrentResult
     */
    protected void setCurrentResultSet(AxionResultSet rset) {
        rset.setMaxRows(_maxRows);
        rset.setStatement(this);
        _rset = rset;
    }

    protected final void setCurrentUpdateCount(int count) {
        _updateCount = count;
    }

    protected final void addToBatchContext(Object obj) throws SQLException {
        if (_batchContext == null) {
            _batchContext = new ArrayList();
        }
        _batchContext.add(obj);
    }

    protected final void clearBatchContext() {
        if (_batchContext != null) {
            _batchContext.clear();
        }
    }

    protected final Iterator getBatchContext() {
        if (_batchContext != null) {
            return _batchContext.iterator();
        } else {
            return Collections.EMPTY_LIST.iterator();
        }
    }

    protected final int getBatchCount() {
        if (_batchContext != null) {
            return _batchContext.size();
        } else {
            return 0;
        }
    }

    // ------------------------------------------------------------------------

    protected ArrayList _batchContext;
    private AxionSqlParser _parser;

    protected SQLWarning _warning;
    private boolean _closed = false;
    protected AxionConnection _conn;

    private int _maxRows = 0;

    private AxionResultSet _rset;
    private int _updateCount = -1;
}
