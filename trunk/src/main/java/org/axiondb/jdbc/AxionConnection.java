/*
 * 
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

package org.axiondb.jdbc;

import java.io.File;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
//import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
//import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
//import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Transaction;
import org.axiondb.TransactionConflictException;
import org.axiondb.engine.Databases;
import org.axiondb.util.ExceptionConverter;

/**
 * A {@link Connection} implementation.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Jonathan Giron
 */
public class AxionConnection implements Connection {
    protected AxionConnection(String name, File path, String url) throws AxionException {
        setUrl(url);
        setDatabase(Databases.getOrCreateDatabase(name, path));
    }

    public AxionConnection(Database db, String url) {
        setDatabase(db);
        setUrl(url);
    }

    public AxionConnection(Database db) {
        this(db, null);
    }

    public void clearWarnings() throws SQLException {
        _warning = null;
    }

    public void close() throws SQLException {
        if (isClosed()) {
            // "Calling the method close on a Connection object that is already closed is
            // a no-op"
        } else {
            if ((!(_db.getTransactionManager().isShutdown()))) {
                rollback(false);
            }
            try {
                _db.checkpoint();
            } catch (AxionException e) {
                throw ExceptionConverter.convert(e);
            }
            _db = null;
        }
    }

    public void commit() throws SQLException {
        commit(true);
    }

    public void rollback() throws SQLException {
        rollback(true);
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        resultSetType = validateResultSetType(resultSetType);
        resultSetConcurrency = validateResultSetConcurrency(resultSetConcurrency);
        return new AxionStatement(this, resultSetType, resultSetConcurrency);
    }

    public boolean getAutoCommit() throws SQLException {
        return _autoCommit;
    }

    public String getCatalog() throws SQLException {
        return "";
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new AxionDatabaseMetaData(this, _db);
    }

    public int getTransactionIsolation() throws SQLException {
        return _isolationLevel;
    }

    public Map getTypeMap() throws SQLException {
        return Collections.EMPTY_MAP;
    }

    public SQLWarning getWarnings() throws SQLException {
        if (isClosed()) {
            // "[Connection.getWarnings] may not be called on a closed connection; doing
            // so will cause an SQLException to be thrown."
            throw new SQLException("Already closed");
        }
        return _warning;
    }

    public boolean isClosed() throws SQLException {
        return (_db == null);
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("not supported");
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        resultSetType = validateResultSetType(resultSetType);
        resultSetConcurrency = validateResultSetConcurrency(resultSetConcurrency);
        return new AxionPreparedStatement(this, sql, resultSetType, resultSetConcurrency); 
    }

    /**
     * Validates that the given result set type is supported by Axion, returning the closest
     * appropriate type if not supported.
     * 
     * @param resultSetType type to validate, one of {@link ResultSet#TYPE_FORWARD_ONLY},
     * {@link ResultSet#TYPE_SCROLL_SENSITIVE} or {@link ResultSet#TYPE_SCROLL_INSENSITIVE}
     * @return <code>resultSetType</code> if it is supported, or the closest supporting 
     * result set type if not.
     */
    private int validateResultSetType(int resultSetType) throws SQLException {
        switch (resultSetType) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                break;
            
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                createAndAddSQLWarning("Result set type TYPE_SCROLL_INSENSITIVE is currently not supported; returning TYPE_SCROLL_SENSITIVE instead.");
                resultSetType = ResultSet.TYPE_SCROLL_SENSITIVE;
                break;

            default:
                throw new SQLException("Unknown or unsupported result set type: " + resultSetType);
        }
        
        return resultSetType;
    }

    /**
     * Validates that the given concurrency type is supported by Axion, returning the closest
     * appropriate concurrency type if not supported.
     * 
     * @param resultSetConcurrency concurrency type to validate, one of {@link ResultSet#CONCUR_READ_ONLY}
     * or {@link ResultSet#CONCUR_UPDATABLE}.
     * @return <code>resultSetConcurrency</code> if it is supported, or the closest supporting 
     * concurrency type if not.
     */
    private int validateResultSetConcurrency(int resultSetConcurrency) throws SQLException {
        switch (resultSetConcurrency) {
            case ResultSet.CONCUR_READ_ONLY:
            case ResultSet.CONCUR_UPDATABLE:
                break;
            
            default:
                throw new SQLException("Unknown or unsupported concurrency type: " + resultSetConcurrency);
        }
        
        return resultSetConcurrency;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        _autoCommit = autoCommit;
    }

    public void setCatalog(String catalog) throws SQLException {
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (level == Connection.TRANSACTION_SERIALIZABLE) {
            _isolationLevel = level;
        } else {
            throw new SQLException("Transcation isolation level " + level + " is not supported.");
        }
    }

    public void setTypeMap(Map types) throws SQLException {
    }

    public String getURL() {
        return _url;
    }

    // **** HELPER METHODS ****

    public org.axiondb.Database getDatabase() {
        return _db;
    }

    Transaction getCurrentTransaction() throws AxionException {
        if (null == _currentTransaction) {
            _currentTransaction = _db.getTransactionManager().createTransaction();
        }
        return _currentTransaction;
    }

    Transaction forgetCurrentTransaction() {
        Transaction temp = _currentTransaction;
        _currentTransaction = null;
        return temp;
    }

    void commitIfAuto() throws SQLException {
        if (getAutoCommit() && !(_db.getTransactionManager().isShutdown())) {
            commit(false);
        }
    }
    

    /**
     * Adds the given SQLWarning instance to the current warning chain.
     * 
     * @param newWarning SQLWarning instance to add to the current warning chain.
     */
    protected synchronized void addWarning(SQLWarning newWarning) {
        if (newWarning != null) {
            if (_warning != null) {
                _warning.setNextWarning(newWarning);
              } else {
                  _warning = newWarning;
              }
        }
    }
    
    /**
     * Creates a new SQLWarning using the given String and adds it to the current SQLWarning chain.   
     * 
     * @param string String to use in creating the new SQLWarning instance
     */
    private void createAndAddSQLWarning(String string) {
        addWarning(new SQLWarning(string));
    }    

    private void assertNotAutoCommit() throws SQLException {
        if (getAutoCommit()) {
            throw new SQLException("This method should only be used when auto-commit mode has been disabled.");
        }
    }

    private void commit(boolean checkmode) throws SQLException {
        if (checkmode) {
            assertNotAutoCommit();
        }
        try {
            if (null != _currentTransaction) {
                _db.getTransactionManager().commitTransaction(_currentTransaction);
                _currentTransaction = null;
            }
        } catch (TransactionConflictException e) {
            throw ExceptionConverter.convert(new AxionException(e, 51000));
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        } 
    }

    private void rollback(boolean checkmode) throws SQLException {
        if (checkmode) {
            assertNotAutoCommit();
        }
        try {
            if (null != _currentTransaction) {
                _db.getTransactionManager().abortTransaction(_currentTransaction);
                _currentTransaction = null;
            }
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }

    private Database _db;
    private String _url;
    private Transaction _currentTransaction;
    private int _isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
    private boolean _autoCommit = true;
    private SQLWarning _warning;

    /** Currently unsupported. */
    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
        throw new SQLException("createStatement(int,int,int) is currently not supported");
    }

    /** Currently unsupported. */
    public int getHoldability() throws SQLException {
        throw new SQLException("getHoldability is currently not supported");
    }

    /** Currently unsupported. */
    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        throw new SQLException("prepareCall(String,int,int,int) is currently not supported");
    }

    /** Currently unsupported. */
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        throw new SQLException("prepareStatement(String,int,int,int) is currently not supported");
    }

    /** Currently unsupported. */
    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        throw new SQLException("prepareStatement(String,int) is currently not supported");
    }

    /** Currently unsupported. */
    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("prepareStatement(String,int[]) is currently not supported");
    }

    /** Currently unsupported. */
    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("prepareStatement(String,String[]) is currently not supported");
    }

    /** Currently unsupported. */
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        throw new SQLException("releaseSavepoint(Savepoint) is currently not supported");
    }

    /** Currently unsupported. */
    public void rollback(Savepoint arg0) throws SQLException {
        throw new SQLException("rollback(Savepoint) is currently not supported");
    }

    /** Currently unsupported. */
    public void setHoldability(int arg0) throws SQLException {
        throw new SQLException("setHoldability(int) is currently not supported");
    }

    /** Currently unsupported. */
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("setSavepoint is currently not supported");
    }

    /** Currently unsupported. */
    public Savepoint setSavepoint(String arg0) throws SQLException {
        throw new SQLException("setSavepoint(String) is currently not supported");
    }

    private void setUrl(String url) {
        _url = url;
    }

    private void setDatabase(Database db) {
        _db = db;
    }

    public Clob createClob() throws SQLException {
        throw new SQLException("Unsupported.");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException("Unsupported.");
    }

    /*public NClob createNClob() throws SQLException {
        throw new SQLException("Unsupported.");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Unsupported.");
    }*/

    public boolean isValid(int timeout) throws SQLException {
        return !isClosed();
    }

    /*public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }*/

    public String getClientInfo(String name) throws SQLException {
        return "";
    }

    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("Unsupported.");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("Unsupported.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Unsupported.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Unsupported.");
    }    
}
