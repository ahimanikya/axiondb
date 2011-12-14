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

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Iterator;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.util.ExceptionConverter;

/** 
 * A {@link Statement} implementation.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */
public class AxionStatement extends BaseAxionStatement implements Statement {

    protected AxionStatement(AxionConnection conn) throws SQLException {
        super(conn);
    }
    
    protected AxionStatement(AxionConnection conn, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn);
        _type = resultSetType;
        _concurrency = resultSetConcurrency;
    }

    public void addBatch(String sql) throws SQLException {
        if(getBatchCount() == 0) {
            clearCurrentResult();
        }
        addToBatchContext(parseCommand(sql));
    }

    public void cancel() throws SQLException {
        throw new SQLException("cancel is not supported");
    }

    public void clearBatch() throws SQLException {
        _batchContext.clear();
        _batchContext.trimToSize();
    }

    public void clearWarnings() throws SQLException {
        _warning = null;
    }

    public int[] executeBatch() throws SQLException {
        SQLException exception = null;
        int[] results = new int[getBatchCount()];
        int i = 0;
        for(Iterator iter = getBatchContext(); iter.hasNext(); i++) {
            AxionCommand cmd = (AxionCommand)iter.next();
            try {
                results[i] = executeUpdate(cmd);
            } catch (SQLWarning w) {
                addWarning(w);
            } catch (SQLException e) {
                exception = e;
                results[i] = EXECUTE_FAILED;
            } 
        }
        clearBatchContext();
        if (null != exception) {
            throw new BatchUpdateException(exception.getMessage(),results);
        }
        return results;
    }

    /**
     * Adds the given SQLWarning to the current chain of SQLWarnings, or sets it as the first
     * in the chain. 
     * 
     * @param w SQLWarning to be added to the warning chain
     */
    protected synchronized void addWarning(SQLWarning newWarning) {
        if (_warning != null) {
          _warning.setNextWarning(newWarning);
        } else {
            _warning = newWarning;
        }
    }

    public boolean execute(String sql) throws SQLException {
        clearCurrentResult();
        return execute(parseCommand(sql));
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        clearCurrentResult();
        return executeQuery(parseCommand(sql));
    }

    public int executeUpdate(String sql) throws SQLException {
        clearCurrentResult();
        return executeUpdate(parseCommand(sql));
    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    public boolean getMoreResults() throws SQLException {
        closeCurrentResultSet();
        return false;
    }

    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = getCurrentResultSet();
        if (rs != null) {
            if (_concurrency == ResultSet.CONCUR_READ_ONLY) {
                rs = new UnmodifiableResultSet(rs);
            }
            
            if (_type == ResultSet.TYPE_FORWARD_ONLY) {
                rs = new ForwardOnlyResultSet(rs);
            }
        }
        
        return rs;
    }
    
    public int getResultSetConcurrency() throws SQLException {
        return _concurrency;
    }

    public int getResultSetType() throws SQLException {
        return _type;
    }
    
    void setResultSetConcurrency(int concurrency) throws SQLException {
        _concurrency = concurrency;
    }

    void setResultSetType(int type) throws SQLException {
        _type = type;
    }

    public int getUpdateCount() throws SQLException {
        return clearCurrentUpdateCount();
    }

    public SQLWarning getWarnings() throws SQLException {
        return _warning;
    }

    public void setCursorName(String name) throws SQLException {
        // "If the database doesn't suport positioned update/delete, this method is a
        // noop."
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (!enable) {
            throw new SQLException("Unsupported");
        }
    }

    public void setFetchDirection(int direction) throws SQLException {
        // setFetchDirection is only a hint anyway
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_UNKNOWN:
            case ResultSet.FETCH_REVERSE:
                break;
            default:
                throw new SQLException("Unrecognized fetch direction: " + direction + ".");
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        // setFecthSize is only a hint
        if (rows < 0) {
            throw new SQLException("FetchSize should be non-negative");
        }
    }

    public void setMaxFieldSize(int size) throws SQLException {
        if (size < 0) {
            throw new SQLException("MaxFieldSize should be non-negative");
        } else if (size != 0) {
            throw new SQLException("MaxFieldSize  " + size + " is not supported.");
        }
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("QueryTimeout should be non-negative");
        } else if (seconds != 0) {
            throw new SQLException("QueryTimeout " + seconds + " is not supported.");
        }
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return AxionResultSet.createEmptyResultSet(this);
    }

    /** Currently unsupported when autoGeneratedKeys is not Statement.NO_GENERATED_KEYS. */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (Statement.NO_GENERATED_KEYS == autoGeneratedKeys) {
            return execute(sql);
        }
        throw new SQLException("autoGeneratedKeys are not supported");
    }

    /** Currently unsupported. */
    public boolean execute(String sql, int columnIndexes[]) throws SQLException {
        throw new SQLException("execute(String,int[]) is currently not supported");
    }

    /** Currently unsupported. */
    public boolean execute(String sql, String columnNames[]) throws SQLException {
        throw new SQLException("execute(String,String[]) is currently not supported");
    }

    /** Currently unsupported when auotGeneratedKeys is not Statement.NO_GENERATED_KEYS. */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (Statement.NO_GENERATED_KEYS == autoGeneratedKeys) {
            return executeUpdate(sql);
        }
        throw new SQLException("autoGeneratedKeys are not supported");
    }

    /** Currently unsupported. */
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("executeUpdate(String,int[]) is currently not supported");
    }

    /** Currently unsupported. */
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("executeUpdate(String,String[]) is currently not supported");
    }

    /**
     * Currently unsupported when current is not Statement.CLOSE_CURRENT_RESULT or
     * Statement.CLOSE_ALL_RESULTS.
     */
    public boolean getMoreResults(int current) throws SQLException {
        if (Statement.CLOSE_CURRENT_RESULT == current || Statement.CLOSE_ALL_RESULTS == current) {
            return getMoreResults();
        }
        throw new SQLException("getMoreResults(" + current + ") is currently not supported");
    }

    /** Supported. */
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    protected final boolean execute(AxionCommand cmd) throws SQLException {
        boolean result = false;
        try {
            result = cmd.execute(getDatabase());
        } catch(AxionException e) {
            throw ExceptionConverter.convert(e);
        } catch(RuntimeException e) {
            throw ExceptionConverter.convert(e);
        }
        setCurrentResult(result,cmd);
        getAxionConnection().commitIfAuto();
        return result;
    }
    
    protected final ResultSet executeQuery(AxionCommand cmd) throws SQLException {
        try {
            setCurrentResultSet(cmd.executeQuery(getDatabase(), ResultSet.CONCUR_READ_ONLY == _concurrency));
        } catch(AxionException e) {
            throw ExceptionConverter.convert(e);
        } catch(RuntimeException e) {
            throw ExceptionConverter.convert(e);
        }
        
        if (getAxionConnection().getAutoCommit()) {
            getCurrentResultSet().setTransaction(getAxionConnection().getDatabase().getTransactionManager(),
                getAxionConnection().forgetCurrentTransaction());
        }
        
        return getResultSet();
    }

    protected final int executeUpdate(AxionCommand cmd) throws SQLException {
        try {
            setCurrentUpdateCount(cmd.executeUpdate(getDatabase()));
        } catch(AxionException e) {
            throw ExceptionConverter.convert(e);
        } catch(RuntimeException e) {
            throw ExceptionConverter.convert(e);
        }
        getAxionConnection().commitIfAuto();
        return getCurrentUpdateCount();
    }

    public void setPoolable(boolean poolable) throws SQLException {
    }

    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Operation not supported.");
    }
    
    private int _type = ResultSet.TYPE_FORWARD_ONLY;
    private int _concurrency = ResultSet.CONCUR_READ_ONLY;
}

