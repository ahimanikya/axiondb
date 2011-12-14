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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
//import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.types.ByteArrayBlob;
import org.axiondb.util.ExceptionConverter;
import org.axiondb.util.ValuePool;

/** 
 * A {@link PreparedStatement} implementation.
 * 
 * @TODO Support currently unsupported JDBC 3 methods.
 * @version  
 * @author Chuck Burdick
 * @author Rod Waldhoff
 * @author Ahimanikya Satapathy
 */
public class AxionPreparedStatement extends AxionStatement implements Statement, PreparedStatement {
    protected AxionPreparedStatement(AxionConnection conn, String sql) throws SQLException {
        super(conn);
        initStatement(sql);
    }
    
    protected AxionPreparedStatement(AxionConnection conn, String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        super(conn, resultSetType, resultSetConcurrency);
        initStatement(sql);
    }
    
    
    private void initStatement(String sql) throws SQLException {
        _cmd = parseCommand(sql);
        _params = new Parameters(_cmd.getBindVariables().size());
    }

    private void setParameter(int i, Object value) throws SQLException {
        assertOpen();
        _params.set(i, value);
    }
    
    private void bindVariables(Parameters params) throws SQLException {
        try {
            params.assertBindVariablesSet();
            _cmd.bindAll(params.getValues());
        } catch (AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }

    // ============= PREPARED STATEMENT METHODS =============

    public void close() throws SQLException {
        _cmd = null;
        super.close();
    }

    public boolean execute(String sql) throws SQLException {
        throw new SQLException("execute(String) not valid for PreparedStatements");
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("executeQuery(String) not valid for PreparedStatements");
    }

    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("executeUpdate(String) not valid for PreparedStatements");
    }

    public void addBatch(String sql) throws SQLException {
        throw new SQLException("addBatch(String) not valid for PreparedStatements");
    }
    
    public void addBatch() throws SQLException {
        closeCurrentResultSet(); // if still open
        addToBatchContext(_params);
        _params = new Parameters(_params.size());
    }
    
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // "Since prepared statements have usually been parsed prior to making this call,
        // disabling escape processing for prepared statements will have no effect"
    }

    public void clearParameters() throws SQLException {
        try {
            _cmd.clearBindings();
            _params.clearBindings();
        } catch(AxionException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    public int[] executeBatch() throws SQLException {
        SQLException exception = null;
        int[] results = new int[getBatchCount()];
        int i = 0;
        for(Iterator iter = getBatchContext(); iter.hasNext(); i++) {
            Parameters params = (Parameters)iter.next();
            try {
                bindVariables(params);
                results[i] = executeUpdate(_cmd);
                clearParameters();
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

    public boolean execute() throws SQLException {
        clearCurrentResult();
        bindVariables(_params);
        return execute(_cmd);
    }

    public ResultSet executeQuery() throws SQLException {
        clearCurrentResult();
        bindVariables(_params);
        return executeQuery(_cmd);
    }

    public int executeUpdate() throws SQLException {
        clearCurrentResult();
        bindVariables(_params);
        return executeUpdate(_cmd);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if(hasCurrentResultSet()) {
            return getCurrentResultSet().getMetaData();
        }
        else if (_params.size() == 0)
        {
        	ResultSet rs = this.executeQuery();
        	return rs.getMetaData();
        }
        throw new SQLException("No current ResultSet");
    }

    public void setArray(int i, Array arr) throws SQLException {
        setParameter(i,arr);
    }

    public void setAsciiStream(int n, InputStream in, int length) throws SQLException {
        try {
            setCharacterStream(n,new InputStreamReader(in,"ASCII"),length);
        } catch(UnsupportedEncodingException e) {
            throw ExceptionConverter.convert(e);
        }
    }

    public void setBigDecimal(int i, BigDecimal big) throws SQLException {
        setParameter(i,big);
    }

    public void setBinaryStream(int n, InputStream in, int length) throws SQLException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(length);
        try {
            for(int i=0,b = in.read();i<length && b != -1;i++,b = in.read()) {
                buffer.write(b);
            }
        } catch(IOException e) {
            throw ExceptionConverter.convert(e);
        }
        setBlob(n,new ByteArrayBlob(buffer.toByteArray()));
    }

    public void setBlob(int i, Blob blob) throws SQLException {
        setParameter(i,blob);
    }

    public void setBoolean(int i, boolean bool) throws SQLException {
        setParameter(i,ValuePool.getBoolean(bool));
    }

    public void setByte(int i, byte b) throws SQLException {
        setParameter(i,new Byte(b));
    }

    public void setBytes(int i, byte[] bytes) throws SQLException {
        setParameter(i,bytes);
    }

    public void setCharacterStream(int n, Reader reader, int length) throws SQLException {
        StringBuffer buf = new StringBuffer(length);
        try {
            reader = new BufferedReader(reader);
            for(int i=0,c=reader.read();i<length && c != -1; c = reader.read(), i++) {
                buf.append((char)c);
            }
        } catch(IOException e) {
            throw ExceptionConverter.convert(e);
        }
        setString(n,buf.toString());
    }

    public void setClob(int i, Clob clob) throws SQLException {
        setParameter(i,clob);
    }

    public void setDate(int i, Date date) throws SQLException {
        setParameter(i,date);
    }

    public void setDate(int i, Date date, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setDouble(int i, double d) throws SQLException {
        setParameter(i,new Double(d));
    }

    public void setFloat(int i, float f) throws SQLException {
        setParameter(i,new Float(f));
    }

    public void setInt(int i, int value) throws SQLException {
        setParameter(i,new Integer(value));
    }

    public void setLong(int i, long l) throws SQLException {
        setParameter(i,new Long(l));
    }

    public void setNull(int i, int sqlType) throws SQLException {
        setParameter(i,null);
    }

    public void setNull(int i, int sqlType, String typeName) throws SQLException {
        setParameter(i,null);
    }

    public void setObject(int i, Object o) throws SQLException {
        setParameter(i,o);
    }

    public void setObject(int i, Object o, int targetSqlType) throws SQLException {
        setParameter(i,o);
    }

    public void setObject(int i, Object o, int targetSqlType, int scale) throws SQLException {
        setParameter(i,o);
    }

    public void setRef(int i, Ref ref) throws SQLException {
        throw new SQLException("setRef is currently not supported");
    }

    public void setShort(int i, short s) throws SQLException {
        setParameter(i,new Short(s));
    }

    public void setString(int i, String str) throws SQLException {
        setParameter(i,str);
    }

    public void setTime(int i, Time time) throws SQLException {
        setParameter(i,time);
    }

    public void setTime(int i, Time time, Calendar cal) throws SQLException {
        throw new SQLException("setTime(int,Time,Calendar) is currently not supported");
    }

    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        setParameter(i,timestamp);
    }

    public void setTimestamp(int i, Timestamp timestamp, Calendar cal) throws SQLException {
        throw new SQLException("setTimestamp(int,Timestamp,Calendar) is currently not supported");
    }

    public void setURL(int i, URL url) throws SQLException {
        setParameter(i,url);
    }

    /** @deprecated See {@link java.sql.PreparedStatement#setUnicodeStream} */
    public void setUnicodeStream(int n, InputStream in, int length) throws SQLException {
        try {
            setCharacterStream(n,new InputStreamReader(in,"UnicodeBig"),length/2);
        } catch(UnsupportedEncodingException e) {
            throw ExceptionConverter.convert(e);
        }
    }
    
    /** Currently unsupported. */
    public ParameterMetaData getParameterMetaData() throws SQLException {
    	return new EmptyParameterMetaData();
        //throw new SQLException("getParameterMetaData is currently not supported");
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

   /* public void setRowId(int parameterIndex, RowId x) throws SQLException {
    }*/

    public void setNString(int parameterIndex, String value) throws SQLException {
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    }

    /*public void setNClob(int parameterIndex, NClob value) throws SQLException {
    }*/

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    /*public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    }*/

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    }
    protected class EmptyParameterMetaData implements ParameterMetaData
    {
    	public int getParameterCount() throws SQLException	{
    		return 0;
    	}
    	public int isNullable(int arg0) throws SQLException {
    		return 0;
    	}
    	public boolean isSigned(int arg0) throws SQLException {
    		return false;
    	}
    	public int getPrecision(int arg0) throws SQLException {
    		return 0;
    	}
    	public int getScale(int arg0) throws SQLException {
    		return 0;
    	}
    	public int getParameterType(int arg0) throws SQLException {
    		return java.sql.Types.OTHER;
    	}
    	public String getParameterTypeName(int arg0) throws SQLException {
    		return "";
    	}
    	public String getParameterClassName(int arg0) throws SQLException {
    		return "";
    	}
    	public int getParameterMode(int arg0) throws SQLException {
    		return ParameterMetaData.parameterModeIn;
    	}
    }
    
    protected class Parameters {
        public Parameters(int size) {
            _vals = new Object[size];
            Arrays.fill(_vals, UNBOUND);
        }

        /** 
         * Get the value of parameter <i>i</i>.
         * Note that the index is zero-based.
         */
        public Object get(int i) {
            return _vals[i];
        }

        /** 
         * Set the value of parameter <i>i</i> to
         * <i>val</i>.
         *
         * Note that the index is zero-based.
         */
        public void set(int i, Object val) throws SQLException {
            if(i < 1 || i > _vals.length) {
                throw new SQLException("Invalid index for Parameter");
            }
            _vals[i-1] = val;
        }
        
        public void assertBindVariablesSet() throws SQLException {
            for (int i = 0, I = _vals.length; i < I; i++) {
                if (_vals[i] == UNBOUND) {
                    throw new SQLException("Unbound variable found.");
                }
            }
        }
        
        public final void clearBindings() {
            Arrays.fill(_vals, UNBOUND);
        }

        /**
         * Return the number of parameters I contain.
         */
        public int size() {
            return _vals.length;
        }
        
        public Object[] getValues() {
            return _vals;
        }
        
        private Object[] _vals;
    }


    private AxionCommand _cmd;
    private Parameters _params;
    private final static Object UNBOUND = new Object();

}

