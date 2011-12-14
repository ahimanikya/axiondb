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

package org.axiondb.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 * @author Chuck Burdick
 * @author Jim Burke
 */
public class BatchSqlCommandRunner {
    public BatchSqlCommandRunner(Connection conn) throws SQLException {
        this(conn.createStatement());
    }

    public BatchSqlCommandRunner(Connection conn, PrintWriter pw) throws SQLException {
        this(conn.createStatement(), pw);
    }
    
    public BatchSqlCommandRunner(Statement stmt) {
       this(stmt, null);
        
    }

    public BatchSqlCommandRunner(Statement stmt, PrintWriter pw) {
        _stmt = stmt;
        if (pw == null){
        	_writer = new PrintWriter(System.out, true);;        	                    	
        }else{
            _writer = pw;        	        	
        }
        _report = new BaseReport(_writer);        
    }
    
    public void runCommands(BufferedReader reader) throws IOException, SQLException {
        try {
            String cmd = null;
            while (!(cmd = readCommand(reader)).equals("")) {
                _log.log(Level.FINE,"executing command: " + cmd);
                long startTime = System.currentTimeMillis();                
                boolean hasResultSet = _stmt.execute(cmd);
                long endTime = System.currentTimeMillis();
                if (hasResultSet) {
                    ResultSet rset = _stmt.getResultSet();
                    _report.reportResultSet(rset);
                    rset.close();
                } else {
                    int ct = _stmt.getUpdateCount();
                    _report.reportUpdateCount(ct);
                }
                _writer.println("Execution time: " + (endTime - startTime) + " ms.");                
            }
        } catch (SQLException e) {
        	_report.reportException(e);
            throw e;
        } finally {
            reader.close();
        }
    }

    public void runCommands(InputStream stream) throws IOException, SQLException {
        runCommands(new BufferedReader(new InputStreamReader(stream,"UTF8")));
    }


    public void close() {
        try { _stmt.close(); } catch (Exception e) {}
    }

    String readLine(BufferedReader reader) throws IOException {
        String result = reader.readLine();
        if (result != null) {
            result.trim();
        }
        return result;
    }

    String readCommand(BufferedReader reader) throws IOException {
        _buf.setLength(0);
        String line = null;
        boolean done = false;
        boolean inQuote = false;
        while (!done && (line = readLine(reader)) != null) {
            if (line.indexOf("/*") == -1) {
                _buf.append(line);
                _buf.append(' ');
                inQuote = isInQuotes(line, inQuote);
                done = (!inQuote && line.trim().endsWith(";"));
            }
        }
        return _buf.toString().trim();
    }

    /**
     * loop through all the quotes in the line to see if we are within
     * a string literal
     */
    boolean isInQuotes(String line, boolean inQuotes) {
        boolean result = inQuotes;
        int quotePos = -1;
        int startPos = 0;
        while ((quotePos = line.indexOf("'", startPos)) > -1) {
            result = !result;
            startPos  = quotePos + 1;
        }
        return result;
    }

    private static Logger _log = Logger.getLogger(BatchSqlCommandRunner.class.getName());
    private StringBuffer _buf = new StringBuffer();
    private Statement _stmt = null;
    private PrintWriter _writer = null;
    private BaseReport _report = null; 
}

