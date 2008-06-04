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
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 * @author Ahimanikya Satapathy
 */
public class ParallelBatchSqlCommandRunner {
    public ParallelBatchSqlCommandRunner(Console console) throws SQLException {
        _console = console;
    }

    public ParallelBatchSqlCommandRunner(Console console, PrintWriter pw) {
        _console = console;
        if (pw == null){
        	_writer = new PrintWriter(System.out, true);;        	                    	
        }else{
            _writer = pw;        	        	
        }
        _report = new BaseReport(_writer);        
    }
    
    public void runCommands(BufferedReader reader) throws IOException, Exception {
        long startTime = System.currentTimeMillis();

        try {
            ArrayList cmds = new ArrayList();
            String cmd = null;
            while(!(cmd = readCommand(reader)).equals("")) {
                cmds.add(cmd);
            }
            
            ExecutorService _threadPool = Executors.newFixedThreadPool(cmds.size());
            for(int i =0, I=cmds.size();  i< I; i++) {
                 Job job = new Job((String)cmds.get(i), _console.getNewConnection());
                _threadPool.execute(job);
            }
            _threadPool.shutdown();  
            
            // wait for the partition readers to complete.
            synchronized(_lockObject) {
                while(!_threadPool.isTerminated()) {
                    try {
                        _lockObject.wait(1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger("global").log(Level.SEVERE, null, ex);
                    }
                }
            }

        } catch (Exception e) {
            _report.reportException(e);
            throw e;
        } finally {
            reader.close();
            long endTime = System.currentTimeMillis();
            _writer.println("\n\nExecution time: " + (endTime - startTime) + " ms."); 
        }
    }

    public void runCommands(InputStream stream) throws IOException, Exception {
        runCommands(new BufferedReader(new InputStreamReader(stream,"UTF8")));
    }
    
    private class Job implements Runnable {
        String _cmd = null;
        Connection _conn;
        //Logger log = Logger.getLogger(BatchSqlCommandRunner.class.getName());
        public Job(String cmd, Connection conn) {
            _cmd = cmd;
            _conn = conn;
        }
        
        public void run() {
            Statement stmt = null;
	    try{
                //log.log(Level.FINE,"executing command: " + _cmd);
                long startTime = System.currentTimeMillis();
                _conn.setAutoCommit(false);
                stmt = _conn.createStatement();
                
                boolean hasResultSet = stmt.execute(_cmd);
                _conn.commit();
                
                long endTime = System.currentTimeMillis();
                if (hasResultSet) {
                    ResultSet rset = stmt.getResultSet();
                    _report.reportResultSet(rset);
                    rset.close();
                } else {
                    int ct = stmt.getUpdateCount();
                    _report.reportUpdateCount(ct);
                }
                _writer.println("Execution time: " + (endTime - startTime) + " ms."); 

            } catch(Exception ex) {
                synchronized(_report){
                    _report.reportException(ex);
                }
            } finally{
                try{
                    if(stmt != null) { stmt.close();}
                }catch(SQLException ex){
                    // ignore
                }
            }
        }
    }


    public void close() {
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
    private static String _lockObject = "";
    private Console _console;
    private PrintWriter _writer = null;
    private BaseReport _report = null; 
}

