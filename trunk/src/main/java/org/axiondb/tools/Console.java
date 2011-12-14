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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;

import org.axiondb.jdbc.ConnectionFactory;

/**
 * Simple console-based Axion client application.
 * <p>
 * Invoke via <code>java org.axiondb.tools.Console <i>dbname</i>
 * [<i>location</i>]</code>.
 * </p>
 *
 * @version  
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class Console {
    public Console(String dbName, PrintWriter writer) throws SQLException {
        this(dbName, null, writer);
    }

    public Console(String dbName, String dbLoc, PrintWriter writer) throws SQLException {
        if (writer == null) {
            throw new NullPointerException("Must provide PrintWriter for output");
        }
        _writer = writer;
        _dbLoc = dbLoc;
        _dbName = dbName;
        connect();

        _report = new BaseReport(_writer);
    }

    private void connect() throws SQLException {
        if (_dbName == null) {
            throw new NullPointerException("Must provide database name");
        }
        StringBuffer buf = new StringBuffer();
        buf.append(ConnectionFactory.URL_PREFIX);
        buf.append(_dbName);
        if (_dbLoc != null) {
            buf.append(":");
            buf.append(_dbLoc);
        }
        try {
            _conn = DriverManager.getConnection(buf.toString());
            _stmt = _conn.createStatement();
        } catch (SQLException e) {
            cleanUp();
            throw e;
        }
    }

    public Connection getNewConnection() throws SQLException {
        if (_dbName == null) {
            throw new NullPointerException("Must provide database name");
        }
        StringBuffer buf = new StringBuffer();
        buf.append(ConnectionFactory.URL_PREFIX);
        buf.append(_dbName);
        if (_dbLoc != null) {
            buf.append(":");
            buf.append(_dbLoc);
        }
        try {
            return DriverManager.getConnection(buf.toString());
        } catch (SQLException e) {
            cleanUp();
            throw e;
        }
    }
        
    public Connection getConnection() {
        return _conn;
    }

    public void execute(String input) throws SQLException {
        if (input != null) {
            input = input.trim();
            if (input.length() != 0) {
                while (input.endsWith(";")) {
                    input = input.substring(0, input.length() - 1).trim();
                }

                StringTokenizer tokens = new StringTokenizer(input);

                String token = null;
                if (tokens.hasMoreTokens()) {
                    token = tokens.nextToken().toUpperCase();
                }

                if (token.equals("DESCRIBE") || token.equals("DESC")) {
                    consumeToken(tokens, "TABLE");
                    describeTable(getToken(tokens, true));

                } else if (token.equals("LIST")) {
                    listTables(getToken(tokens, false));
                } else if (token.equals("SHOW")) {
                    if (tokens.hasMoreTokens()) {
                        token = tokens.nextToken().toUpperCase();
                    } else {
                        throw new IllegalArgumentException(
                            "Parser Error: Expected TABLES, DBLINKS, INDICES, INDEXES found EOF");
                    }

                    if (token.equals("TABLE")) {
                        consumeToken(tokens, "PROPERTIES");
                        showTableProperties(getToken(tokens, false));
                    } else if (token.equals("DBLINKS")) {
                        showLinks(getToken(tokens, false));
                    } else if (token.equals("INDICES") || token.equals("INDEXES")) {
                        showIndices(getToken(tokens, false));
                    }
                } else if(token.equals("SET")) {
                    consumeToken(tokens, "AUTOCOMMIT");
                    if (tokens.hasMoreTokens()) {
                        token = tokens.nextToken().toUpperCase();
                    } else {
                        throw new IllegalArgumentException(
                            "Parser Error: Expected AUTOCIMMIT [ON|OFF]");
                    }

                    if (token.equals("ON")) {
                        _conn.setAutoCommit(true);
                    } else if (token.equals("OFF")) {
                        _conn.setAutoCommit(false);
                    }
                } else if(token.equals("COMMIT")) {
                    if(_conn.getAutoCommit() == false) {
                        _conn.commit();
                    }
                } else if(token.equals("ROLLBACK")) {
                    if(_conn.getAutoCommit() == false) {
                        _conn.rollback();
                    }
                } else if(token.equals("RESET")) {
                    if(_conn.isClosed() == false) {
                        cleanUp();
                        connect();
                    }
                } else if (input.startsWith("@")) {
                    String filename = null;
                    File batch = null;
                    BufferedReader reader = null;
                    try {
                        if(input.startsWith("@@")){
                            filename = input.substring(2);
                        batch = new File(filename);
                        reader = new BufferedReader(new FileReader(batch));
                            ParallelBatchSqlCommandRunner runner = new ParallelBatchSqlCommandRunner(this, _writer);
                            runner.runCommands(reader);
                        } else {
                            filename = input.substring(1);
                            batch = new File(filename);
                            reader = new BufferedReader(new FileReader(batch));
                        BatchSqlCommandRunner runner = new BatchSqlCommandRunner(getConnection(), _writer);
                        runner.runCommands(reader);
                        }
                        _writer.println("Successfully loaded file " + batch);
                    } catch (IOException e) {
                        _writer.println("Error reading file " + filename);
                        _report.reportException(e);
                    } catch (Exception e) {
                        _report.reportException(e);
                    } finally {
                        try {
                            reader.close();
                        } catch (Exception e) {
                        }
                        reader = null;
                        batch = null;
                    }
                } else {
                    executeSql(input);
                }
            }
        }
    }

    private String getToken(StringTokenizer tokens, boolean throwErr) throws SQLException {
        if (tokens.hasMoreTokens()) {
            return tokens.nextToken();
        }
        if (throwErr) {
            throw new SQLException("Parser Error: found EOF");
        }
        return "";
    }

    private void consumeToken(StringTokenizer tokens, String keyword) throws SQLException {
        String token;
        if (tokens.hasMoreTokens()) {
            token = tokens.nextToken().toUpperCase();
            if (!token.equals(keyword)) {
                throw new SQLException("Parser Error: Unrecognized Token");
            }
        } else {
            throw new SQLException("Parser Error: Expected " + keyword + " found EOF");
        }
    }

    private void showLinks(String linkname) {
        StringBuffer query = new StringBuffer(
            "select LINK_NAME, LINK_URL, LINK_USERNAME from AXION_DB_LINKS");
        if (linkname != null && linkname.trim().length() != 0) {
            query.append(" where LINK_NAME ");
            query.append((linkname.indexOf("%") == -1) ? "= '" : "like '");
            query.append(linkname.toUpperCase()).append("'");
        }
        executeSql(query.toString());
    }

    private void showTableProperties(String tablename) {
        String query = "select PROPERTY_NAME, PROPERTY_VALUE from AXION_TABLE_PROPERTIES "
            + "where TABLE_NAME = '" + tablename.toUpperCase() + "' ORDER BY PROPERTY_NAME";
        executeSql(query);
    }

    private void describeTable(String table) {
        // here's the axion-centric but terse form
        String query = "select COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS, "
            + "IS_NULLABLE from AXION_COLUMNS where TABLE_NAME = '" + table.toUpperCase()
            + "' order by ORDINAL_POSITION";
        executeSql(query);
    }

    private void listTables(String type) {
        // here's the axion-centric but terse form
        String query = "select TABLE_NAME, TABLE_TYPE from AXION_TABLES "
            + "where TABLE_TYPE LIKE '%" + type.toUpperCase() + "%' " + "order by TABLE_NAME";
        executeSql(query);
    }

    private void showIndices(String indexName) {
        StringBuffer query = new StringBuffer("select index_name, table_name, column_name, "
            + "index_type from AXION_INDEX_INFO order by INDEX_NAME");

        if (indexName != null && indexName.trim().length() != 0) {
            query.append(" where index_name ");
            query.append((indexName.indexOf("%") == -1) ? "= '" : "like '");
            query.append(indexName.toUpperCase()).append("'");
        }
        executeSql(query.toString());
    }

    private void executeSql(String sql) {
        try {
            long startTime = System.currentTimeMillis();
            boolean hasResultSet = _stmt.execute(sql);
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
        } catch (SQLException e) {
            _report.reportException(e);
        }
    }

    public void cleanUp() {
        try {
            _stmt.close();
        } catch (Exception e) {
        }
        try {
            _conn.close();
        } catch (Exception e) {
        }
    }

    public static void main(String[] args) {
        String input = null;
        boolean quit = false;

        if (args.length < 1) {
            System.out.println("Usage: java org.axiondb.tools.Console dbName");
            System.out.println(" or    java org.axiondb.tools.Console dbName location");
            System.out.println(" or    java org.axiondb.tools.Console dbName location outputFilePath");
        } else {
            Console axion = null;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            try {
                PrintWriter out = new PrintWriter(System.out, true);
                switch (args.length) {
                case 3:
                    try {
                        PrintWriter fw = new PrintWriter(new FileWriter(new File(args[2])), true);
                        out = fw;
                    }catch (Exception ex){
                        // output will be written by default to System.out
                    }
                    axion = new Console(args[0], args[1], out);
                    break;

                case 2:
                    axion = new Console(args[0], args[1], out);
                    break;

                case 1:
                    axion = new Console(args[0], out);
                    break;

                default:
                    break;
                }

                System.out.println();
                System.out.println("Type 'quit' to quit the program.");
                while (!quit) {
                    System.out.print(_PROMPT);
                    input = in.readLine();
                    quit = ("quit".equalsIgnoreCase(input) || "exit".equalsIgnoreCase(input));
                    if (!quit && input != null && !"".equals(input.trim())) {
                        try {
                            axion.execute(input);
                        } catch (SQLException sqle) {
                            sqle.printStackTrace();
                        }
                    }
                }
            } catch (SQLException sqle) {
                System.out.println("Unable to connect to database");
                sqle.printStackTrace();
            } catch (IOException ioe) {
                System.out.println("Error while reading input");
                ioe.printStackTrace();
            } finally {
                try {
                    axion.cleanUp();
                } catch (Exception e) {
                }
            }
        }
    }

    private Connection _conn = null;
    private Statement _stmt = null;
    private PrintWriter _writer = null;
    private BaseReport _report = null;
    private String _dbName = null;
    private String _dbLoc = null;

    private static final String _PROMPT = "axion> ";
    static {
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch (ClassNotFoundException e) {
        }
    }
}
