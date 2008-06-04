/*
 * $Id: TestBatchSqlCommandRunner.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author Chuck Burdick
 */
public class TestBatchSqlCommandRunner extends TestCase {

    //------------------------------------------------------------- Constructor

    public TestBatchSqlCommandRunner(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestBatchSqlCommandRunner.class);
    }

    //--------------------------------------------------------------- Utilities

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        Class.forName("org.axiondb.jdbc.AxionDriver");

        _conn = DriverManager.getConnection("jdbc:axiondb:batchtest");
        _stmt = _conn.createStatement();

        _runner = new BatchSqlCommandRunner(_conn);
        _createdTables = false;
        _reader = new BufferedReader(
            new InputStreamReader(
                new ByteArrayInputStream(_input)));
    }

    public void tearDown() throws Exception {
        if (_createdTables) {
            _stmt.execute("DROP TABLE products");
            _stmt.execute("DROP TABLE preference_key_info");
        }
        _runner.close();
        try { _rset.close(); } catch (Exception e) {}
        try { _stmt.close(); } catch (Exception e) {}
        try { _conn.close(); } catch (Exception e) {}
        _reader.close();
    }

    public void testReadLine() throws Exception {
        String line = "/****************************************************************************/";
        assertEquals("Should find first line",
                     line, _runner.readLine(_reader));
    }

    public void testInQuotes() throws Exception {
        assertTrue("done: no quotes",
                   !_runner.isInQuotes("foo", false));
        assertTrue("not done: one quote",
                   _runner.isInQuotes("'foo", false));
        assertTrue("done: matched quotes",
                   !_runner.isInQuotes("'foo'", false));
        assertTrue("done: apos literal",
                   !_runner.isInQuotes("'foo '' bar'", false));

        boolean inQuote = _runner.isInQuotes("' foo", false);
        assertTrue("not done: multi quote, multi line 1", inQuote);

        inQuote = _runner.isInQuotes(" ' , ';", inQuote);
        assertTrue("not done: multi quote, multi line 2", inQuote);

        inQuote = _runner.isInQuotes(" bar');", inQuote);
        assertTrue("not done: multi quote, multi line 3", !inQuote);
    }

    public void testReadCommands() throws Exception {
        String[] snippets = new String[] {
            "CREATE TABLE products ",
            "CREATE TABLE preference_key_info " };

        for (int i = 0; i < snippets.length; i++) {
            String cmd = _runner.readCommand(_reader);
            assertNotNull("Should find command " + i, cmd);
            assertTrue("Should have snippet " + snippets[i] + " but found " +
                       cmd, cmd.startsWith(snippets[i]));
        }

        assertEquals("Should have no more commands",
                     "", _runner.readCommand(_reader));
    }

    public void testRun() throws Exception {
        _runner.runCommands(_reader);
        _createdTables = true;
        _rset = _stmt.executeQuery("SELECT product_id FROM products");
        assertTrue("Should execute but find no results", !_rset.next());

        _rset.close();
        _rset = null;

        _rset = _stmt.executeQuery("SELECT description FROM preference_key_info");
        assertTrue("Should execute but find no results", !_rset.next());
    }

    //-------------------------------------------------------------- Attributes

    private boolean _createdTables = false;
    private Connection _conn = null;
    private Statement _stmt = null;
    private ResultSet _rset = null;
    private BatchSqlCommandRunner _runner = null;
    private BufferedReader _reader = null;
    private static final byte[] _input = "/****************************************************************************/\nCREATE TABLE products ( product_id INTEGER,\n                        description VARCHAR2,\n                        identifier_pattern VARCHAR2, \n                        created_date LONG, \n                        created_by VARCHAR2, \n                        modified_date LONG, \n                        modified_by VARCHAR2 );  \n\n/* some */\n/* multi-line */\n/* comment */\n/* block */\n\n/* some comment */\nCREATE TABLE preference_key_info ( preference_key_id INTEGER,\n                                    pref_key INTEGER,\n                                    description VARCHAR2,\n                                    notes VARCHAR2,\n                                    created_date LONG,\n                                    created_by VARCHAR2,\n                                    modified_date LONG,\n                                    modified_by VARCHAR2);".getBytes();

}
