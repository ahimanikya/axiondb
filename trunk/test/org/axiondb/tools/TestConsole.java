/*
 * $Id: TestConsole.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AbstractDbdirTest;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Chuck Burdick
 */
public class TestConsole extends AbstractDbdirTest {

    //------------------------------------------------------------ Conventional

    public TestConsole(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestConsole.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestConsole.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private static final String SEP = System.getProperty("line.separator");
    private Console _console = null;
    private StringWriter _out = null;
    private PrintWriter _writer = null;

    public void setUp() throws Exception {
        super.setUp();
        _out = new StringWriter();
        _writer = new PrintWriter(_out, true);
        _console = new Console("foo", _writer);
    }

    public void tearDown() throws Exception {
        _console.execute("drop table foo");
        _console.cleanUp();
        _writer.close();
        _out.close();
        _console = null;
        super.tearDown();
    }

    public void testDatabaseNameIsRequired() throws Exception {
        try {
            new Console(null, _writer);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
    }

    public void testWriterIsRequred() throws Exception {
        try {
            new Console("foo", null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
    }

    public void testExecuteNullOrEmptyIsNoOp() throws Exception {
        _console.execute(null);
        _console.execute("");
        _console.execute(" ");
    }

    public void testCreateInMemory() throws Exception {
        assertNotNull("Should have interface", _console);
        assertNotNull("Should have connection", _console.getConnection());
    }

    public void testCreateOnDisk() throws Exception {
        _console = new Console("foo", "./testdb", _writer);
        assertNotNull("Should have interface", _console);
        assertNotNull("Should have connection", _console.getConnection());
    }

    public void testPrematureSelect() throws Exception {
        _console.execute("select * from foo");
        String msg = _out.toString();
        assertTrue("Should have error message but found: " + msg,
                   msg.indexOf("object does not exist") != -1);
    }

    private void assertEmpty(String msg, StringBuffer buf)  {
        assertEquals(msg,0,buf.length());
    }
    
    private void assertNotEmpty(StringBuffer buf)  {
        assertTrue(buf.length() > 0);
    }

    private void assertNotEmpty(String msg, StringBuffer buf)  {
        assertTrue(msg,buf.length() > 0);
    }
    
    private void clearOutput(StringBuffer buf) {
        buf.setLength(0);           
    }
    
    public void testCreateTableThenSelect() throws Exception {
        assertEmpty("Should have no output yet",_out.getBuffer());
        _console.execute("create table foo (id varchar2)");
        assertNotEmpty("Should have some output",_out.getBuffer());
        clearOutput(_out.getBuffer());
        _console.execute("select id from foo");
        StringBuffer expected = new StringBuffer();
        expected.append("+====+");
        expected.append(SEP);
        expected.append("| ID |");
        expected.append(SEP);
        expected.append("+====+");
        expected.append(SEP);
        expected.append("Execution time: ");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue(_out.toString().startsWith(expected.toString()));
    }

    public void testCreateTableThenDescribe() throws Exception {
        _console.execute("create table foo (id varchar2)");
        _console.execute("describe table foo");
        assertNotEmpty("Should have some output",_out.getBuffer());
    }
    
    public void testCreateDBLinkThenShow() throws Exception {
        _console.execute("create dblink foo (driver='org.axiondb.jdbc.AxionDriver' url='jdbc:axiondb:testdb' username='ignored' password='ignored')");
        _console.execute("show dblinks foo");
        assertNotEmpty("Should have some output",_out.getBuffer());
    }
    
    public void testCreateShowTableProperties() throws Exception {
        _console.execute("create external table foo (id varchar2) organization(loadtype='delimited'");
        _console.execute("SHOW TABLE PROPERTIES foo");
        assertNotEmpty("Should have some output",_out.getBuffer());
    }

    public void testCreateTableThenList() throws Exception {
        _console.execute("create table foo (id varchar2)");
        _console.execute("create index fooidx on foo(id)");
        _console.execute("list table");
        assertNotEmpty("Should have some output",_out.getBuffer());
        _out.getBuffer().setLength(0);
        _console.execute("list tables");
        assertNotEmpty("Should have some output",_out.getBuffer());
        _out.getBuffer().setLength(0);
        _console.execute("list table;");
        assertNotEmpty("Should have some output",_out.getBuffer());
        _out.getBuffer().setLength(0);
        _console.execute("list tables;");
        assertNotEmpty("Should have some output",_out.getBuffer());
        
        _out.getBuffer().setLength(0);
        _console.execute("show INDICES;");
        assertNotEmpty("Should have some output",_out.getBuffer());
        
        _out.getBuffer().setLength(0);
        _console.execute("show INDEXES;");
        assertNotEmpty("Should have some output",_out.getBuffer());

    }
    

    public void testUpdateCountMessages() throws Exception {
        // create
        _out.getBuffer().setLength(0);
        _console.execute("create table foo (id varchar2)");
        assertTrue(_out.getBuffer().indexOf("Executed.") != -1);

        // insert
        _out.getBuffer().setLength(0);
        _console.execute("insert into foo values ('X')");
        assertTrue(_out.getBuffer().indexOf("1 row changed.") != -1);

        // insert
        _out.getBuffer().setLength(0);
        _console.execute("insert into foo values ('X')");
        assertTrue(_out.getBuffer().indexOf("1 row changed.") != -1);

        // update
        _out.getBuffer().setLength(0);
        _console.execute("update foo set id = 'Y' where id = 'X'");
        assertTrue(_out.getBuffer().indexOf("2 rows changed.") != -1);

        // delete
        _out.getBuffer().setLength(0);
        _console.execute("delete from foo");
        assertTrue(_out.getBuffer().indexOf("2 rows changed.") != -1);

        // delete
        _out.getBuffer().setLength(0);
        _console.execute("delete from foo");
        assertTrue(_out.getBuffer().indexOf("No rows changed.") != -1);
    }


    public void testCreateTableWithDataThenSelect() throws Exception {
        assertEmpty("Should have no output yet",_out.getBuffer());
        _console.execute("create table foo (id varchar2(3))");
        _console.execute("insert into foo (id) values ('a')");
        _console.execute("insert into foo (id) values ('b')");
        _console.execute("insert into foo (id) values ('ccc')");
        _console.execute("insert into foo (id) values ( NULL )");
        assertNotEmpty(_out.getBuffer());
        clearOutput(_out.getBuffer());
        _console.execute("select id from foo");
        StringBuffer expected = new StringBuffer();
        expected.append("+======+");
        expected.append(SEP);
        expected.append("|  ID  |");
        expected.append(SEP);
        expected.append("+======+");
        expected.append(SEP);
        expected.append("|    a |");
        expected.append(SEP);
        expected.append("+------+");
        expected.append(SEP);
        expected.append("|    b |");
        expected.append(SEP);
        expected.append("+------+");
        expected.append(SEP);
        expected.append("|  ccc |");
        expected.append(SEP);
        expected.append("+------+");
        expected.append(SEP);
        expected.append("| NULL |");
        expected.append(SEP);
        expected.append("+------+");
        expected.append(SEP);
        expected.append("Execution time: ");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue(_out.toString().startsWith(expected.toString()));
    }

    public void testCreateTableWithMultiColsThenSelect() throws Exception {
        assertEmpty("Should have no output yet",_out.getBuffer());
        _console.execute("create table foo (id varchar2(3), val integer)");
        _console.execute("insert into foo (id, val) values ('a', 1)");
        _console.execute("insert into foo (id, val) values ('b', 1234567)");
        _console.execute("insert into foo (id, val) values ('ccc', -400)");
        assertNotEmpty(_out.getBuffer());
        clearOutput(_out.getBuffer());
        _console.execute("select id, val from foo");
        StringBuffer expected = new StringBuffer();
        expected.append("+=====+=========+");
        expected.append(SEP);
        expected.append("|  ID |   VAL   |");
        expected.append(SEP);
        expected.append("+=====+=========+");
        expected.append(SEP);
        expected.append("|   a |       1 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("|   b | 1234567 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("| ccc |    -400 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("Execution time: ");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue(_out.toString().startsWith(expected.toString()));
    }

    public void testBatchFile() throws Exception {
        File batchFile = new File(getDbdir(), "test.batch");
        PrintWriter writer = new PrintWriter(new FileWriter(batchFile));
        writer.println("CREATE TABLE foo (id VARCHAR2(3), val INTEGER);");
        writer.println("INSERT INTO foo (id, val) VALUES ('a', 1);");
        writer.println("INSERT INTO foo (id, val) VALUES ('b', 1234567);");
        writer.println("INSERT INTO foo (id, val) VALUES ('ccc', -400);");
        writer.flush();
        writer.close();
        writer = null;
        batchFile = null;
        
        StringBuffer expected = new StringBuffer();
        expected.append("+=====+=========+");
        expected.append(SEP);
        expected.append("|  ID |   VAL   |");
        expected.append(SEP);
        expected.append("+=====+=========+");
        expected.append(SEP);
        expected.append("|   a |       1 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("|   b | 1234567 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("| ccc |    -400 |");
        expected.append(SEP);
        expected.append("+-----+---------+");
        expected.append(SEP);
        expected.append("Execution time: ");

        _console.execute("@" + getDbdir() + "/test.batch");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue("Should get output",
                   _out.toString().indexOf("Successfully loaded") > 0);
        _out.getBuffer().setLength(0);

        _console.execute("SELECT id, val FROM foo");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue(_out.toString().startsWith(expected.toString()));
    }

    public void testMissingBatchFile() throws Exception {
        _console.execute("@" + getDbdir() + "/test.batch");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue("Should get output",
                   _out.toString().startsWith("Error reading file"));
    }

    public void testErrorInBatchFile() throws Exception {
        File batchFile = new File(getDbdir(), "test.batch");
        PrintWriter writer = new PrintWriter(new FileWriter(batchFile));
        writer.println("XYZZY xyzzy XYZZY;");
        writer.flush();
        writer.close();
        writer = null;
        batchFile = null;
        
        _console.execute("@" + getDbdir() + "/test.batch");
        assertNotEmpty("Should have some output",_out.getBuffer());
        assertTrue("Expected SQLException in |" + _out.getBuffer().toString() + "|",
                   _out.getBuffer().indexOf("SQLException") != -1);
    }
    
    public void testPrintUsage() throws Exception {
        Console.main(new String[0]);
    }

    public void testOpenThenQuit() throws Exception {
        InputStream oldin = System.in;
        try {
            System.setIn(new ByteArrayInputStream("list system tables;\nexit\n".getBytes()));
            Console.main(new String[] { "foo" });
        } finally {
            System.setIn(oldin);
        }
    }

    public void testOpenThenExit() throws Exception {
        InputStream oldin = System.in;
        try {
            System.setIn(new ByteArrayInputStream("list system tables;\nexit\n".getBytes()));
            Console.main(new String[] { "foo" });
        } finally {
            System.setIn(oldin);
        }
    }
}
