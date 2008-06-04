/*
 * $Id: TestDefrag.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.HashBag;
import org.axiondb.functional.AbstractFunctionalTest;
import org.axiondb.jdbc.AxionConnection;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 */
public class TestDefrag extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestDefrag(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDefrag.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //-------------------------------------------------------------------------
    
    protected String getConnectString() {
        return "jdbc:axiondb:diskdb:testdb";
    }

    protected File getDatabaseDirectory() {
        return new File("testdb");
    }    

    //------------------------------------------------------------------- Tests

    public void testMainWithNoArgs() throws Exception {
        // could test that we get output to system.out
        Defrag.main(new String[0]);
    }

    public void testMainWithTooManyArgs() throws Exception {
        // could test that we get output to system.out
        Defrag.main(new String[2]);
    }

    public void testWhenDatabaseIsInUse() throws Exception {
        // could test that we get output to system.err
        Defrag.main(new String[] { getDatabaseDirectory().getCanonicalPath() });
    }

    public void testDefragWithNoIndex() throws Exception {
        dotest(null);
    }

    public void testDefragWithArrayIndex() throws Exception {
        dotest("array");
    }

    public void testDefragWithBtreeIndex() throws Exception {
        dotest("btree");
    }

    //------------------------------------------------------------------- Utils
    
    private void dotest(String indextype) throws Exception, SQLException {
        createSchema(indextype);
        modifyTables();
        shutdownDatabase();
        File datafile = new File(new File(getDatabaseDirectory(),"FOO"),"FOO.DATA");
        long initialsize = datafile.length();
        Defrag.main(new String[] { getDatabaseDirectory().getCanonicalPath() });
        // alternatively, just use:
        // Defrag.defragDatabase(getDatabaseDirectory().getCanonicalPath());
        // but we want to excercise the main method in this test
        long finalsize = datafile.length();
        assertTrue(finalsize < initialsize);
        openDatabase();
        executeQuery();
        shutdownDatabase();
    }

    private void modifyTables() throws SQLException {
        assertEquals(NUM_ROWS_IN_FOO,_stmt.executeUpdate("update FOO set STR = 'test'"));
        assertEquals(1,_stmt.executeUpdate("delete FOO where NUM = " + (NUM_ROWS_IN_FOO-1)));
        assertEquals(NUM_ROWS_IN_FOO-1,_stmt.executeUpdate("update FOO set STR = 'xyzzy'"));
    }

    private void createSchema(String indextype) throws Exception, SQLException {
        createTableFoo();
        if(null != indextype) { 
            _stmt.execute("create unique " + indextype + " index FOO_NUM_NDX on FOO ( NUM )");
            _stmt.execute("create " + indextype + " index FOO_STR_NDX on FOO ( STR )");
        }
        populateTableFoo();
    }

    private void executeQuery() throws SQLException {
        String sql = "select NUM, STR from FOO";
        _rset = _stmt.executeQuery(sql);
        assertNotNull("Should have been able to create ResultSet",_rset);
        
        // can't assume the order in which rows will be returned
        // so populate a set and compare 'em
        Bag expected = new HashBag();
        Bag found = new HashBag();
        
        for(int i=0;i<(NUM_ROWS_IN_FOO-1);i++) {
            assertTrue("ResultSet should contain more rows [" + i + "]",_rset.next());
            expected.add(new Integer(i));
            Integer val = new Integer(_rset.getInt(1));
            assertTrue("ResultSet shouldn't think value was null",!_rset.wasNull());
            assertTrue("Shouldn't have seen \"" + val + "\" yet",!found.contains(val));
            found.add(val);
            assertEquals("xyzzy",_rset.getString(2));
            assertTrue(!_rset.wasNull());
        }
        assertTrue("ResultSet shouldn't have any more rows",!_rset.next());
        _rset.close();
        assertEquals(expected,found);
    }

    private void openDatabase() throws SQLException {
        _conn = (AxionConnection)(DriverManager.getConnection(getConnectString()));
        _stmt = _conn.createStatement();
    }

    private void shutdownDatabase() throws SQLException {
        try { _stmt.close(); } catch(Exception t) { }
        try { _conn.close(); } catch(Exception t) { }
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection(getConnectString());
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
    }
}
