/*
 * $Id: TestDiskDatabases.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

package org.axiondb.engine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.tables.BaseFlatfileTable;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Ahimanikya Satapathy
 */
public class TestDiskDatabases extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestDiskDatabases(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestDiskDatabases.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestDiskDatabases.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private File _dbDir = new File(new File("."), "testdb");
    protected Database _db = null;

    public void setUp() throws Exception {
        getDbdir().mkdirs();
    }

    public void tearDown() throws Exception {
        deleteFile(_dbDir);
    }

    protected boolean deleteFile(File file) throws Exception {
        if (!file.exists()) {
            return true;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                deleteFile(files[i]);
            }
        }
        return file.delete();
    }

    protected File getDbdir() {
        return _dbDir;
    }

    //-------------------------------------------------------------------- Util

    public void testStartUpFile() throws Exception {
        String eol = System.getProperty("line.separator");
        String startupFileNameKey = "axiondb.database.testdb.runonstartup";
        String startupFileName = "startup.sql";
        File startupFile = new File(getDbdir(), startupFileName);
        System.setProperty(startupFileNameKey, startupFile.getCanonicalPath());

        File data = new File(getDbdir(), "FFTest.csv");
        populateDataFile(data);
        {
            // Bad startup script
            FileWriter out = new FileWriter(startupFile);
            out.write("create table FFTest (id int, name varchar) "
                + " organization(loadtype='delimited' filename='FFTest.csv' recorddelimiter='" 
                + BaseFlatfileTable.addEscapeSequence(eol) +"')" + eol);
            out.close();

            try {
                _db = Databases.getOrCreateDatabase("testdb", getDbdir());
                fail("Expected SQLException");
            } catch (AxionException e) {
                // expected
            }
        }

        tearDown();
        setUp();
        {
            // No startup script
            _db = Databases.getOrCreateDatabase("testdb", getDbdir());
            assertFalse(_db.hasTable(new TableIdentifier("FFTest")));
        }
        
        Databases.forgetDatabase("testdb");
        _db.shutdown();
        tearDown();
        setUp();
        
        populateDataFile(data);
        {
            FileWriter out = new FileWriter(startupFile);
            out.write("create external table FFTest (id int, name varchar(5)) "
                + " organization(loadtype='delimited' filename='FFTest.csv' recorddelimiter='" 
                + BaseFlatfileTable.addEscapeSequence(eol) +"')" + eol);
            out.close();

            _db = Databases.getOrCreateDatabase("testdb", getDbdir());
            assertTrue(_db.hasTable(new TableIdentifier("FFTEST")));
            Table table = _db.getTable("FFTEST");
            RowIterator itr = table.getRowIterator(false);

            int rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
            assertEquals("Valid row count should have correct value", 7, rowCount);
            table.drop();
        }

        Databases.forgetDatabase("testdb");
        _db.shutdown();

        data.delete();
        startupFile.delete();
        System.getProperties().remove(startupFileNameKey);
    }

    protected void populateDataFile(File data) throws IOException {
        FileWriter out = new FileWriter(data);
        String eol = System.getProperty("line.separator");

        out.write("1,aa" + eol); // 1
        out.write("2.00,bbb" + eol); // 2
        out.write("3.00,ccc" + eol); // 3
        out.write("4.00,ddd" + eol); // 4
        out.write("" + eol); // skip
        out.write("we," + eol); // bad 1
        out.write("7,dfdf" + eol); // 5
        out.write("7.0f,ccc" + eol); // bad 3 ?
        out.write("xx,xx" + eol); // bad 4
        out.write("5,test" + eol); // 6
        out.write("2004-10-10,hhhh" + eol); // bad 5
        out.write("" + eol); // skip
        out.write("3.00,cccdd" + eol); // 7
        out.write("" + eol); // skip
        out.close();
    }

}