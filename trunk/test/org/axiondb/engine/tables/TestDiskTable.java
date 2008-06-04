/*
 * $Id: TestDiskTable.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

package org.axiondb.engine.tables;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Database;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.DiskDatabase;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Chuck Burdick
 */
public class TestDiskTable extends AbstractTableTest {

    //------------------------------------------------------------ Conventional

    public TestDiskTable(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDiskTable.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private DiskDatabase _db = null; 
    
    protected Table createTable(String name) throws Exception {
        return new DiskTable(name, _db);
    }
    
    protected Database getDatabase() throws Exception {
        return _db;
    }
    
    protected File getDataFile() throws Exception {
        File tabledir = new File(getDbdir(),"FOO");
        return new File(tabledir, "FOO.DATA");
    }

    public void setUp() throws Exception {
        getDbdir().mkdirs();
        _db = new DiskDatabase(getDbdir());
        super.setUp();
    }

    public void tearDown() throws Exception {
        _db.shutdown();
        super.tearDown();
    }

    public void testDiskInsert() throws Exception {
        testAddRow();
        table.shutdown();
        File tabledir = new File(getDbdir(),"FOO");
        File data = new File(tabledir, "FOO.DATA");
        assertTrue("Should have data file", data.exists());
        assertTrue("Should have some data in data file", data.length() > 16);
    }

    public void testDiskDrop() throws Exception {
        testAddRow();
        File tabledir = new File(getDbdir(),"FOO");
        File meta = new File(tabledir, "FOO.META");
        assertTrue("Table directory should exist", tabledir.exists());
        assertTrue("Meta file should exist", meta.exists());
        table.drop();
        assertTrue("Meta file should not exist",!meta.exists());
        assertTrue("Table directory should not exist",!tabledir.exists());
    }
    
    public void testRestartDB() throws Exception {
        testAddRow();
        table.shutdown();
        table = null;
        _db.shutdown();

        _db = new DiskDatabase(getDbdir());
        assertTrue(_db.hasTable(getTableName()));
        table = _db.getTable(getTableName());
        testGetName();
        RowIterator iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
    }
}
