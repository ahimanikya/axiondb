/*
 * $Id: TestDefragCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

package org.axiondb.engine.commands;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Ahimanikya Satapathy
 */
public class TestDefragCommand extends BaseAxionCommandTest {

    //------------------------------------------------------------ Conventional

    public TestDefragCommand(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDefragCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
        populateTable();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected AxionCommand makeCommand() {
        return new DefragCommand();
    }

    protected void populateTable() throws Exception {
        {
            CreateTableCommand cmd = new CreateTableCommand("FOO");
            cmd.addColumn("ID", "integer");
            cmd.addColumn("NAME", "varchar", "10");
            cmd.execute(getDatabase());
        }

        Table table = getDatabase().getTable("FOO");
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(1));
            row.set(1, "one");
            table.addRow(row);
        }
        {
            Row row = new SimpleRow(2);
            row.set(0, new Integer(2));
            row.set(1, "two");
            table.addRow(row);
        }
    }

    //------------------------------------------------------------------- Tests

    public void testExecute() throws Exception {
        DefragCommand cmd = new DefragCommand();
        try {
            cmd.setObjectName("BOGUS");
            cmd.execute(getDatabase());
            fail("Exception Excepted");
        } catch (AxionException e) {
            // expected
        }

        Table table = getDatabase().getTable("FOO");
        File tabledir = new File(getDbdir(), "FOO");
        
        File data = new File(tabledir, "FOO.DATA");
        assertTrue("Should have data file", data.exists());
        
        FileInputStream fis = new FileInputStream(data);
        long oldLen = fis.getChannel().size();
        fis.close();

        RowIterator iter = table.getRowIterator(false);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        Row oldRow = iter.next();
        assertNotNull(oldRow);
        Row newRow = new SimpleRow(oldRow);
        iter.set(newRow);

        assertTrue(iter.hasNext());
        oldRow = iter.next();
        assertNotNull(oldRow);
        newRow = new SimpleRow(oldRow);
        iter.remove();

        assertTrue(!iter.hasNext());
        
        fis = new FileInputStream(data);
        long newLen = fis.getChannel().size();
        fis.close();
        assertTrue("Should have more data in data in file before defrag", newLen > oldLen);
        oldLen = newLen;

        cmd.setObjectName("FOO");
        assertTrue(!cmd.execute(getDatabase()));

        // execute again
        cmd = new DefragCommand();
        cmd.setObjectName("FOO");
        assertTrue(!cmd.execute(getDatabase()));

        // should have only one row now.
        table = getDatabase().getTable("FOO");
        iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());

        fis = new FileInputStream(data);
        newLen = fis.getChannel().size();
        fis.close();
        assertTrue("Should have less data in data in file after defrag", newLen < oldLen);
    }

    public void testExecuteQuery() throws Exception {
        DefragCommand cmd = new DefragCommand();
        cmd.setObjectName("FOO");
        assertExecuteQueryIsNotSupported(cmd);
    }
    
    public void testNullTableName() throws Exception {
        DefragCommand cmd = new DefragCommand();
        try {
            cmd.executeUpdate(getDatabase());
            fail("Exception Excepted");
        } catch (AxionException e) {
            // expected
        }
    }
    
    public void testBindVaiables() {
        DefragCommand cmd = new DefragCommand();
        assertTrue(!cmd.getBindVariableIterator().hasNext());
    }

}