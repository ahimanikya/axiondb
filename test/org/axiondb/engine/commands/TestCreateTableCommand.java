/*
 * $Id: TestCreateTableCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

package org.axiondb.engine.commands;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.Literal;
import org.axiondb.Table;
import org.axiondb.types.CharacterVaryingType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TestCreateTableCommand extends BaseAxionCommandTest {

    //------------------------------------------------------------ Conventional

    public TestCreateTableCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestCreateTableCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestCreateTableCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    protected AxionCommand makeCommand() {
        return new CreateTableCommand();
    }

    //------------------------------------------------------------------- Tests

    public void testCreateTable() throws Exception {
        assertNull(getDatabase().getTable("FOO"));
        AxionCommand create = new CreateTableCommand("FOO");
        create.execute(getDatabase());
        assertNotNull("Should get table back", getDatabase().getTable("FOO"));
    }

    public void testCreateTableWithCols() throws Exception {
        assertNull(getDatabase().getTable("FOO"));
        CreateTableCommand create = new CreateTableCommand("FOO");
        Object defaultVal =  new Literal("xyzzy", new CharacterVaryingType(10));
        create.addColumn("A","varchar", "10", "0", defaultVal);
        create.addColumn("B","integer");
        
        create.execute(getDatabase());
        Table foo = getDatabase().getTable("FOO");
        assertNotNull("Should get table back", foo);
        assertEquals("Should have column", 0, foo.getColumnIndex("A"));
        assertEquals("Should have column", 1, foo.getColumnIndex("B"));
    }
    
    public void testCreateTableWithDupCols() throws Exception {
        assertNull(getDatabase().getTable("FOO"));
        CreateTableCommand create = new CreateTableCommand("FOO");
        create.addColumn("A","varchar", "10");
        create.addColumn("A","varchar", "10");
        create.addColumn("B","integer");
        try {
            create.execute(getDatabase());
            fail("Expected duplicate column exception");
        }catch (Exception e) {
            // expected
        }
        assertFalse(getDatabase().hasTable("FOO"));
    }
    
    public void testCreateTableWithBadType() throws Exception {
        assertNull(getDatabase().getTable("FOO"));
        CreateTableCommand create = new CreateTableCommand("FOO");
        create.addColumn("A","badtype");
        try {
            create.execute(getDatabase());
            fail("Expected duplicate column exception");
        }catch (Exception e) {
            // expected
        }
        assertFalse(getDatabase().hasTable("FOO"));
    }
    
    public void testCreateTableWithBadSize() throws Exception {
        assertNull(getDatabase().getTable("FOO"));
        CreateTableCommand create = new CreateTableCommand("FOO");
        create.addColumn("A","varchar", "badsize", "badscale", "xx", null);
        create.execute(getDatabase());
        assertTrue(getDatabase().hasTable("FOO"));
    }
    
    public void testExecuteQuery() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand();
        cmd.setObjectName("FOO");
        assertExecuteQueryIsNotSupported(cmd);
    }
}
