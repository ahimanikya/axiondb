/*
 * $Id: TestCreateIndexCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.types.CharacterVaryingType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Ahimanikya Satapathy
 */
public class TestCreateIndexCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestCreateIndexCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestCreateIndexCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestCreateIndexCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private Database _db = null;

    public void setUp() throws Exception {
        _db = new MemoryDatabase();
    }

    public void tearDown() throws Exception {
    }

    //------------------------------------------------------------------- Tests

    public void testCreateIndex() throws Exception {
        assertNull(_db.getTable("FOO"));
        CreateTableCommand create = new CreateTableCommand("FOO");
        Object defaultVal =  new Literal("xyzzy", new CharacterVaryingType(10));
        create.addColumn("A", "varchar", "10",  "0", defaultVal);
        create.addColumn("B", "integer");
        create.execute(_db);

        CreateIndexCommand icreate = new CreateIndexCommand();
        icreate.setObjectName("FOO_INDEX");
        icreate.addColumn(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        icreate.setTable(new TableIdentifier("FOO"));
        icreate.execute(_db);
        Table foo = _db.getTable("FOO");

        assertNotNull("Should get table back", foo);
        assertEquals(true, foo.isColumnIndexed(foo.getColumn("A")));

        DropIndexCommand idrop = new DropIndexCommand("FOO_INDEX", true);
        idrop.execute(_db);

        icreate = new CreateIndexCommand();
        icreate.setObjectName("FOO_INDEX");
        icreate.addColumn(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        icreate.setType("BOGUS");
        icreate.setTable(new TableIdentifier("FOO"));
        
        try {
            icreate.execute(_db);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        
        icreate.setType("BTREE");
        icreate.setTable(new TableIdentifier("BOGUS"));
        try {
            icreate.execute(_db);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        
        icreate.setTable(new TableIdentifier("FOO"));
        icreate.setObjectName("SYS_Bogus");
        try {
            icreate.execute(_db);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        
        icreate.setObjectName("FOO_INDEX");
        icreate.addColumn(new ColumnIdentifier(new TableIdentifier("FOO"), "B"));
        try {
            icreate.execute(_db);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        
    }
}