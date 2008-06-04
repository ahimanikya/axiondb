/*
 * $Id: TestAddConstraintCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.engine.MemoryDatabase;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Rodney Waldhoff
 */
public class TestAddConstraintCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestAddConstraintCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestAddConstraintCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestAddConstraintCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private Database _db = null;
    private NotNullConstraint _constraint = null;

    public void setUp() throws Exception {
        _db = new MemoryDatabase();
        CreateTableCommand create = new CreateTableCommand("FOO");
        create.addColumn("A","varchar", "10");
        create.addColumn("B","integer");
        create.execute(_db);
        _constraint = new NotNullConstraint();
        _constraint.addSelectable(new ColumnIdentifier("A"));
    }

    public void tearDown() throws Exception {
        _db = null;
        _constraint = null;
    }

    //------------------------------------------------------------------- Tests

    public void testExecute() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand("FOO",_constraint);
        assertTrue(!cmd.execute(_db));
    }

    public void testExecuteUpdate() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand("FOO",_constraint);
        assertEquals(0,cmd.executeUpdate(_db));
    }

    public void testExecuteQuery() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand("FOO",_constraint);
        try {
            cmd.executeQuery(_db);
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }

    public void testExecuteWithNullTable() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand(null,_constraint);
        try {
            cmd.execute(_db);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testExecuteWithNullConstraint() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand("FOO",null);
        try {
            cmd.execute(_db);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testExecuteWithBadTable() throws Exception {
        AddConstraintCommand cmd = new AddConstraintCommand("XYZZY",_constraint);
        try {
            cmd.execute(_db);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

}
