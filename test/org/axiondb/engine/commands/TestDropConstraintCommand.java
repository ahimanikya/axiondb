/*
 * $Id: TestDropConstraintCommand.java,v 1.3 2008/02/21 13:00:28 jawed Exp $
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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.constraints.NotNullConstraint;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:28 $
 * @author Rodney Waldhoff
 */
public class TestDropConstraintCommand extends BaseAxionCommandTest {

    //------------------------------------------------------------ Conventional

    public TestDropConstraintCommand(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDropConstraintCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private NotNullConstraint _constraint = null;

    public void setUp() throws Exception {
        super.setUp();
        CreateTableCommand create = new CreateTableCommand("FOO");
        create.addColumn("A","varchar", "10");
        create.addColumn("B","integer");
        create.execute(getDatabase());
        _constraint = new NotNullConstraint("BAR");
        _constraint.addSelectable(new ColumnIdentifier("A"));
        AddConstraintCommand cmd = new AddConstraintCommand("FOO",_constraint);
        cmd.execute(getDatabase());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _constraint = null;
    }

    protected AxionCommand makeCommand() {
        return new DropConstraintCommand("FOO","BAR", false);
    }
    
    //------------------------------------------------------------------- Tests

    public void testExecute() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("FOO","BAR", false);
        assertTrue(!cmd.execute(getDatabase()));
    }

    public void testExecuteUpdate() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("FOO","BAR", false);
        assertEquals(0,cmd.executeUpdate(getDatabase()));
    }

    public void testExecuteQuery() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("FOO","BAR", false);
        try {
            cmd.executeQuery(getDatabase());
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            // expected
        }
    }

    public void testExecuteWithNullTable() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand(null,"BAR", false);
        try {
            cmd.execute(getDatabase());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testExecuteWithNullConstraint() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("FOO",null, false);
        try {
            cmd.execute(getDatabase());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testExecuteWithBadTable() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("XYZZY","BAR", false);
        try {
            cmd.execute(getDatabase());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    
    public void testExecuteWithBadConstraint() throws Exception {
        DropConstraintCommand cmd = new DropConstraintCommand("FOO","XYZZY", false);
        try {
            cmd.execute(getDatabase());
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

}
