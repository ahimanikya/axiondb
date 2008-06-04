/*
 * $Id: TestShutdownCommand.java,v 1.1 2007/11/28 10:01:24 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003-2004 Axion Development Team.  All rights reserved.
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

import java.io.InputStream;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.engine.BaseDatabase;
import org.axiondb.engine.MemoryDatabase;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:24 $
 * @author Rodney Waldhoff
 */
public class TestShutdownCommand extends BaseAxionCommandTest {

    //------------------------------------------------------------ Conventional

    public TestShutdownCommand(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestShutdownCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle

    protected AxionCommand makeCommand() {
        return new ShutdownCommand();
    }

    ShutdownCommand _cmd = null;

    public void setUp() throws Exception {
        super.setUp();
        _cmd = new ShutdownCommand();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _cmd = null;
    }

    //------------------------------------------------------------------- Tests

    public void testExecuteQuery() throws Exception {
        assertExecuteQueryIsNotSupported(_cmd);
    }

    public void testShutdown() throws Exception {
        assertTrue(!getDatabase().getTransactionManager().isShutdown());
        _cmd.execute(getDatabase());
        assertTrue(getDatabase().getTransactionManager().isShutdown());
    }

    public void testExecuteUpdate() throws Exception {
        assertTrue(!getDatabase().getTransactionManager().isShutdown());
        assertEquals(0, _cmd.executeUpdate(getDatabase()));
        assertTrue(getDatabase().getTransactionManager().isShutdown());
    }

    public void testMultipleShutdowns() throws Exception {
        assertTrue(!getDatabase().getTransactionManager().isShutdown());
        _cmd.execute(getDatabase());
        try {
            _cmd.execute(getDatabase());
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
    }
    
    public void testReadonlyDB() throws Exception {
        InputStream in = BaseDatabase.class.getClassLoader().getResourceAsStream("org/axiondb/axiondb.properties");
        if(in == null) {
            in = BaseDatabase.class.getClassLoader().getResourceAsStream("axiondb.properties");
        }

        Properties prop = new Properties();
        prop.load(in);
        prop.setProperty("readonly", "yes");
        MemoryDatabase db = new MemoryDatabase("readonlydb", prop);
        try {
            _cmd.assertNotReadOnly(db);
            fail("Expected Exception");
        }catch(Exception e) {
            // expected
        }
    }
}