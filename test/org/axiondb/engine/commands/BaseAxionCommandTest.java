/*
 * $Id: BaseAxionCommandTest.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

import junit.framework.TestCase;

import org.axiondb.AxionCommand;
import org.axiondb.Database;
import org.axiondb.engine.Databases;
import org.axiondb.io.FileUtil;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Rodney Waldhoff
 */
public abstract class BaseAxionCommandTest extends TestCase {

    public BaseAxionCommandTest(String testName) {
        super(testName);
    }

    protected abstract AxionCommand makeCommand();

    protected Database getDatabase() {
        return _db;
    }

    private File _dbDir = new File(new File("."), "testdb");
    private Database _db = null;

    public void setUp() throws Exception {
        super.setUp();
        getDbdir().mkdirs();
        _db = Databases.getOrCreateDatabase("testdb", getDbdir());
    }

    public void tearDown() throws Exception {
        try {
            Databases.forgetDatabase("testdb");
        } catch (Exception e) {
            // ignored
        }
        _db.shutdown();
        deleteFile(_dbDir);
        _db = null;
    }

    protected boolean deleteFile(File file) throws Exception {
        return FileUtil.delete(file);
    }

    protected File getDbdir() {
        return _dbDir;
    }

    protected void assertExecuteUpdateIsNotSupported(AxionCommand cmd) throws Exception {
        try {
            cmd.executeUpdate(getDatabase());
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    protected void assertExecuteQueryIsNotSupported(AxionCommand cmd) throws Exception {
        try {
            cmd.executeQuery(getDatabase());
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    //------------------------------------------------------------------- Tests

    public void testToString() throws Exception {
        assertNotNull(makeCommand().toString());
    }
}