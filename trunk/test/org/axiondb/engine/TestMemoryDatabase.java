/*
 * $Id: TestMemoryDatabase.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Database;
import org.axiondb.AbstractDatabaseTest;
import org.axiondb.engine.tables.ExternalTableFactory;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Chuck Burdick
 */
public class TestMemoryDatabase extends AbstractDatabaseTest {

    //------------------------------------------------------------ Conventional

    public TestMemoryDatabase(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestMemoryDatabase.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestMemoryDatabase.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //-------------------------------------------------------------------- Util

    protected Database createDatabase(String name) throws Exception {
        return new MemoryDatabase(name);
    }

    //------------------------------------------------------------------- Tests

    public void testloadProp() throws Exception {
        Properties prop = new Properties();
        assertNotNull(new MemoryDatabase("bogus11", prop));
    }
    
    public void testNullDbDir() throws Exception {
        Database db = createDatabase("testdb");
        assertNull(db.getDBDirectory());
        assertTrue(db.getTableFactory("EXTERNAL") instanceof ExternalTableFactory);
    }

}
