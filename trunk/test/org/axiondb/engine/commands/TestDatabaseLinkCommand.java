/*
 * $Id: TestDatabaseLinkCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.Database;
import org.axiondb.ExternalConnectionProvider;
import org.axiondb.engine.MemoryDatabase;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Ahimanikya Satapathy
 */
public class TestDatabaseLinkCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestDatabaseLinkCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestDatabaseLinkCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestDatabaseLinkCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private Database _db = null;

    public void setUp() throws Exception {
        _db = new MemoryDatabase();
    }

    public void tearDown() throws Exception {
    }

    //------------------------------------------------------------------- Tests

    public void testCreateAndThenDropDatabaseLink() throws Exception {
        assertFalse(_db.hasDatabaseLink("TESTSERVER"));
        CreateDatabaseLinkCommand create = new CreateDatabaseLinkCommand();
        assertFalse(_db.hasDatabaseLink("TESTSERVER"));

        create = new CreateDatabaseLinkCommand("TESTSERVER");

        Properties props = new Properties();
        props.setProperty(ExternalConnectionProvider.PROP_DRIVERCLASS, "org.axiondb.jdbc.AxionDriver");
        props.setProperty(ExternalConnectionProvider.PROP_JDBCURL, "jdbc:axiondb:testdb");
        props.setProperty(ExternalConnectionProvider.PROP_USERNAME, "ignored");
        props.setProperty(ExternalConnectionProvider.PROP_PASSWORD, "ignored");

        create.setProperties(props);
        create.setIfNotExists(true);
        create.execute(_db);
        assertTrue(_db.hasDatabaseLink("TESTSERVER"));
        assertNotNull("Should get server back", _db.getDatabaseLink("TESTSERVER"));
        create.execute(_db); // should be silent since IfNotExists is set to true

        DropDatabaseLinkCommand drop = new DropDatabaseLinkCommand("TESTSERVER", true, false);
        drop.execute(_db);
        drop.execute(_db); // should be silent since exists is set to true
        assertFalse(_db.hasDatabaseLink("TESTSERVER"));
    }
}
