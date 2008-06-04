/*
 * $Id: AllTest.java,v 1.1 2007/11/29 16:42:57 jawed Exp $
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
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Root test suite.
 *
 * @version $Revision: 1.1 $ $Date: 2007/11/29 16:42:57 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class AllTest extends TestCase {

    public AllTest(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { AllTest.class.getName()  };
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(TestAddConstraintCommand.suite());
        suite.addTest(TestDropConstraintCommand.suite());
        suite.addTest(TestCreateSequenceCommand.suite());
        suite.addTest(TestCreateTableCommand.suite());
        suite.addTest(TestDatabaseLinkCommand.suite());
        suite.addTest(TestInsertCommand.suite());
        suite.addTest(TestSelectCommand.suite());
        suite.addTest(TestShutdownCommand.suite());
        suite.addTest(TestCheckFileStateCommand.suite());
        suite.addTest(TestDMLWhenClause.suite());
        suite.addTest(TestAxionQueryPlanner.suite());
        suite.addTest(TestDefragCommand.suite());
        suite.addTest(TestTruncateCommand.suite());
        suite.addTest(TestRemountCommand.suite());
        suite.addTest(TestCreateIndexCommand.suite());
        suite.addTest(TestDropTableCommand.suite());
        suite.addTest(TestDeleteCommand.suite());
        suite.addTest(TestUpdateCommand.suite());
        suite.addTest(TestUpsertCommand.suite());
        return suite;
    }
}

