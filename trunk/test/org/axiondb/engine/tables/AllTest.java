/*
 * $Id: AllTest.java,v 1.4 2008/02/21 13:00:27 jawed Exp $
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

package org.axiondb.engine.tables;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.4 $ $Date: 2008/02/21 13:00:27 $
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
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
        suite.addTest(TestExternalDatabaseTable.suite());
        suite.addTest(TestExternalAxionDBTable.suite());
        suite.addTest(TestExternalDatabaseTableRowIterator.suite());  
        suite.addTest(TestExternalAxionDBTableRowIterator.suite());

        suite.addTest(TestBaseTableOrganizationContext.suite());
        suite.addTest(TestComplexTableView.suite());
        suite.addTest(TestDelimitedFlatfileTable.suite());
        suite.addTest(TestDelimitedFlatfileTableRowIterator.suite());
        suite.addTest(TestDiskTable.suite());
        suite.addTest(TestDiskTableRowIterator.suite());
        suite.addTest(TestFixedWidthFlatfileTable.suite());
        suite.addTest(TestFixedWidthFlatfileTableRowIterator.suite());
        suite.addTest(TestMemoryTable.suite());
        suite.addTest(TestTableView.suite());
        suite.addTest(TestTaggedEBCDICFlatfileTable.suite());
        
        return suite;
    }
}
