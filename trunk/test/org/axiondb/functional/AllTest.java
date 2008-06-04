/*
 * $Id: AllTest.java,v 1.1 2007/11/29 16:58:03 jawed Exp $
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

package org.axiondb.functional;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/29 16:58:03 $
 * @author Rodney Waldhoff
 * @author Chuck Burdick
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
        suite.addTest(TestDatatypes.suite());
        suite.addTest(TestDDL.suite());
        suite.addTest(TestDQL.suite());
        suite.addTest(TestDQLMisc.suite());
        suite.addTest(TestDQLDisk.suite());
        suite.addTest(TestDQLWithArrayIndex.suite());
        suite.addTest(TestDQLDiskWithArrayIndex.suite());
        suite.addTest(TestDQLWithBTreeIndex.suite());
        suite.addTest(TestDQLDiskWithBTreeIndex.suite());
        suite.addTest(TestDML.suite());
        suite.addTest(TestDMLMisc.suite());
        suite.addTest(TestDMLDisk.suite());
        suite.addTest(TestDMLWithArrayIndex.suite());
        suite.addTest(TestDMLDiskWithArrayIndex.suite());
        suite.addTest(TestDMLWithBTreeIndex.suite());
        suite.addTest(TestDMLDiskWithBTreeIndex.suite());
        suite.addTest(TestMemoryClob.suite());
        suite.addTest(TestDiskClob.suite());
        suite.addTest(TestMemoryBlob.suite());
        suite.addTest(TestDiskBlob.suite());
        suite.addTest(TestThreadedSelect.suite());
        suite.addTest(TestIndexedJoin.suite());
        suite.addTest(TestBugs.suite());
        suite.addTest(TestAxionBTreeDelete.suite());
        suite.addTest(TestSpecials.suite());
        suite.addTest(TestFunctions.suite());
        suite.addTest(TestThreadedDML.suite());
        suite.addTest(TestTransactions.suite());
        suite.addTest(TestTransactionsDisk.suite());
        suite.addTest(TestConstraints.suite());
        suite.addTest(TestBooleanLiterals.suite());
        suite.addTest(TestTransactionalLobs.suite());
        suite.addTest(TestTransactionalLobsDisk.suite());
        suite.addTest(TestMetaData.suite());
        suite.addTest(TestMetaDataDisk.suite());
        suite.addTest(TestDatabaseLock.suite());
        suite.addTest(TestIndexSpecials.suite());
        suite.addTest(TestForElmar.suite());
        suite.addTest(TestPrepareStatement.suite());
        suite.addTest(TestGroupByAndOrderBy.suite());
        suite.addTest(TestBinaryStream.suite());
        //suite.addTest(TestJoins.suite()); Commeneted 28-Nov-2007
        return suite;
    }
}


