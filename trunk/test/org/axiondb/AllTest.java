/*
 * $Id: AllTest.java,v 1.2 2008/01/24 13:13:44 jawed Exp $
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

package org.axiondb;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Root test suite.
 *
 * @version $Revision: 1.2 $ $Date: 2008/01/24 13:13:44 $
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
        suite.addTest(TestTableIdentifier.suite());
        suite.addTest(TestColumnIdentifier.suite());
        suite.addTest(TestColumn.suite());
        suite.addTest(TestLiteral.suite());
        suite.addTest(TestSequence.suite());
        suite.addTest(TestPersistentDatabase.suite());
        suite.addTest(TestFunctional.suite());
        suite.addTest(TestOrderNode.suite());
        suite.addTest(TestBindVariable.suite());
        suite.addTest(TestBaseSelectable.suite());
        suite.addTest(TestSequenceEvaluator.suite());
        suite.addTest(TestRowComparator.suite());
        
        suite.addTest(org.axiondb.util.AllTest.suite());

        suite.addTest(org.axiondb.constraints.AllTest.suite());
        suite.addTest(org.axiondb.parser.AllTest.suite());
        suite.addTest(org.axiondb.types.AllTest.suite());
        suite.addTest(org.axiondb.jdbc.AllTest.suite());
        suite.addTest(org.axiondb.functions.AllTest.suite());
        suite.addTest(org.axiondb.functional.AllTest.suite());
        
        suite.addTest(org.axiondb.engine.AllTest.suite());
        suite.addTest(org.axiondb.engine.commands.AllTest.suite());
        suite.addTest(org.axiondb.engine.indexes.AllTest.suite());
        suite.addTest(org.axiondb.engine.rowiterators.AllTest.suite());
        suite.addTest(org.axiondb.engine.rows.AllTest.suite());
        suite.addTest(org.axiondb.engine.tables.AllTest.suite());
        suite.addTest(org.axiondb.engine.visitors.AllTest.suite());
        suite.addTest(org.axiondb.event.AllTest.suite());
        
        // BTree tests execute very slowly if we run the util suite at this point - moved to
        // top of order.  Perhaps this is caused by memory/gc issues?
        // suite.addTest(org.axiondb.util.AllTest.suite());
        
        suite.addTest(org.axiondb.tools.AllTest.suite());
        suite.addTest(TestAxionException.suite()); 

        return suite;
    }
}

