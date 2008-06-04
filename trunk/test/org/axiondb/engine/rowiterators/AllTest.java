/*
 * $Id: AllTest.java,v 1.1 2007/11/29 16:44:39 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.rowiterators;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/29 16:44:39 $
 */
public class AllTest extends TestCase {

    public AllTest(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { AllTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(TestIntRowMapValuesRowIterator.suite());
        suite.addTest(TestFilteringRowIterator.suite());
        suite.addTest(TestListIteratorRowIterator.suite());
        suite.addTest(TestListRowIterator.suite());
        suite.addTest(TestChainedRowIterator.suite());
        suite.addTest(TestCollatingRowIterator.suite());
        suite.addTest(TestSingleRowIterator.suite());
        suite.addTest(TestEmptyRowIterator.suite());
        suite.addTest(TestUnmodifiableRowIterator.suite());
        suite.addTest(TestTransformingRowIterator.suite());
        suite.addTest(TestLimitingRowIterator.suite());
        suite.addTest(TestLazyRowRowIterator.suite());
        suite.addTest(TestGroupedRowIterator.suite());
        suite.addTest(TestDistinctRowIterator.suite());
        suite.addTest(TestSortedRowIterator.suite());
        suite.addTest(TestMutableSortedRowIterator.suite());
        suite.addTest(TestReverseSortedRowIterator.suite());
        suite.addTest(TestRowViewRowIterator.suite());
        
        suite.addTest(TestDelegatingRowIterator.suite());
        suite.addTest(TestRowIteratorRowDecoratorIterator.suite());
        suite.addTest(TestFilteringChangingIndexedRowIterator.suite());
        suite.addTest(TestRebindableIndexedRowIterator.suite());

        suite.addTest(TestChangingIndexedRowIterator.suite());
        suite.addTest(TestJoinRowIterator.suite());

        suite.addTest(TestIndexNestedLoopJoinedRowIterator_InnerJoinCase.suite());
        suite.addTest(TestIndexNestedLoopJoinedRowIterator_LeftOuterJoinCase.suite());
        suite.addTest(TestIndexNestedLoopJoinedRowIterator_OuterJoinWithAdditionalCriteriaCase.suite());
        suite.addTest(TestIndexNestedLoopJoinedRowIterator_RightOuterJoinCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_CrossProductCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_IndexedCrossProductCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_IndexedInnerJoinCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_IndexedLeftOuterJoinCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_IndexedRightOuterJoinCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_InnerJoinCase.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_LeftOuterJoinCase1.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_LeftOuterJoinCase2.suite());
        suite.addTest(TestNestedLoopJoinedRowIterator_RightOuterJoinCase.suite());
        return suite;
    }
}

