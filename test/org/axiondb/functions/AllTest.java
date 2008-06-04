/*
 * $Id: AllTest.java,v 1.1 2007/11/29 16:58:51 jawed Exp $
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

package org.axiondb.functions;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Root test suite.
 *
 * @version $Revision: 1.1 $ $Date: 2007/11/29 16:58:51 $
 * @author Chuck Burdick
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

        suite.addTest(TestFunctionIdentifier.suite());

        suite.addTest(TestAddFunction.suite());
        suite.addTest(TestSubtractFunction.suite());
        suite.addTest(TestMultiplyFunction.suite());
        suite.addTest(TestDivideFunction.suite());

        suite.addTest(TestLessThanFunction.suite());
        suite.addTest(TestLessThanOrEqualFunction.suite());
        suite.addTest(TestEqualFunction.suite());
        suite.addTest(TestGreaterThanOrEqualFunction.suite());
        suite.addTest(TestGreaterThanFunction.suite());
        suite.addTest(TestNotEqualFunction.suite());

        suite.addTest(TestIsNullFunction.suite());
        suite.addTest(TestIsNotNullFunction.suite());

        suite.addTest(TestInFunction.suite());
        suite.addTest(TestNotInFunction.suite());
        suite.addTest(TestNotFunction.suite());
        suite.addTest(TestAndFunction.suite());
        suite.addTest(TestOrFunction.suite());
        
        suite.addTest(TestLikeToRegexpFunction.suite());
        suite.addTest(TestLowerFunction.suite());
        suite.addTest(TestUpperFunction.suite());
        suite.addTest(TestConcatFunction.suite());
        suite.addTest(TestIfThenFunction.suite());
        suite.addTest(TestNullIfFunction.suite());
        suite.addTest(TestNowFunction.suite());
        
        suite.addTest(TestAvgFunction.suite());
        suite.addTest(TestCountFunction.suite());
        suite.addTest(TestMaxFunction.suite());
        suite.addTest(TestMinFunction.suite());
        suite.addTest(TestSumFunction.suite());
        
        suite.addTest(TestABSFunction.suite());
        suite.addTest(TestAsciiFunction.suite());
        suite.addTest(TestHexFunction.suite());
        suite.addTest(TestBitAndFunction.suite());
        suite.addTest(TestBitOrFunction.suite());
        suite.addTest(TestCoalesceFunction.suite());
        suite.addTest(TestDifferenceFunction.suite());
        suite.addTest(TestSoundexFunction.suite());
        suite.addTest(TestSoundsLikeFunction.suite());
        suite.addTest(TestInStringFunction.suite());
        suite.addTest(TestLengthFunction.suite());
        suite.addTest(TestLog10Function.suite());
        suite.addTest(TestLPadFunction.suite());
        suite.addTest(TestLTrimFunction.suite());
        suite.addTest(TestModFunction.suite());
        suite.addTest(TestReplaceFunction.suite());
        suite.addTest(TestRoundFunction.suite());
        suite.addTest(TestRPadFunction.suite());
        suite.addTest(TestRTrimFunction.suite());
        suite.addTest(TestTrimFunction.suite());
        suite.addTest(TestSignFunction.suite());
        suite.addTest(TestSpaceFunction.suite());
        suite.addTest(TestSubstringFunction.suite());
        suite.addTest(TestTruncateFunction.suite());
        suite.addTest(TestCastAsFunction.suite());
        
        suite.addTest(TestCharToDateFunction.suite());
        suite.addTest(TestDateAddFunction.suite());
        suite.addTest(TestDateDiffFunction.suite());
        suite.addTest(TestDatePartFunction.suite());
        suite.addTest(TestDateToCharFunction.suite());

        suite.addTest(TestIsValidDateTimeFunction.suite());
        
        suite.addTest(TestBase64DecodeFunction.suite());
        suite.addTest(TestBase64EncodeFunction.suite());
        suite.addTest(TestCharFunction.suite());
        suite.addTest(TestExistsFunction.suite());
        
        suite.addTest(TestMatchesFunction.suite());

        return suite;
    }
}
