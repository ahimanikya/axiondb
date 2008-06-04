/*
 * $Id: AllTest.java,v 1.1 2007/11/29 17:01:33 jawed Exp $
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

package org.axiondb.types;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/29 17:01:33 $
 * @author Rodney Waldhoff
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
        
        suite.addTest(TestAnyType.suite());
        suite.addTest(TestBaseDataType.suite());
        suite.addTest(TestBLOBType.suite());
        suite.addTest(TestBooleanType.suite());
        suite.addTest(TestByteType.suite());
        suite.addTest(TestCharacterType.suite());
        suite.addTest(TestCharacterVaryingType.suite());
        suite.addTest(TestClobSource.suite());
        suite.addTest(TestCLOBType.suite());
        suite.addTest(TestCompressedFileBlob.suite());
        suite.addTest(TestCompressedFileClob.suite());
        suite.addTest(TestDateType.suite());
        suite.addTest(TestDoubleType.suite());
        suite.addTest(TestFloatType.suite());
        suite.addTest(TestFileBlob.suite());
        suite.addTest(TestFileClob.suite());
        suite.addTest(TestIntegerType.suite());
        suite.addTest(TestBigIntType.suite());
        suite.addTest(TestObjectType.suite());
        suite.addTest(TestShortType.suite());
        suite.addTest(TestStringClob.suite());
        suite.addTest(TestByteArrayBlob.suite());
        suite.addTest(TestStringType.suite());
        suite.addTest(TestTimestampType.suite());
        suite.addTest(TestTimeType.suite());
        suite.addTest(TestUnsignedByteType.suite());
        suite.addTest(TestUnsignedShortType.suite());
        suite.addTest(TestUnsignedIntegerType.suite());
        suite.addTest(TestBigDecimalType.suite());
        suite.addTest(TestUtf8StreamConverter.suite());
        suite.addTest(TestVarBinaryType.suite());
        
        return suite;
    }
}
