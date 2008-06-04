/*
 * $Id: TestOrFunction.java,v 1.1 2007/11/28 10:01:34 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003-2005 Axion Development Team.  All rights reserved.
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
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:34 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestOrFunction extends BaseBooleanBranchFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestOrFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestOrFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {        
        return new OrFunction();
    }

    protected Boolean evaluate(Boolean[] values) {
        for (int i=0;i<values.length;i++) {
            if (values[i].booleanValue()) {
                return Boolean.TRUE;           
            }
        }
        return Boolean.FALSE;
    }

    /*
     * Defined in Table 12, ISO/IEC 9075-2:2003, Section 6.34.  
     * @see org.axiondb.functions.BaseBooleanBranchFunctionTest#getTruthTable()
     */
    protected Object[][] getTruthTable() {
        return new Object[][] {
            new Object[] { Boolean.TRUE, Boolean.TRUE, Boolean.TRUE },
            new Object[] { Boolean.TRUE, Boolean.FALSE, Boolean.TRUE },
            new Object[] { Boolean.TRUE, null, Boolean.TRUE },
            new Object[] { Boolean.FALSE, Boolean.TRUE, Boolean.TRUE },
            new Object[] { Boolean.FALSE, Boolean.FALSE, Boolean.FALSE },
            new Object[] { Boolean.FALSE, null, null },
            new Object[] { null, Boolean.TRUE, Boolean.TRUE },
            new Object[] { null, Boolean.FALSE, null },
            new Object[] { null, null, null}
        };
    }

    protected String getExpressionDisplay(Object[] arguments) {
        StringBuffer buf = new StringBuffer(25);
        for (int i = 0; i < arguments.length; i++) {
            if (i != 0) { 
                buf.append(" OR ");
            }
            buf.append(arguments[i]);
        }
        return buf.toString();
    }

    //------------------------------------------------------------------- Tests

    public void testMakeNewInstance() {
        OrFunction function = new OrFunction();
        assertTrue(function.makeNewInstance() instanceof OrFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
}
