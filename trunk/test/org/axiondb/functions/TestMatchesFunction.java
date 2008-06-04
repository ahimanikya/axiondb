/*
 * $Id: TestMatchesFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Function;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Jonathan Giron
 */
public class TestMatchesFunction extends BaseFunctionTest {

    private static final String REGEX_ZIP_PLUS_4 = "^[0-9]{5}([ \\-]?[0-9]{4})?$";
    private static final String REGEX_US_SSN = "^[0-9]{3}-?[0-9]{2}-?[0-9]{4}$";
    

    //------------------------------------------------------------ Conventional
    public TestMatchesFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestMatchesFunction.class);
        return suite;
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestMatchesFunction.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new MatchesFunction();
    }
    
    //------------------------------------------------------------------- Tests
    public void testEval() throws Exception {
        Function f = makeFunction();
        doTestEvaluate(f, "90041-2417", REGEX_ZIP_PLUS_4, Boolean.TRUE);
        doTestEvaluate(f, "90041 2417", REGEX_ZIP_PLUS_4, Boolean.TRUE);
        
        doTestEvaluate(f, "94112*1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112_1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112=1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112+1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112#1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112/1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112\\1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112[1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "94112]1234", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        doTestEvaluate(f, "V5P 1B2", REGEX_ZIP_PLUS_4, Boolean.FALSE);
        
        
        doTestEvaluate(f, "123-45-6789", REGEX_US_SSN, Boolean.TRUE);
        doTestEvaluate(f, "123456789", REGEX_US_SSN, Boolean.TRUE);
        
        doTestEvaluate(f, "123-cc-41bc", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "123_45_6789", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "123+45+6789", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "123=45=6789", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "12-3456789", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "12-abcdefg", REGEX_US_SSN, Boolean.FALSE);
        doTestEvaluate(f, "123=cc=41bc", REGEX_US_SSN, Boolean.FALSE);
        
        doTestEvaluate(f, "A123", null, null);
        doTestEvaluate(f, null, ".*", null);
        doTestEvaluate(f, null, null, null);
    }
    
    public void testNegative() throws Exception {
        Function f = makeFunction();
        
        try {
            doTestEvaluate(f, "A123", "A{2", null);
            fail("Expected AxionException(2201B)");
        } catch (AxionException e) {
            if (!"2201B".equals(e.getSQLState())) { 
                fail("Expected AxionException(2201B)");
            }
        }
    }
    
    private void doTestEvaluate(Function f, String source, String regexp, Boolean shouldMatch) throws AxionException {
        Selectable sel1 = new ColumnIdentifier("arg1");
        Selectable sel2 = new ColumnIdentifier("arg2");
        f.addArgument(sel1);
        f.addArgument(sel2);
        Map map = new HashMap();
        map.put(sel1, new Integer(0));
        map.put(sel2, new Integer(1));
        RowDecorator dec = new RowDecorator(map);
        
        dec.setRow(new SimpleRow(new Object[] { source, regexp }));
        assertEquals("Matches function failed to evaluate to " + shouldMatch + " for " + source + " using regexp '" + regexp + "'", 
            shouldMatch, f.evaluate(dec));
    }    
}
