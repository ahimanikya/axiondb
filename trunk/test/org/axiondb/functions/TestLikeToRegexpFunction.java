/*
 * $Id: TestLikeToRegexpFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Chuck Burdick
 * @author Jonathan Giron
 */
public class TestLikeToRegexpFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestLikeToRegexpFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestLikeToRegexpFunction.class);
        return suite;
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestLikeToRegexpFunction.class.getName() };
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
        return new LikeToRegexpFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testLike2Regexp() throws Exception {
        LikeToRegexpFunction f = new LikeToRegexpFunction();
        assertEquals("^ooga.*", f.convertLike("ooga%"));
        assertEquals(".*ooga$", f.convertLike("%ooga"));
        assertEquals(".*ooga.*", f.convertLike("%ooga%"));
        assertEquals("^oo.*a$", f.convertLike("oo%a"));
        assertEquals("^.oga$", f.convertLike("_oga"));
        assertEquals("^b.o.*a$", f.convertLike("b_o%a"));
        assertEquals("^bo\\?ga$", f.convertLike("bo?ga"));
        assertEquals("^bo\\*ga$", f.convertLike("bo*ga"));
        assertEquals("^bo\\.ga$", f.convertLike("bo.ga"));
        assertEquals("^bo\\\\ga$", f.convertLike("bo\\ga"));
    }
    
    public void testEval() throws Exception {
        LikeToRegexpFunction f = new LikeToRegexpFunction();
        Selectable sel1 = new ColumnIdentifier("arg1");
        Selectable sel2 = new ColumnIdentifier("arg2");
        f.addArgument(sel1);
        f.addArgument(sel2);
        Map map = new HashMap();
        map.put(sel1, new Integer(0));
        map.put(sel2, new Integer(1));
        RowDecorator dec = new RowDecorator(map);
        
        dec.setRow(new SimpleRow(new Object[] { "oogga", "o"}));
        assertEquals("^ogga$", f.evaluate(dec));
        
        dec.setRow(new SimpleRow(new Object[] { "oogga", "o"}));
        assertEquals("^ogga$", f.evaluate(dec));
        
        dec.setRow(new SimpleRow(new Object[] { null }));
        assertNull(f.evaluate(dec));

        // ISO/IEC 9075-2:2003:  Section 8.5, General Rule 3(a)(ii).
        dec.setRow(new SimpleRow(new Object[] { "h%llo", null }));
        assertNull(f.evaluate(dec));
    }
}
