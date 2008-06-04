/*
 * $Id: TestHexFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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

import org.axiondb.ColumnIdentifier;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Ahimanikya Satapathy
 */
public class TestHexFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestHexFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestHexFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new HexFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testMakekNwInstance() {
        HexFunction function = new HexFunction();
        assertTrue(function.makeNewInstance() instanceof HexFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testHexFunctionEval() throws Exception {
        HexFunction function = new HexFunction();
        ColumnIdentifier managerNumber = new ColumnIdentifier("MGRNO");
        function.addArgument(managerNumber);
        Map map = new HashMap();
        map.put(managerNumber, new Integer(0));                             
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] {"000020"}));
        assertEquals("303030303230", function.evaluate(dec));       
        
        dec.setRow(new SimpleRow(new Object[] {"B"}));
        assertEquals("42", function.evaluate(dec));
        
        dec.setRow(new SimpleRow(new Object[] {null}));
        assertNull(function.evaluate(dec));
        
        // FIXME: Column with a data type of decimal(6,2) and a value of 40.1. 
        // An eight-character string '0004010C' is the result of applying the 
        // HEX function to the internal representation of the decimal value, 40.1. 

        // dec.setRow(new SimpleRow(new Object[] {new BigDecimal("40.1")}));
        // assertEquals("0004010C", function.evaluate(dec));
    }

    public void testAsciiFunctionInvalid() throws Exception {
        HexFunction function = new HexFunction();
        assertTrue(! function.isValid());
    }

    public void testAsciiFunctionValid() throws Exception {
        HexFunction function = new HexFunction();
        function.addArgument(new ColumnIdentifier("MGRNO"));
        assertTrue(function.isValid());
    }
}
