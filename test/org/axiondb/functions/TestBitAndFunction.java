/*
 * $Id: TestBitAndFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Rodney Waldhoff
 */
public class TestBitAndFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestBitAndFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBitAndFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework

    protected ConcreteFunction makeFunction() {
        return new BitAndFunction();
    }

    //------------------------------------------------------------------- Tests

    public void testMkNwInstance() {
        BitAndFunction function = new BitAndFunction();
        assertTrue(function.makeNewInstance() instanceof BitAndFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testBitAndFunctionEval() throws Exception {
        BitAndFunction function = new BitAndFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier("arg1");
        // function.addArgument(sel1);
        ColumnIdentifier sel2 = new ColumnIdentifier("arg2");
        function.addArgument(sel2);
        function.addArgument(sel1);
        Map map = new HashMap();
        map.put(sel1, new Integer(0));
        map.put(sel2, new Integer(1));
        RowDecorator dec = new RowDecorator(map);
        dec.setRow(new SimpleRow(new Object[] { new Integer(13), new Integer(27)}));
        assertEquals(new Integer(9), function.evaluate(dec));

        dec.setRow(new SimpleRow(new Object[] { null, new Integer(27)}));
        assertNull(function.evaluate(dec));

        dec.setRow(new SimpleRow(new Object[] { new Integer(27), null}));
        assertNull(function.evaluate(dec));

        try {
            dec.setRow(new SimpleRow(new Object[] { "string", new Integer(27)}));
            assertEquals(new Integer(31), function.evaluate(dec));
            fail("Expected conversion exception");
        } catch (AxionException e) {
            // Expected
        }

        try {
            dec.setRow(new SimpleRow(new Object[] { new Integer(27), "string"}));
            assertEquals(new Integer(31), function.evaluate(dec));
            fail("Expected conversion exception");
        } catch (AxionException e) {
            // Expected
        }

    }

    public void testInvalid() throws Exception {
        BitAndFunction function = new BitAndFunction();
        assertTrue(!function.isValid());
        function.addArgument(new ColumnIdentifier("arg1"));
        function.addArgument(new ColumnIdentifier("arg2"));
        function.addArgument(new ColumnIdentifier("arg3"));
        assertTrue(!function.isValid());
    }

    public void testValid() throws Exception {
        BitAndFunction function = new BitAndFunction();
        function.addArgument(new ColumnIdentifier("arg1"));
        function.addArgument(new ColumnIdentifier("arg2"));
        assertTrue(function.isValid());
    }
}