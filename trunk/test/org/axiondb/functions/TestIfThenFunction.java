/*
 * $Id: TestIfThenFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestIfThenFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestIfThenFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestIfThenFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    private RowDecorator _dec = null;
    private ColumnIdentifier _cola, _colb = null;
    private IfThenFunction _function = null;
        
    public void setUp() throws Exception {
        super.setUp();
        _cola = new ColumnIdentifier("A");
        _colb = new ColumnIdentifier("B");
        Map map = new HashMap();
        map.put(_cola,new Integer(0));
        map.put(_colb,new Integer(1));
        _dec = new RowDecorator(map);
        _function = new IfThenFunction();
        _function.addArgument(_cola);
        _function.addArgument(_colb);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _cola = null;
        _colb = null;
        _dec = null;
        _function = null;
    }

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new IfThenFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testMakeNewInstance() {
        IfThenFunction function = new IfThenFunction();
        assertTrue(function.makeNewInstance() instanceof IfThenFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testEvaluateWhenTrue() throws Exception {
        _dec.setRow(new SimpleRow(new Object[] { Boolean.TRUE, "foo" }));
        assertEquals("foo",_function.evaluate(_dec));
    }

    public void testEvaluateWhenFalse() throws Exception {
        _dec.setRow(new SimpleRow(new Object[] { Boolean.FALSE, "foo" }));
        assertNull(_function.evaluate(_dec));
    }
    
    public void testEvaluateWhenNull() throws Exception {
        _dec.setRow(new SimpleRow(new Object[] { null, "foo" }));
        assertNull(_function.evaluate(_dec));
    }

    public void testValid() throws Exception {
        IfThenFunction function = new IfThenFunction();
        assertTrue(! function.isValid());
        function.addArgument(_cola);
        assertTrue(! function.isValid());
        function.addArgument(_colb);
        assertTrue(function.isValid());
        function.addArgument(_colb);
        assertTrue(! function.isValid());
    }
}
