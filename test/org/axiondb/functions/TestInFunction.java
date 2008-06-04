/*
 * $Id: TestInFunction.java,v 1.1 2007/11/28 10:01:33 jawed Exp $
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

import org.axiondb.ColumnIdentifier;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:33 $
 * @author Rodney Waldhoff
 */
public class TestInFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestInFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestInFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new InFunction();
    }
    
    private RowDecorator setupFunctionArguments(ConcreteFunction function) {
        ColumnIdentifier sel1 = new ColumnIdentifier("one");
        function.addArgument(sel1);
        ColumnIdentifier sel2 = new ColumnIdentifier("two");
        function.addArgument(sel2);
        ColumnIdentifier sel3 = new ColumnIdentifier("three");
        function.addArgument(sel3);
        
        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        map.put(sel3,new Integer(2));                
        RowDecorator dec = new RowDecorator(map);
        return dec;
    }
    
    //------------------------------------------------------------------- Tests

    public void testMakeNewInstance() {
        InFunction function = new InFunction();
        assertTrue(function.makeNewInstance() instanceof InFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testEvaluate() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(function);
        
        row.setRow(new SimpleRow(new Object[] { new Integer(1), new Integer(2), new Integer(3) }));        
        assertEquals(Boolean.FALSE,function.evaluate(row));

        row.setRow(new SimpleRow(new Object[] { new Integer(1), new Integer(1), new Integer(3) }));        
        assertEquals(Boolean.TRUE,function.evaluate(row));

        row.setRow(new SimpleRow(new Object[] { new Integer(1), new Integer(2), new Integer(1) }));        
        assertEquals(Boolean.TRUE,function.evaluate(row));
    }

    public void testValidity() throws Exception {
        ConcreteFunction function = makeFunction();
        assertTrue(! function.isValid() );
        function.addArgument(new ColumnIdentifier("foo"));
        assertTrue(function.isValid() );
        function.addArgument(new ColumnIdentifier("bar"));
        assertTrue(function.isValid() );
    }
}
