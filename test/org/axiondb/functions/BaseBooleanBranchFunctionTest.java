/*
 * $Id: BaseBooleanBranchFunctionTest.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public abstract class BaseBooleanBranchFunctionTest extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public BaseBooleanBranchFunctionTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected RowDecorator setupFunctionArguments(int count, ConcreteFunction function) {
        return setupFunctionArguments(count,function,null);
    }
    
    protected RowDecorator setupFunctionArguments(int count, ConcreteFunction function, DataType type) {
        Map map = new HashMap();
        for(int i=0;i<count;i++) {
            ColumnIdentifier sel = new ColumnIdentifier(null,"col"+i,null,type);
            function.addArgument(sel);
            map.put(sel,new Integer(i));                
        }
        RowDecorator dec = new RowDecorator(map);
        return dec;
    }
    
    protected abstract Boolean evaluate(Boolean[] values);
    
    protected abstract Object[][] getTruthTable();
    protected abstract String getExpressionDisplay(Object[] arguments);
    
    private static final Boolean[] EMPTY = new Boolean[0];
    private static final Boolean[] TRUE = new Boolean[] { Boolean.TRUE };
    private static final Boolean[] FALSE = new Boolean[] { Boolean.FALSE };
    private static final Boolean[] TRUE_FALSE = new Boolean[] { Boolean.TRUE , Boolean.FALSE };
    private static final Boolean[] FALSE_TRUE = new Boolean[] { Boolean.FALSE, Boolean.TRUE  };
    private static final Boolean[] TRUE_TRUE = new Boolean[] { Boolean.TRUE , Boolean.TRUE  };
    private static final Boolean[] FALSE_FALSE = new Boolean[] { Boolean.FALSE, Boolean.FALSE };
    private static final Boolean[] TRUE_TRUE_FALSE = new Boolean[] { Boolean.TRUE , Boolean.TRUE , Boolean.FALSE };
    private static final Boolean[] TRUE_TRUE_TRUE = new Boolean[] { Boolean.TRUE , Boolean.TRUE , Boolean.TRUE  };

    //------------------------------------------------------------------- Tests

    public void testEvaluateNoArg() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(0,function);
        row.setRow(new SimpleRow(EMPTY));  
        assertEquals(evaluate(EMPTY),function.evaluate(row));
    }

    public void testEvaluateOneArg() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(1,function);
        
        row.setRow(new SimpleRow(TRUE));  
        assertEquals(evaluate(TRUE),function.evaluate(row));
        
        row.setRow(new SimpleRow(FALSE));  
        assertEquals(evaluate(FALSE),function.evaluate(row));
    }

    public void testEvaluateNull() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(1,function);
        
        row.setRow(new SimpleRow(new Object[] { null }));  
        assertNull(function.evaluate(row));
    }

    public void testEvaluateTwoArg() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(2,function);
        
        row.setRow(new SimpleRow(TRUE_TRUE));  
        assertEquals(evaluate(TRUE_TRUE),function.evaluate(row));
        
        row.setRow(new SimpleRow(TRUE_FALSE));  
        assertEquals(evaluate(TRUE_FALSE),function.evaluate(row));
        
        row.setRow(new SimpleRow(FALSE_FALSE));  
        assertEquals(evaluate(FALSE_FALSE),function.evaluate(row));
        
        row.setRow(new SimpleRow(FALSE_TRUE));  
        assertEquals(evaluate(FALSE_TRUE),function.evaluate(row));
    }

    public void testEvaluateThreeArg() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(3,function);
        
        row.setRow(new SimpleRow(TRUE_TRUE_TRUE));  
        assertEquals(evaluate(TRUE_TRUE_TRUE),function.evaluate(row));
        
        row.setRow(new SimpleRow(TRUE_TRUE_FALSE));  
        assertEquals(evaluate(TRUE_TRUE_FALSE),function.evaluate(row));
    }

    public void testEvaluateBadArgument() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator row = setupFunctionArguments(1,function,new IntegerType());

        row.setRow(new SimpleRow(new Object[] { new Integer(17) }));  
        try {
            function.evaluate(row);
            fail("Expected AxionException");      
        } catch(AxionException e) {
            // expected
        }
    }

    public void testValidity() throws Exception {
        ConcreteFunction function = makeFunction();
        assertTrue(function.isValid() );
        function.addArgument(new ColumnIdentifier("foo"));
        assertTrue(function.isValid() );
        function.addArgument(new ColumnIdentifier("bar"));
        assertTrue(function.isValid() );
    }

    public void testTruthTable() throws Exception {
        ConcreteFunction function = makeFunction();
        RowDecorator dec = setupFunctionArguments(2, function);

        Object[][] truthTable = getTruthTable();
        
        for (int i = 0; i < truthTable.length; i++) {
            Object[] assertionRow = truthTable[i];
            Object[] rowContents = new Object[assertionRow.length - 1];
            for (int j = 0; j < rowContents.length; j++) {
                rowContents[j] = assertionRow[j];
            }
            
            Object expected = assertionRow[assertionRow.length - 1];
        
            dec.setRow(new SimpleRow(rowContents));
            assertEquals("Failed test for <" + getExpressionDisplay(rowContents) + ">", 
                expected, function.evaluate(dec));
        }
    }    
}
