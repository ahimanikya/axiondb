/*
 * $Id: AbstractComparisonFunctionTest.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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
import org.axiondb.Function;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public abstract class AbstractComparisonFunctionTest extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public AbstractComparisonFunctionTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(AbstractComparisonFunctionTest.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    protected abstract void checkResult(Object left, Object right, int leftIndex, int rightIndex, boolean result);
    
    //------------------------------------------------------------------- Util

    private void setupFunctionArguments(Function function) {
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        function.addArgument(sel1);
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);
    }

    private RowDecorator setupRowDecorator() {
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        
        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);
        return dec;
    }
    //------------------------------------------------------------------- Tests

    public final void testEvaluateInts() throws Exception {
        Function function = makeFunction();
        setupFunctionArguments(function);
        RowDecorator dec = setupRowDecorator();
        Integer[] values = new Integer[] {
            new Integer(Integer.MIN_VALUE),
            new Integer(-7),
            new Integer(-1),
            new Integer(0),
            new Integer(1),
            new Integer(7),
            new Integer(Integer.MAX_VALUE)
        };
        for(int i=0;i<values.length;i++) {
            for(int j=0;j<values.length;j++) {
                dec.setRow(new SimpleRow(new Object[] { values[i], values[j] } ));
                checkResult(values[i],values[j],i,j,((Boolean)(function.evaluate(dec))).booleanValue());
            }
        }
    }
    
    public final void testFlip() throws Exception {
        ComparisonFunction function = (ComparisonFunction)makeFunction();
        setupFunctionArguments(function);
        RowDecorator straight = setupRowDecorator();
        
        ComparisonFunction flip = function.flip();
        assertNotNull(flip);
        RowDecorator flipped = setupRowDecorator();

        Integer[] values = new Integer[] {
            new Integer(Integer.MIN_VALUE),
            new Integer(-7),
            new Integer(-1),
            new Integer(0),
            new Integer(1),
            new Integer(7),
            new Integer(Integer.MAX_VALUE)
        };
        for(int i=0;i<values.length;i++) {
            for(int j=0;j<values.length;j++) {
                straight.setRow(new SimpleRow(new Object[] { values[i], values[j] } ));
                flipped.setRow(new SimpleRow(new Object[] { values[j], values[i] } ));
                assertEquals(function.evaluate(straight),flip.evaluate(flipped));
            }
        }
    }
    
    public void testValidity() throws Exception {
        ConcreteFunction function = makeFunction();
        assertTrue(!function.isValid());
        function.addArgument(new ColumnIdentifier("foo"));
        assertTrue(!function.isValid());
        function.addArgument(new ColumnIdentifier("bar"));
        assertTrue(function.isValid());
        function.addArgument(new ColumnIdentifier("xyz"));
        assertTrue(!function.isValid());
    }

    /**
     * Tests enforcement of ISO/IEC 9075-2:2003 Section 8.2, &lt;comparison predicate&gt; 
     * General Rule 1(a), for values XV and YV represented by value expressions X and Y, 
     * respectively:  &quot;If either XV or YV is the null value, then X <comp op> Y is Unknown
     * (i.e., NULL).&quot;
     * 
     * @throws Exception if unexpected exception occurs during test evaluation.
     */
    public void testNullArguments() throws Exception {
        ConcreteFunction function = makeFunction();
        setupFunctionArguments(function);
        
        RowDecorator dec = setupRowDecorator();
        dec.setRow(new SimpleRow(new Object[] { null, new Integer(0) }));
        assertNull(function.evaluate(dec));
        
        dec.setRow(new SimpleRow(new Object[] { new Integer(0), null }));
        assertNull(function.evaluate(dec));

        dec.setRow(new SimpleRow(new Object[] { null, null }));
        assertNull(function.evaluate(dec));
    }
}
