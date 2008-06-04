/*
 * $Id: TestMultiplyFunction.java,v 1.1 2007/11/28 10:01:34 jawed Exp $
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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Function;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.FloatType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:34 $
 * @author Rodney Waldhoff
 */
public class TestMultiplyFunction extends AbstractArithmeticFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestMultiplyFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestMultiplyFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new MultiplyFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testMakeNewInstance() {
        MultiplyFunction function = new MultiplyFunction();
        assertTrue(function.makeNewInstance() instanceof MultiplyFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testInts() throws Exception {
        Function function = makeFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        sel1.setDataType(new IntegerType());
        
        function.addArgument(sel1);
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);
        sel2.setDataType(new IntegerType());

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { new Integer(2), new Integer(15) } ));
        assertEquals(new BigDecimal("30"),function.evaluate(dec));
    }

    public void testLargeInts() throws Exception {
        Function function = makeFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        sel1.setDataType(new IntegerType());
        function.addArgument(sel1);
        
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        sel2.setDataType(new IntegerType());
        function.addArgument(sel2);

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { new Integer(Integer.MAX_VALUE), new Integer(2) } ));        
        assertEquals(new BigDecimal(((Integer.MAX_VALUE)) * 2L),function.evaluate(dec));
    }

    public void testIntAndFloat() throws Exception {
        Function function = makeFunction();
        
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        sel1.setDataType(new IntegerType());
        function.addArgument(sel1);
        
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        sel2.setDataType(new FloatType());
        function.addArgument(sel2);

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { new Integer(3), new Float(1.5f) }));
        assertEquals(new BigDecimal("4.5"), function.evaluate(dec));
    }

    public void testNumericAndNumeric() throws Exception {
        Function function = makeFunction();
        
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        function.addArgument(sel1);
        
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        BigDecimal left = new BigDecimal("3.000");
        sel1.setDataType(new BigDecimalType(left));
        
        BigDecimal right = new BigDecimal("1.50");
        sel2.setDataType(new BigDecimalType(right));
        
        dec.setRow(new SimpleRow(new Object[] { left, right }));
        assertEquals(new BigDecimal("4.50000"),function.evaluate(dec));
    }

    public void testNumericAndFloat() throws Exception {
        Function function = makeFunction();
        
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        function.addArgument(sel1);
        
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        BigDecimal left = new BigDecimal("3.000");
        sel1.setDataType(new BigDecimalType(left));
        
        sel2.setDataType(new FloatType());
        
        dec.setRow(new SimpleRow(new Object[] { left, new Float(1.5f) }));
        assertEquals(new BigDecimal("4.5000"),function.evaluate(dec));
    }

    public void testNumericAndInt() throws Exception {
        Function function = makeFunction();
        
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        function.addArgument(sel1);
        
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);

        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        map.put(sel2,new Integer(1));                
        RowDecorator dec = new RowDecorator(map);

        BigDecimal left = new BigDecimal("3.000");
        sel1.setDataType(new BigDecimalType(left));
        
        sel2.setDataType(new IntegerType());
        
        dec.setRow(new SimpleRow(new Object[] { left, new Integer(2) }));
        assertEquals(new BigDecimal("6.000"),function.evaluate(dec));
    }
}
