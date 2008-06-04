/*
 * $Id: TestABSFunction.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.FloatType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestABSFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestABSFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestABSFunction.class);
        return suite;
    }

    public void setUp() throws Exception {
        super.setUp();
        
        _sel = new ColumnIdentifier("arg1");
        
        _function = new ABSFunction();
        _function.addArgument(_sel);
        
        _selectableToFieldMap = new HashMap(2);
        _selectableToFieldMap.put(_sel, new Integer(0));                
        
        _rowDec = new RowDecorator(_selectableToFieldMap);        
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new ABSFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testMakekNwInstance() {
        ABSFunction function = new ABSFunction();
        assertTrue(function.makeNewInstance() instanceof ABSFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testFloat() throws Exception {
        final String[] values = new String[] { "-38.978", "38.978" };
        final String[] expectedValues = new String[] { "38.978", "38.978" };
        
        _sel.setDataType(new FloatType());
        for (int i = 0; i < values.length; i++) {
            _rowDec.setRow(new SimpleRow(new Object[] {new Float(values[i])}));
            assertEquals(new Float(expectedValues[i]), _function.evaluate(_rowDec));
        }
    }    

    public void testInteger() throws Exception {
        final String[] values = new String[] { "-42", "500" };
        final String[] expectedValues = new String[] { "42", "500" };
        
        _sel.setDataType(new IntegerType());
        for (int i = 0; i < values.length; i++) {
            _rowDec.setRow(new SimpleRow(new Object[] {new Integer(values[i])}));
            assertEquals(new Integer(expectedValues[i]), _function.evaluate(_rowDec));
        }
    }
    
    public void testNumeric() throws Exception {
        final String[] values = new String[] { "-50", "100", "0", "-152.5", "-0.43" };
        final String[] expectedValues = new String[] { "50.00", "100.00", "0.00", "152.50", "0.43" };
        
        _sel.setDataType(new BigDecimalType(5, 2));
        for (int i = 0; i < values.length; i++) {
            _rowDec.setRow(new SimpleRow(new Object[] {new BigDecimal(values[i])}));
            assertEquals(new BigDecimal(expectedValues[i]), _function.evaluate(_rowDec));
        }
    }   
    
    public void testNullArgument() throws Exception {
        _sel.setDataType(new IntegerType());
        _rowDec.setRow(new SimpleRow(new Object[] { null }));
        assertNull(_function.evaluate(_rowDec));
    }
    
    public void testInvalidArgument() throws Exception {
        try {
            _sel.setDataType(new CharacterVaryingType(10));
            _rowDec.setRow(new SimpleRow(new Object[] {"invalid"}));
            _function.evaluate(_rowDec);
            fail("Expected type conversion error");
        } catch (AxionException e) {
            // Expected
        }
    }

    public void testIsValid() throws Exception {
        ABSFunction function = new ABSFunction();
        assertFalse(function.isValid());
        
        function.addArgument(new ColumnIdentifier("arg1"));
        assertTrue(function.isValid());
        
        function.addArgument(new ColumnIdentifier("arg1"));
        assertFalse(function.isValid());
    }
    
    public void testGetDataType() throws Exception {
        // Default datatype of ModFunction is BigDecimalType with default precision and scale.
        // When function is evaluated, its precision changes to match the precision of the
        // divisor.
        BigDecimalType defaultType = new BigDecimalType();
        assertTrue(_function.getDataType() instanceof BigDecimalType);
        assertEquals(defaultType.getPrecision(), _function.getDataType().getPrecision());
        assertEquals(defaultType.getScale(), _function.getDataType().getScale());
        assertEquals(defaultType.getPrecisionRadix(), _function.getDataType().getPrecisionRadix());
        
        _sel.setDataType(new BigDecimalType(5, 0));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("-10099") }));
        _function.evaluate(_rowDec);
        
        DataType resultType = _function.getDataType();
        DataType expectedType = _sel.getDataType();
        assertEquals(expectedType.getPrecision(), resultType.getPrecision());
        assertEquals(expectedType.getScale(), resultType.getScale());
    }
    
    private ColumnIdentifier _sel;
    private ABSFunction _function;
    private HashMap _selectableToFieldMap;
    private RowDecorator _rowDec;
}
