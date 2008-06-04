/*
 * $Id: TestModFunction.java,v 1.1 2007/11/28 10:01:34 jawed Exp $
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

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.RowDecorator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.CharacterType;
import org.axiondb.types.FloatType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:34 $
 * @author Rodney Waldhoff
 */
public class TestModFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestModFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestModFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
        
        _sel1 = new ColumnIdentifier("arg1");
        _sel2 = new ColumnIdentifier("arg2");
        
        _function = new ModFunction();
        _function.addArgument(_sel1);
        _function.addArgument(_sel2);
        
        _selectableToFieldMap = new HashMap(2);
        _selectableToFieldMap.put(_sel1, new Integer(0));                
        _selectableToFieldMap.put(_sel2, new Integer(1));
        
        _rowDec = new RowDecorator(_selectableToFieldMap);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        
        _function = null;
        _sel1 = null;
        _sel2 = null;
        _selectableToFieldMap = null;
        _rowDec = null;
    }

    //--------------------------------------------------------------- Framework
    
    protected ConcreteFunction makeFunction() {
        return new ModFunction();
    }
    
    //------------------------------------------------------------------- Tests

    public void testMkNwInstance() {
        ModFunction function = new ModFunction();
        assertTrue(function.makeNewInstance() instanceof ModFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }
    
    public void testEvalNulls() throws Exception {
        _sel1.setDataType(new IntegerType());
        _sel2.setDataType(new IntegerType());
        
        _rowDec.setRow(new SimpleRow(new Object[] { null, new Integer(2)}));
        assertNull(_function.evaluate(_rowDec));
        
        _rowDec.setRow(new SimpleRow(new Object[] {  new Integer(5), null}));
        assertNull(_function.evaluate(_rowDec));
    }

    public void testEvalIntegers() throws Exception {
        _sel1.setDataType(new IntegerType());
        _sel2.setDataType(new IntegerType());
        
        _rowDec.setRow(new SimpleRow(new Object[] { new Integer(2), new Integer(5)}));
        assertEquals(new BigDecimal("2"), _function.evaluate(_rowDec));
    }
    
    public void testEvalInvalidTypes() throws Exception {
        _sel1.setDataType(new CharacterType());
        _sel2.setDataType(new IntegerType());
        
        try {
            _rowDec.setRow(new SimpleRow(new Object[] { "string", new Integer(2)}));
            _function.evaluate(_rowDec);
            fail("Expected conversion error");
        } catch (AxionException e) {
            // expected
        }
        
        _sel1.setDataType(new IntegerType());
        _sel2.setDataType(new CharacterType());

        try {
            _rowDec.setRow(new SimpleRow(new Object[] { new Integer(5), "string"}));
            _function.evaluate(_rowDec);
            fail("Expected conversion error");
        } catch (AxionException e) {
            // expected
        }
    }

    public void testEvalNumerics() throws Exception {
        _sel1.setDataType(new BigDecimalType(5, 0));
        _sel2.setDataType(new BigDecimalType(3, 0));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("10099"), new BigDecimal("100")}));
        assertEquals(new BigDecimal("99"), _function.evaluate(_rowDec));
        
        _sel1.setDataType(new BigDecimalType(8, 0));
        _sel2.setDataType(new BigDecimalType(4, 0));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("12345678"), new BigDecimal("1234")}));
        assertEquals(new BigDecimal("742"), _function.evaluate(_rowDec));
        
        _sel1.setDataType(new BigDecimalType(8, 0));
        _sel2.setDataType(new BigDecimalType(7, 0));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("25000000"), new BigDecimal("1234567")}));
        assertEquals(new BigDecimal("308660"), _function.evaluate(_rowDec));
        
        _sel1.setDataType(new BigDecimalType(5, 0));
        _sel2.setDataType(new BigDecimalType(4, 0));

        // Tests whether ISO rules for MOD are implemented correctly - sign of result should be
        // same as that of the dividend.
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("26000"), new BigDecimal("2500")}));
        assertEquals(new BigDecimal("1000"), _function.evaluate(_rowDec));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("-26000"), new BigDecimal("2500")}));
        assertEquals(new BigDecimal("-1000"), _function.evaluate(_rowDec));        
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("26000"), new BigDecimal("-2500")}));
        assertEquals(new BigDecimal("1000"), _function.evaluate(_rowDec));        
    }    
    
    public void testZeroDivisor() throws Exception {        
        _rowDec.setRow(new SimpleRow(new Object[] { new Integer(5), new Integer(0)}));
        
        final String msg = "Expected AxionException (22012) - data exception: division by zero";
        try {
            _function.evaluate(_rowDec);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22012", expected.getSQLState());
        }
    }

    public void testIsValid() throws Exception {
        ModFunction function = new ModFunction();
        assertFalse(function.isValid());
        
        function.addArgument(new ColumnIdentifier("arg1"));
        assertFalse(function.isValid());

        function.addArgument(new ColumnIdentifier("arg2"));
        assertTrue(function.isValid());
        
        function.addArgument(new ColumnIdentifier("arg3"));
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
        
        _sel1.setDataType(new BigDecimalType(5, 0));
        _sel2.setDataType(new BigDecimalType(3, 0));
        
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("10099"), new BigDecimal("100")}));
        _function.evaluate(_rowDec);
        
        DataType resultType = _function.getDataType();
        DataType expectedType = _sel2.getDataType();
        assertEquals(expectedType.getPrecision(), resultType.getPrecision());
        assertEquals(expectedType.getScale(), resultType.getScale());
        
        _sel2.setDataType(new FloatType());
        _rowDec.setRow(new SimpleRow(new Object[] { new BigDecimal("10099"), new Float("100.0")}));
        _function.evaluate(_rowDec);
        
        resultType = _function.getDataType();
        expectedType = new BigDecimalType(_sel2.getDataType().getPrecision(), 0);
        assertEquals(expectedType.getPrecision(), resultType.getPrecision());
        assertEquals(expectedType.getScale(), resultType.getScale());
    }
    
    private ModFunction _function;
    private ColumnIdentifier _sel1;
    private ColumnIdentifier _sel2;
    private Map _selectableToFieldMap;
    private RowDecorator _rowDec;
}
