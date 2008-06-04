/*
 * $Id: TestBase64EncodeFunction.java,v 1.2 2007/12/11 16:24:48 jawed Exp $
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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.ByteArrayBlob;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.util.Base64;

/**
 * @version $Revision: 1.2 $ $Date: 2007/12/11 16:24:48 $
 * @author Ahimanikya Satapathy
 */
public class TestBase64EncodeFunction extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public TestBase64EncodeFunction(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBase64EncodeFunction.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Framework

    protected ConcreteFunction makeFunction() {
        return new ABSFunction();
    }

    //------------------------------------------------------------------- Tests

    public void testMakekNwInstance() {
        Base64EncodeFunction function = new Base64EncodeFunction();
        assertTrue(function.makeNewInstance() instanceof Base64EncodeFunction);
        assertTrue(function.makeNewInstance() != function.makeNewInstance());
    }

    public void testFunctionEval() throws Exception {
        Base64EncodeFunction function = new Base64EncodeFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier("arg1");
        function.addArgument(sel1);
        Map map = new HashMap();
        map.put(sel1, new Integer(0));
        RowDecorator dec = new RowDecorator(map);

        dec.setRow(new SimpleRow(new Object[] { "string".getBytes()}));
        assertEquals(new String(Base64.encodeBase64("string".getBytes())), function.evaluate(dec));

        dec.setRow(new SimpleRow(new Object[] { null}));
        assertNull(function.evaluate(dec));

        try {
            dec.setRow(new SimpleRow(new Object[] { new Object[] { "invalid"}}));
            function.evaluate(dec);
            fail("Expected type conversion error");
        } catch (AxionException e) {
            // Expected
        }

        try {
            dec.setRow(new SimpleRow(new Object[] { new Literal(new BigDecimal(23), new BigDecimalType())}));
            function.evaluate(dec);
            fail("Expected type conversion error");
        } catch (AxionException e) {
            // Expected
        }
    }
    
    public void testBadArguments() throws Exception {
        Base64EncodeFunction function = new Base64EncodeFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier(new TableIdentifier(), "arg1", null, new BadDataType1());
        function.addArgument(sel1);

        Map map = new HashMap();
        map.put(sel1, new Integer(0));
        RowDecorator dec = new RowDecorator(map);

        try {
            dec.setRow(new SimpleRow(new Object[] { "string".getBytes()}));
            function.evaluate(dec);
            fail("Expected bad input steam exception");
        } catch (AxionException e) {
            // Expected
        }
        
        sel1 = new ColumnIdentifier(new TableIdentifier(), "arg1", null, new BadDataType2());
        function.setArgument(0, sel1);

        try {
            dec.setRow(new SimpleRow(new Object[] { "string".getBytes()}));
            function.evaluate(dec);
            fail("Expected io exception");
        } catch (AxionException e) {
            // Expected
        }
    }

    public void testFunctionInvalid() throws Exception {
        Base64EncodeFunction function = new Base64EncodeFunction();
        assertTrue(!function.isValid());
        function.addArgument(new ColumnIdentifier("arg1"));
        function.addArgument(new ColumnIdentifier("arg1"));
        assertTrue(!function.isValid());
    }

    public void testFunctionValid() throws Exception {
        Base64EncodeFunction function = new Base64EncodeFunction();
        function.addArgument(new ColumnIdentifier("arg1"));
        assertTrue(function.isValid());
    }

    private class BadDataType1 extends CharacterVaryingType {
        public Blob toBlob(Object val) {
            return new BadBlob1("string".getBytes());
        }
    }

    private class BadBlob1 extends ByteArrayBlob {
        public BadBlob1(byte[] value) {
            super(value);
        }

        public InputStream getBinaryStream() throws SQLException {
            throw new SQLException("simulate for test");
        }
    }
    
    private class BadDataType2 extends CharacterVaryingType {
        public Blob toBlob(Object val) {
            return new BadBlob2("string".getBytes());
        }
    }

    private class BadBlob2 extends ByteArrayBlob {
        public BadBlob2(byte[] value) {
            super(value);
        }

        public InputStream getBinaryStream() throws SQLException {
            return new BadInputStream();
        }
    }

    private class BadInputStream extends  InputStream {
        
        public int read() throws IOException{
            throw new IOException("simulate for test");
        }
    }
}