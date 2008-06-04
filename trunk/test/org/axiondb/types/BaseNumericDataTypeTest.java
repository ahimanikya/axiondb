/*
 * $Id: BaseNumericDataTypeTest.java,v 1.1 2007/11/28 10:01:39 jawed Exp $
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

package org.axiondb.types;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.axiondb.AxionException;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:39 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public abstract class BaseNumericDataTypeTest extends BaseDataTypeTest {

    //------------------------------------------------------------ Conventional

    public BaseNumericDataTypeTest(String testName) {
        super(testName);
    }

    //------------------------------------------------------------------- Tests

    public void testAcceptsNumeric() throws Exception {
        assertTrue("Should accept Byte",getDataType().accepts(new Byte((byte)3)));
        assertTrue("Should accept Short",getDataType().accepts(new Short((short)3)));
        assertTrue("Should accept Integer",getDataType().accepts(new Integer(3)));
        assertTrue("Should accept Long",getDataType().accepts(new Long(3L)));
        
        assertTrue("Should accept Double",getDataType().accepts(new Double(3.14D)));
        assertTrue("Should accept Float",getDataType().accepts(new Float(3.14F)));
    }

    public void testConvertNonNumericString() throws Exception {
        expectExceptionWhileConvertingNonNumericString("99999");
    }

    public void testToBigDecimal() throws Exception {
        BigDecimal dec = getDataType().toBigDecimal(getDataType().convert(new Integer(17)));
        assertNotNull(dec);
        assertEquals(17,dec.intValue()); // can't assume the scale here, it could be 17, 17.0, 17.00, etc.
    }

    public void testToBigInteger() throws Exception {
        assertEquals(BigInteger.valueOf(17), getDataType().toBigInteger(getDataType().convert(new Integer(17))));
    }
    
    protected void expectExceptionWhileConvertingNonNumericString(String sqlStateExpected) {
        final String msg = "Expected AxionException (" + sqlStateExpected + ")";
        try {
            getDataType().convert("this is not a number");
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, sqlStateExpected, expected.getSQLState());
        }
    }
}
