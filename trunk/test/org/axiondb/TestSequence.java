/*
 * $Id: TestSequence.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

package org.axiondb;

import java.math.BigInteger;

import org.axiondb.types.IntegerType;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TestSequence extends TestCase {

    //------------------------------------------------------------ Conventional


    public TestSequence(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestSequence.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestSequence.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private Sequence _seq = null;

    public void setUp() {
        _seq = new Sequence("foo", 0);
    }

    public void tearDown() {
    }

    //-------------------------------------------------------------------- Util

    //------------------------------------------------------------------- Tests

    public void testSequence() throws Exception {
        for (int i = 0; i < 10; i++) {
            assertEquals("For access num " + i + " should get next val",
                         new Integer(i), _seq.evaluate());
        }
    }
    
    public void testSequenceBad() throws Exception {
        DataType type = new IntegerType();

        //  IncrementBy value should be non-zero numeric        
        try{
            _seq = getSequence("foo", type, 10, 0, -1, -1, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }
        
        //  StartValue value should be greater than MinValue        
        try{
            _seq = getSequence("foo", type, 0, 1, 100, 1, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }            
        
        //  MaxValue value should be greater than MinValue        
        try{
            _seq = getSequence("foo", type, 101, 1, 1, 100, false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }            
        
        _seq = getSequence("foo", type, 0, -10, 20, 0, true);
        assertEquals(new Integer(0),_seq.evaluate());
        assertEquals(new Integer(20),_seq.evaluate());
        assertEquals(new Integer(10),_seq.evaluate());
        assertEquals(new Integer(0),_seq.evaluate());
        
        _seq = getSequence("foo", type, 0, 10, 20, 0, true);
        assertEquals(new Integer(0),_seq.evaluate());
        assertEquals(new Integer(10),_seq.evaluate());
        assertEquals(new Integer(20),_seq.evaluate());
        assertEquals(new Integer(0),_seq.evaluate());
        assertTrue(_seq.equals(_seq));
        
        assertFalse(_seq.equals(new Literal("abc")));
    }
    
    private Sequence getSequence(String name, DataType type, int startVal, int incrementBy, int maxValue,
            int minValue, boolean isCycle) {
        return new Sequence(name, type,  BI(startVal), BI(incrementBy), BI(maxValue), BI(minValue), isCycle);
    }
    
    private BigInteger BI(long value) {
        return BigInteger.valueOf(value);
    }
}
