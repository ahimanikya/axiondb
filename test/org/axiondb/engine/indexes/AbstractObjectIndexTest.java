/*
 * $Id: AbstractObjectIndexTest.java,v 1.1 2007/11/28 10:01:24 jawed Exp $
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

package org.axiondb.engine.indexes;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.axiondb.AbstractIndexTest;
import org.axiondb.DataType;
import org.axiondb.types.CharacterVaryingType;

/**
 * For testing indices that rely on the data type being an integer
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:24 $
 * @author Chuck Burdick
 */
public abstract class AbstractObjectIndexTest extends AbstractIndexTest {
    private Log _log = LogFactory.getLog(this.getClass());
    private Random _random = new Random();
    private StringBuffer _buf = new StringBuffer();

    //------------------------------------------------------------ Conventional
        

    public AbstractObjectIndexTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle
        
    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSeq() {
        assertEquals("Should get string", "A", (String)getSequentialValue(0));
    }

    public void testRandom() {
        String random = (String)getRandomValue();
        assertTrue("Should have some length", random.length() > 0);
        assertTrue("Should be within length", random.length() <= 20);
        for (int i = 0; i < random.length(); i++) {
            assertTrue("Char " + i + " in " + random + " should be at least A",
                       random.charAt(i) >= 65);
            assertTrue("Char " + i + " in " + random + " should be at most Z",
                       random.charAt(i) < 91);
        }
    }

    //========================================================= TEST FRAMEWORK

    protected DataType getDataType() {
        return new CharacterVaryingType(20);
    }

    protected Object getSequentialValue(int i) {
        String result = null;
        _buf.setLength(0);
        for (int j = 0; j < (i/26) + 1; j++) {
            _buf.append((char)(65 + (i%26)));
        }
        result = _buf.toString();
        _log.debug("Sequentially chose string " + result);
        return result;
    }

    protected Object getRandomValue() {
        String result = null;
        int size = _random.nextInt(19) + 1;
        _buf.setLength(0);
        for (int i = 0; i < size; i++) {
            int r = _random.nextInt(26);
            char c = (char)(65 + r);
            _buf.append(c);
        }
        result = _buf.toString();
        _log.debug("Randomly chose string " + result);
        return result;
    }
}
