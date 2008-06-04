/*
 * $Id: TestRowComparator.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Ahimanikya Satapathy
 */
public class TestRowComparator extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestRowComparator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestRowComparator.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests
    
    public void testRowComp() {
        ColumnIdentifier colId = new ColumnIdentifier(null, "ID", null, new IntegerType());
        Literal lit = new Literal(new Integer(2), new IntegerType()); 
        EqualFunction eqFun = new EqualFunction();
        eqFun.addArgument(colId);
        eqFun.addArgument(lit);
        
        Map map = new HashMap(2);
        map.put(colId, new Integer(0));
        map.put(lit, new Integer(1));
        RowDecorator dec = new RowDecorator(map);
        
        RowComparator comparator = new RowComparator(eqFun, dec);
        
        Row row1 = new SimpleRow(2);
        row1.set(0, new Integer(2));
        row1.set(1, "some string");
        Row row2 = new SimpleRow(row1);
        assertEquals(0, comparator.compare(row1, row2));
        
        try {
            row2.set(0, row1);
            comparator.compare(row1, row2);
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
        
        try {
            comparator.compare(row1, new String("bad row"));
            fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }
    
}
