/*
 * $Id: TestDMLWhenClause.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.commands;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.BooleanType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Rod Waldhoff
 * @author Jonathan Giron
 */
public class TestDMLWhenClause extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestDMLWhenClause(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDMLWhenClause.class);
    }

    //--------------------------------------------------------------- Lifecycle
    private static final Literal _true = new Literal(Boolean.TRUE,new BooleanType());
    private static final Literal _false = new Literal(Boolean.FALSE,new BooleanType());
    private RowDecorator _decorator = null;
    private Database _db = null;

    public void setUp() throws Exception {
        _decorator = new RowDecorator(Collections.EMPTY_MAP);
        _decorator.setRow(1,new SimpleRow(0));
        _db = new MemoryDatabase();            
    }

    public void tearDown() throws Exception {
        _decorator = null;
        _db = null;
    }

    //------------------------------------------------------------------- Tests

    public void testEvaluateNullCondition() throws Exception {
        DMLWhenClause clause = new DMLWhenClause(null);
        try {
            clause.evaluate(_decorator);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testEvaluateTrue() throws Exception {
        DMLWhenClause clause = new DMLWhenClause(_true);
        assertTrue(clause.evaluate(_decorator));
    }

    public void testEvaluateFalse() throws Exception {
        DMLWhenClause clause = new DMLWhenClause(_false);
        assertFalse(clause.evaluate(_decorator));
    }
    
    public void testEvaluateConditionEvaluatingToNull() throws Exception {
        ConcreteFunction function = new EqualFunction();
        ColumnIdentifier sel1 = new ColumnIdentifier("foo");
        function.addArgument(sel1);
        ColumnIdentifier sel2 = new ColumnIdentifier("bar");
        function.addArgument(sel2);
        
        Map map = new HashMap();
        map.put(sel1, new Integer(0));                
        map.put(sel2, new Integer(1));                
        _decorator = new RowDecorator(map);
        
        DMLWhenClause clause = new DMLWhenClause(function);
        
        _decorator.setRow(new SimpleRow(new Object[] { null, new Integer(0) }));
        assertFalse(clause.evaluate(_decorator));
        
        _decorator.setRow(new SimpleRow(new Object[] { new Integer(0), null }));
        assertFalse(clause.evaluate(_decorator));
        
        _decorator.setRow(new SimpleRow(new Object[] { null, null }));
        assertFalse(clause.evaluate(_decorator));
    }

    public void testResolve() throws Exception {        
        DMLWhenClause clause = new DMLWhenClause(_false);
        clause.resolve(_db,new TableIdentifier[0]);
        assertFalse(clause.evaluate(_decorator));
    }
}
