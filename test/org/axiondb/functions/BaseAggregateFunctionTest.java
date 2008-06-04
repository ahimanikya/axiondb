/*
 * $Id: BaseAggregateFunctionTest.java,v 1.1 2007/11/28 10:01:32 jawed Exp $
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

package org.axiondb.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rowiterators.ListRowIterator;
import org.axiondb.engine.rowiterators.RowIteratorRowDecoratorIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.IntegerType;


/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:32 $
 * @author Rodney Waldhoff
 */
public abstract class BaseAggregateFunctionTest extends BaseFunctionTest {

    //------------------------------------------------------------ Conventional

    public BaseAggregateFunctionTest(String testName) {
        super(testName);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //--------------------------------------------------------------- Framework

    protected abstract ConcreteFunction makeFunction();
        
    //------------------------------------------------------------------- Tests

    public void testInvalid() throws Exception {
        ConcreteFunction function = makeFunction();
        assertTrue(! function.isValid());
    }

    public void testValid() throws Exception {
        ConcreteFunction function = makeFunction();
        function.addArgument(new ColumnIdentifier("arg1"));
        assertTrue(function.isValid());
        
        function = makeFunction();
        function.addArgument(new ColumnIdentifier("arg1"));
        function.addArgument(new Literal("DISTINCT"));
        assertTrue(function.isValid());
    }

    protected Object evaluate() throws AxionException {
        AggregateFunction function = (AggregateFunction) makeFunction();
        TableIdentifier tid = new TableIdentifier("t");
        ColumnIdentifier sel1 = new ColumnIdentifier(tid, "id", null, new IntegerType());
        function.addArgument(sel1);
        
        Map map = new HashMap();
        map.put(sel1,new Integer(0));                
        RowDecorator dec = new RowDecorator(map);
        
        List rows = new ArrayList(4);
        rows.add(new SimpleRow(new Object[] { new Integer(1) } ));
        rows.add(new SimpleRow(new Object[] { new Integer(3) } ));
        rows.add(new SimpleRow(new Object[] { new Integer(5) } ));
        rows.add(new SimpleRow(new Object[] { new Integer(2) } ));
        RowIterator rowiter = new ListRowIterator(rows);
        
        RowIteratorRowDecoratorIterator rowdecIter = new RowIteratorRowDecoratorIterator(rowiter, dec);
        return function.evaluate(rowdecIter);
    }
}
