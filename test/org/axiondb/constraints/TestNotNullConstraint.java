/*
 * $Id: TestNotNullConstraint.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
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

package org.axiondb.constraints;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.DataType;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Rodney Waldhoff
 */
public class TestNotNullConstraint extends BaseConstraintTest {

    //------------------------------------------------------------ Conventional

    public TestNotNullConstraint(String testName) {
        super(testName);        
    }
    
    public static Test suite() {
        return new TestSuite(TestNotNullConstraint.class);
    }

    //---------------------------------------------------------- TestConstraint

    protected Constraint createConstraint() {
        return new NotNullConstraint();
    }
    
    protected Constraint createConstraint(String name) {
        return new NotNullConstraint(name);
    }
    
    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    private Constraint makeConstraint(String name, String tablename, String columnname, DataType columntype) {
        NotNullConstraint constraint = new NotNullConstraint(name);
        constraint.addSelectable(new ColumnIdentifier(new TableIdentifier(tablename),columnname,null,columntype));
        return constraint;
    }

    //------------------------------------------------------------------- Tests

    public void testEvaluateOnDelete() throws Exception {
        Constraint constraint = makeConstraint("C1","FOO","NAME",new CharacterVaryingType(10));
        Constraint constraint2 = makeConstraint("C2","FOO","NUM",new IntegerType());
        SimpleRow row = createRow("testing",new Integer(17));
        RowEvent event = new RowInsertedEvent(getTable(),row,null);
        assertTrue(constraint.evaluate(event));
        assertTrue(constraint2.evaluate(event));
    }

    public void testEvaluateOnInsertOfNonNullString() throws Exception {
        Constraint constraint = makeConstraint("C1","FOO","NAME",new CharacterVaryingType(10));
        SimpleRow row = createRow("testing",null);
        RowEvent event = new RowInsertedEvent(getTable(),null,row);
        assertTrue(constraint.evaluate(event));
    }

    public void testEvaluateOnUpdateToNonNullString() throws Exception {
        Constraint constraint = makeConstraint("C1","FOO","NAME",new CharacterVaryingType(10));
        SimpleRow old = createRow(null,null);
        SimpleRow row = createRow("testing",null);
        RowEvent event = new RowInsertedEvent(getTable(),old,row);
        assertTrue(constraint.evaluate(event));
    }

    public void testEvaluateOnInsertOfNullString() throws Exception {
        Constraint constraint = makeConstraint("C1","FOO","NAME",new CharacterVaryingType(10));
        SimpleRow row = createRow(null,null);
        RowEvent event = new RowInsertedEvent(getTable(),null,row);
        assertTrue(!constraint.evaluate(event));
    }

    public void testEvaluateOnUpdateToNullString() throws Exception {
        Constraint constraint = makeConstraint("C1","FOO","NAME",new CharacterVaryingType(10));
        SimpleRow old = createRow(null,null);
        SimpleRow row = createRow(null,null);
        RowEvent event = new RowInsertedEvent(getTable(),old,row);
        assertTrue(!constraint.evaluate(event));
    }

    public void testEvaluateOnInsertOfNonNullInt() throws Exception {
        Constraint constraint = makeConstraint("C2","FOO","NUM",new IntegerType());
        SimpleRow row = createRow(null,new Integer(17));
        RowEvent event = new RowInsertedEvent(getTable(),null,row);
        assertTrue(constraint.evaluate(event));
    }

    public void testEvaluateOnUpdateToNonNullInt() throws Exception {
        Constraint constraint = makeConstraint("C2","FOO","NUM",new IntegerType());
        SimpleRow old = createRow(null,null);
        SimpleRow row = createRow(null,new Integer(17));
        RowEvent event = new RowInsertedEvent(getTable(),old,row);
        assertTrue(constraint.evaluate(event));
    }

    public void testEvaluateOnInsertOfNullInt() throws Exception {
        Constraint constraint = makeConstraint("C2","FOO","NUM",new IntegerType());
        SimpleRow row = createRow(null,null);
        RowEvent event = new RowInsertedEvent(getTable(),null,row);
        assertTrue(!constraint.evaluate(event));
    }

    public void testEvaluateOnUpdateToNullInt() throws Exception {
        Constraint constraint = makeConstraint("C2","FOO","NUM",new IntegerType());
        SimpleRow old = createRow(null,null);
        SimpleRow row = createRow(null,null);
        RowEvent event = new RowInsertedEvent(getTable(),old,row);
        assertTrue(!constraint.evaluate(event));
    }

}

