/*
 * $Id: TestCheckConstraint.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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
import org.axiondb.Function;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.IsNotNullFunction;
import org.axiondb.functions.NotEqualFunction;
import org.axiondb.types.CharacterVaryingType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestCheckConstraint extends BaseConstraintTest {

    //------------------------------------------------------------ Conventional

    public TestCheckConstraint(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestCheckConstraint.class);
    }

    //---------------------------------------------------------- TestConstraint

    protected Constraint createConstraint() {
        return new CheckConstraint(null);
    }

    protected Constraint createConstraint(String name) {
        return new CheckConstraint(name);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //-------------------------------------------------------------------- Util

    private Constraint makeConstraint(String name, Selectable condition) {
        CheckConstraint c = new CheckConstraint(name);
        c.setCondition(condition);
        return c;
    }

    private Selectable makeWhereNameIsNotEmpty() {
        Function where = new NotEqualFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("FOO"), "NAME", null, new CharacterVaryingType(10)));
        where.addArgument(new Literal("", new CharacterVaryingType(1)));
        return where;
    }

    private Selectable makeWhereNameIsNotNull() {
        Function where = new IsNotNullFunction();
        where.addArgument(new ColumnIdentifier(new TableIdentifier("FOO"), "NAME", null, new CharacterVaryingType(10)));
        return where;
    }

    private Selectable makeWhereNameIsNotEmptyAndNameIsNotNull() {
        AndFunction fn = new AndFunction();
        fn.addArgument(makeWhereNameIsNotEmpty());
        fn.addArgument(makeWhereNameIsNotNull());
        return fn;
    }

    //------------------------------------------------------------------- Tests

    public void testNotEmptyCheckSuccess() throws Exception {
        Constraint constraint = makeConstraint("NOT_EMPTY", makeWhereNameIsNotEmpty());
        {
            Row row = createRow(" ", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
        {
            Row row = createRow("test", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
    }

    public void testCheckWithNoNewRow() throws Exception {
        Constraint constraint = makeConstraint("NOT_EMPTY", makeWhereNameIsNotEmpty());
        Row row = createRow(null, null);
        RowEvent event = new RowInsertedEvent(getTable(), row, null);
        assertTrue(constraint.evaluate(event));
    }

    public void testNotEmptyCheckFailure() throws Exception {
        Constraint constraint = makeConstraint("NOT_EMPTY", makeWhereNameIsNotEmpty());
        {
            Row row = createRow("", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(!constraint.evaluate(event));
        }
    }

    public void testNotNullCheckSuccess() throws Exception {
        Constraint constraint = makeConstraint("NOT_NULL", makeWhereNameIsNotNull());
        {
            Row row = createRow("", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
        {
            Row row = createRow(" ", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
        {
            Row row = createRow("test", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
    }

    public void testNotNullCheckFailure() throws Exception {
        Constraint constraint = makeConstraint("NOT_NULL", makeWhereNameIsNotNull());
        {
            Row row = createRow(null, null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(!constraint.evaluate(event));
        }
    }

    public void testAndCheckSuccess() throws Exception {
        Constraint constraint = makeConstraint("NOT_NULL_OR_EMPTY", makeWhereNameIsNotEmptyAndNameIsNotNull());
        {
            Row row = createRow(" ", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
        {
            Row row = createRow("test", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertTrue(constraint.evaluate(event));
        }
    }

    public void testAndCheckFailure() throws Exception {
        Constraint constraint = makeConstraint("NOT_NULL_OR_EMPTY", makeWhereNameIsNotEmptyAndNameIsNotNull());
        {
            Row row = createRow("", null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertFalse(constraint.evaluate(event));
        }
        {
            Row row = createRow(null, null);
            RowEvent event = new RowInsertedEvent(getTable(), null, row);
            assertFalse(constraint.evaluate(event));
        }
    }
}