/*
 * $Id: TestRowDecorator.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

package org.axiondb.engine.rows;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Ahimanikya Satapathy
 */
public class TestRowDecorator extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestRowDecorator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestRowDecorator.class);
        return suite;
    }

    protected Row makeRow(Object[] values) {
        return new SimpleRow(values);
    }

    protected RowDecorator makeRowDecorator() {
        Map map = new HashMap(4);
        map.put(makeVarcharCol("one"), new Integer(0));
        map.put(makeVarcharCol("two"), new Integer(1));
        map.put(makeVarcharCol("three"), new Integer(2));
        map.put(makeVarcharCol("four"), new Integer(3));
        return new RowDecorator(map);
    }

    protected RowDecorator makeRowDecoratorInt() {
        Map map = new HashMap(4);
        map.put(makeIntCol("one"), new Integer(0));
        map.put(makeIntCol("two"), new Integer(1));
        map.put(makeIntCol("three"), new Integer(2));
        map.put(makeIntCol("four"), new Integer(3));
        return new RowDecorator(map);
    }

    protected RowDecorator makeRowDecoratorWithNoType() {
        Map map = new HashMap(4);
        map.put(makeColWithNullType("one"), new Integer(0));
        map.put(makeColWithNullType("two"), new Integer(1));
        map.put(makeColWithNullType("three"), new Integer(2));
        map.put(makeColWithNullType("four"), new Integer(3));
        return new RowDecorator(map);
    }

    private ColumnIdentifier makeVarcharCol(String colName) {
        return new ColumnIdentifier(null, colName, null, new CharacterVaryingType(10));
    }

    private ColumnIdentifier makeIntCol(String colName) {
        return new ColumnIdentifier(null, colName, null, new IntegerType());
    }

    private ColumnIdentifier makeColWithNullType(String colName) {
        return new ColumnIdentifier(null, colName, null, null);
    }

    private void assertRowIndexException(RowDecorator dec) {
        try {
            dec.getRowIndex();
            fail("Expected Exception");
        } catch (AxionException e) {
            // expected
        }
    }

    private void assertFieldNotFound1(RowDecorator dec) {
        try {
            dec.get(makeVarcharCol("bogus"));
            fail("Expected Exception");
        } catch (AxionException e) {
            // expected
        }
    }

    private void assertFieldNotFound2(RowDecorator dec) {
        try {
            dec.get(null);
            fail("Expected Exception");
        } catch (AxionException e) {
            // expected
        }
    }

    private void assertStringRowValue(RowDecorator dec, Object[] val) throws Exception {
        for (int i = 0; i < 4; i++) {
            assertEquals(val[i], dec.get(makeVarcharCol(COL_NAMES[i])));
        }
    }

    private void assertRowValueForNullType(RowDecorator dec, Object[] val) throws Exception {
        for (int i = 0; i < 4; i++) {
            ColumnIdentifier col = makeColWithNullType(COL_NAMES[i]);
            col.setDataType(null);
            assertEquals(val[i], dec.get(col));
        }
    }

    private void assertIntRowValue(RowDecorator dec, Object[] val) throws Exception {
        for (int i = 0; i < 4; i++) {
            Integer colVal = new Integer((String) dec.get(makeVarcharCol(COL_NAMES[i])));
            assertEquals(new Integer((String) val[i]), colVal);
        }
    }

    //------------------------------------------------------------------- Tests

    public void testSimpleRowDecorator() throws Exception {
        {
            Row row = makeRow(STRINGS_ONE);

            RowDecorator dec = makeRowDecorator();

            dec.setRow(row);
            assertEquals(row, dec.getRow());
            assertStringRowValue(dec, STRINGS_ONE);
            assertRowIndexException(dec);

            dec.setRow(50, row);
            assertStringRowValue(dec, STRINGS_ONE);
            assertFieldNotFound1(dec);
            assertFieldNotFound2(dec);
        }
        {
            Row row = makeRow(STRINGS_ONE);
            RowDecorator dec = makeRowDecoratorInt();

            dec.setRow(row);
            assertEquals(row, dec.getRow());
            assertIntRowValue(dec, STRINGS_ONE);
            assertRowIndexException(dec);

            dec.setRow(50, row);
            assertIntRowValue(dec, STRINGS_ONE);
            assertFieldNotFound1(dec);
            assertFieldNotFound2(dec);
        }
        {
            Row row = makeRow(NULLS);

            RowDecorator dec = makeRowDecorator();

            dec.setRow(row);
            assertEquals(row, dec.getRow());
            assertStringRowValue(dec, NULLS);
            assertRowIndexException(dec);

            dec.setRow(50, row);
            assertStringRowValue(dec, NULLS);
            assertFieldNotFound1(dec);
            assertFieldNotFound2(dec);
        }
        {
            Row row = makeRow(STRINGS_ONE);

            RowDecorator dec = makeRowDecoratorWithNoType();

            dec.setRow(row);
            assertEquals(row, dec.getRow());
            assertRowValueForNullType(dec, STRINGS_ONE);
            assertRowIndexException(dec);

            dec.setRow(50, row);
            assertRowValueForNullType(dec, STRINGS_ONE);
            assertFieldNotFound1(dec);
            assertFieldNotFound2(dec);
        }

    }

    private static final String[] COL_NAMES = { "one", "two", "three", "four"};
    private static final String[] STRINGS_ONE = { "1", "2", "3", "4"};
    private static final Object[] NULLS = { null, null, null, null};
}