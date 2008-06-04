/*
 * $Id: TestFilteringRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

package org.axiondb.engine.rowiterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Function;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.VariableContext;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanOrEqualFunction;
import org.axiondb.types.BooleanType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 * @author Amrish Lal
 */
public class TestFilteringRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestFilteringRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestFilteringRowIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    public RowIterator makeRowIterator() {
        ArrayList list = new ArrayList();
        for (int i = 0; i < 10; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            list.add(row);
        }
        Selectable evenFilter = new Selectable() {
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 2 == 0);
                return result ? Boolean.TRUE : Boolean.FALSE;
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        RowDecorator decorator = new RowDecorator((Map) null);
        return new FilteringRowIterator(new ListIteratorRowIterator(list.listIterator()), decorator, evenFilter);
    }
    
    protected int getSize() {
        return 5;
    }

    public List makeRowList() {
        ArrayList list = new ArrayList();
        for (int i = 0; i < 10; i += 2) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));
            list.add(row);
        }
        return list;
    }

    //--------------------------------------------------------------- Lifecycle

    private List _empty = null;
    private List _list = null;
    private List _evens = null;
    private List _odds = null;
    private List _threes = null;
    private List _fours = null;
    private List _sixes = null;

    private ListRowIterator _emptyIter = null;
    private ListRowIterator _listIter = null;
    private ListRowIterator _evensIter = null;
    private ListRowIterator _oddsIter = null;
    private ListRowIterator _threesIter = null;
    private ListRowIterator _foursIter = null;
    private ListRowIterator _sixesIter = null;

    private Selectable _trueFilter = null;
    private Selectable _falseFilter = null;
    private Selectable _evenFilter = null;
    private Selectable _oddFilter = null;
    private Selectable _threeFilter = null;
    private Selectable _fourFilter = null;
    private Selectable _sixFilter = null;

    private RowDecorator _decorator = null;

    public void setUp() throws Exception {
        super.setUp();
        _empty = new ArrayList();
        _list = new ArrayList();
        _evens = new ArrayList();
        _odds = new ArrayList();
        _threes = new ArrayList();
        _fours = new ArrayList();
        _sixes = new ArrayList();
        for (int i = 0; i < 20; i++) {
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i));

            _list.add(row);
            if (i % 2 == 0) {
                _evens.add(row);
            }
            if (i % 2 == 1) {
                _odds.add(row);
            }
            if (i % 3 == 0) {
                _threes.add(row);
            }
            if (i % 4 == 0) {
                _fours.add(row);
            }
            if (i % 6 == 0) {
                _sixes.add(row);
            }
        }
        _emptyIter = new ListRowIterator(_empty);
        _listIter = new ListRowIterator(_list);
        _evensIter = new ListRowIterator(_evens);
        _oddsIter = new ListRowIterator(_odds);
        _threesIter = new ListRowIterator(_threes);
        _foursIter = new ListRowIterator(_fours);
        _sixesIter = new ListRowIterator(_sixes);

        _trueFilter = new Selectable() {
            public Object evaluate(RowDecorator row) {
                return Boolean.TRUE;
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _falseFilter = new Selectable() {
            
            public Object evaluate(RowDecorator row) {
                return Boolean.FALSE;
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _evenFilter = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 2 == 0);
                if (result) {
                    return (Boolean.TRUE);
                }
                return (Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _oddFilter = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 2 == 1);
                if (result) {
                    return (Boolean.TRUE);
                }
                return (Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _threeFilter = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 3 == 0);
                if (result) {
                    return (Boolean.TRUE);
                }
                return (Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _fourFilter = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 4 == 0);
                if (result) {
                    return (Boolean.TRUE);
                }
                return (Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };
        _sixFilter = new Selectable() {
            
            public Object evaluate(RowDecorator drow) {
                boolean result = false;
                Row row = drow.getRow();
                result = (((Integer) (row.get(0))).intValue() % 6 == 0);
                if (result) {
                    return (Boolean.TRUE);
                }
                return (Boolean.FALSE);
            }

            public DataType getDataType() {
                return (new BooleanType());
            }

            public String getName() {
                return "";
            }

            public String getAlias() {
                return "";
            }

            public String getLabel() {
                return "";
            }

            public void setVariableContext(VariableContext ctx) {
            }
        };

        _decorator = new RowDecorator((Map) null);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _empty = null;
        _list = null;
        _evens = null;
        _odds = null;
        _threes = null;
        _fours = null;
        _sixes = null;

        _emptyIter = null;
        _listIter = null;
        _evensIter = null;
        _oddsIter = null;
        _threesIter = null;
        _foursIter = null;
        _sixesIter = null;

        _trueFilter = null;
        _falseFilter = null;
        _evenFilter = null;
        _oddFilter = null;
        _threeFilter = null;
        _fourFilter = null;
        _sixFilter = null;
    }

    //------------------------------------------------------------------- Tests

    public void testTruePredicateWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _trueFilter);
        walkLists(_listIter, filtered, _list.size());
    }

    public void testTruePredicateNextPreviousNext() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _trueFilter);
        nextPreviousNext(_listIter, filtered);
    }

    public void testFalsePredicateWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _falseFilter);
        walkLists(_emptyIter, filtered, _empty.size());
    }

    public void testEvensWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _evenFilter);
        walkLists(_evensIter, filtered, _evens.size());
    }

    public void testOddsWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _oddFilter);
        walkLists(_oddsIter, filtered, _odds.size());
    }

    public void testThreesWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _threeFilter);
        walkLists(_threesIter, filtered, _threes.size());
    }

    public void testFoursWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _fourFilter);
        walkLists(_foursIter, filtered, _fours.size());
    }

    public void testSixesWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _sixFilter);
        walkLists(_sixesIter, filtered, _sixes.size());
    }

    public void testNestedTrueWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(_list),
            _decorator, _trueFilter), _decorator, _trueFilter);
        walkLists(_listIter, filtered, _list.size());
    }

    public void testNestedEvensWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(_list),
            _decorator, _evenFilter), _decorator, _trueFilter);
        walkLists(_evensIter, filtered, _evens.size());
    }

    public void testNestedOddsWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(_list),
            _decorator, _oddFilter), _decorator, _trueFilter);
        walkLists(_oddsIter, filtered, _odds.size());
    }

    public void testNestedSixesWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(_list),
            _decorator, _evenFilter), _decorator, _threeFilter);
        walkLists(_sixesIter, filtered, _sixes.size());
    }

    public void testNestedSixesWalkLists2() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(_list),
            _decorator, _threeFilter), _decorator, _evenFilter);
        walkLists(_sixesIter, filtered, _sixes.size());
    }

    public void testNestedSixesWalkLists3() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new FilteringRowIterator(new FilteringRowIterator(new ListRowIterator(
            _list), _decorator, _threeFilter), _decorator, _evenFilter), _decorator, _trueFilter);
        walkLists(_sixesIter, filtered, _sixes.size());
    }

    public void testTrivialJoinOnFilteredWalkLists() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _fourFilter);
        NestedLoopJoinedRowIterator join = new NestedLoopJoinedRowIterator(filtered, new SingleRowIterator(new SimpleRow(0)), 0);
        walkLists(_foursIter, join, _fours.size());
    }

    public void testFilteredJoinWalkLists() throws Exception {
        List foolist = new ArrayList();
        List barlist = new ArrayList();

        for (int i = 0; i < 10; i++) {
            Row foorow = new SimpleRow(1);
            foorow.set(0, new Integer(i));
            foolist.add(foorow);

            Row barrow = new SimpleRow(1);
            barrow.set(0, new Integer(i));
            barlist.add(barrow);
        }

        List joinlist = new ArrayList();
        {
            Iterator fooiter = foolist.iterator();
            while (fooiter.hasNext()) {
                Row foorow = (Row) (fooiter.next());
                Iterator bariter = barlist.iterator();
                while (bariter.hasNext()) {
                    Row barrow = (Row) (bariter.next());
                    JoinedRow joinrow = new JoinedRow();
                    joinrow.addRow(foorow);
                    joinrow.addRow(barrow);
                    joinlist.add(joinrow);
                }
            }
        }

        ListRowIterator fooiter = new ListRowIterator(foolist);
        ListRowIterator bariter = new ListRowIterator(barlist);

        NestedLoopJoinedRowIterator join = new NestedLoopJoinedRowIterator(fooiter, bariter, 1);

        RowDecorator joindec = null;
        {
            HashMap fieldmap = new HashMap();
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()), new Integer(0));
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("bar"), "num", null, new IntegerType()), new Integer(1));
            joindec = new RowDecorator(fieldmap);
        }
        Selectable joinfilter = makeLeafWhereNode(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()),
            new EqualFunction(), new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()));
        FilteringRowIterator joinfiltered = new FilteringRowIterator(join, joindec, joinfilter);
        walkLists(new ListRowIterator(joinlist), joinfiltered, joinlist.size());
    }

    public void testFilteredJoinOnFilteredWalkLists() throws Exception {
        List foolist = new ArrayList();
        List barlist = new ArrayList();

        for (int i = 0; i < 10; i++) {
            Row foorow = new SimpleRow(1);
            foorow.set(0, new Integer(i));
            foolist.add(foorow);

            Row barrow = new SimpleRow(1);
            barrow.set(0, new Integer(i));
            barlist.add(barrow);
        }

        List joinlist = new ArrayList();
        {
            Iterator fooiter = foolist.iterator();
            while (fooiter.hasNext()) {
                Row foorow = (Row) (fooiter.next());
                Iterator bariter = barlist.iterator();
                while (bariter.hasNext()) {
                    Row barrow = (Row) (bariter.next());
                    JoinedRow joinrow = new JoinedRow();
                    joinrow.addRow(foorow);
                    joinrow.addRow(barrow);
                    joinlist.add(joinrow);
                }
            }
        }

        RowIterator fooiter = new ListRowIterator(foolist);
        RowIterator bariter = new ListRowIterator(barlist);

        RowDecorator foodec = null;
        {
            HashMap fieldmap = new HashMap();
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()), new Integer(0));
            foodec = new RowDecorator(fieldmap);
        }
        Selectable foofilter = makeLeafWhereNode(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()),
            new GreaterThanOrEqualFunction(), new Literal(new Integer(0), new IntegerType()));
        fooiter = new FilteringRowIterator(fooiter, foodec, foofilter);

        NestedLoopJoinedRowIterator join = new NestedLoopJoinedRowIterator(fooiter, bariter, 1);

        RowDecorator joindec = null;
        {
            HashMap fieldmap = new HashMap();
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()), new Integer(0));
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("bar"), "num", null, new IntegerType()), new Integer(1));
            joindec = new RowDecorator(fieldmap);
        }
        Selectable joinfilter = makeLeafWhereNode(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()),
            new EqualFunction(), new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()));
        FilteringRowIterator joinfiltered = new FilteringRowIterator(join, joindec, joinfilter);
        walkLists(new ListRowIterator(joinlist), joinfiltered, joinlist.size());
    }

    public void testBug() throws Exception {
        FilteringRowIterator filtered = new FilteringRowIterator(new ListRowIterator(_list), _decorator, _fourFilter);
        while (_foursIter.hasNext()) {
            _foursIter.next();
            filtered.next();
        }
        assertTrue(filtered.hasPrevious());
        assertTrue(!filtered.hasNext());
        assertEquals(_foursIter.previous(), filtered.previous());
    }

    public void testBug2() throws Exception {
        List foolist = new ArrayList();

        for (int i = 0; i < 3; i++) {
            Row foorow = new SimpleRow(1);
            foorow.set(0, new Integer(i));
            foolist.add(foorow);
        }

        RowIterator fooiter = new ListRowIterator(foolist);

        RowDecorator foodec = null;
        {
            HashMap fieldmap = new HashMap();
            fieldmap.put(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()), new Integer(0));
            foodec = new RowDecorator(fieldmap);
        }
        Selectable foofilter = makeLeafWhereNode(new ColumnIdentifier(new TableIdentifier("foo"), "id", null, new IntegerType()),
            new GreaterThanOrEqualFunction(), new Literal(new Integer(0), new IntegerType()));
        fooiter = new FilteringRowIterator(fooiter, foodec, foofilter);

        RowIterator testing = fooiter;

        assertEquals(new SimpleRow(new Object[] { new Integer(0)}), testing.next());
        assertTrue(testing.currentIndex() != testing.nextIndex());
        assertTrue(testing.hasNext());
        assertTrue(testing.currentIndex() != testing.nextIndex());
        assertEquals(new SimpleRow(new Object[] { new Integer(1)}), testing.next());
    }

    //------------------------------------------------------------------- Utils

    private static void walkForward(RowIterator expected, RowIterator testing) throws Exception {
        while (expected.hasNext()) {
            assertEquals(expected.nextIndex(), testing.nextIndex());
            assertEquals(expected.previousIndex(), testing.previousIndex());
            assertTrue(testing.hasNext());
            assertEquals(expected.next(), testing.next());
            assertEquals(expected.current(), testing.current());
        }
    }

    private static void walkBackward(RowIterator expected, RowIterator testing) throws Exception {
        while (expected.hasPrevious()) {
            assertEquals(expected.nextIndex(), testing.nextIndex());
            assertEquals(expected.previousIndex(), testing.previousIndex());
            assertTrue(testing.hasPrevious());
            assertEquals(expected.previous(), testing.previous());
            assertEquals(expected.current(), testing.current());
        }
    }

    private static void nextPreviousNext(RowIterator expected, RowIterator testing) throws Exception {
        assertEquals(expected.next(), testing.next());
        assertEquals(expected.previous(), testing.previous());
        assertEquals(expected.next(), testing.next());
    }

    protected static void walkLists(RowIterator expected, RowIterator testing, int numRowsInExpected) throws Exception {
        // walk all the way forward
        walkForward(expected, testing);

        // walk all the way back
        walkBackward(expected, testing);

        // forward,back,foward
        while (expected.hasNext()) {
            assertEquals(expected.nextIndex(), testing.nextIndex());
            assertEquals(expected.previousIndex(), testing.previousIndex());
            assertTrue(testing.hasNext());
            assertEquals(expected.next(), testing.next());
            assertTrue(testing.hasPrevious());
            assertEquals(expected.previous(), testing.previous());
            assertTrue(testing.hasNext());
            assertEquals(expected.next(), testing.next());
        }

        // walk all the way back
        walkBackward(expected, testing);

        for (int i = 0; i < numRowsInExpected; i++) {
            // walk forward i
            for (int j = 0; j < i; j++) {
                assertEquals(expected.nextIndex(), testing.nextIndex());
                assertEquals(expected.previousIndex(), testing.previousIndex());
                assertTrue("i=" + i + "; j=" + j, expected.hasNext());
                assertTrue(testing.hasNext());
                assertEquals(expected.next(), testing.next());
            }

            // walk back i/2
            for (int j = 0; j < i / 2; j++) {
                assertEquals(expected.nextIndex(), testing.nextIndex());
                assertEquals(expected.previousIndex(), testing.previousIndex());
                assertTrue(expected.hasPrevious());
                assertTrue(testing.hasPrevious());
                assertEquals(expected.previous(), testing.previous());
            }
            // walk foward i/2
            for (int j = 0; j < i / 2; j++) {
                assertEquals(expected.nextIndex(), testing.nextIndex());
                assertEquals(expected.previousIndex(), testing.previousIndex());
                assertTrue(expected.hasNext());
                assertTrue(testing.hasNext());
                assertEquals(expected.next(), testing.next());
            }

            // walk back i
            for (int j = 0; j < i; j++) {
                assertEquals(expected.nextIndex(), testing.nextIndex());
                assertEquals(expected.previousIndex(), testing.previousIndex());
                assertTrue(expected.hasPrevious());
                assertTrue(testing.hasPrevious());
                assertEquals("i=" + i + ";j=" + j, expected.previous(), testing.previous());
            }
        }

        // random walk
        Random random = new Random();
        StringBuffer walkdescr = new StringBuffer(200);
        for (int i = 0; i < 200; i++) {
            switch (random.nextInt(6)) {
                case 0:
                    // step foward
                    if (expected.hasNext()) {
                        walkdescr.append("0");
                        assertEquals(walkdescr.toString(), expected.next(), testing.next());
                    }
                    break;
                case 1:
                    // step backward
                    if (expected.hasPrevious()) {
                        walkdescr.append("1");
                        assertEquals(walkdescr.toString(), expected.previous(), testing.previous());
                    }
                    break;
                case 2:
                    // hasPrevious
                    walkdescr.append("2");
                    assertEquals(walkdescr.toString(), expected.hasPrevious(), testing.hasPrevious());
                    break;
                case 3:
                    // hasNext
                    walkdescr.append("3");
                    assertEquals(walkdescr.toString(), expected.hasNext(), testing.hasNext());
                    break;
                case 4:
                    // previousIndex
                    walkdescr.append("4");
                    assertEquals(walkdescr.toString(), expected.previousIndex(), testing.previousIndex());
                    break;
                case 5:
                    // nextIndex
                    walkdescr.append("5");
                    assertEquals(walkdescr.toString(), expected.nextIndex(), testing.nextIndex());
                    break;
            }
        }

    }

    private Selectable makeLeafWhereNode(Selectable left, Function fun, Selectable right) {
        fun.addArgument(left);
        if (null != right) {
            fun.addArgument(right);
        }
        return fun;
    }

    public void testUnsupported() throws Exception {
        RowIterator rows = makeRowIterator();
        try {
            rows.add(new SimpleRow(1));
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testSet() throws Exception {
        RowIterator iterator = makeRowIterator();
        for (int i = 0; i < 10; i += 2) {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            Row row = new SimpleRow(1);
            row.set(0, new Integer(i + 2));
            iterator.set(row);
        }

        iterator.reset();
        for (int i = 0; i < 10; i += 2) {
            assertTrue(iterator.hasNext());
            Row row = iterator.next();
            assertEquals(new Integer(i + 2), row.get(0));
        }
        
        // test invalid set
        Row row = new SimpleRow(1);
        row.set(0, new Integer(3));
        try {
            iterator.set(row);
            fail("Expected IllegalStateException: Out of range");
        }catch (IllegalStateException e) {
            // expected
        }
    }
    
    public void testNegative() throws Exception {
        RowIterator iter = new AbstractFilteringRowIterator(new EmptyRowIterator()) {
            protected boolean determineNextRow() throws AxionException {
                throw new AxionException("Test Exception");
            }

            protected boolean determinePreviousRow() throws AxionException {
                throw new AxionException("Test Exception");
            }
        };
        
        try {
            iter.hasNext();
            fail("Expected Exception");
        } catch (RuntimeException e) {
            // expected
        }
        
        try {
            iter.hasPrevious();
            fail("Expected Exception");
        } catch (RuntimeException e) {
            // expected
        }
    }
}