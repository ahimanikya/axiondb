/*
 * $Id: TestGroupedRowIterator.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

package org.axiondb.engine.rowiterators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.engine.rows.SimpleRow;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rahul Dwivedi
 * @author Ahimanikya Satapathy
 */
public class TestGroupedRowIterator extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestGroupedRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestGroupedRowIterator.class);
        return suite;
    }

    //---------------------------------------------------------- Abstract Impls

    public RowIterator makeRowIterator() {
        ArrayList list = new ArrayList();
        for (int i = 0; i <= 2; i++) {
            for (int j = 0; j <= 2; j++) {
                Row row = new SimpleRow(2);
                row.set(0, new Integer(j));
                row.set(1, new Integer(i));
                list.add(row);
            }
        }
        Map map = new HashMap();
        map.put(new ColumnIdentifier("a"), new Integer(0));
        map.put(new ColumnIdentifier("b"), new Integer(1));
        List groupList = new ArrayList();
        groupList.add(new ColumnIdentifier("a"));
        groupList.add(new ColumnIdentifier("b"));

        // no aggregation involved selected both column and have both col in the group by
        // caluse.. valid group by ..
        // and since we are just testing Iterator only.
        List selectList = new ArrayList();
        selectList.add(new ColumnIdentifier("a"));
        selectList.add(new ColumnIdentifier("b"));

        List orderBy = new ArrayList();
        orderBy.add(new OrderNode(new ColumnIdentifier("a"), true));
        orderBy.add(new OrderNode(new ColumnIdentifier("b"), true));

        RowIterator iter = null;
        try {
            iter = new GroupedRowIterator(new ListRowIterator(list), map, groupList, selectList, null, orderBy);
        } catch (Exception e) {
            // do nothing pass null along.
        }

        return iter;
    }
    
    protected int getSize() {
        return 9;
    }


    // we need to form a a list that replicate the result that we expect after this group
    // by.....initial iterator we pass in makeRowIterator() method is...
    /*
     * 0,0 1,0 2,0 0,1 1,1 2,1 0,2 1,2 2,2 the iterator created is similar to what we'll
     * get for this query select a,b from table x group by a,b; the result should look
     * like this ... taking into cosideration that group by sorts result in descending
     * order..... 2,2 2,1 2,0 1,2 1,1 1,0 0,2 0,1 0,0
     */
    public List makeRowList() {
        ArrayList list = new ArrayList();
        for (int i = 2; i >= 0; i--) {
            for (int j = 2; j >= 0; j--) {
                Row row = new SimpleRow(2);
                row.set(0, new Integer(i));
                row.set(1, new Integer(j));
                list.add(row);
            }
        }

        return list;
    }

    //--------------------------------------------------------------- Lifecycle

    private ArrayList _selectList;
    private ArrayList _groups;
    private List _list;

    public void setUp() throws Exception {
        super.setUp();
        _list = new ArrayList();
        for (int i = 0; i <= 2; i++) {
            for (int j = 0; j <= 2; j++) {
                Row row = new SimpleRow(2);
                row.set(0, new Integer(j));
                row.set(1, new Integer(i));
                _list.add(row);
            }
        }
        _groups = new ArrayList();
        _groups.add(new ColumnIdentifier("a"));
        _groups.add(new ColumnIdentifier("b"));

        // no aggregation involved selected both column and have both col in the group by
        // caluse.. valid group by ..
        // and since we are just testing Iterator only.
        _selectList = new ArrayList();
        _selectList.add(new ColumnIdentifier("a"));
        _selectList.add(new ColumnIdentifier("b"));

    }

    public void tearDown() throws Exception {
        super.tearDown();
        _selectList = null;
        _groups = null;
        _list = null;
    }

    //------------------------------------------------------------------- Tests

    public void testUnsupported() throws Exception {
        GroupedRowIterator iterator = (GroupedRowIterator) makeRowIterator();
        assertNotNull(iterator);
        try {
            iterator.add(null);
            fail("Expected Exception: Unsupported");
        } catch (UnsupportedOperationException e) {

        }

        try {
            iterator.remove();
            fail("Expected Exception: Unsupported");
        } catch (UnsupportedOperationException e) {

        }

        try {
            iterator.set(null);
            fail("Expected Exception: Unsupported");
        } catch (UnsupportedOperationException e) {

        }
    }

}