/*
 * $Id: TestIndexNestedLoopJoinedRowIterator_LeftOuterJoinCase.java,v 1.1 2007/11/28 10:01:25 jawed Exp $
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

package org.axiondb.engine.rowiterators;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Index;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.indexes.ObjectArrayIndex;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.StringType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:25 $
 * @author Rodney Waldhoff
 */
public class TestIndexNestedLoopJoinedRowIterator_LeftOuterJoinCase extends AbstractRowIteratorTest {

    //------------------------------------------------------------ Conventional

    public TestIndexNestedLoopJoinedRowIterator_LeftOuterJoinCase(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestIndexNestedLoopJoinedRowIterator_LeftOuterJoinCase.class);
        return suite;
    }

    private List _left;
    private Table _table;
    private ConcreteFunction _where;
    private Index _index;
    private Column _column;

    protected void setUp() throws Exception {
        _left = new ArrayList();
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "A");
            _left.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "B");
            _left.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "B");
            row.set(1, "A");
            _left.add(row);
        }
        _column = new Column("c", StringType.instance());
        _table = new MemoryTable("x");
        _table.addColumn(_column);
        _table.addColumn(new Column("d", StringType.instance()));
        _index = new ObjectArrayIndex("y", _column, false);
        _table.addIndex(_index);
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "A");
            _table.addRow(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "B");
            _table.addRow(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, "A");
            row.set(1, "C");
            _table.addRow(row);
        }
        _where = new EqualFunction();
        _where.addArgument(new ColumnIdentifier("b"));
        _where.addArgument(new ColumnIdentifier(new TableIdentifier("x"), "c"));
    }

    protected RowIterator makeRowIterator() {
        try {
            RowIterator left = new ListRowIterator(_left);
            ChangingIndexedRowIterator right = new ChangingIndexedRowIterator(_index, _table, new EqualFunction());
            RowIterator iter = new IndexNestedLoopJoinedRowIterator(left, 1, right, 2, true, false);
            return iter;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }

    protected int getSize() {
        return 7;
    }

    protected List makeRowList() {
        List list = new ArrayList();
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "A");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "B");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "A");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "C");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "A");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "A");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "A");
            row.set(1, "B");
            row.set(2, null);
            row.set(3, null);
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "B");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "B");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "B");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "C");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(4);
            row.set(0, "B");
            row.set(1, "A");
            row.set(2, "A");
            row.set(3, "A");
            list.add(row);
        }
        return list;
    }

    //------------------------------------------------------------------- Tests

}