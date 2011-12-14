/*
 * 
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.axiondb.AxionException;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowComparator;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.visitors.FindAggregateFunctionVisitor;
import org.axiondb.functions.AggregateFunction;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.util.ComparatorChain;

/**
 * Processes a "raw" iterator to implement GROUP BY functionality.
 * 
 * @version  
 * @author Rahul Dwivedi
 * @author Rod Waldhoff
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 * @author Jonathan Giron
 */
public class GroupedRowIterator extends DelegatingRowIterator {

    public GroupedRowIterator(boolean sort, RowIterator rows, Map fieldMap, List groupBy, List selected, Selectable having, Selectable where,
            List orderBy) throws AxionException {
        super(null);

        _colIdToFieldMap = fieldMap;
        _groupByCols = groupBy;
        _selected = selected;
        _having = having;
        _where = where;
        _orderByNodes = orderBy;
        _aggrFnValueCache = new HashMap(_selected.size());

        SortedMap groupedRowMap = null;
        // If rows are not sorted then we need to sort them
        if (!isEmptyGroupBy() && sort) {
            groupedRowMap = doSort(rows);
        }

        // Determine in advance whether elements of Selectable list is an aggregate
        // function, so that we don't have to determines while making row for each group.
        // Keep the last element of the array reserved for having clause.
        _isAggregateFunction = new boolean[_selected.size() + 1];
        for (int i = 0, I = _selected.size();  i < I; i++) {
            _isAggregateFunction[i] = isAggregateFunction(_selected.get(i));
        }
        _isAggregateFunction[_selected.size()] = isAggregateFunction(_having);

        List grList = null;
        if (groupedRowMap == null) {
            grList = makeGroupRows(rows);
        } else {
            grList = makeGroupRows(groupedRowMap);
        }
        setDelegate(new ListRowIterator(grList));
    }

    public GroupedRowIterator(RowIterator rows, Map fieldMap, List groupBy, List selected, Selectable having, List orderBy) throws AxionException {
        this(true, rows, fieldMap, groupBy, selected, having, null, orderBy);
    }

    /** Not supported in the base implementation. */
    public void add(Row row) throws AxionException {
        throw new UnsupportedOperationException();
    }

    /** Not supported in the base implementation. */
    public void set(Row row) throws AxionException {
        throw new UnsupportedOperationException();
    }

    /** Not supported in the base implementation. */
    public void remove() throws AxionException {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(20);
        buf.append("Grouped(").append(_groupByCols == null ? "" : _groupByCols.toString());
        if(_having != null) {
            buf.append("; having=").append(_having).append(";");
        }
        buf.append("preSorted=").append(_preSorted).append(")");
        return  buf.toString();
    }

    // Returns true if the HAVING node evaluates to true
    private boolean acceptable(RowIterator currGroupRowIter, RowDecorator dec) throws AxionException {
        if (_having == null) {
            return true;
        }

        if (_isAggregateFunction[_selected.size()]) {
            Object val = evaluateAggregateFunction(dec, (ConcreteFunction) _having, currGroupRowIter);
            return ((Boolean) val).booleanValue();
        }
        
        // ISO/IEC 9075-2:2003, Section 7.10, General Rule 1 - clause is applied if having condition
        // evaluates to true; null evaluation thus maps to false.
        Boolean result = (Boolean) _having.evaluate(dec); 
        return (result == null) ? false : result.booleanValue();
    }

    private void addToCurrentGroup(RowDecorator dec, Row row, List currGroup) throws AxionException {
        if (_where == null) {
            currGroup.add(row);
            return;
        }

        // Else add to current group if WHERE node is null or evaluates to true
        dec.setRow(row);
        
        Boolean result = (Boolean) _where.evaluate(dec); 
        if (result != null && result.booleanValue()) {
            currGroup.add(row);
        }
    }

    // Collect rows for each group, groups will be maintained in natual sort order.
    private SortedMap doSort(RowIterator rows) throws AxionException {
        ComparatorChain sortChain = generateOrderChain();
        SortedMap groupedRows = new TreeMap(sortChain);
        RowDecorator dec = new RowDecorator(_colIdToFieldMap);
        while (rows.hasNext()) {
            Row row = rows.next();
            List currGroup = (List) groupedRows.get(row);
            if (currGroup == null) {
                currGroup = new ArrayList();
                groupedRows.put(row, currGroup);
            }
            addToCurrentGroup(dec, row, currGroup);
        }
        _preSorted = false;
        return groupedRows;
    }

    private Object evaluateAggregateFunction(RowDecorator dec, ConcreteFunction fn, RowIterator rows) throws AxionException {
        if (fn instanceof AggregateFunction) {
            rows.reset();
            AggregateFunction vfn = (AggregateFunction) fn;
            Object val = _aggrFnValueCache.get(vfn);
            if (val == null) {
                val = vfn.evaluate(new RowIteratorRowDecoratorIterator(rows, dec));
                _aggrFnValueCache.put(vfn, val);
            }
            return val;
        } else {
            // Aggregate function might have been nested with another aggregate or scalar
            // function
            List fnArgs = new ArrayList(fn.getArgumentCount());
            for (int i = 0, I = fn.getArgumentCount();  i < I; i++) {
                Object arg = fn.getArgument(i);
                fnArgs.add(i, arg); // Keep original selectables
                if (arg instanceof ConcreteFunction) {
                    ConcreteFunction innerFn = (ConcreteFunction) arg;
                    Object val = evaluateAggregateFunction(dec, innerFn, rows);
                    fn.setArgument(i, new Literal(val, ((ConcreteFunction) arg).getDataType()));
                }
            }
            
            if(dec.getRow() == null) {
                dec.setRow(rows.first());
            }
            Object val = fn.evaluate(dec);
            for (int i = 0, I = fn.getArgumentCount();  i < I; i++) {
                fn.setArgument(i, (Selectable) fnArgs.get(i)); // Reset func argument
            }
            return val;
        }
    }

    private ComparatorChain generateOrderChain() {
        ComparatorChain chain = new ComparatorChain();
        for (int i = 0, I = _groupByCols.size(); i < I; i++) {
            Selectable sel = (Selectable) _groupByCols.get(i);
            if (isDescending(sel, _orderByNodes)) {
                chain.setReverseSort(i);
            }
            chain.addComparator(new RowComparator(sel, new RowDecorator(_colIdToFieldMap)));
        }
        return chain;
    }

    private boolean isAggregateFunction(Object sel) {
        if (sel instanceof ConcreteFunction) {
            FindAggregateFunctionVisitor findAggr = new FindAggregateFunctionVisitor();
            findAggr.visit((Selectable) sel); // Check for aggregate functions
            if (findAggr.foundAggregateFunction()) {
                return true;
            }
        }
        return false;
    }

    private boolean isDescending(Selectable sel, List orderNodes) {
        if (null != orderNodes && null != sel) {
            for (Iterator iter = orderNodes.iterator(); iter.hasNext();) {
                OrderNode node = (OrderNode) (iter.next());
                if (node.getSelectable().equals(sel)) {
                    return node.isDescending();
                }
            }
        }
        return false;
    }

    private boolean isEmptyGroupBy() {
        return (_groupByCols == null || _groupByCols.isEmpty());
    }

    private Row makeGroupRow(List currGroup, RowDecorator dec, Row row) throws AxionException {
        List myCurrGroup = new ArrayList(currGroup);
        RowIterator currGroupRowIter = new ListRowIterator(myCurrGroup);

        dec.setRow(row);
        _aggrFnValueCache.clear();
        if (acceptable(currGroupRowIter, dec)) {
            return makeGroupRow(currGroupRowIter, dec);
        }
        return null;
    }

    private Row makeGroupRow(RowIterator currGroupRowIter, RowDecorator dec) throws AxionException {
        SimpleRow rowOut = new SimpleRow(_selected.size());
        for (int i = 0, I = _selected.size(); i < I; i++) {
            if (_isAggregateFunction[i]) {
                rowOut.set(i, evaluateAggregateFunction(dec, (ConcreteFunction) _selected.get(i), currGroupRowIter));
            } else {
                rowOut.set(i, ((Selectable) _selected.get(i)).evaluate(dec));
            }
        }
        return rowOut;
    }

    // This assumes rows are sorted.
    private List makeGroupRows(RowIterator rows) throws AxionException {
        if (_groupByCols != null && !_groupByCols.isEmpty() && !rows.hasNext()) {
            return Collections.EMPTY_LIST;
        }

        List comparators = new ArrayList();
        if (_groupByCols != null && !_groupByCols.isEmpty()) {
            RowDecorator dec = new RowDecorator(_colIdToFieldMap);
            for (int i = 0,  I = _groupByCols.size(); i < I; i++) {
                comparators.add(new RowComparator((Selectable) _groupByCols.get(i), dec));
            }
        }

        List groupedRows = new ArrayList();
        ArrayList currGroup = new ArrayList();
        RowDecorator dec = new RowDecorator(_colIdToFieldMap);

        Row lastRow = null;
        if (rows.hasNext() && !comparators.isEmpty()) {
            lastRow = rows.next();
            addToCurrentGroup(dec, lastRow, currGroup);
            while (rows.hasNext()) {
                Row row = rows.next();
                for (int i = 0, I = comparators.size(); i < I && !currGroup.isEmpty(); i++) {
                    RowComparator comp = (RowComparator) comparators.get(i);

                    // Once we have a group we can apply group by aggregate function
                    if (comp.compare(lastRow, row) != 0) {
                        Row groupdRow = makeGroupRow(currGroup, dec, lastRow);
                        if (groupdRow != null) {
                            groupedRows.add(groupdRow);
                        }
                        currGroup.clear();
                        break;
                    }
                }
                addToCurrentGroup(dec, row, currGroup);
                lastRow = row;
            }
            // Make group row if any pending group exists.
            Row groupdRow = makeGroupRow(currGroup, dec, lastRow);
            if (groupdRow != null) {
                groupedRows.add(groupdRow);
            }
        } else {
            groupedRows.add(makeGroupRow(rows, dec));
        }
        
        return groupedRows;
    }

    private List makeGroupRows(SortedMap groupedRowMap) throws AxionException {
        if (_groupByCols != null && groupedRowMap.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        List groupedRows = new ArrayList();
        List currGroup = new ArrayList();
        RowDecorator dec = new RowDecorator(_colIdToFieldMap);

        Iterator iter = groupedRowMap.values().iterator();
        while (iter.hasNext()) {
            currGroup = (List) iter.next();
            Row groupdRow = makeGroupRow(currGroup, dec, (Row) currGroup.get(0));
            if (groupdRow != null) {
                groupedRows.add(groupdRow);
            }
        }
        return groupedRows;
    }

    private Map _aggrFnValueCache;
    private Map _colIdToFieldMap;
    private List _groupByCols;
    private Selectable _having;
    private boolean[] _isAggregateFunction;
    private List _orderByNodes;
    private List _selected;
    private Selectable _where;
    private boolean _preSorted = true;
}
