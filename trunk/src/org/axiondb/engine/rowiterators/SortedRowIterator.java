/*
 * 
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;
import org.axiondb.AxionException;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowComparator;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.util.ComparatorChain;

/**
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 * @version 
 */
public abstract class SortedRowIterator extends DelegatingRowIterator {

    protected SortedRowIterator() {
        super(null);
    }

    public String toString() {
        return "SortedRowIterator (key=" + _keyString + ")";
    }
    
    protected static final ComparatorChain buildComparatorChain(List orderNodes, RowDecorator rowDecorator) {
        ComparatorChain comparator = new ComparatorChain();
        for (int i = 0, I = orderNodes.size(); i < I; i++) {
            OrderNode node = (OrderNode) orderNodes.get(i);
            if (node.isDescending()) {
                comparator.setReverseSort(i);
            }
            comparator.addComparator(new RowComparator(node.getSelectable(), rowDecorator));
        }
        return comparator;
    }
    
    public static class MergeSort extends SortedRowIterator {

        public MergeSort(RowIterator unsortedRows, Comparator comparator) throws AxionException {
            List sortedList = getSortedRowList(unsortedRows, comparator);
            this.setDelegate(new ListRowIterator(sortedList));
        }

        public MergeSort(RowIterator unsortedRows, List orderNodes, RowDecorator rowDecorator) throws AxionException {
            this(unsortedRows, buildComparatorChain(orderNodes, rowDecorator));
            _keyString = orderNodes.toString();
        }

        private List getSortedRowList(RowIterator unsortedRows, Comparator comparator) throws AxionException {
            List list = new ArrayList();

            while (unsortedRows.hasNext()) {
                list.add(unsortedRows.next());
            }

            Collections.sort(list, comparator);
            return list;
        }
    }

    public static class MutableMergeSort extends SortedRowIterator {

        public MutableMergeSort(RowSource source, RowIterator unsortedRows, Comparator comparator) throws AxionException {
            IntList ids = getSortedRowIds(unsortedRows, comparator);
            this.setDelegate(new LazyRowRowIterator(source, ids.listIterator(), ids.size()));
        }

        public MutableMergeSort(RowSource source, RowIterator unsortedRows, List orderNodes, RowDecorator rowDecorator) throws AxionException {
            this(source, unsortedRows, buildComparatorChain(orderNodes, rowDecorator));
            _keyString = orderNodes.toString();
        }

        public void remove() throws AxionException {
            getDelegate().remove();
        }

        public void set(Row row) throws AxionException {
            getDelegate().set(row);
        }

        private IntList getSortedRowIds(RowIterator unsortedRows, Comparator comparator) throws AxionException {
            List list = new ArrayList();

            while (unsortedRows.hasNext()) {
                list.add(unsortedRows.next());
            }

            Object rowArry[] = list.toArray();
            Arrays.sort(rowArry, comparator);

            IntList ids = new ArrayIntList(rowArry.length);
            for (int j = 0; j < rowArry.length; j++) {
                ids.add(((Row) rowArry[j]).getIdentifier());
            }
            return ids;
        }
    }

    protected String _keyString = "";
    protected RowIterator _rowIter = null;
}
