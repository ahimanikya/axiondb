/* 
 * =======================================================================
 * Copyright (c) 2005-2006 Axion Development Team.  All rights reserved.
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
package org.axiondb.engine.rowcollection;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.engine.rowiterators.RebindableIndexedRowIterator;
import org.axiondb.engine.rowiterators.UnmodifiableRowIterator;
import org.axiondb.event.RowDeletedEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.functions.ComparisonFunction;

/**
 * Int key and Row value Map, this does not implement java.util.Map interface and has
 * limited Map like API. Does not implement EntrySet and and KeySet, tather it just
 * retunds their iterator.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */

public class IntRowMap extends IntHashMap {

    /** Creates an IntRowMap of small initial capacity. */
    public IntRowMap() {
        super(16);
    }

    /**
     * Creates an IntRowMap of specified initial capacity. Unless the map size exceeds the
     * specified capacity no memory allocation is ever performed.
     * 
     * @param capacity the initial capacity.
     */
    public IntRowMap(int capacity) {
        super(capacity);
    }

    /**
     * Creates a IntRowMap containing the specified entries, in the order they are
     * returned by the map's iterator.
     * 
     * @param map the map whose entries are to be placed into this map.
     */
    public IntRowMap(IntRowMap map) {
        super(map);
    }

    public final void addIndex(Index index) {
        _indices.add(index);
    }

    public Row addRow(Table table, Row row) throws AxionException {
        RowEvent event = null;
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) (_indices.get(i));
            if(event == null) {
                event = new RowInsertedEvent(table, null, row);
            }
            index.rowInserted(event);
        }
        return putRow(row.getIdentifier(), row);
    }

    /**
     * Removes all mappings from this {@link IntRowMap}.
     */
    public void clear() {
        super.clear();
        try {
            truncateIndices();
        } catch (AxionException e) {
            clearIndexes();
        }
    }

    public final void clearIndexes()  {
        _indices.clear();
    }

    public Row deleteRow(Table table, Row deleted) throws AxionException {
        deleted = removeRow(deleted.getIdentifier());
        if (deleted != null) {
            RowEvent event = null;
            for (int i = 0, I = _indices.size(); i < I; i ++) {
                Index index = (Index) (_indices.get(i));
                if(event == null) {
                    event = new RowDeletedEvent(table, deleted, null);
                }
                index.rowDeleted(event);
            }
        }
        return deleted;
    }

    public RowIterator getIndexedRows(Table source, Selectable node, boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getIndexedRows(source, node));
        }
        return getIndexedRows(source, node);
    }

    public Index getIndexForColumn(Column column) {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) (_indices.get(i));
            if (column.equals(index.getIndexedColumn())) {
                return index;
            }
        }
        return null;
    }

    /**
     * Returns the value to which this {@link IntRowMap}maps the specified key.
     * 
     * @param key the key whose associated value is to be returned.
     * @return the value to which this map maps the specified key, or <code>null</code>
     *         if there is no mapping for the key.
     */
    public final Row getRow(int key) {
        return (Row) super.get(key);
    }

    public boolean isColumnIndexed(Column column) {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) (_indices.get(i));
            if (column.equals(index.getIndexedColumn())) {
                return true;
            }
        }
        return false;
    }

    public void populateIndex(Table table, Index index) throws AxionException {
        for (RowIterator i = rowValues().rowIterator(); i.hasNext();) {
            Row row = i.next();
            if (row != null) {
                index.rowInserted(new RowInsertedEvent(table, null, row));
            }
        }
    }

    /**
     * Associates the specified value with the specified key in this {@link IntRowMap}.
     * If the {@link IntRowMap}previously contained a mapping for this key, the old value
     * is replaced.
     * 
     * @param key the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @return the previous value associated with specified key, or <code>null</code> if
     *         there was no mapping for key. A <code>null</code> return can also
     *         indicate that the map previously associated <code>null</code> with the
     *         specified key.
     */
    public final Row putRow(int key, Row value) {
        return (Row) super.put(key, value);
    }

    /**
     * Removes the mapping for this key from this {@link IntRowMap}if present.
     * 
     * @param key the key whose mapping is to be removed from the map.
     * @return previous value associated with specified key, or <code>null</code> if
     *         there was no mapping for key. A <code>null</code> return can also
     *         indicate that the map previously associated <code>null</code> with the
     *         specified key.
     */
    public final Row removeRow(int key) {
        return (Row) super.remove(key);
    }

    /**
     * Returns a list iterator over the values in this list in proper sequence, (this map
     * maintains the insertion order).
     * 
     * @return a list iterator of the values in this list (in proper sequence).
     */
    public final RowIterator rowIterator() {
        return new ValueRowIterator();
    }


    /**
     * Returns a {@link RowCollection}view of the values contained in this
     * {@link IntRowMap}. The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa. The collection supports element
     * removal, which removes the corresponding mapping from this map, via the
     * <code>RowIterator.remove</code>,<code>RowCollection.remove</code> and
     * <code>clear</code> operations.
     * 
     * @return a row collection view of the values contained in this map.
     */
    public final RowCollection rowValues() {
        return _rowValues;
    }
    
    public void shutdown() {
        clear();
        _indices = null;
    }

    public void truncateIndices() throws AxionException {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) (_indices.get(i));
            index.truncate();
        }
    }
    
    public Row updateRow(Table table, Row oldrow, Row newrow) throws AxionException {
        newrow.setIdentifier(oldrow.getIdentifier());
        oldrow = putRow(newrow.getIdentifier(), newrow);

        RowEvent event = null;
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) (_indices.get(i));
            if(event == null) {
                event = new RowUpdatedEvent(table, oldrow, newrow);
            }
            if (oldrow != null) {
                index.rowUpdated(event);
            } else {
                index.rowInserted(event);
            }
        }
        return oldrow;
    }
    
    private RowIterator getIndexedRows(Table source, Selectable node) throws AxionException {
        if (node instanceof ComparisonFunction) {
            // attempting to map comparison function to existing index
            ComparisonFunction function = (ComparisonFunction) node;

            Column column = null;
            Literal literal = null;
            Selectable left = function.getArgument(0);
            Selectable right = function.getArgument(1);
            if (left instanceof ColumnIdentifier && right instanceof Literal) {
                column = source.getColumn(((ColumnIdentifier) left).getName());
                literal = (Literal) (right);
            } else if (left instanceof Literal && right instanceof ColumnIdentifier) {
                column = source.getColumn(((ColumnIdentifier) right).getName());
                literal = (Literal) (left);
                function = function.flip();
            } else {
                return null;
            }

            if (!isColumnIndexed(column)) {
                // no index for column
                return null;
            }

            Index index = getIndexForColumn(column);
            if (!index.supportsFunction(function)) {
                // index does not support required function
                return null;
            } else if (literal instanceof BindVariable) {
                // index found
                return new RebindableIndexedRowIterator(index, source, function, (BindVariable) literal);
            } else {
                // index found
                return index.getRowIterator(source, function, literal.evaluate(null));
            }
        } else if (node instanceof ColumnIdentifier) {
            Column column = source.getColumn(((ColumnIdentifier) node).getName());
            Index index = getIndexForColumn(column);
            if (index != null) {
                return index.getInorderRowIterator(source);
            }
        } else if (node instanceof Function) { // IS NULL and IS NOT NULL
            Function function = (Function) node;
            if (function.getArgumentCount() != 1) {
                return null;
            }

            Selectable colid = function.getArgument(0);
            if (colid instanceof ColumnIdentifier) {
                Column column = source.getColumn(((ColumnIdentifier) colid).getName());
                if (!isColumnIndexed(column)) {
                    // no index for column
                    return null;
                }

                Index index = getIndexForColumn(column);
                if (!index.supportsFunction(function)) {
                    // index does not support required function
                    return null;
                } else {
                    // index found
                    return index.getRowIterator(source, function, null);
                }
            }
        }

        return null; // No matching index found
    }
    
    private final class RowValues extends Values implements RowCollection {
        public boolean add(Row row) {
            addEntry(row.getIdentifier(), row.getIdentifier(), row);
            return true;
        }

        public boolean contains(Row row) {
            return IntRowMap.this.containsValue(row);
        }

        public boolean remove(Row row) {
            return super.remove(row);
        }

        public RowIterator rowIterator() {
            return IntRowMap.this.rowIterator();
        }
    }

    private class ValueRowIterator extends EntryIterator implements RowIterator {

        public void add(Row row) throws UnsupportedOperationException, AxionException {
            addEntry(row.getIdentifier(), row.getIdentifier(), row);
        }

        public Row current() throws NoSuchElementException {
            return (Row)currentEntry().getValue();
        }

        public Row first() throws NoSuchElementException, AxionException {
            return (Row)firstEntry().getValue();
        }

        public Row last() throws NoSuchElementException, AxionException {
            return (Row)lastEntry().getValue();
        }

        public Row next() throws NoSuchElementException, AxionException {
            return (Row)nextEntry().getValue();
        }

        public int next(int count) throws AxionException {
            for (int i = 0; i < count; i++) {
                next();
            }
            return current().getIdentifier();
        }

        public Row peekNext() throws NoSuchElementException, AxionException {
            return (Row)peekNextEntry().getValue();
        }

        public Row peekPrevious() throws NoSuchElementException, AxionException {
            return (Row)peekPreviousEntry().getValue();
        }

        public Row previous() throws NoSuchElementException, AxionException {
            return (Row)previousEntry().getValue();
        }

        public int previous(int count) throws AxionException {
            for (int i = 0; i < count; i++) {
                previous();
            }
            return current().getIdentifier();
        }

        public void set(Row row) throws UnsupportedOperationException, AxionException {
            if (!hasCurrent()) {
                throw new IllegalStateException();
            }
            currentEntry().setValue(row);
        }

    }

    private transient List _indices = new ArrayList(4);

    /** Holds the values view. */
    private transient RowValues _rowValues = new RowValues();
}
