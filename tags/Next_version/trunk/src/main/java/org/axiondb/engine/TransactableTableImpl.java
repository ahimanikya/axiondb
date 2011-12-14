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

package org.axiondb.engine;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowComparator;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TransactableTable;
import org.axiondb.Transaction;
import org.axiondb.engine.rowcollection.IntRowMap;
import org.axiondb.engine.rowcollection.IntSet;
import org.axiondb.engine.rowiterators.AbstractAcceptingRowIterator;
import org.axiondb.engine.rowiterators.ChainedRowIterator;
import org.axiondb.engine.rowiterators.CollatingRowIterator;
import org.axiondb.engine.rowiterators.DelegatingRowIterator;
import org.axiondb.engine.rowiterators.LazyRowRowIterator;
import org.axiondb.engine.rowiterators.TransformingRowIterator;
import org.axiondb.engine.rowiterators.UnmodifiableRowIterator;
import org.axiondb.engine.tables.AbstractBaseTable;
import org.axiondb.event.RowDeletedEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.functions.ComparisonFunction;

/**
 * An implemenation of {@link TransactableTable}.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public final class TransactableTableImpl extends AbstractBaseTable implements TransactableTable {

    public TransactableTableImpl(Table table) {
        _table = table;
    }

    public final String getName() {
        return _table.getName();
    }

    public final Table getTable() {
        return _table;
    }

    public final String getType() {
        return _table.getType();
    }

    public final RowDecorator makeRowDecorator() {
        if (_dec == null) {
            _dec = _table.makeRowDecorator();
        }   
        return _dec;
    }

    public void addConstraint(Constraint constraint) throws AxionException {
        _table.addConstraint(constraint);
    }

    public Constraint removeConstraint(String name) {
        return _table.removeConstraint(name);
    }

    public final Constraint getConstraint(String name) {
        return _table.getConstraint(name);
    }

    public final Iterator getConstraints() {
        return _table.getConstraints();
    }

    /**
     * Check if unique constraint exists on a column
     * 
     * @param columnName name of the columm
     * @return true if uniqueConstraint exists on the column
     */
    public boolean isUniqueConstraintExists(String columnName) {
        return _table.isUniqueConstraintExists(columnName);
    }

    /**
     * Check if primary constraint exists on a column
     * 
     * @param ColumnName name of the column
     * @return if PrimaryKeyConstraint exists on the column
     */
    public boolean isPrimaryKeyConstraintExists(String columnName) {
        return _table.isPrimaryKeyConstraintExists(columnName);
    }

    public void addIndex(Index index) throws AxionException {
        _table.addIndex(index);
    }

    public void removeIndex(Index index) throws AxionException {
        _table.removeIndex(index);
        _insertedRows.clearIndexes();
        _updatedRows.clearIndexes();
    }

    public final boolean hasIndex(String name) throws AxionException {
        return _table.hasIndex(name);
    }

    public final void populateIndex(Index index) throws AxionException {
        _table.populateIndex(index);
    }

    public final Index getIndexForColumn(Column column) {
        return _table.getIndexForColumn(column);
    }

    public final boolean isColumnIndexed(Column column) {
        return _table.isColumnIndexed(column);
    }

    public void addColumn(Column col) throws AxionException {
        _table.addColumn(col);
    }

    public final Column getColumn(int index) {
        return _table.getColumn(index);
    }

    public final Column getColumn(String name) {
        return _table.getColumn(name);
    }

    public final boolean hasColumn(ColumnIdentifier id) {
        return _table.hasColumn(id);
    }

    public final int getColumnIndex(String name) throws AxionException {
        return _table.getColumnIndex(name);
    }

    public final List getColumnIdentifiers() {
        return _table.getColumnIdentifiers();
    }

    public final int getColumnCount() {
        return _table.getColumnCount();
    }

    public final Iterator getIndices() {
        return _table.getIndices();
    }

    public void addRow(Row row) throws AxionException {
        int rowid = _table.getNextRowId();
        row.setIdentifier(rowid);
        RowEvent event = new RowInsertedEvent(this, null, row);
        try {
            checkConstraints(event, makeRowDecorator());
        } catch (AxionException e) {
            freeRowId(rowid);
            throw e;
        }
        _insertedRows.addRow(this, row);
        publishEvent(event);
    }

    public RowIterator getRowIterator(boolean readOnly) throws AxionException {
        if ((!readOnly) || hasUpdates() || hasDeletes() || hasInserts()) {
            ChainedRowIterator chain = new ChainedRowIterator();
            chain.addRowIterator(excludeDeletedTransformUpdated(_table.getRowIterator(readOnly)));
            chain.addRowIterator(new InsertedRowIterator(_insertedRows.rowIterator()));
            return chain;
        }
        return UnmodifiableRowIterator.wrap(_table.getRowIterator(readOnly));
    }

    public RowIterator getIndexedRows(Selectable node, boolean readOnly) throws AxionException {
        return getIndexedRows(this.getTable(), node, readOnly);
    }

    public RowIterator getIndexedRows(RowSource source, Selectable node, boolean readOnly) throws AxionException {
        RowIterator rows = _table.getIndexedRows(source, node, readOnly);
        if (null != rows) {
            if ((!readOnly) || hasUpdates() || hasDeletes() || hasInserts()) {
                // Ensure rows in transaction are returned in natural order.
                // CollatingRowIterator takes two ordered row iterators and
                // collates their rows
                Selectable col = getIndexColumn(node);
                RowComparator comparator = new RowComparator(col, makeRowDecorator());
                CollatingRowIterator collator = new CollatingRowIterator(comparator);
                collator.addRowIterator(excludeDeletedAndUpdated(rows));

                Column column = this.getColumn(col.getName());
                Index basendx = getIndexForColumn(column);
                addUpdatedRowIterator(basendx, node, collator, readOnly);
                addInsertedRowIterator(basendx, node, collator, readOnly);
                rows = collator;
            } else {
                rows = UnmodifiableRowIterator.wrap(rows);
            }
        }
        return rows;
    }

    private Selectable getIndexColumn(Selectable node) {
        if (node instanceof ColumnIdentifier) {
            return node;
        } else if (node instanceof ComparisonFunction) {
            ComparisonFunction fn = (ComparisonFunction) node;
            Selectable left = fn.getArgument(0);
            Selectable right = fn.getArgument(1);
            if (left instanceof ColumnIdentifier && right instanceof Literal) {
                return left;
            } else if (left instanceof Literal && right instanceof ColumnIdentifier) {
                return right;
            }
        } else if (node instanceof Function) {
            Function fn = (Function) node;
            if (fn.getArgumentCount() == 1) {
                Selectable column = fn.getArgument(0);
                if (column instanceof ColumnIdentifier) {
                    return column;
                }
            }
        }
        return null;
    }

    private void addUpdatedRowIterator(Index basendx, Selectable node, CollatingRowIterator collator, boolean readOnly) throws AxionException {
        makeIndexForRowsInTransaction(basendx, _updatedRows);
        collator.addRowIterator(_updatedRows.getIndexedRows(this, node, readOnly));
    }

    private void addInsertedRowIterator(Index basendx, Selectable node, CollatingRowIterator collator, boolean readOnly) throws AxionException {
        makeIndexForRowsInTransaction(basendx, _insertedRows);
        collator.addRowIterator(new InsertedRowIterator(_insertedRows.getIndexedRows(this, node, readOnly)));
    }

    private void makeIndexForRowsInTransaction(Index basendx, IntRowMap rowMap) throws AxionException {
        if (rowMap.getIndexForColumn(basendx.getIndexedColumn()) == null) {
            Index index = null;
            if (basendx.getType().equals(Index.ARRAY)) {
                index = ARRAY_INDEX_FACTORY.makeNewInstance(basendx.getName(), basendx.getIndexedColumn(), false, true);
            } else {
                index = BTREE_INDEX_FACTORY.makeNewInstance(basendx.getName(), basendx.getIndexedColumn(), false, true);
            }
            rowMap.addIndex(index);
            rowMap.populateIndex(this, index);
        }
    }

    public final int getRowCount() {
        return _table.getRowCount() + _insertedRows.size() - _deletedRows.size();
    }

    public final int getNextRowId() {
        return _table.getNextRowId();
    }

    public final void freeRowId(int id) {
        _table.freeRowId(id);
    }

    public void drop() throws AxionException {
        _table.drop();
        _table = null;
        _dec = null;
        _insertedRows = null;
        _updatedRows = null;
        _deletedRows = null;
    }

    public void checkpoint() throws AxionException {
        _table.checkpoint();
    }

    public void shutdown() throws AxionException {
        if (hasInserts()) {
            freeRowIds();
            _insertedRows.shutdown();
        }
        if (hasUpdates()) {
            _updatedRows.shutdown();
        }
        if (hasDeletes()) {
            _deletedRows.clear();
        }

        if (_dec != null) {
            _dec = null;
        }

        if (_table != null) {
            _table.shutdown();
        }
    }

    @Override
    public void setDeferAllConstraints(boolean deferAll) {
        _deferAll = deferAll;
    }
    
    public void setSequence(Sequence seq) throws AxionException {
        _table.setSequence(seq);
    }

    public final Sequence getSequence() {
        return _table.getSequence();
    }

    public void remount(File dir, boolean dataOnly) throws AxionException {
        _table.remount(dir, dataOnly);
    }

    public void rename(String oldName, String newName) throws AxionException {
        _table.rename(oldName, newName);
    }

    public Row getRow(int id) throws AxionException {
        if (isDeleted(id)) {
            return null;
        }
        Row row = _updatedRows.getRow(id);
        if (null != row) {
            return row;
        }
        row = getInsertedRow(id);
        if (null != row) {
            return row;
        }
        return _table.getRow(id);
    }

    public final void applyInserts(RowCollection rows) throws AxionException {
        _table.applyInserts(rows);
    }

    public final void applyDeletes(IntCollection rowids) throws AxionException {
        _table.applyDeletes(rowids);
    }

    public final void applyUpdates(RowCollection rows) throws AxionException {
        _table.applyUpdates(rows);
    }

    public void commit() throws AxionException {
        assertOpen();
        if (hasDeferredConstraint()) {
            if (hasInserts()) {
                checkConstraints(null, _insertedRows.rowIterator());
            }
            if (hasDeletes()) {
                checkConstraints(new LazyRowRowIterator(_table, _deletedRows.listIterator(), _deletedRows.size()), null);
            }
            if (hasUpdates()) {
                checkConstraints(new LazyRowRowIterator(_table, _updatedRows.keyIterator(), _updatedRows.size()), _updatedRows.rowIterator());
            }
        }
        _state = Transaction.STATE_COMMITTED;
    }

    public void rollback() throws AxionException {
        // No need to assertOpen, we need to rollback even if previous "commit" attempt failed.
        assertOpenOrCommitted(); 
        freeRowIds();

        _table = null;
        _dec = null;
        _insertedRows = null;
        _updatedRows = null;
        _deletedRows = null;
        _state = Transaction.STATE_ABORTED;
    }

    private void freeRowIds() {
        IntIterator itr = _insertedRows.keyIterator();
        while (itr.hasNext()) {
            freeRowId(itr.next());
        }
    }

    public void apply() throws AxionException {
        assertCommitted();

        // apply deletes
        if (!_deletedRows.isEmpty()) {
            _table.applyDeletes(_deletedRows);
            _deletedRows.clear();
        }

        // apply updates
        if (!_updatedRows.isEmpty()) {
            _table.applyUpdates(_updatedRows.rowValues());
            _updatedRows.clear();
        }

        // apply inserts
        if (!_insertedRows.isEmpty()) {
            _table.applyInserts(_insertedRows.rowValues());
            _insertedRows.clear();
        }

        _state = Transaction.STATE_APPLIED;
    }

    public final TransactableTable makeTransactableTable() {
        return new TransactableTableImpl(this);
    }

    public void deleteRow(Row row) throws AxionException {
        // by construction, this method should never be called for a row that only exists
        // in _insertedRows, so we'll ignore that case

        RowEvent event = new RowDeletedEvent(this, row, null);
        checkConstraints(event, makeRowDecorator());

        // add the row to our list of deleted rows and
        // delete from updated/inserted Rows, if it's in there
        if(_deletedRows.add(row.getIdentifier())) {
            _updatedRows.deleteRow(this, row);
            //_insertedRows.deleteRow(this, row);
            publishEvent(event);
        }
    }

    public void updateRow(Row oldrow, Row newrow) throws AxionException {
        newrow.setIdentifier(oldrow.getIdentifier());
        RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
        checkConstraints(event, makeRowDecorator());
        _updatedRows.updateRow(this, oldrow, newrow);
        publishEvent(event);
    }

    private final Row getInsertedRow(int id) {
        return _insertedRows.getRow(id);
    }

    private void assertOpen() throws AxionException {
        if (Transaction.STATE_OPEN != _state) {
            throw new AxionException("Already committed or rolled back [" + _state + "].");
        }
    }

    private void assertOpenOrCommitted() throws AxionException {
        if ( (Transaction.STATE_OPEN != _state)  && (Transaction.STATE_COMMITTED != _state)){
            throw new AxionException("Already committed or rolled back [" + _state + "].");
        }
    }
    
    private void assertCommitted() throws AxionException {
        if (Transaction.STATE_COMMITTED != _state) {
            throw new AxionException("Not committed [" + _state + "].");
        }
    }

    /**
     * Overrides {@link #remove}and {@link #set}to apply them to the current
     * transaction.
     */
    private class TransactableTableRowIterator extends DelegatingRowIterator {
        public TransactableTableRowIterator(RowIterator iter) {
            super(iter);
        }

        @Override
        public void add(Row row) throws AxionException {
            addRow(row);
        }

        @Override
        public void remove() throws AxionException {
            deleteRow(current());
        }

        @Override
        public void set(Row row) throws AxionException {
            updateRow(current(), row);
        }
    }

    /**
     * Filters out rows that have been deleted in the current transaction.
     */
    private class ExcludeDeleted extends AbstractAcceptingRowIterator {
        public ExcludeDeleted(RowIterator iter) {
            super(iter);
        }

        protected boolean acceptable(int rowindex, Row row) throws AxionException {
            return !isDeleted(row.getIdentifier());
        }

        @Override
        public int size() throws AxionException {
            return getDelegate().size() - _deletedRows.size();
        }
    }

    private final boolean isDeleted(int rowid) {
        return _deletedRows.contains(rowid);
    }
    
    @Override
    protected final boolean isDeferAll() {
        return _deferAll;
    }

    /**
     * Filters out rows that have been updated in the current transaction.
     */
    private class ExcludeUpdated extends AbstractAcceptingRowIterator {

        public ExcludeUpdated(RowIterator iter) {
            super(iter);
        }

        protected boolean acceptable(int rowindex, Row row) throws AxionException {
            return !(_updatedRows.containsKey(row.getIdentifier()));
        }

        @Override
        public int size() throws AxionException {
            return getDelegate().size() - _updatedRows.size();
        }
    }

    /**
     * Transforms rows that have been updated within the current transaction.
     */
    private class TransformUpdated extends TransformingRowIterator {
        public TransformUpdated(RowIterator iter) {
            super(iter);
        }

        protected Row transform(Row row) {
            Row updated = _updatedRows.getRow(row.getIdentifier());
            if (null != updated) {
                return updated;
            }
            return row;
        }
    }

    private RowIterator excludeDeletedTransformUpdated(RowIterator base) {
        if (null == base) {
            return null;
        }
        return new TransactableTableRowIterator(new ExcludeDeleted(new TransformUpdated(base)));
    }

    private RowIterator excludeDeletedAndUpdated(RowIterator base) {
        if (null == base) {
            return null;
        }
        return new TransactableTableRowIterator(new ExcludeDeleted(new ExcludeUpdated(base)));
    }

    private class InsertedRowIterator extends DelegatingRowIterator {
        public InsertedRowIterator(RowIterator iter) {
            super(iter);
        }

        @Override
        public void add(Row row) throws AxionException {
            addRow(row);
        }

        @Override
        public void remove() throws AxionException {
            _table.freeRowId(current().getIdentifier());
            super.remove();
        }

        @Override
        public void set(Row row) throws AxionException {
            updateRow(current(), row);
        }
    }

    private final boolean hasUpdates() {
        return !(_updatedRows == null || _updatedRows.isEmpty());
    }

    private final boolean hasDeletes() {
        return !(_deletedRows == null || _deletedRows.isEmpty());
    }

    private final boolean hasInserts() {
        return !(_insertedRows == null || _insertedRows.isEmpty());
    }

    public void truncate() throws AxionException {
        _insertedRows.clear();
        _updatedRows.clear();
        _deletedRows.clear();
        _table.truncate();
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer(10);
        buf.append("TransactableTable[");
        buf.append("name=").append(_table.getName()).append(":");
        buf.append("inserted=").append(_insertedRows.size()).append(",");
        buf.append("updated=").append(_updatedRows.size()).append(",");
        buf.append("deleted=").append(_deletedRows.size()).append("]");
        return buf.toString();
    }

    private Table _table;
    private RowDecorator _dec;
    List _constraints;
    List _indices;

    /** {@link IntRowMap}of {@link Row}s that have been inserted. */
    private IntRowMap _insertedRows = new IntRowMap();

    /**
     * {@link IntRowMap}of {@link Row}s that have been updated, keyed by row identifier.
     */
    private IntRowMap _updatedRows = new IntRowMap();

    /** {@link IntSet}of row identifiers that have been deleted. */
    private IntSet _deletedRows = new IntSet();

    private static ArrayIndexFactory ARRAY_INDEX_FACTORY = new ArrayIndexFactory();
    private static BTreeIndexFactory BTREE_INDEX_FACTORY = new BTreeIndexFactory();

    /** My current state. */
    private int _state = Transaction.STATE_OPEN;
    private boolean _deferAll =  false;
}
