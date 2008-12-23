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

package org.axiondb.engine.tables;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntCollections;
import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.ConstraintViolationException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.constraints.UniqueConstraint;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.engine.rowcollection.RowCollections;
import org.axiondb.engine.rowiterators.RebindableIndexedRowIterator;
import org.axiondb.engine.rowiterators.UnmodifiableRowIterator;
import org.axiondb.event.ColumnEvent;
import org.axiondb.event.ConstraintEvent;
import org.axiondb.event.RowDeletedEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.event.TableModificationListener;
import org.axiondb.functions.ComparisonFunction;
import org.axiondb.types.LOBType;
import org.axiondb.util.ValuePool;

/**
 * An abstract base implementation of {@link Table}.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public abstract class BaseTable extends AbstractBaseTable {
    // having these interface methods defined here as abstract seems to keep some j2me
    // implementations happy
    public abstract void applyDeletes(IntCollection rowids) throws AxionException;

    public abstract void applyInserts(RowCollection rows) throws AxionException;

    public abstract void applyUpdates(RowCollection rows) throws AxionException;

    public abstract void freeRowId(int id);

    public abstract int getNextRowId();

    public abstract int getRowCount();

    public abstract void populateIndex(Index index) throws AxionException;

    public abstract Row getRow(int id) throws AxionException;

    protected abstract RowIterator getRowIterator() throws AxionException;

    public BaseTable(String name) {
        _name = (name + "").toUpperCase();
        setType(REGULAR_TABLE_TYPE);
    }

    public RowIterator getRowIterator(boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getRowIterator());
        }
        return getRowIterator();
    }

    public void addRow(Row row) throws AxionException {
        int rowid = getNextRowId();
        row.setIdentifier(rowid);
        RowInsertedEvent event = new RowInsertedEvent(this, null, row);
        try {
            checkConstraints(event, makeRowDecorator());
        } catch (AxionException e) {
            freeRowId(rowid);
            throw e;
        }
        applyInserts(RowCollections.singletonList(row));
    }

    public void checkpoint() throws AxionException {
    }

    public void setSequence(Sequence seq) throws AxionException {
        _sequence = seq;
    }

    public final Sequence getSequence() {
        return _sequence;
    }

    public void deleteRow(Row row) throws AxionException {
        RowDeletedEvent event = new RowDeletedEvent(this, row, null);
        checkConstraints(event, makeRowDecorator());
        applyDeletes(IntCollections.singletonIntList(row.getIdentifier()));
    }

    public void updateRow(Row oldrow, Row newrow) throws AxionException {
        newrow.setIdentifier(oldrow.getIdentifier());
        RowUpdatedEvent event = new RowUpdatedEvent(this, oldrow, newrow);
        checkConstraints(event, makeRowDecorator());
        applyUpdates(RowCollections.singletonList(newrow));
    }

    protected void truncateIndices() throws AxionException {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            ((Index)_indices.get(i)).truncate();
        }
    }

    protected void recreateIndices() throws AxionException {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            populateIndex((Index) _indices.get(i));
        }
    }

    public String toString() {
        return getName();
    }

    public final String getName() {
        return _name;
    }

    public final String getType() {
        return _type;
    }

    protected void setType(String type) {
        _type = type;
    }

    protected void setName(String name) {
        _name = name;
    }

    public void addConstraint(Constraint constraint) throws AxionException {
        addConstraint(constraint, true);
    }

    private void addConstraint(Constraint constraint, boolean checkExistingRows) throws AxionException {
        if (constraint instanceof PrimaryKeyConstraint && null != getPrimaryKey()) {
            throw new AxionException("This table already has a primary key");
        } else if (_constraints.containsKey(constraint.getName())) {
            throw new AxionException("A constraint named " + constraint.getName() + " already exists.");
        } else {
            if (checkExistingRows) {
                RowDecorator dec = makeRowDecorator();
                for (RowIterator iter = getRowIterator(); iter.hasNext();) {
                    Row current = iter.next();
                    RowEvent event = new RowUpdatedEvent(this, current, current);
                    if (!constraint.evaluate(event, dec)) {
                        throw new ConstraintViolationException(constraint);
                    }
                }
            }
            _constraints.put(constraint.getName(), constraint);

            Iterator iter = getTableModificationListeners();
            while (iter.hasNext()) {
                TableModificationListener listener = (TableModificationListener) (iter.next());
                listener.constraintAdded(new ConstraintEvent(this, constraint));
            }
        }
    }
    
    public final Constraint getConstraint(String name) {
        return (Constraint) _constraints.get(name);
    }

    public Constraint removeConstraint(String name) {
        if (name != null) {
            if ("PRIMARYKEY".equals(name)) {
                Constraint pk = getPrimaryKey();
                if (pk != null) {
                    name = pk.getName();
                } else {
                    name = null;
                }
            }
        }
        if (_constraints.containsKey(name)) {
            Constraint constraint = (Constraint) _constraints.get(name);
            Iterator iter = getTableModificationListeners();
            while (iter.hasNext()) {
                TableModificationListener listener = (TableModificationListener) (iter.next());
                try {
                    listener.constraintRemoved(new ConstraintEvent(this, constraint));
                }catch (AxionException e) {
                    _log.log(Level.SEVERE,  "Unable to publish constraint removed event", e);
                }
            }
            return (Constraint) _constraints.remove(name);
        }
        return null;
    }

    /**
     * check if unique constraint exists on a column
     * 
     * @param columnName name of the columm
     * @return true if uniqueConstraint exists on the column
     */
    public boolean isUniqueConstraintExists(String columnName) {
        boolean result = false;
        for (Iterator iter = _constraints.values().iterator(); iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof UniqueConstraint) {
                UniqueConstraint uk = (UniqueConstraint) constraint;
                if (uk.getSelectableCount() == 1) {
                    ColumnIdentifier cid = (ColumnIdentifier) (uk.getSelectableList().get(0));
                    if (columnName.equals(cid.getName())) {
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Check if primary constraint exists on a column
     * @param ColumnName name of the column
     * @return if PrimaryKeyConstraint exists on the column
     */
    // TODO: keep Pk as memerber variable ?
    public boolean isPrimaryKeyConstraintExists(String columnName) {
        boolean result = false;
        for (Iterator iter = _constraints.values().iterator(); iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof PrimaryKeyConstraint) {
                UniqueConstraint uk = (UniqueConstraint) constraint;
                if (uk.getSelectableCount() == 1) {
                    ColumnIdentifier cid = (ColumnIdentifier) (uk.getSelectableList().get(0));
                    if (columnName.equals(cid.getName())) {
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    private PrimaryKeyConstraint getPrimaryKey() {
        for (Iterator iter = _constraints.values().iterator(); iter.hasNext();) {
            Constraint constraint = (Constraint) (iter.next());
            if (constraint instanceof PrimaryKeyConstraint) {
                return (PrimaryKeyConstraint) (constraint);
            }
        }
        return null;
    }

    public Iterator getConstraints() {
        return _constraints.values().iterator();
    }
    
    public void addIndex(Index index) throws AxionException {
        _indices.add(index);
        addTableModificationListener(index);
    }

    public void removeIndex(Index index) throws AxionException {
        _indices.remove(index);
        this.removeTableModificationListener(index);
    }

    public Index getIndexForColumn(Column column) {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) _indices.get(i);
            if (column.equals(index.getIndexedColumn())) {
                return index;
            }
        }
        return null;
    }

    public boolean isColumnIndexed(Column column) {
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) _indices.get(i);
            if (column.equals(index.getIndexedColumn())) {
                return true;
            }
        }
        return false;
    }

    public RowIterator getIndexedRows(Selectable node, boolean readOnly) throws AxionException {
        return getIndexedRows(this, node, readOnly);
    }

    public RowIterator getIndexedRows(RowSource source, Selectable node, boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getIndexedRows(source, node));
        }
        return getIndexedRows(source, node);
    }

    /**
     * Add the given {@link Column}to this table. This implementation throws an
     * {@link AxionException}if rows have already been added to the table.
     */
    public void addColumn(Column col) throws AxionException {
        if (getRowCount() > 0) {
            throw new AxionException("Cannot add column because table already contains rows.");
        }

        // FIXME: ?
        if (col.getDataType() instanceof LOBType) {
            LOBType lob = (LOBType) (col.getDataType());
            if (null == lob.getLobDir()) {
                File lobDir = new File(System.getProperty("axiondb.lobdir", ".").toUpperCase(), col.getName().toUpperCase());
                lob.setLobDir(lobDir);
            }
        }
        _cols.add(col);
        clearCache();
        publishEvent(new ColumnEvent(this, col));
    }

    protected final void clearCache() {
        _colIndexToColIdMap = null;
    }

    public boolean hasColumn(ColumnIdentifier id) {
        boolean result = false;
        String tableName = id.getTableName();
        if (tableName == null || tableName.equals(getName())) {
            result = (getColumn(id.getName()) != null);
        }
        return result;
    }

    public final Column getColumn(int index) {
        return (Column) (_cols.get(index));
    }

    public Column getColumn(String name) {
        for (int i = 0, I = _cols.size(); i < I; i++) {
            Column col = (Column) (_cols.get(i));
            if (col.getName().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null;
    }

    public int getColumnIndex(String name) throws AxionException {
        for (int i = 0, I = _cols.size(); i < I; i++) {
            Column col = (Column) (_cols.get(i));
            if (col.getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new AxionException("Column " + name + " not found.");
    }

    public List getColumnIdentifiers() {
        List colids = new ArrayList();
        for (int i = 0, I = _cols.size(); i < I; i++) {
            Column col = (Column) (_cols.get(i));
            colids.add(new ColumnIdentifier(new TableIdentifier(getName()), col.getName(), null, col.getDataType()));
        }
        return Collections.unmodifiableList(colids);
    }

    public final int getColumnCount() {
        return _cols.size();
    }

    public final DataType[] getDataTypes() {
        DataType[] types = new DataType[_cols.size()];
        for (int i = 0, I = _cols.size(); i < I; i++) {
            types[i] = getColumn(i).getDataType();
        }
        return types;
    }

    public void drop() throws AxionException {
    }

    public void remount(File dir, boolean datafilesonly) throws AxionException {
    }

    public void rename(String oldName, String newName) throws AxionException {
        setName(newName);
        clearCache();
    }

    public void shutdown() throws AxionException {
    }

    public RowDecorator makeRowDecorator() {
        if (null == _colIndexToColIdMap) {
            int size = _cols.size();
            Map map = new HashMap(size);
            for (int i = 0; i < size; i++) {
                Column col = (Column) (_cols.get(i));
                ColumnIdentifier colid = new ColumnIdentifier(new TableIdentifier(getName()), col.getName(), null, col.getDataType());
                map.put(colid, ValuePool.getInt(i));
            }
            _colIndexToColIdMap = map;
        }
        return new RowDecorator(_colIndexToColIdMap);
    }

    public TransactableTable makeTransactableTable() {
        return new TransactableTableImpl(this);
    }

    public Iterator getIndices() {
        return _indices.iterator();
    }
    
    public boolean hasIndex(String name) {
        String upperName = name.toUpperCase();
        for (int i = 0, I = _indices.size(); i < I; i ++) {
            Index index = (Index) _indices.get(i);
            if (upperName.equals(index.getName())) {
                return true;
            }
        }
        return false;
    }

    protected void notifyColumnsOfNewLobDir(File directory) throws AxionException {
        for (int i = 0, I = _cols.size(); i < I; i++) {
            Column col = (Column) (_cols.get(i));
            if (col.getDataType() instanceof LOBType) {
                LOBType lob = (LOBType) (col.getDataType());
                lob.setLobDir(new File(directory, col.getName()));
            }
        }
    }

    protected void writeColumns(ObjectOutputStream out) throws IOException {
        out.writeObject(_cols);
    }

    protected void readColumns(ObjectInputStream in) throws IOException, ClassNotFoundException {
        _cols = (List) (in.readObject());
    }

    protected void writeConstraints(ObjectOutputStream out) throws IOException {
        out.writeObject(_constraints);
    }

    protected void readConstraints(ObjectInputStream in, Database db) throws IOException, ClassNotFoundException, AxionException {
        _constraints = (Map) (in.readObject());
        
        for (Iterator iter = _constraints.values().iterator(); iter.hasNext();) {
            Constraint constraint = (Constraint) (iter.next());
            if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = (ForeignKeyConstraint)constraint;
                fk.setParentTable(db.getTable(fk.getParentTableName()));
                fk.setChildTable(db.getTable(fk.getChildTableName()));
            }
        }
    }

    // XXX: Index can be applied in batch ? 
    protected void applyDeletesToIndices(IntCollection rowIds) throws AxionException {
        for (IntIterator ids = rowIds.iterator(); ids.hasNext();) {
            Row deleted = getRow(ids.next());
            if (deleted != null) {
                RowEvent event = new RowDeletedEvent(this, deleted, null);
                for (int i = 0, I = _indices.size(); i < I; i ++) {
                    Index index = (Index) _indices.get(i);
                    index.rowDeleted(event);
                }
            }
        }
    }

    protected void applyUpdatesToIndices(RowCollection rows) throws AxionException {
        for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
            Row newrow = iter.next();
            Row oldrow = getRow(newrow.getIdentifier());
            if (oldrow != null) {
                RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
                for (int i = 0, I = _indices.size(); i < I; i ++) {
                    Index index = (Index) _indices.get(i);
                    index.rowUpdated(event);
                }
            }
        }
    }

    protected void applyInsertsToIndices(RowCollection rows) throws AxionException {
        for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
            Row row = iter.next();
            RowEvent event = new RowInsertedEvent(this, null, row);
            for (int i = 0, I = _indices.size(); i < I; i ++) {
                Index index = (Index) _indices.get(i);
                index.rowInserted(event);
            }
        }
    }

    private RowIterator getIndexedRows(RowSource source, Selectable node) throws AxionException {
        if (node instanceof ComparisonFunction) {
            // attempting to map comparison function to existing index
            ComparisonFunction function = (ComparisonFunction) node;

            Column column = null;
            Literal literal = null;
            Selectable left = function.getArgument(0);
            Selectable right = function.getArgument(1);
            if (left instanceof ColumnIdentifier && right instanceof Literal) {
                column = getColumn(((ColumnIdentifier) left).getName());
                literal = (Literal) (right);
            } else if (left instanceof Literal && right instanceof ColumnIdentifier) {
                column = getColumn(((ColumnIdentifier) right).getName());
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
            Column column = getColumn(((ColumnIdentifier) node).getName());
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
                Column column = getColumn(((ColumnIdentifier) colid).getName());
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

    private String _name;
    private String _type;
    private List _cols = new ArrayList();
    private List _indices = new ArrayList(4);
    private Map _constraints = new HashMap(4);
    private Map _colIndexToColIdMap;

    // as per ANSI at most we can have one internal sequence per table,
    // hence one identity column per table
    private Sequence _sequence;

    private static Logger _log = Logger.getLogger(BaseTable.class.getName());
}
