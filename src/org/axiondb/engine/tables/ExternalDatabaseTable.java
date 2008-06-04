/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.ConstraintViolationException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.ExternalTable;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Selectable;
import org.axiondb.SelectableBasedConstraint;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.constraints.UniqueConstraint;
import org.axiondb.engine.indexes.BaseIndex;
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.engine.rowiterators.FilteringRowIterator;
import org.axiondb.engine.rowiterators.ListIteratorRowIterator;
import org.axiondb.engine.rowiterators.UnmodifiableRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.ColumnEvent;
import org.axiondb.event.ConstraintEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.event.TableModificationListener;
import org.axiondb.event.TableModifiedEvent;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.ComparisonFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.jdbc.AxionConnection;
import org.axiondb.types.TimestampType;
import org.axiondb.util.ExceptionConverter;
import org.axiondb.util.Utils;
import org.axiondb.util.ValuePool;

/**
 * Base implementation of ExternalTable interface.
 * <p>
 * Example: <code>
 *      create external table emp (lname varchar(80), sid integer(6),
 *      fname varchar(80), id integer(6) not null, dob timestamp(6))
 *      organization (loadtype='remote' SERVER='myserver' 
 *      REMOTETABLE='mytablename' SCHEMA='myschema' WHERE='id > 1000');
 * </code>
 * <p>
 * Note : We have tested this for Oracle 8i/9i/10g , SQL Server, Sybase and DB2. <br>
 * 
 * @version  
 * @author Jonathan Giron
 * @author Rahul Dwivedi
 * @author Ahimanikya Satapathy
 * @see org.axiondb.DatabaseLink
 */
@SuppressWarnings("unchecked")
public class ExternalDatabaseTable implements ExternalTable, TransactableTable {

    // TODO: Vendor specific impl for truncate, table exists check, escape id, datatypes
    // TODO: Support remote table drop, create/drop index, create/drop constraint

    /** Set of recognized keys for organization properties */
    private static final Set PROPERTY_KEYS = new HashSet(6);

    /** Set of required keys for organization properties */
    private static final Set REQUIRED_KEYS = new HashSet(1);

    static {
        // Build set of recognized property keys for external db tables.
        PROPERTY_KEYS.add(PROP_DB);
        PROPERTY_KEYS.add(PROP_REMOTETABLE);
        PROPERTY_KEYS.add(PROP_ORDERBY);
        PROPERTY_KEYS.add(PROP_CATALOG);
        PROPERTY_KEYS.add(PROP_SCHEMA);
        PROPERTY_KEYS.add(PROP_WHERE);
        PROPERTY_KEYS.add(PROP_CREATE_IF_NOT_EXIST);

        // Build set of required property keys for external db tables.
        REQUIRED_KEYS.add(PROP_DB);
    }

    public ExternalDatabaseTable(String name, Database db) {
        _name = (name + "").toUpperCase();
        _db = db;
    }

    /**
     * Add the given {@link Column}to this table. This implementation throws an
     * {@link AxionException}if rows have already been added to the table.
     */
    public void addColumn(Column col) throws AxionException {
        if (getRowCount() > 0) {
            throw new AxionException("Cannot add column because table already contains rows.");
        }

        if (col.isDerivedColumn() || col.isIdentityColumn() || col.isGeneratedAlways()) {
            throw new AxionException("Generated column is not supported for table type");
        }

        _cols.add(new CaseSensitiveColumn(col));
        clearCache();
        publishEvent(new ColumnEvent(this, col));
    }

    // Can we add this constraint to remote table if not defined already ?
    public void addConstraint(Constraint constraint) throws AxionException {
        if (_constraints.containsKey(constraint.getName())) {
            throw new AxionException("A constraint named " + constraint.getName() + " already exists.");
        } else if (constraint instanceof PrimaryKeyConstraint) {
            if (null != _pk) {
                throw new AxionException("This table already has a primary key");
            }
            _pk = (PrimaryKeyConstraint) constraint;
        } else if (constraint instanceof NotNullConstraint) {
            NotNullConstraint notNull = (NotNullConstraint) constraint;
            _notNullColumns.addAll(getConstraintColumns(notNull));
        } else if (constraint instanceof UniqueConstraint) {
            _uniqueConstraints.add(constraint);
        } else {
            return; // ignore other constraint types
        }

        doAddConstraint(constraint);
    }

    public void addConstraint(Constraint constraint, boolean checkExistingRows) throws AxionException {
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

    public void addIndex(Index index) throws AxionException {
        // TODO If index does not exist in remote table create it now.
        Column idxCol = index.getIndexedColumn();
        if (!isColumnIndexed(idxCol)){
            String colName = idxCol.getName();
            _indexes.add(colName);
            // Since we are no longer maintaining index in Axion for remote tables,
            // we do not need to listen by addTableModificationListener(index);
        }
    }

    public void addRow(Row row) throws AxionException {
    	addRow(row, null);
    }
    
    public void addRow(Row row, List cols) throws AxionException {
        assertConnection();
        try {
            if (_insertPS == null) {
                 createInsertPS(cols);
             } else if (_insertCols != null && cols == null) {
                createInsertPS(cols);
            } else if (cols != null && (_insertCols == null || !_insertCols.equals(cols))) {
            	createInsertPS(cols);
            }
           
            setValueParamsForInsert(_insertPS, row);
            _insertModCount = addBatch(_insertPS, _insertModCount);
            row.setIdentifier(UNKNOWN_ROWID);
        } catch (Exception e) {
            throw new AxionException(e);
        }
    }

    public void addTableModificationListener(TableModificationListener listener) {
        _tableModificationListeners.add(listener);
    }

    public void apply() throws AxionException {
    }

    public void applyDeletes(IntCollection rowIds) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public void applyInserts(RowCollection rows) {
        throw new UnsupportedOperationException();
    }

    public void applyUpdates(RowCollection rows) {
        throw new UnsupportedOperationException();
    }

    public void checkpoint() throws AxionException {
    }

    public void commit() throws AxionException {
        if (_conn == null) {
            return; // Nothing to commit.
        }

        try {
            if (_modCount > 0) {
                if (_insertModCount > 0 && _insertPS != null) {
                    _insertPS.executeBatch();
                    _insertModCount = 0;
                }

                if (_updateModCount > 0 && _updatePS != null) {
                    _updatePS.executeBatch();
                    _updateModCount = 0;
                }

                if (_deleteModCount > 0 && _deletePS != null) {
                    _deletePS.executeBatch();
                    _deleteModCount = 0;
                }

                _conn.commit();
                _modCount = 0;
                remount();
            }
        } catch (SQLException ex) {
            int sqlState = 0;
            rollback();
            if (ex instanceof BatchUpdateException && ex.getNextException() != null) {
                ex = ex.getNextException();
            }

            try {
                sqlState = Integer.parseInt(ex.getSQLState());
            }catch (NumberFormatException numEx){
                sqlState = 0;
            }

            throw new AxionException(ex, sqlState);
        }
    }

    public void deleteRow(Row row) throws AxionException {
        assertUpdatable();
        assertConnection();
        try {
            if (_deletePS == null) {
                _deletePS = _conn.prepareStatement(getDeleteSQL());
            }
            setWhereParams(_deletePS, row, 0);
            _deleteModCount = addBatch(_deletePS, _deleteModCount);
        } catch (SQLException e) {
            rollback();
            throw convertException("Failed to apply deletes ", e);
        }
    }

    public void drop() throws AxionException {
        if (_conn == null || _stmt == null) {
            throw new AxionException("Invalid State: " + "Remote connection has been already closed");
        }
        try {
            _stmt.executeUpdate("DROP TABLE " + _remoteTableName);
        } catch (SQLException ignore) {
            // Ignore this exception
        } finally {
            shutdown();
        }
    }

    public void freeRowId(int id) {
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

    public final int getColumnCount() {
        return _cols.size();
    }

    public List getColumnIdentifiers() {
        List colids = new ArrayList();
        for (int i = 0, I = _cols.size(); i < I; i++) {
            Column col = (Column) (_cols.get(i));
            colids.add(new ColumnIdentifier(new TableIdentifier(getName()), col.getName(), null, col.getDataType()));
        }
        return Collections.unmodifiableList(colids);
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

    public final Constraint getConstraint(String name) {
        return null; // Don't allow explicit operations on Constraint
    }

    public Iterator getConstraints() {
        return _constraints.values().iterator();
    }

    public String getDBLinkName() {
        return _dblink;
    }

    public RowIterator getIndexedRows(RowSource source, Selectable node, boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getIndexedRows(source, node));
        }
        return getIndexedRows(source, node);
    }

    public RowIterator getIndexedRows(Selectable node, boolean readOnly) throws AxionException {
        return getIndexedRows(this, node, readOnly);
    }

    public Index getIndexForColumn(Column column) {
        if ((_indexes != null) && (_indexes.contains(column.getName()))) {
            return new ExternalTableIndex(column);
        }
        return null;
    }

    public Iterator getIndices() {
        return Collections.EMPTY_LIST.iterator();
    }

    public RowIterator getMatchingRows(List selectables, List values, boolean readOnly) throws AxionException {
        if (null == selectables || selectables.isEmpty()) {
            return getRowIterator(readOnly);
        }

        RowIterator baseIterator = null;
        Selectable filter = null;
        for (int i = 0, I = selectables.size(); i < I; i++) {
            Selectable sel = (Selectable) selectables.get(i);
            Object val = values.get(i);

            EqualFunction function = new EqualFunction();
            function.addArgument(sel);
            function.addArgument(new Literal(val));

            if (null == baseIterator) {
                baseIterator = getIndexedRows(function, readOnly);
                if (baseIterator != null) {
                    function = null;
                }
            }

            if (function != null) {
                if (null == filter) {
                    filter = function;
                } else {
                    AndFunction fn = new AndFunction();
                    fn.addArgument(filter);
                    fn.addArgument(function);
                    filter = fn;
                }
            }
        }

        if (null == baseIterator) {
            baseIterator = getRowIterator(readOnly);
        }

        if (null != filter) {
            return new FilteringRowIterator(baseIterator, makeRowDecorator(), filter);
        }
        return baseIterator;
    }

    public final String getName() {
        return _name;
    }

    public int getNextRowId() {
        return 0;
    }

    public Row getRow(int id) throws AxionException {
        return getRowByOffset(id);
    }

    public int getRowCount() {
        if (-1 == _rowCount) {
            _rowCount = getTableSize();
        }
        return _rowCount;
    }

    public RowIterator getRowIterator(boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getRowIterator());
        }
        return getRowIterator();
    }

    public final Sequence getSequence() {
        return null;
    }

    public Iterator getTableModificationListeners() {
        return _tableModificationListeners.iterator();
    }

    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    public final String getType() {
        return EXTERNAL_DB_TABLE_TYPE;
    }

    public boolean hasColumn(ColumnIdentifier id) {
        boolean result = false;
        String tableName = id.getTableName();
        if (tableName == null || tableName.equals(getName())) {
            result = (getColumn(id.getName()) != null);
        }
        return result;
    }

    public boolean hasIndex(String name) {
        // XXX Check with remote table whether we have an index
        return false;
    }

    public boolean isColumnIndexed(Column column) {
        try {
            return isColumnIndexed(column.getName());
        } catch (AxionException e) {
            throw ExceptionConverter.convertToRuntimeException(e);
        }
    }

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

    /**
     * check if unique constraint exists on a column
     * 
     * @param columnName name of the column
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

    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new ExternalDatabaseTableOrganizationContext();
        context.readOrSetDefaultProperties(props);
        context.updateProperties();
        return true;
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

    public Table getTable() {
        return this;
    }

    public TransactableTable makeTransactableTable() {
        return this;
    }

    public void migrate() throws AxionException {
    }

    // Can we create index on remote table if not defined already ?
    public void populateIndex(Index index) throws AxionException {
        // Ignore as of now, for remote tables Axion will not maintain indexes.
    }

    public void remount() throws AxionException {
        if (_externalRs != null) {
            try {
                _externalRs.close();
            } catch (SQLException e) {
            }
            _externalRs = null;
        }
        _rowCount = -1;
    }

    public void remount(File dir, boolean datafilesonly) throws AxionException {
    }

    public Constraint removeConstraint(String name) {
        if (name != null) {
            name = name.toUpperCase();
            if ("PRIMARYKEY".equals(name)) {
                if (_pk != null) {
                    name = _pk.getName();
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
                } catch (AxionException e) {
                    _log.log(Level.SEVERE,"Unable to publish constraint removed event", e);
                }
            }
            _constraints.remove(name);

            if (constraint == _pk) {
                _pk = null;
            } else if (constraint instanceof NotNullConstraint) {
                List columns = getConstraintColumns((NotNullConstraint) constraint);
                _notNullColumns.removeAll(columns);
            } else if (constraint instanceof UniqueConstraint) {
                _uniqueConstraints.remove(constraint);
            }
            return constraint;
        }
        return null;
    }

    public void removeIndex(Index index) throws AxionException {
        // TODO try to remove index from remote table
        this.removeTableModificationListener(index);
    }

    public void removeTableModificationListener(TableModificationListener listener) {
        _tableModificationListeners.remove(listener);
    }

    public void rename(String oldName, String newName) throws AxionException {
        setName(newName);
        clearCache();
    }

    public void rollback() throws AxionException {
        if (_conn == null) {
            return; // Nothing to rollback.
        }

        try {
            if (_insertModCount > 0 && _insertPS != null) {
                _insertPS.clearBatch();
                _insertPS.clearWarnings();
                _insertModCount = 0;
            }

            if (_updateModCount > 0 && _updatePS != null) {
                _updatePS.clearBatch();
                _updatePS.clearWarnings();
                _updateModCount = 0;
            }

            if (_deleteModCount > 0 && _deletePS != null) {
                _deletePS.clearBatch();
                _deletePS.clearWarnings();
                _deleteModCount = 0;
            }
            _conn.rollback();
            _modCount = 0;
            remount();
        } catch (SQLException e) {
            throw new AxionException(e);
        }
    }

    public void setDeferAllConstraints(boolean deferAll) {
    }

    public void setSequence(Sequence seq) throws AxionException {
    }

    public void shutdown() throws AxionException {
        try {
            if (_externalRs != null)
                _externalRs.close();
            if (_stmt != null)
                _stmt.close();
            if (_insertPS != null)
                _insertPS.close();
            if (_updatePS != null)
                _updatePS.close();
            if (_deletePS != null)
                _deletePS.close();
            if (_rowCountPS != null)
                _rowCountPS.close();

            if (_indexSelectPSs != null) {
                Iterator itr = _indexSelectPSs.values().iterator();
                Statement stmnt = null;
                while (itr.hasNext()) {
                    stmnt = (Statement) itr.next();
                    try {
                        stmnt.close();
                    } catch (Exception ex) {
                        // ignore
                    }
                }
            }
            if (_conn != null)
                _conn.close();
        } catch (SQLException ignore) {
            // Ignore this exception
        } finally {
            _externalRs = null;
            _conn = null;
            _stmt = null;
            _rowCountPS = null;
            _insertPS = null;
            _updatePS = null;
            _deletePS = null;
            _indexSelectPSs = null;
            _db = null;
        }
    }

    @Override
    public String toString() {
        return getName();
    }

    // NOTE: DB specific subclass could optimize truncate operation
    public void truncate() throws AxionException {
        assertConnection();
        Statement stmt = null;
        try {
            stmt = _conn.createStatement();
            stmt.executeUpdate(getTruncateSQL());
            _modCount++;
            commit();
        } catch (SQLException e) {
            throw new AxionException(e);
        } finally {
            closeStatement(stmt);
        }
    }

    public void updateRow(Row oldrow, Row newrow) throws AxionException {
        updateRow(oldrow, newrow, null);
    }

    public void updateRow(Row oldrow, Row newrow, List cols) throws AxionException {
        assertUpdatable();
        assertConnection();
        try {
            if (_updatePS == null) {
                createUpdatePS(cols);
            } else if (_updateCols != null && cols == null) {
                createUpdatePS(cols);
            } else if (cols != null && (_updateCols == null || !_updateCols.equals(cols))) {
                createUpdatePS(cols);
            }

            setValueParamsForUpdate(_updatePS, newrow);
            setWhereParams(_updatePS, oldrow, _updateCols != null ? _updateCols.size() : oldrow.size());
            _updateModCount = addBatch(_updatePS, _updateModCount);
        } catch (SQLException e) {
            rollback();
            throw convertException("Failed to apply updates ", e);
        }
    }

    protected void checkConstraints(RowEvent event) throws AxionException {
        // let remote db handle this.
    }

    protected String getDeleteSQL() {
        StringBuffer stmtBuf = new StringBuffer(30);
        String rTable = getQualifiedTable();

        stmtBuf.append("DELETE FROM ").append(rTable);
        populateWhere(stmtBuf.append(" WHERE "));
        return stmtBuf.toString();
    }

    // TODO: Make sure the row that is getting inserted/updated is not out of scope for
    // the given _where condition
    protected String getInsertSQL() {
        StringBuffer stmtBuf = new StringBuffer(30);
        String rTable = getQualifiedTable();

        stmtBuf.append("INSERT INTO ").append(rTable).append(" ");
        stmtBuf.append(" (");

        populateColumns(stmtBuf, null);
        populateValues(stmtBuf.append(") VALUES ("));

        stmtBuf.append(")");
        return stmtBuf.toString();
    }

    private RowIterator getBaseRowIterator(){
        return new BaseRowIterator() {
            Row _current = null;
            int _currentId = -1;
            int _currentIndex = -1;
            int _nextId = 0;
            int _nextIndex = 0;

            public Row current() {
                if (!hasCurrent()) {
                    throw new NoSuchElementException("No current row.");
                }
                return _current;
            }

            public int currentIndex() {
                return _currentIndex;
            }

            public boolean hasCurrent() {
                return (null != _current);
            }

            public boolean hasNext() {
                return nextIndex() < getRowCount();
            }

            public boolean hasPrevious() {
                return nextIndex() > 0;
            }

            public Row next() throws AxionException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next row");
                }

                do {
                    _currentId = _nextId++;
                    _current = getRowByOffset(_currentId);
                } while (null == _current);
                _currentIndex = _nextIndex;
                _nextIndex++;
                return _current;
            }

            public int nextIndex() {
                return _nextIndex;
            }

            public Row previous() throws AxionException {
                if (!hasPrevious()) {
                    throw new NoSuchElementException("No previous row");
                }

                do {
                    _currentId = (--_nextId);
                    _current = getRowByOffset(_currentId);
                } while (null == _current);
                _nextIndex--;
                _currentIndex = _nextIndex;
                return _current;
            }

            public int previousIndex() {
                return _nextIndex - 1;
            }

            @Override
            public void remove() throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                deleteRow(_current);
                _currentIndex = -1;
            }

            public void reset() {
                _current = null;
                _nextIndex = 0;
                _currentIndex = -1;
                _nextId = 0;
            }

            @Override
            public void set(Row row) throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                updateRow(_current, row);
            }

            @Override
            public int size() throws AxionException {
                return getRowCount();
            }

            @Override
            public String toString() {
                return "ExternalDatabaseTable(" + getName() + ")";
            }
        };        
    }
    
    private RowIterator getForwardOnlyRowIterator(){
        return new BaseRowIterator() {
            Row _current = null;
            int _currentId = -1;
            int _currentIndex = -1;
            int _nextId = 0;
            int _nextIndex = 0;

            public Row current() {
                if (!hasCurrent()) {
                    throw new NoSuchElementException("No current row.");
                }
                return _current;
            }

            public int currentIndex() {
                return _currentIndex;
            }

            public boolean hasCurrent() {
                return (null != _current);
            }

            public boolean hasNext() {
                return nextIndex() < getRowCount();
            }

            public boolean hasPrevious() {
                return false;                
            }

            private Row getNextResultSetRow(int rowId) throws AxionException{
                assertExternalResultSet();
                try {
                    synchronized (_externalRs) {
                        if (_externalRs.next()) {
                            return getRowFromRS(rowId, _externalRs);
                        }
                        return null;
                    }
                } catch (Exception e) {
                    throw new AxionException(e);
                }
            }
            
            public Row next() throws AxionException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next row");
                }

                _currentId = _nextId++;
                _current = getNextResultSetRow(_currentId);

                _currentIndex = _nextIndex;
                _nextIndex++;
                return _current;
            }

            public int nextIndex() {
                return _nextIndex;
            }

            public Row previous() throws AxionException {
                throw new UnsupportedOperationException("previous() not supported.");
            }

            public int previousIndex() {
                return _nextIndex - 1;
            }

            @Override
            public void remove() throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                deleteRow(_current);
                _currentIndex = -1;
            }

            public void reset() {
                _current = null;
                _currentId = -1;                
                _currentIndex = -1;
                _nextId = 0;
                _nextIndex = 0;
                
                synchronized (_externalRs) {
                    try {
                    	if (_externalRs != null){
                    	    _externalRs.close();
                        }
                    } catch (Exception ex){
                        _externalRs = null;
                    }
                    
                    try {
                        _externalRs = getResultSet(getSelectSQL(_where));
                    } catch (Exception ex){
                        _externalRs = null;
                    }
                }                
            }

            @Override
            public void set(Row row) throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                updateRow(_current, row);
            }

            @Override
            public int size() throws AxionException {
                return getRowCount();
            }

            @Override
            public String toString() {
                return "ExternalDatabaseTable(" + getName() + ")";
            }
        };        
    }    
    protected RowIterator getRowIterator() throws AxionException {
        createOrLoadResultSet();
        DatabaseLink server = _db.getDatabaseLink(_dblink);
        if (server.getJdbcUrl().toUpperCase().indexOf("ORACLE") != -1){
            return  getForwardOnlyRowIterator();            
        } else {
            return  getBaseRowIterator();
        }
    }

    /**
     * Gets appropriate string for use in indicating wildcard search for schemas.
     * 
     * @return wildcard string.
     */
    protected String getSchemaWildcardForRemoteDB() {
        String wildcard = "%";

        try {
            String dbName = _conn.getMetaData().getDatabaseProductName();
            if (dbName.lastIndexOf("DB2") != -1) {
                // Don't use schema wildcard
                wildcard = null;
            }
        } catch (SQLException ignore) {
            wildcard = "%";
        }

        return wildcard;
    }

    protected String getSelectSQL(String where) {
        StringBuffer stmtBuf = new StringBuffer(60);
        String rTable = getQualifiedTable();

        stmtBuf.append("SELECT ");
        populateColumns(stmtBuf, rTable);

        stmtBuf.append(" FROM ").append(rTable);

        if (where != null && where.trim().length() != 0) {
            stmtBuf.append(" WHERE " + where.trim());
        }

        return stmtBuf.toString();
    }

    protected String getUpdateSQL() {
        StringBuffer stmtBuf = new StringBuffer(30);
        String rTable = getQualifiedTable();

        stmtBuf.append("UPDATE ").append(rTable);

        populateSet(stmtBuf.append(" SET "));
        populateWhere(stmtBuf.append(" WHERE "));

        return stmtBuf.toString();
    }

    protected void setUp(DatabaseLink server) throws AxionException {
        try {
            _conn = server.getConnection();
            assertConnection();

            _isAxion = _conn instanceof AxionConnection;
            // Below code is due to database driver inconsistencies.
            if (server.getJdbcUrl().toUpperCase().indexOf("ORACLE") >= 0){
                _stmt = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                _stmt.setFetchSize(FETCH_SIZE);
            } else if (_conn.getMetaData().supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                _stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                _stmt.setFetchSize(FETCH_SIZE);
            } else if ( (_isAxion) || (server.getJdbcUrl().toUpperCase().indexOf("DERBY") >= 0)){
                _stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            } else {
                _stmt = _conn.createStatement();
            }

            setAutoCommit(_conn, true);

            if (_isCreateIfNotExist) {
                createRemoteTableIfNotExists();
            } else {
                if (!tableExistsInRemoteDB()) {
                    throw new AxionException("Table " + _name + " does not exists.");
                }
            }
        } catch (SQLException e) {
            throw convertException("Initialization error for remote table " + getName(), e);
        } finally {
            setAutoCommit(_conn, false);
        }
    }

    private int addBatch(PreparedStatement pstmt, int stmtModCount) throws SQLException {
        pstmt.addBatch();
        if (++stmtModCount == BATCH_SIZE) {
            pstmt.executeBatch();
            stmtModCount = 0;
        }
        _modCount++;
        return stmtModCount;
    }

    private void appendColumnNames(List cols, StringBuffer buf) {
        ListIterator listIter = cols.listIterator();
        while (listIter.hasNext()) {
            String colName = (String) listIter.next();
            if (listIter.previousIndex() != 0) {
                buf.append(", ");
            }
            buf.append(colName);
        }
    }

    private void assertConnection() throws AxionException {
        if (_conn == null) {
            throw new AxionException("Could not connect to remote database: " + _dblink);
        }
    }

    private void assertExternalResultSet() throws AxionException {
        if (_externalRs == null) {
            throw new AxionException("Invalid state: <null> ResultSet for external table " + getName());
        }
    }

    private void assertUpdatable() throws AxionException {
        if (!_isUpdatable) {
            throw new AxionException("Not an updatable view - operation not allowed.");
        }
    }

    private void buildDBSpecificDatatypeMap() throws SQLException {
        _typeInfoMap.clear();
        ResultSet typeInfo = _conn.getMetaData().getTypeInfo();
        Map tmpTypeSizeMap = new HashMap();
        Object typeName = null;
        Object type = null;
        int jdbcType = 0;
        int currPrecision = 0;
        Integer prevPrecisionObj = null;
        while (typeInfo.next()) {
            typeName = typeInfo.getString("TYPE_NAME");
            jdbcType  = typeInfo.getInt("DATA_TYPE");
            try {
                currPrecision = typeInfo.getInt("PRECISION");
            } catch (Exception ex){
                // Oracle throws SQLEx instead numeric overflow.
                // Assume overflow.
                currPrecision = Integer.MAX_VALUE;
            }
            type = ValuePool.getInt(jdbcType);
            if (_isAxion){
                _typeInfoMap.put(type, typeName);
            } else {
                // If more than one type is mapped to same JDBC type, then use type which has maxium precision.
                prevPrecisionObj = (Integer)tmpTypeSizeMap.get(type);
                if ((prevPrecisionObj == null) || (prevPrecisionObj.intValue() < currPrecision)){
                    _typeInfoMap.put(type, typeName);
                    tmpTypeSizeMap.put(type, ValuePool.getInt(currPrecision));
                }
            }
        }
    }

    private void buildTableIndex() throws AxionException {
        if (_indexes == null) {
            if (!tableExistsInRemoteDB()) {
                _indexes = Collections.EMPTY_LIST;
            }
        }
    }

    private final void clearCache() {
        _colIndexToColIdMap = null;
    }

    private void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception ex) {
                // Ignore
            }
        }
    }

    private void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e1) {
            }
        }
    }

    private AxionException convertException(String errMsg, SQLException sqlEx) {
        StringBuffer msg = new StringBuffer(100);
        msg.append(errMsg == null ? "" : errMsg);
        msg.append(" ( CODE " + sqlEx.getErrorCode());
        msg.append(" - SQLSTATE " + sqlEx.getSQLState() + " ) ");
        msg.append(" : " + sqlEx.getMessage());
        return new AxionException(msg.toString());
    }

    private void createOrLoadResultSet() throws AxionException {
        if (_externalRs == null) {
            _externalRs = getResultSet(getSelectSQL(_where));
            _rowCount = -1;
        }
    }

    private void setAutoCommit(Connection conn, boolean val){
        if (_conn == null){
            return ;
        }

        if (_sybase == null) {
            try {
                String url = _conn.getMetaData().getURL();
                if (url != null){
                    url = url.toLowerCase();
                    if (url.indexOf(":sybase:") > -1){
                        _sybase = Boolean.TRUE;
                    } else {
                        _sybase = Boolean.FALSE;
                    }
                } else {
                    _sybase = Boolean.FALSE;
                }
            } catch (SQLException ex) {
                _sybase = Boolean.FALSE;
            }
        }

        if (_sybase.booleanValue()){
            try {
                conn.commit();
                conn.setAutoCommit(val);
            } catch (Exception ex){
                // Ignore for now.
            }
        }
    }

    private void createRemoteTableIfNotExists() throws AxionException {
        assertConnection();
        Statement stmt = null;
        try {
            if (!tableExistsInRemoteDB()) {
                stmt = _conn.createStatement();
                buildDBSpecificDatatypeMap();
                stmt.executeUpdate(getCreateSQL());
            }
        } catch (SQLException e) {
            throw convertException("Could not create remote table " + getName(), e);
        } finally {
            closeStatement(stmt);
        }
    }

    private void createUpdatePS(List cols) throws SQLException {
        _updateCols = cols;
        if (_updatePS != null) {
            _updatePS.close();
            _updatePS = null;
        }
        _updatePS = _conn.prepareStatement(getUpdateSQL());
    }
    
    private void createInsertPS(List cols) throws SQLException {
        _insertCols = cols;
        if (_insertPS != null) {
            _insertPS.close();
            _insertPS = null;
        }
        _insertPS = _conn.prepareStatement(getInsertSQL());
    }

    private void doAddConstraint(Constraint constraint) throws AxionException {
        _constraints.put(constraint.getName(), constraint);
        Iterator iter = getTableModificationListeners();
        while (iter.hasNext()) {
            TableModificationListener listener = (TableModificationListener) (iter.next());
            listener.constraintAdded(new ConstraintEvent(this, constraint));
        }
    }

    /**
     * Indicates whether a table with the given name exists in the remote database for
     * schemas that match the given schema pattern.
     * 
     * @param name name of table whose existence is being tested
     * @param schemaPattern schema pattern to use; may be null
     * @return true if a table exists with <code>name</code> as its object name in a
     *         schema that matches <code>schemaPattern</code>; false otherwise
     * @throws SQLException if error occurs while testing for existence of
     *         <code>name</code>
     */
    private boolean doesRemoteTableExist(String name, String schemaPattern) throws SQLException {
        ResultSet tableExist = _conn.getMetaData().getTables(_conn.getCatalog(), schemaPattern, name, JDBC_TABLE_OBJECT_TYPE);
        boolean found = (tableExist.next());
        if (found && _indexes == null ) {
            _indexes = getIndexInfo(name, tableExist.getString("TABLE_SCHEM"));
        }
        closeResultSet(tableExist);
        return found;
    }

    private List getConstraintColumns(SelectableBasedConstraint constraint) {
        List columnList = new ArrayList();
        for (int i = 0, I = constraint.getSelectableCount(); i < I; i++) {
            Selectable sel = constraint.getSelectable(i);
            if (sel instanceof ColumnIdentifier && hasColumn((ColumnIdentifier) sel)) {
                columnList.add(sel.getName());
            }
        }
        return columnList;
    }

    private String getCreateSQL() throws SQLException {
        StringBuffer stmtBuf = new StringBuffer(30);
        String rTable = _remoteTableName;

        String dbName = _conn.getMetaData().getDatabaseProductName();
        boolean isdb2 = dbName.lastIndexOf("DB2") != -1 ? true : false;

        stmtBuf.append("CREATE TABLE ").append(rTable);
        stmtBuf.append(" ( ");
        for (int i = 0, I = getColumnCount(); i < I; i++) {
            Column col = getColumn(i);
            int jdbcType = col.getDataType().getJdbcType();
            Integer jdbcTypeObj = ValuePool.getInt(jdbcType);

            if (i != 0) {
                stmtBuf.append(", ");
            }
            stmtBuf.append(col.getName());

            String typeName = (String) (_typeInfoMap.containsKey(jdbcTypeObj) ? _typeInfoMap.get(jdbcTypeObj) : col.getSqlType());
            stmtBuf.append(" ").append(typeName);

            int scale = col.getScale();
            int precision = col.getSize();
            if (precision > 0 && isPrecisionRequired(jdbcType, isdb2)) {
                stmtBuf.append("(").append(precision);
                if (scale > 0 && Utils.isScaleRequired(jdbcType)) {
                    stmtBuf.append(",").append(scale).append(") ");
                } else {
                    stmtBuf.append(")");
                }
            }

            if (Utils.isBinary(jdbcType) && isdb2) {
                stmtBuf.append("  FOR BIT DATA ");
            }

            if (_notNullColumns.contains(col.getName())) {
                stmtBuf.append(" NOT NULL");
            }

            if (_pk != null) {
                stmtBuf.append(" PRIMARY KEY (");
                appendColumnNames(getConstraintColumns(_pk), stmtBuf);
                stmtBuf.append(") ");
            }

            if (!_uniqueConstraints.isEmpty()) {
                Iterator it = _uniqueConstraints.iterator();
                while (it.hasNext()) {
                    UniqueConstraint unique = (UniqueConstraint) it.next();
                    stmtBuf.append(" UNIQUE (");
                    appendColumnNames(getConstraintColumns(unique), stmtBuf);
                    stmtBuf.append(") ");
                }
            }
        }
        stmtBuf.append(" )");
        return stmtBuf.toString();
    }

    private RowIterator getIndexedRows(RowSource source, Selectable node) throws AxionException {
        if (node instanceof ComparisonFunction) {
            ComparisonFunction function = (ComparisonFunction) node;

            String column = null;
            Literal literal = null;
            Selectable left = function.getArgument(0);
            Selectable right = function.getArgument(1);

            if (left instanceof ColumnIdentifier && right instanceof Literal) {
                column = ((ColumnIdentifier) left).getName();
                literal = (Literal) (right);
                return getIndexedRows(function, column, literal.evaluate());
            } else if (left instanceof Literal && right instanceof ColumnIdentifier) {
                column = ((ColumnIdentifier) right).getName();
                literal = (Literal) (left);
                return getIndexedRows(function, column, literal.evaluate());
            } else {
                return null;
            }
        }
        return null; // No matching index found
    }

    private RowIterator getIndexedRows(ComparisonFunction function, String columnName, Object value) throws AxionException {
        String psKey = columnName + "_" + function.toString();
        PreparedStatement ps = (PreparedStatement) _indexSelectPSs.get(psKey);

        if (ps == null) {
            String where = "";
            String strPS = "";
            CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(columnName);
            where = col.getCaseSensitiveName() + " " + function.getOperatorString() + " ? ";

            if (_where != null && _where.trim().length() != 0) {
                where = (_where + " AND " + where);
            }

            strPS = getSelectSQL(where);
            try {
                ps = this._conn.prepareStatement(strPS, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } catch (SQLException ex) {
                throw convertException("while creating PS for remote DB:", ex);
            }
            _indexSelectPSs.put(psKey, ps);
        }

        List rows = new ArrayList();
        ResultSet rs = null;
        try {
            ps.setObject(1, value);
            rs = ps.executeQuery();

            while (rs.next()) {
                rows.add(getRowFromRS(UNKNOWN_ROWID, rs));
            }
        } catch (SQLException e) {
            throw new AxionException(e);
        } finally {
            closeResultSet(rs);
        }

        return new IndexRowIterator(rows);
    }

    private List getIndexInfo(String name, String schemaPattern) throws SQLException {
        ResultSet rs = _conn.getMetaData().getIndexInfo(_conn.getCatalog(), schemaPattern, name, false, true);
        List indexes = new ArrayList();
        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            if (colName != null) {
                indexes.add(colName);
            }
        }
        closeResultSet(rs);
        return indexes;
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

    private String getQualifiedTable() {
        if (_qualifiedTableName == null) {
            String rTable = _remoteTableName;

            if (_schemaName != null && _schemaName.trim().length() != 0) {
                rTable = _schemaName + "." + rTable;
            }

            if (_catalogName != null && _catalogName.trim().length() != 0) {
                rTable = _catalogName + "." + rTable;
            }
            _qualifiedTableName = rTable;
        }
        return _qualifiedTableName;
    }

    private ResultSet getResultSet(String sql) throws AxionException {
        assertConnection();
        if (_stmt == null) {
            throw new AxionException("Invalid state: <null> statement for external table " + getName());
        }
        try {
            return _stmt.executeQuery(sql);
        } catch (SQLException e) {
            throw convertException("Could not load remote table " + getName(), e);
        }
    }

    private Row getRowByOffset(int rowId) throws AxionException {
        assertExternalResultSet();
        try {
            synchronized (_externalRs) {
                if (_externalRs.absolute(rowId + 1)) {
                    return getRowFromRS(rowId, _externalRs);
                }
                return null;
            }
        } catch (Exception e) {
            throw new AxionException(e);
        }
    }

    /**
     * Handle differently only if Oracle and one of the columns is of type Timestamp
     * @return
     */
    private boolean handleTimestampSpecially(){
        boolean ret = false;
        DatabaseLink server = _db.getDatabaseLink(_dblink);
        if (server.getJdbcUrl().toUpperCase().indexOf("ORACLE") != -1) {
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                if (getColumn(i).getDataType() instanceof TimestampType){
                    ret = true;
                    break;
                }
            }            
        }
        return ret;
    }
    private Row getRowFromRS(int rowId, ResultSet rs) throws SQLException, AxionException {
        Row aRow = new SimpleRow(rowId, getColumnCount());
        Object colValue = null;
        Object externalColValue = null;

        DataType dataType = null;
        if (handleTimestampSpecially()){
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                dataType = getColumn(i).getDataType();
                if (dataType instanceof TimestampType){
                    externalColValue = rs.getTimestamp(i + 1);
                    colValue = externalColValue;
                }else{
                    externalColValue = rs.getObject(i + 1);
                    colValue = getColumn(i).getDataType().convert(externalColValue);                    
                }
                aRow.set(i, colValue);
            }
        }else{
            // Keep this loop separate for performance reasons.
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                externalColValue = rs.getObject(i + 1);
                colValue = getColumn(i).getDataType().convert(externalColValue);
                aRow.set(i, colValue);
            }
        }
        return aRow;
    }

    private int getTableSize() {
        int tableSize = 0;
        ResultSet rs = null;
        try {
            assertConnection();
            if (_rowCountPS == null) {
                StringBuffer sb = new StringBuffer(60);
                sb.append("SELECT COUNT(*) FROM ");
                sb.append(getQualifiedTable());

                if ((_where != null) && (!"".equals(_where.trim()))) {
                    sb.append(" WHERE ");
                    sb.append(_where);
                }

                _rowCountPS = _conn.prepareStatement(sb.toString());
            }

            rs = _rowCountPS.executeQuery();
            if (rs.next()) {
                tableSize = rs.getInt(1);
            }
        } catch (Exception ex) {
            throw ExceptionConverter.convertToRuntimeException(ex);
        } finally {
            closeResultSet(rs);
        }

        return tableSize;
    }

    private String getTruncateSQL() {
        if (_trunncateSQL == null) {
            StringBuffer truncateSql = new StringBuffer(20);
            DatabaseLink server = _db.getDatabaseLink(_dblink);
            String url = server.getJdbcUrl().toUpperCase();
            if (url.indexOf("ORACLE") != -1 || url.indexOf("AXIONDB") != -1) {
                truncateSql.append("TRUNCATE TABLE ");
            } else {
                truncateSql.append("DELETE FROM ");
            }
            truncateSql.append(getQualifiedTable());
            _trunncateSQL = truncateSql.toString();
        }
        return _trunncateSQL;
    }

    private boolean isColumnIndexed(String column) throws AxionException {
        buildTableIndex();
        return _indexes.contains(column);
    }

    private boolean isPrecisionRequired(int jdbcType, boolean isdb2) {
        if (isdb2 && jdbcType == Types.BLOB || jdbcType == Types.CLOB) {
            return true;
        } else {
            return Utils.isPrecisionRequired(jdbcType);
        }
    }

    private void populateColumns(StringBuffer stmtBuf, String rTable) {
    	if (_insertCols != null) {
            for (int i = 0, I = _insertCols.size(); i < I; i++) {
                if (i != 0) {
                    stmtBuf.append(", ");
                }
                ColumnIdentifier colid = (ColumnIdentifier) _insertCols.get(i);
                CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(colid.getName());
                if (rTable == null) {
	                // DB2 does not like qualified column name in INSERT
	                stmtBuf.append(col.getCaseSensitiveName());
	            } else {
	                stmtBuf.append(rTable).append(".").append(col.getCaseSensitiveName());
	            }
            }
        } else {
	        for (int i = 0, I = getColumnCount(); i < I; i++) {
	            CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(i);
	            if (i != 0) {
	                stmtBuf.append(", ");
	            }
	            if (rTable == null) {
	                // DB2 does not like qualified column name in INSERT
	                stmtBuf.append(col.getCaseSensitiveName());
	            } else {
	                stmtBuf.append(rTable).append(".").append(col.getCaseSensitiveName());
	            }
	        }
        }
    }

    private void populateSet(StringBuffer stmtBuf) {
        if (_updateCols != null) {
            for (int i = 0, I = _updateCols.size(); i < I; i++) {
                if (i != 0) {
                    stmtBuf.append(", ");
                }
                ColumnIdentifier colid = (ColumnIdentifier) _updateCols.get(i);
                CaseSensitiveColumn csCol = (CaseSensitiveColumn) getColumn(colid.getName());  
                stmtBuf.append(csCol.getCaseSensitiveName()).append("=").append("?");
            }
        } else {
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(i);
                if (i != 0) {
                    stmtBuf.append(", ");
                }
                stmtBuf.append(col.getCaseSensitiveName()).append("=").append("?");
            }
        }
    }

    private void populateValues(StringBuffer stmtBuf) {
    	if (_insertCols != null) {
            for (int i = 0, I = _insertCols.size(); i < I; i++) {
            	if (i != 0) {
	                stmtBuf.append(", ");
	            }
	            stmtBuf.append("?");
            }
        } else {
        for (int i = 0, I = getColumnCount(); i < I; i++) {
            if (i != 0) {
                stmtBuf.append(", ");
            }
            stmtBuf.append("?");
	        }
        }
    }

    private void populateWhere(StringBuffer stmtBuf) {
        if (_pk != null) {
            // create where based on PK
            for (int i = 0, I = _pk.getSelectableCount(); i < I; i++) {
                String colName = _pk.getSelectable(i).getName();
                if (i != 0) {
                    stmtBuf.append(" AND ");
                }
                stmtBuf.append(colName).append("=").append("?");
            }
        } else if (!_uniqueConstraints.isEmpty()) {
            // create where based on one unique column
            UniqueConstraint uc = (UniqueConstraint) _uniqueConstraints.get(0);
            for (int i = 0, I = uc.getSelectableCount(); i < I; i++) {
                String colName = uc.getSelectable(i).getName();
                if (i != 0) {
                    stmtBuf.append(" AND ");
                }
                stmtBuf.append(colName).append("=").append("?");
            }
        } else {
            // use all columns in where condition
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(i);
                if (i != 0) {
                    stmtBuf.append(" AND ");
                }

                stmtBuf.append("( ( ");
                stmtBuf.append(col.getCaseSensitiveName());
                stmtBuf.append(" = ? ) OR ((");
                stmtBuf.append(NULL_CHECK);
                stmtBuf.append(" = ?) AND (");
                stmtBuf.append(col.getCaseSensitiveName());
                stmtBuf.append(" IS NULL))) ");
            }
        }
    }

    private void publishEvent(TableModifiedEvent e) throws AxionException {
        for (int i = 0, I = _tableModificationListeners.size(); i < I; i++) {
            e.visit((TableModificationListener) (_tableModificationListeners.get(i)));
        }
    }

    private void setName(String name) {
        _name = name;
    }

    private void setObject(PreparedStatement pstmt, int i,int columnIndex, Object obj) throws SQLException {
        if (obj == null) {
             int jdbcType = getColumn(columnIndex).getDataType().getJdbcType();
            pstmt.setNull(i + 1, jdbcType);
        } else {
            pstmt.setObject(i + 1, obj);
        }
    }

    private void setValueParamsForInsert(PreparedStatement pstmt, Row row) throws SQLException, AxionException{
        if (_insertCols != null) {
            for (int i = 0, I = _insertCols.size(); i < I; i++) {
                ColumnIdentifier colid = (ColumnIdentifier) _insertCols.get(i);
                int columnIndex = getColumnIndex(colid.getName());
                setObject(pstmt, i, columnIndex, row.get(columnIndex));
            }
        } else {
        	for (int i = 0, I = getColumnCount(); i < I; i++) {
        		CaseSensitiveColumn col = (CaseSensitiveColumn) getColumn(i);
                int columnIndex = getColumnIndex(col.getName());
                setObject(pstmt, i, columnIndex, row.get(getColumnIndex(col.getName())));
        	}
    	}
    }

    private void setValueParamsForUpdate(PreparedStatement pstmt, Row row) throws SQLException, AxionException {
        if (_updateCols != null) {
            for (int i = 0, I = _updateCols.size(); i < I; i++) {
                ColumnIdentifier colid = (ColumnIdentifier) _updateCols.get(i);
                setObject(pstmt, i, getColumnIndex(colid.getName()), row.get(getColumnIndex(colid.getName())));
            }
        } else {
            for (int i = 0, I = row.size(); i < I; i++) {
                setObject(pstmt, i,i, row.get(i));
            }
        }
    }

    private void setWhereParams(PreparedStatement pstmt, Row oldrow, int size) throws AxionException, SQLException {
        if (_pk != null) {
            // create where based on PK
            for (int i = 0, I = _pk.getSelectableCount(); i < I; i++) {
                Object obj = oldrow.get(getColumnIndex(_pk.getSelectable(i).getName()));
                pstmt.setObject(size + i + 1, obj);
            }
        } else if (!_uniqueConstraints.isEmpty()) {
            // create where based on one unique column
            UniqueConstraint uc = (UniqueConstraint) _uniqueConstraints.get(0);
            for (int i = 0, I = uc.getSelectableCount(); i < I; i++) {
                Object obj = oldrow.get(getColumnIndex(uc.getSelectable(i).getName()));
                pstmt.setObject(size + i + 1, obj);
            }
        } else {
            // use all columns in where condition
            int base = 0;
            int colCnt = getColumnCount();
            for (int i = 0; i < colCnt; i++) {
                Object obj = oldrow.get(i);
                base = size + (i * 2);
                if (obj == null) {
                    int jdbcType = getColumn(i).getDataType().getJdbcType();
                    pstmt.setNull(base + 1, jdbcType);
                    pstmt.setInt(base + 2, NULL_CHECK);
                } else {
                    pstmt.setObject(base + 1, obj);
                    pstmt.setInt(base + 2, EQUALITY_CHECK);
                }
            }
        }
    }

    private boolean tableExistsInRemoteDB() throws AxionException {
        assertConnection();
        try {
            String schemaPattern = getSchemaWildcardForRemoteDB();

            // Use known schema name if it has been given.
            if (_schemaName != null && _schemaName.trim().length() != 0) {
                schemaPattern = _schemaName;
            }

            // First, try upper-case name with default-case schema pattern.
            if (doesRemoteTableExist(_remoteTableName.toUpperCase(), schemaPattern)) {
                _remoteTableName = _remoteTableName.toUpperCase();
                return true;
                // Next, try default-case name.
            } else if (doesRemoteTableExist(_remoteTableName, schemaPattern)) {
                return true;
            } else if (schemaPattern != null) {
                // Now try upper-case name and schema pattern.
                if (doesRemoteTableExist(_remoteTableName.toUpperCase(), schemaPattern.toUpperCase())) {
                    _remoteTableName = _remoteTableName.toUpperCase();
                    return true;
                }

                // Finally, try default-case name and upper-case schema pattern.
                return doesRemoteTableExist(_remoteTableName, schemaPattern.toUpperCase());
            }

            return false;
        } catch (SQLException e) {
            throw convertException("Remote table/view " + _remoteTableName + " not found", e);
        }
    }

    private class ExternalDatabaseTableOrganizationContext extends BaseTableOrganizationContext {

        public Set getPropertyKeys() {
            Set baseKeys = getBasePropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);

            return keys;
        }

        public Set getRequiredPropertyKeys() {
            Set baseRequiredKeys = getBaseRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);

            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            // Validate all supplied property keys to ensure they are recognized.
            super.assertValidPropertyKeys(props);

            _dblink = props.getProperty(PROP_DB);

            DatabaseLink server = _db.getDatabaseLink(_dblink);
            if (server == null) {
                throw new AxionException("Database link " + _dblink + " does not exist.");
            }

            _where = props.getProperty(PROP_WHERE);
            _remoteTableName = props.getProperty(PROP_REMOTETABLE);

            _catalogName = props.getProperty(PROP_CATALOG);
            if (_catalogName == null) {
                _catalogName = server.getCatalogName();
            }

            _schemaName = props.getProperty(PROP_SCHEMA);
            if (_schemaName == null) {
                _schemaName = server.getSchemaName();
            }

            if (_where != null && _where.trim().length() != 0) {
                _isUpdatable = false;
            }

            if (_dblink == null || _dblink.trim().length() == 0 || !_db.hasDatabaseLink(_dblink)) {
                throw new AxionException("Please provide a valid server name");
            }

            _remoteTableName = (_remoteTableName != null) ? _remoteTableName : getName();
            if ((props.getProperty(PROP_CREATE_IF_NOT_EXIST) != null) && ("TRUE".equalsIgnoreCase(props.getProperty(PROP_CREATE_IF_NOT_EXIST)))) {
                _isCreateIfNotExist = true;
            }

            setUp(server);

            _log.log(Level.FINE,"External DB Table " + _remoteTableName + " created (Updatable=" + _isUpdatable + ")");

        }

        @Override
        public void updateProperties() {
            super.updateProperties();

            setProperty(PROP_DB, _dblink);
            setProperty(PROP_WHERE, _where);
            setProperty(PROP_REMOTETABLE, _remoteTableName);
            setProperty(PROP_CATALOG, _catalogName);
            setProperty(PROP_SCHEMA, _schemaName);
            setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_REMOTE);
        }
    }

    private class IndexRowIterator extends ListIteratorRowIterator {
        public IndexRowIterator(List list) {
            super(list.listIterator());
        }

        @Override
        public void add(Row row) throws AxionException {
            addRow(row);
        }

        @Override
        public void remove() {
            try {
                deleteRow(current());
            } catch (AxionException e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }

        @Override
        public void set(Row row) {
            try {
                updateRow(current(), row);
            } catch (AxionException e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }
    }

    /**
     * AxionDB Index which delegates to remote DB by using SELECT ... WHERE
     * INDEX_COL_COMPARE
     */
    private class ExternalTableIndex extends BaseIndex {
        public ExternalTableIndex(Column column) {
            super(column.getName(), column, false);
        }

        public IndexLoader getIndexLoader() {
            throw new UnsupportedOperationException("getIndexLoader");
        }

        public RowIterator getInorderRowIterator(RowSource source) throws AxionException {
            return null;
        }

        public RowIterator getRowIterator(RowSource source, Function fn, Object value) throws AxionException {
            return getIndexedRows(((ComparisonFunction) fn), getIndexedColumn().getName(), value);
        }

        public void save(File dataDirectory) throws AxionException {
            throw new UnsupportedOperationException("save");
        }

        public void saveAfterTruncate(File dataDirectory) throws AxionException {
            throw new UnsupportedOperationException("saveAfterTruncate");
        }

        public boolean supportsFunction(Function fn) {
        	if (fn instanceof ComparisonFunction){
        		return true;
        	}
            return false;
        }

        public void truncate() throws AxionException {
            // No action
        }

        public String getType() {
            return XTERNAL_DB;
        }

        public void changeRowId(Table table, Row row, int oldId, int newId) throws AxionException {
            throw new UnsupportedOperationException("changeRowId");
        }
    }

    private class CaseSensitiveColumn extends Column {
        private static final String CASE_SENSITIVE_NAME_CONFIG_KEY = "caseSensitiveName" ;
        public CaseSensitiveColumn(Column column){
            super(column.getName(), column.getDataType());
            getConfiguration().putAll(column.getConfiguration());
            getConfiguration().put(CASE_SENSITIVE_NAME_CONFIG_KEY, column.getName());
            getConfiguration().put(NAME_CONFIG_KEY, column.getName().toUpperCase());
        }
        
        public String getCaseSensitiveName(){
            return (String) getConfiguration().get(CASE_SENSITIVE_NAME_CONFIG_KEY);
        }
    }

    private static Logger _log = Logger.getLogger(ExternalDatabaseTable.class.getName());
    private static final String XTERNAL_DB = "externalDB";;
    private final static int NULL_CHECK = 1;
    private final static int EQUALITY_CHECK = 0;
    private final static int BATCH_SIZE = 1000;
    private final static int FETCH_SIZE = 100; // 100 seems to be the best choice
    private final static String[] JDBC_TABLE_OBJECT_TYPE = { "TABLE", "VIEW"};

    private Boolean _sybase = null;
    private List _indexes;
    private String _catalogName;
    private Map _colIndexToColIdMap;
    private List _cols = new ArrayList();
    private Connection _conn;
    private Map _constraints = new HashMap(4);

    private Database _db;
    private String _dblink;

    private int _deleteModCount;
    private PreparedStatement _deletePS;
    private ResultSet _externalRs;
    private int _insertModCount;
    private PreparedStatement _insertPS;
    private List _insertCols;

    private boolean _isAxion = false;
    private boolean _isCreateIfNotExist = false;
    private boolean _isUpdatable = true;
    private int _modCount = 0;
    private String _name;

    private Set _notNullColumns = new HashSet(4);
    private PrimaryKeyConstraint _pk;
    private String _qualifiedTableName;
    private String _remoteTableName;
    private int _rowCount = 0;
    private PreparedStatement _rowCountPS;

    private String _schemaName;
    private Statement _stmt;
    private List _tableModificationListeners = new ArrayList();
    private String _trunncateSQL;
    private Map _typeInfoMap = new HashMap(20);

    private List _uniqueConstraints = new ArrayList(2);
    private List _updateCols;
    private int _updateModCount;
    private PreparedStatement _updatePS;
    private Map _indexSelectPSs = new HashMap();
    private String _where;
    private ExternalDatabaseTableOrganizationContext context;

}
