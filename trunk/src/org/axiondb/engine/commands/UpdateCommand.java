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

package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.SnapshotIsolationTransaction;
import org.axiondb.engine.rowcollection.IntSet;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.ExternalDatabaseTable;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.engine.visitors.ResolveFromNodeVisitor;
import org.axiondb.jdbc.AxionResultSet;

/**
 * An <tt>UPDATE</tt> command.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 * @author Sudhendra Seshachala
 * @author Ritesh Adval
 */
public class UpdateCommand extends ChildTableUpdater {

    // ------------------------------------------------------------ Constructors

    public UpdateCommand() {
    }

    // ---------------------------------------------------------- Public Methods

    @SuppressWarnings("unchecked")
    public void addColumn(ColumnIdentifier col) {
        _cols.add(col);
    }

    @SuppressWarnings("unchecked")
    public void addValue(Selectable val) {
        _vals.add(val);
    }

    public boolean execute(Database database) throws AxionException {
        setEffectedRowCount(executeUpdate(database));
        return false;
    }

    /**
     * Unsupported, use {@link #executeUpdate}instead.
     * 
     * @throws UnsupportedOperationException
     */
    public AxionResultSet executeQuery(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use executeUpdate.");
    }

    public int executeUpdate(org.axiondb.Database db) throws AxionException {
        assertNotReadOnly(db);
        preProcess(db);
        resolve(db);

        int updatedRows = 0;
        if (_context != null && _context.getFrom() != null) {
            updatedRows = updateUsingFromClauseTables(db);
        } else {
            updatedRows = updateUsingStaticValues(db);
        }

        return updatedRows;
    }

    public int getColumnCount() {
        return _cols.size();
    }

    public Iterator getColumnIterator() {
        return _cols.iterator();
    }

    public ExceptionWhenClause getExceptionWhenClause() {
        return _exceptionWhenClause;
    }

    public TableIdentifier getTable() {
        return _tableId;
    }

    public int getValueCount() {
        return _vals.size();
    }

    public Iterator getValueIterator() {
        return _vals.iterator();
    }

    public Selectable getWhere() {
        return _where;
    }

    private void preProcess(Database db) throws AxionException {
        _count = 0;

        _table = db.getTable(getTable());
        if (null == _table) {
            throw new AxionException("Table " + getTable() + " not found.", 42704);
        }
        setDeferAllConstraintIfRequired(_table);

        if (_exceptionWhenClause != null) {
            _exceptionWhenClause.preProcess(db);
        }
    }

    public void setExceptionWhenClause(DMLWhenClause w, TableIdentifier t, List cols, List vals) {
        _exceptionWhenClause = new ExceptionWhenClause(w, t, cols, vals);
    }

    public void setQueryContext(AxionQueryContext context) {
        _context = context;
        _planner = new AxionQueryPlanner(context);
    }
    
    public AxionQueryContext getQueryContext() {
        return _context;
    }

    public void setTable(TableIdentifier table) {
        _tableId = table;
    }

    public void setWhere(Selectable where) {
        _where = where;
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        getBindVariableVisitor().visit(this);
    }

    private void commitIfRequired(Database db) throws AxionException {
        if (getCommitSize(db) == 0) {
            return;
        }

        if (db instanceof SnapshotIsolationTransaction && (++_count % getCommitSize(db)) == 0) {
            _table = ((SnapshotIsolationTransaction) db).commit(_tableId);
        }
    }

    private Row prepareRow(Row oldrow, RowDecorator dec, Database db) throws AxionException {
        Table targetTable = _table;
        Row newrow = new SimpleRow(oldrow);
        Iterator colids = getColumnIterator();
        Iterator values = this.getValueIterator();
        while (colids.hasNext()) {
            ColumnIdentifier colid = (ColumnIdentifier) (colids.next());
            Selectable sel = (Selectable) (values.next());
            Object val = sel.evaluate(dec);
            DataType type = db.getTable(colid.getTableName()).getColumn(colid.getName()).getDataType();
            val = attemptToConvertValue(val, type, colid);
            newrow.set(targetTable.getColumnIndex(colid.getName()), val);
        }
        return newrow;
    }

    protected void resolve(Database db) throws AxionException {
        if (!_resolved) {
            resolveSelectableList(_cols, db, getTable());
            resolveGeneratedColumns(_table, _tableId, _cols);

            // resolve FROM part
            if (_context != null && _context.getFrom() != null) {
                // resolve from node for any sub-select
                ResolveFromNodeVisitor fnVisitor = new ResolveFromNodeVisitor();
                fnVisitor.resolveFromNode(_context.getFrom(), db);
                _context.setTables(_context.getFromArray());

                fnVisitor.resolveFromNode(_context.getFrom(), db, null);
                resolveSelectableList(_vals, db, _context.getTables());
            } else {
                resolveSelectableList(_vals, db, getTable());
            }

            // resolve WHERE part
            if (getWhere() != null) {
                setWhere(resolveSelectable(getWhere(), db, new TableIdentifier[] { getTable()}));
            }

            // check and resolve Exception When clause
            if (_exceptionWhenClause != null) {
                _exceptionWhenClause.resolve(db);
            }
            _isExternalDBTable = _table instanceof ExternalDatabaseTable;
            _resolved = true;
        }
    }

    @SuppressWarnings("unchecked")
    private int updateUsingFromClauseTables(Database db) throws AxionException {
        IntSet rowcount = new IntSet();
        int extTblCnt = 0;
        int colId = -1;
        // 1. Execute - "SELECT * FROM targetTable LOJ srcTable/view"
        List list = _context.getResolvedSelect();
        _context.setSelected((Selectable[]) (list.toArray(new Selectable[list.size()])));
        RowIterator joinedRowIter = _planner.makeRowIterator(db, false);

        // 2. Build decorator now, build this before executing STEP 1
        RowDecorator dec = new RowDecorator(_planner.getColumnIdToFieldMap());

        // 3. Loop thru and merge(insert or update as appropriate)
        while (joinedRowIter.hasNext()) {
            // Since we createrd a LOJ assume the the joined row has
            // left table's row should be at index(0)
            // and right table's row at index(1)
            JoinedRow joinRow = (JoinedRow) joinedRowIter.next();

            Row targetRow = joinRow.getRow(0); // get target table row
            Row sourceRow = joinRow.getRow(1); // get source table row

            dec.setRow(joinedRowIter.currentIndex(), joinRow);

            // if current row match exception when condition process else
            if (_exceptionWhenClause != null && _exceptionWhenClause.insertMatchingRow(db, dec, sourceRow)) {
                continue; // pick next row
            }

            // UPDATE: replace old row with new row
            Row newrow = prepareRow(targetRow, dec, db);
            if(_isExternalDBTable) {
                ((ExternalDatabaseTable)_table).updateRow(targetRow, newrow, _cols);
            } else {
                updateGeneratedValues(db, _table, _tableId, newrow);
                if (!targetRow.equals(newrow)) {
                    _table.updateRow(targetRow, newrow);
                    updateOrSetNullChildRows(db, _table, targetRow, newrow);
                }
            }
            colId = newrow.getIdentifier();
            if (colId == ExternalTable.UNKNOWN_ROWID){
                extTblCnt++;                
            }else{
                rowcount.add(colId);
            }
            
            commitIfRequired(db);
        }
        return (rowcount.size() + extTblCnt);
    }

    private int updateUsingStaticValues(Database db) throws AxionException {
        int rowcount = 0;
        RowIterator iter = getRowIterator(db, getTable(), _table, getWhere(), false, makeRowDecorator());
        RowDecorator dec = makeRowDecorator();
        while (iter.hasNext()) {
            _count = 0;
            Row oldrow = iter.next();
            dec.setRow(iter.currentIndex(), oldrow);
            Row newrow = prepareRow(oldrow, dec, db);
            
            // if current row match exception when condition process else
            if (_exceptionWhenClause != null && _exceptionWhenClause.insertMatchingRow(db, dec, newrow)) {
                continue;
            }

            if(_isExternalDBTable) {
                ((ExternalDatabaseTable)_table).updateRow(oldrow, newrow, _cols);
            } else {
                updateGeneratedValues(db, _table, _tableId, newrow);
                if (!oldrow.equals(newrow)) {
                    _table.updateRow(oldrow, newrow);
                    updateOrSetNullChildRows(db, _table, oldrow, newrow);
                }
            }
            commitIfRequired(db);
            rowcount++;
        }

        return rowcount;
    }
    
    private final RowDecorator makeRowDecorator() {
        if (_dec == null) {
            _dec = _table.makeRowDecorator();
        } 
        return _dec;
    }
    
    public class ExceptionWhenClause extends InsertIntoClause {
        public ExceptionWhenClause(DMLWhenClause when, TableIdentifier tid, List cols, List vals) {
            super(when, tid, cols, vals);
        }

        @Override
        protected boolean isTargetTablePartOfSubQuery() throws AxionException {
            return _isTargetPartOfSubQuery;
        }

        @Override
        protected void resolve(Database db) throws AxionException {
            super.resolve(db);

            if (_context != null) {
                _isTargetPartOfSubQuery = _context.isTablePartOfSelect(getTargetTableId());

                // resolve when condition
                getWhenClause().resolve(db, _context.getTables());
                resolveSelectableList(getValues(), db, _context.getTables());
            } else {
                // resolve when condition
                getWhenClause().resolve(db, new TableIdentifier[] { getTable()});
                resolveSelectableList(getValues(), db, getTable());
            }

            assertRules(getTargetTable());
        }

        private boolean _isTargetPartOfSubQuery = false;
    }
    
    // -------------------------------------------------------------- Attributes

    private List _cols = new ArrayList();
    private AxionQueryContext _context;
    private RowDecorator _dec;

    private int _count;
    private ExceptionWhenClause _exceptionWhenClause;
    private AxionQueryPlanner _planner;
    private boolean _resolved = false;
    private boolean _isExternalDBTable = false;
    private Table _table;

    private TableIdentifier _tableId;
    private List _vals = new ArrayList();
    private Selectable _where;
}
