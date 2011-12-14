/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Function;
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
import org.axiondb.engine.visitors.TableColumnsUsedInFunctionVisitor;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.util.ValuePool;

/**
 * An <tt>UPSERT or MERGE</tt> command.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class UpsertCommand extends ChildTableUpdater {

    //------------------------------------------------------------ Constructors

    public UpsertCommand() {
    }

    //---------------------------------------------------------- Public Methods

    @SuppressWarnings("unchecked")
    public void addUpdateColumn(ColumnIdentifier col) {
        _columnsForUpdate.add(col);
    }

    @SuppressWarnings("unchecked")
    public void addUpdateValue(Selectable val) {
        _valuesForUpdate.add(val);
    }

    public boolean execute(Database database) throws AxionException {
        executeUpdate(database);
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
        IntSet rowcount = new IntSet();
        int exTblCnt = 0;
        int rowId = -1;
        
        RowIterator joinedRowIter;
        JoinedRow joinRow;

        Set sourceColsUsedInCondition = new HashSet();
        Set targetColsUsedInCondition = new HashSet();
        Set uniqueSourceRowSet = new HashSet();

        try {
            preProcess(db);
            resolve(db);

            // 1. execute the 'select * from srcTable left outer join targetTable...'
            joinedRowIter = _selectCommand.makeRowIterator(db, true);

            // 2. build decorator now, building this before executing the above
            RowDecorator dec = buildDecorator(_sourceTable, _targetTable);

            // 3. check if any target columns that participated in
            //    merge condition used in update
            TableColumnsUsedInFunctionVisitor tVisitor = new TableColumnsUsedInFunctionVisitor();
            tVisitor.visit((Function) _condition, _targetTable);
            targetColsUsedInCondition = tVisitor.getColumnsUsedInFunction();

            if (isTargetColumnUsedInUpdate(targetColsUsedInCondition)) {
                throw new AxionException(
                    "Updates Not allowed for cols used in Merge/Upsert Condition");
            }

            // 4. Find source columns used in Merge condition.
            TableColumnsUsedInFunctionVisitor sVisitor = new TableColumnsUsedInFunctionVisitor();
            sVisitor.visit((Function) _condition, _sourceTable);
            sourceColsUsedInCondition = sVisitor.getColumnsUsedInFunction();
            int ttColCount = _targetTable.getColumnCount();
            
            setDeferAllConstraintIfRequired(_targetTable);

            // 5. Loop thru and merge(insert or update as appropriate)
            while (joinedRowIter.hasNext()) {
                Iterator colids;
                Iterator values;
                Row newrow;

                // Since we createrd a LOJ assume the the joined row has
                // left table's row should be at index(0)
                // and right table's row at index(1)
                joinRow = (JoinedRow) joinedRowIter.next();

                Row sourceRow = joinRow.getRow(0); // get source table row
                Row targetRow = joinRow.getRow(1); // get target table row

                dec.setRow(joinedRowIter.currentIndex(), joinRow);

                // check for unstable/duplicate row set in source table
                if (hasDuplicateRow(dec, sourceColsUsedInCondition, uniqueSourceRowSet)) {
                    throw new AxionException(
                        "Unable to get a stable set of rows in the source tables...");
                }

                // if current row match exception when condition process else
                if (_exceptionWhenClause != null
                    && _exceptionWhenClause.insertMatchingRow(db, dec, sourceRow)) {
                    continue; // pick next row
                }

                // check for null in columns that are participating in merge condition
                // if true then it's an insert
                // else we have matching row in the target table , so it's an update
                if (isNullRow(targetRow)) {
                    // INSERT: add sourceRow to targetTable
                    newrow = new SimpleRow(ttColCount);
                    colids = this.getInsertColumnIterator();
                    values = this.getInsertValueIterator();
                    prepareRow(newrow, colids, values, dec, _targetTable, db);
                    
                    RowDecorator trgtDec = makeTargetRowDecorator();
                    trgtDec.setRow(newrow);
                    populateDefaultValues(db, _targetTable, _targetTableId, trgtDec);
                    if(_populateSequence) {
                        _populateSequence = populateSequenceColumns(db, _targetTable, newrow);
                    }
                    _targetTable.addRow(newrow);
                    rowId = newrow.getIdentifier();
                    if (rowId == ExternalTable.UNKNOWN_ROWID){
                        exTblCnt++;
                    }else{
                        rowcount.add(rowId);                        
                    }
                } else {
                    // UPDATE: replace old row with new row
                    newrow = new SimpleRow(targetRow);
                    colids = this.getUpdateColumnIterator();
                    values = this.getUpdateValueIterator();
                    prepareRow(newrow, colids, values, dec, _targetTable, db);
                    if (_isExternalDBTable) {
                        ((ExternalDatabaseTable) _targetTable).updateRow(targetRow, newrow, _columnsForUpdate);
                        
                        rowId = newrow.getIdentifier();                        
                        if (rowId == ExternalTable.UNKNOWN_ROWID){
                            exTblCnt++;
                        }else{
                            rowcount.add(rowId);                        
                        }                        
                    } else {
                        updateGeneratedValues(db, _targetTable, _targetTableId, newrow);
                        if (!targetRow.equals(newrow)) {
                            _targetTable.updateRow(targetRow, newrow);
                            updateOrSetNullChildRows(db, _targetTable, targetRow, newrow);
                            
                            rowId = newrow.getIdentifier();
                            if (rowId == ExternalTable.UNKNOWN_ROWID){
                                exTblCnt++;
                            }else{
                                rowcount.add(rowId);                        
                            }
                        }
                    }
                }
                commitIfRequired(db);
            }
        } finally {
            //cleanup if a view exist.
            if (_usingSubselect != null && _sourceTable != null) {
                if (db.hasTable(_sourceTable.getName())) {
                    db.dropTable(_sourceTable.getName());
                }
            }
        }

        setEffectedRowCount(rowcount.size() + exTblCnt);
        return rowcount.size() + exTblCnt;
    }

    public void setColumnsForInsert(List columnForInsert) {
        _columnsForInsert = columnForInsert;
    }

    public void setCondition(Selectable condition) {
        _condition = condition;
    }

    public Selectable getCondition() {
        return _condition;
    }

    public void setExceptionWhenClause(DMLWhenClause w, TableIdentifier t, List cols, List vals) {
        _exceptionWhenClause = new ExceptionWhenClause(w, t, cols, vals);
    }

    public ExceptionWhenClause getExceptionWhenClause() {
        return _exceptionWhenClause;
    }

    public void setSelectCommand(SubSelectCommand command) {
        _selectCommand = command;
    }

    public void setSourceTable(TableIdentifier table) {
        _sourceTableId = table;
    }

    public void setTargetTable(TableIdentifier table) {
        _targetTableId = table;
    }

    public void setUsingSubSelectAlias(String alias) {
        _usingSubSelectAlias = alias;
    }

    public void setUsingSubSelectCommand(SubSelectCommand command) {
        _usingSubselect = command;
    }

    public SubSelectCommand getUsingSubSelectCommand() {
        return _usingSubselect;
    }

    public void setValuesForInsert(List valuesForInsert) {
        _valuesForInsert = valuesForInsert;
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        getBindVariableVisitor().visit(this);
    }

    // hashCode method must consistently return the same integer,
    // provided no information used in equals comparisons on the object is modified.
    // So this has to be built after we execute our internal LOJ select Query
    // Otherwise hashCode for columnIdentifier will not match.
    @SuppressWarnings("unchecked")
    private RowDecorator buildDecorator(Table sourceTable, Table targetTable) throws AxionException {
        if (_dec == null) {
            // build the indexMap (ColumnIdentifiers --> Integer index in Row)
            Map indexMap = new HashMap();
            int index = 0;

            Iterator iter = getColIdentifierList(sourceTable, getSourceTable()).iterator();
            while (iter.hasNext()) {
                indexMap.put(iter.next(), ValuePool.getInt(index++));
            }

            iter = getColIdentifierList(targetTable, getTargetTable()).iterator();
            while (iter.hasNext()) {
                indexMap.put(iter.next(), ValuePool.getInt(index++));
            }
            _dec = new RowDecorator(indexMap);
        }
        return _dec;
    }
    
    private void commitIfRequired(Database db) throws AxionException {
        if (getCommitSize(db) == 0) {
            return;
        }

        if (db instanceof SnapshotIsolationTransaction && (++_count % getCommitSize(db)) == 0) {
            _targetTable = ((SnapshotIsolationTransaction) db).commit(_targetTableId);
        }
    }

    private TableIdentifier[] getAllTables() {
        return new TableIdentifier[] { getSourceTable(), getTargetTable()};
    }

    private Iterator getInsertColumnIterator() {
        return _columnsForInsert.iterator();
    }

    public Iterator getInsertValueIterator() {
        return _valuesForInsert.iterator();
    }

    private TableIdentifier getSourceTable() {
        return _sourceTableId;
    }

    private TableIdentifier getTargetTable() {
        return _targetTableId;
    }

    private Iterator getUpdateColumnIterator() {
        return _columnsForUpdate.iterator();
    }

    public Iterator getUpdateValueIterator() {
        return _valuesForUpdate.iterator();
    }

    private String getUsingSubSelectAlias() {
        return _usingSubSelectAlias;
    }

    @SuppressWarnings("unchecked")
    private boolean hasDuplicateRow(RowDecorator dec, Set sourceColsUsedInCondition,
            Set uniqueSourceRowSet) throws AxionException {
        Row row = new SimpleRow(dec.getRowIndex(), sourceColsUsedInCondition.size());
        int i = 0;
        boolean found = false;

        for (Iterator colids = sourceColsUsedInCondition.iterator(); colids.hasNext();) {
            Selectable colid = (Selectable) (colids.next());
            Object val = colid.evaluate(dec);
            row.set(i++, val);
        }

        if (uniqueSourceRowSet.contains(row)) {
            found = true;
        } else {
            uniqueSourceRowSet.add(row);
        }

        return found;
    }

    private boolean isNullRow(Row row) {
        return (row.getIdentifier() == -1);
    }

    private boolean isTargetColumnUsedInUpdate(Set cols) {
        for (Iterator colItr = getUpdateColumnIterator(); colItr.hasNext();) {
            if (cols.contains(colItr.next())) {
                return true;
            }
        }
        return false;
    }

    private void prepareRow(Row newrow, Iterator colids, Iterator values, RowDecorator dec,
            Table targetTable, Database db) throws AxionException {

        while (colids.hasNext()) {
            ColumnIdentifier colid = (ColumnIdentifier) (colids.next());
            Selectable sel = (Selectable) (values.next());
            Object val = sel.evaluate(dec);
            DataType type = db.getTable(colid.getTableName()).getColumn(colid.getName())
                .getDataType();
            val = attemptToConvertValue(val, type, colid);
            newrow.set(targetTable.getColumnIndex(colid.getName()), val);
        }
    }

    private void preProcess(Database db) throws AxionException {
        _count = 0;
        // process sub-query/view
        if (null != _usingSubselect) {
            _sourceTable = _usingSubselect.getTableView(db, null, true);

            TableIdentifier tid = new TableIdentifier(_sourceTable.getName(),
                getUsingSubSelectAlias());
            setSourceTable(tid);
            _selectCommand.getQueryContext().getFrom().setLeft(tid);

            // get from the database to enable transaction
            _sourceTable = db.getTable(getSourceTable());
        }

        // grab the table
        if (null == _usingSubselect && null != getSourceTable()) {
            _sourceTable = db.getTable(getSourceTable());
        }

        if (null == _sourceTable) {
            throw new AxionException("Table " + getSourceTable() + " not found.");
        }

        _targetTable = db.getTable(getTargetTable());
        if (null == _targetTable) {
            throw new AxionException("Table " + getTargetTable() + " not found.");
        }

        if (_exceptionWhenClause != null) {
            _exceptionWhenClause.preProcess(db);
        }
    }

    protected void resolve(Database db) throws AxionException {
        if (!_resolved) {
            preProcess(db);
            
            resolveSelectableList(_columnsForInsert, db, getTargetTable());
            resolveSelectableList(_valuesForInsert, db, getAllTables());
            resolveSelectableList(_columnsForUpdate, db, getTargetTable());
            resolveSelectableList(_valuesForUpdate, db, getAllTables());

            resolveGeneratedColumns(_targetTable, _targetTableId, _columnsForInsert);
            resolveGeneratedColumns(_targetTable, _targetTableId, _columnsForUpdate);

            _condition = resolveSelectable(_condition, db, getAllTables());

            // check and resolve Exception When clause
            if (_exceptionWhenClause != null) {
                _exceptionWhenClause.resolve(db);
            }

            _isExternalDBTable = _targetTable instanceof ExternalDatabaseTable;
            _resolved = true;
        }
    }

    public class ExceptionWhenClause extends InsertIntoClause {

        private boolean _isTargetPartOfSubQuery = false;

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

            //resolve when condition
            getWhenClause().resolve(db, new TableIdentifier[] { getSourceTable()});
            if (_usingSubselect != null) {
                _isTargetPartOfSubQuery = _usingSubselect.getQueryContext().isTablePartOfSelect(
                    getTargetTableId());
            }

            resolveSelectableList(getValues(), db, getSourceTable());
            assertRules(_sourceTable);
        }
    }
    
    protected final RowDecorator makeTargetRowDecorator() {
        if (_trgtDec == null) {
            _trgtDec = _targetTable.makeRowDecorator();
        }
        return _trgtDec;
    }

    //-------------------------------------------------------------- Attributes

    private List _columnsForInsert;
    private List _columnsForUpdate = new ArrayList();
    private Selectable _condition;

    private int _count;
    private ExceptionWhenClause _exceptionWhenClause;
    private boolean _resolved = false;
    private boolean _populateSequence = true;
    private boolean _isExternalDBTable = false;

    private SubSelectCommand _selectCommand;
    private Table _sourceTable;
    private TableIdentifier _sourceTableId;
    private Table _targetTable;
    private RowDecorator _dec;
    private RowDecorator _trgtDec;

    private TableIdentifier _targetTableId;
    private SubSelectCommand _usingSubselect;
    private String _usingSubSelectAlias;
    private List _valuesForInsert;
    private List _valuesForUpdate = new ArrayList();
}
