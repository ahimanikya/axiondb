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
package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.SnapshotIsolationTransaction;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.ExternalDatabaseTable;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.jdbc.AxionResultSet;

/**
 * An <tt>INSERT INTO</tt> Clause.
 * 
 * @author Ahimanikya Satapathy
 * @author Ritesh Adval
 */
public abstract class InsertIntoClause extends BaseAxionCommand {

    public InsertIntoClause(DMLWhenClause when, TableIdentifier tid, List cols, List vals) {
        _whenClause = when;
        _tableId = tid;
        _tables = new TableIdentifier[] { _tableId};
        _cols = (null == cols ? new ArrayList() : cols);
        _vals = (null == vals ? new ArrayList() : vals);
    }
    
    public InsertIntoClause(DMLWhenClause when, TableIdentifier tid, List cols, boolean useDefaultValues) {
        this(when,tid,cols, null);
        _useDefaultValues = useDefaultValues;
    }

    public boolean execute(Database db) throws AxionException {
        throw new UnsupportedOperationException("Not Implemented...");
    }

    public AxionResultSet executeQuery(Database db) throws AxionException {
        throw new UnsupportedOperationException("Not Implemented...");
    }

    public int executeUpdate(Database db) throws AxionException {
        throw new UnsupportedOperationException("Not Implemented...");
    }

    public final Iterator getColumnIterator() {
        return _cols.iterator();
    }

    public final int getProcessedRowCount() {
        return _count;
    }

    public TableIdentifier getTargetTableId() {
        return _tableId;
    }

    public final Iterator getValueIterator() {
        return _vals.iterator();
    }

    public boolean insertMatchingRow(Database db, RowDecorator dec, Row srcRow) throws AxionException {
        if (_whenClause != null) {
            // if current row does not match when condition return
            if (!_whenClause.evaluate(dec)) {
                return false;
            }
        }
        
        if(_vals != null && !_vals.isEmpty()) {
            addRowToTable(db, null, dec);
        } else {
            addRowToTable(db, srcRow, dec);
        }
        
        _count++;
        commitIfRequired(db);
        return true;
    }

    public void preProcess(Database db) throws AxionException {
        _count = 0;
        _table = db.getTable(_tableId);
        if (null == _table) {
            throw new AxionException("Table " + _tableId + " not found.");
        }
        setDeferAllConstraintIfRequired(_table);
    }

    protected void addRowToTable(Database db, Row srcRow, RowDecorator dec) throws AxionException {

        Row row = new SimpleRow(_table.getColumnCount());
        if (dec.getRow() == null) {
            dec.setRow(row);
        }

        int size = (srcRow != null ? srcRow.size() : _vals.size());
        for (int i = 0; i < size; i++) {
            setValue((srcRow != null ? srcRow.get(i) : _vals.get(i)), dec, row, i);
        }

        RowDecorator trgtDec = makeRowDecorator();
        trgtDec.setRow(row);
        populateDefaultValues(db, _table, _tableId, trgtDec);
        if(_populateSequence) {
            _populateSequence = populateSequenceColumns(db, _table, row);
        }
        if(_isExternalDBTable) {
            ((ExternalDatabaseTable)_table).addRow(row, _cols);
        } else {
        	_table.addRow(row);
        }
    }

    private void setValue(Object val, RowDecorator dec, Row row, int i) throws AxionException {
        ColumnIdentifier colid = (ColumnIdentifier) (_cols.get(i));
        DataType type = colid.getDataType();

        // if we have a non-null selectable, evaluate it
        if (val instanceof Selectable) {
            val = ((Selectable) val).evaluate(dec);
        }
        val = attemptToConvertValue(val, type, colid);
        row.set(_colIndex[i], val);
    }
    
    // If values are not specified we need to assume that all the identifers in sub
    // select needs to be inserted.
    protected void assertRules(Table source) throws AxionException {
        // Currently we don't support DEFAULT VALUES for multi table insert
        // But one could tweak the grammar to support this, 
        // uncomment the following line in such case 
        // if(_useDefaultValues) {return;}
        
        // if no values clause specified
        String errMsg = "Number of columns and values must match.";
        if (_vals.isEmpty()) {
            // number of selectables in subselect
            // should be same as number of columns in target table
            if (getColumnCount() != source.getColumnCount()) {
                throw new IllegalArgumentException(errMsg);
            }
        } else {
            // if columns are also specified then number of selectables in
            // columns should be same as number of selectables in values clause
            if (getColumnCount() != getValueCount()) {
                throw new IllegalArgumentException(errMsg);
            }
        }
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        getBindVariableVisitor().visit(this);
    }
    
    protected final int getColumnCount() {
        return _cols.size();
    }

    protected final Table getTargetTable() {
        return _table;
    }

    protected final int getValueCount() {
        return _vals.size();
    }

    protected final List getValues() {
        return _vals;
    }

    public final DMLWhenClause getWhenClause() {
        return _whenClause;
    }

    protected boolean isTargetTablePartOfSubQuery() throws AxionException {
        return false;
    }

    protected void resolve(Database db) throws AxionException {
        resolveSelectableList(_cols, db, _tables);
        resolveGeneratedColumns(_table, _tableId, _cols, _useDefaultValues);
        
        if (_colIndex == null) {
            _colIndex = new int[_cols.size()];
            for (int i = 0, I = _cols.size(); i < I; i++) {
                _colIndex[i] = _table.getColumnIndex(((ColumnIdentifier) _cols.get(i)).getName());
            }
        }
        _isExternalDBTable = _table instanceof ExternalDatabaseTable;
    }

    private void commitIfRequired(Database db) throws AxionException {
        if (getCommitSize(db) == 0 || isTargetTablePartOfSubQuery()) {
            return;
        }

        if (db instanceof SnapshotIsolationTransaction && (_count % getCommitSize(db)) == 0) {
            _table = ((SnapshotIsolationTransaction) db).commit(_tableId);
        }
    }

    protected final RowDecorator makeRowDecorator() {
        if (_dec == null) {
            _dec = _table.makeRowDecorator();
        }
        return _dec;
    }
    
    private List _cols;
    private transient int[] _colIndex;
    private Table _table;
    private RowDecorator _dec;
    private List _vals;
    private boolean _useDefaultValues = false;
    private boolean _populateSequence = true;
    private boolean _isExternalDBTable = false;

    private int _count = 0;
    private TableIdentifier _tableId;
    private TableIdentifier[] _tables;

    private DMLWhenClause _whenClause;
}
