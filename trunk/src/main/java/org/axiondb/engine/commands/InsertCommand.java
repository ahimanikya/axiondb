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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.util.ValuePool;

/**
 * An <tt>INSERT</tt> statement.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Rahul Dwivedi
 * @author Ahimanikya Satapathy
 * @author Ritesh Adval
 */
public class InsertCommand extends BaseAxionCommand {

    //------------------------------------------------------------ Constructors

    public InsertCommand() {
    }

    public InsertCommand(TableIdentifier table, List columns, AxionCommand subSelect) {
        this(table, columns, (SubSelectCommand) subSelect);
    }

    @SuppressWarnings("unchecked")
    public InsertCommand(TableIdentifier table, List columns, SubSelectCommand subSelect) {
        _subQuery = subSelect;
        InsertIntoClause ip = new InsertMultipleRow(null, table, columns, null);
        _insertIntoList.add(ip);
    }

    /**
     * @param table The table in which to insert
     * @param columns List of {@link ColumnIdentifier ColumnIdentifiers}, which may be
     *        <code>null</code>
     * @param values List of {@link Object Objects}, which may be <code>null</code>
     * @throws InvalidArgumentException if
     *         <code>columns.size() &gt; 0 &amp;&amp; columns.size() != values.size()</code>
     */
    public InsertCommand(TableIdentifier table, List columns, List values) {
        _simpleInsert = new InsertSingleRow(table, columns, values);
    }
    
    public InsertCommand(TableIdentifier table, List columns, boolean defaultVAlues) {
        _simpleInsert = new InsertSingleRow(table, columns, defaultVAlues);
    }

    //---------------------------------------------------------- Public Methods

    @SuppressWarnings("unchecked")
    public void addInsertIntoClause(DMLWhenClause when, TableIdentifier table, List columns,
            List values) {
        InsertIntoClause into = new InsertMultipleRow(when, table, columns, values);
        _insertIntoList.add(into);
    }

    public final boolean isInsertIntoListEmpty() {
        return _insertIntoList.isEmpty();
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

    public int executeUpdate(Database db) throws AxionException {
        assertNotReadOnly(db);
        preProcess(db);
        resolve(db);

        int count = 0;
        try {
            if (_insertIntoList.size() != 0) {
                count = handleMultiTableInsert(db);
            } else {
                count = _simpleInsert.insertRow(db);
            }
        } finally {
            //cleanup if a view exist.
            if (_source != null) {
                db.dropTable(_source.getName());
            }
        }
        setEffectedRowCount(count);
        return count;
    }

    public final Iterator getColumnIterator() {
        return _simpleInsert.getColumnIterator();
    }

    public final TableIdentifier getTable() {
        return _simpleInsert.getTargetTableId();
    }

    public final Iterator getValueIterator() {
        return _simpleInsert.getValueIterator();
    }

    public void setElseClause(TableIdentifier table, List tableColumns, List tableValues) {
        _elseClause = new ElseClause(table, tableColumns, tableValues);
    }

    public void setMultiTableEvaluationMode(int mode) {
        _evaluationMode = mode;
    }

    public void setSubSelect(SubSelectCommand select) {
        _subQuery = select;
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        for (int i = 0, I = _insertIntoList.size(); i < I; i++) {
            getBindVariableVisitor().visit((InsertIntoClause) _insertIntoList.get(i));
        }
        if(_elseClause != null) {
        	getBindVariableVisitor().visit(_elseClause);
        }
        if (_simpleInsert != null) {
            getBindVariableVisitor().visit(_simpleInsert);
        }
        if(_subQuery != null) {
            getBindVariableVisitor().visit((SelectCommand)_subQuery);
        }
    }

    @SuppressWarnings("unchecked")
    private RowDecorator buildDecorator() {
        if (_srcDec == null) {
            int size = _source.getColumnCount();
            HashMap colToIndexMap = new HashMap(size);
            for (int i = 0; i < size; i++) {
                Column col = _source.getColumn(i);
                colToIndexMap.put(new ColumnIdentifier(_sourceId, col.getName(), null, col.getDataType()), ValuePool.getInt(i));
            }
            _srcDec = new RowDecorator(colToIndexMap);
        }
        return _srcDec;
    }
    
    private final int getMultiTableEvaluationMode() {
        return _evaluationMode;
    }

    private final TableIdentifier getSourceTableId() {
        return _sourceId;
    }

    private final Table getSourceTable() {
        return _source;
    }

    private int handleMultiTableInsert(Database db) throws AxionException {
        // create RowDecorator out of source view
        RowDecorator dec = buildDecorator();

        // iterator through source rows and process insert clauses
        RowIterator rowItr = _source.getRowIterator(false);
        while (rowItr.hasNext()) {
            Row row = rowItr.next();
            dec.setRow(row);

            // process all the Insert into clauses
            boolean isAtLeastOneMatched = false;
            for (int i = 0, I = _insertIntoList.size(); i < I; i++) {
                if (((InsertIntoClause) _insertIntoList.get(i)).insertMatchingRow(db, dec, row)) {
                    isAtLeastOneMatched = true;
                }

                if (isAtLeastOneMatched && getMultiTableEvaluationMode() == WHEN_FIRST) {
                    break;
                }
            }

            // if non of the when condition matched
            // then handle else condition if specified
            if (!isAtLeastOneMatched && _elseClause != null) {
                _elseClause.insertMatchingRow(db, dec, row);
            }
        }

        // go through all Insert clause do post processing
        int count = 0;
        for (int i = 0, I = _insertIntoList.size(); i < I; i++) {
            InsertIntoClause insertClause = (InsertIntoClause) _insertIntoList.get(i);
            count += insertClause.getProcessedRowCount();
        }

        return count;
    }

    private void preProcess(Database db) throws AxionException {
        // process sub-query/view
        if (null != _subQuery) {
            _source = _subQuery.getTableView(db, null, true);
            _sourceId = new TableIdentifier(_source.getName(), _subQuery.getAlias());

            // get from the database to enable transaction
            _source = db.getTable(_sourceId);
        }
        
        // go through all Insert clause do pre processing
        for (int i = 0, I = _insertIntoList.size(); i < I; i++) {
            InsertIntoClause insertClause = (InsertIntoClause) _insertIntoList.get(i);
            insertClause.preProcess(db);
        }
        
        if(_simpleInsert != null) {
            _simpleInsert.preProcess(db);
        }

        // pre process else clause
        if (_elseClause != null) {
            _elseClause.preProcess(db);
        }
    }

    protected void resolve(Database db) throws AxionException {
        if (!_resolved) {

            //if single table insert
            if (_insertIntoList.size() == 0) {
                _simpleInsert.resolve(db);
            } else {

                //check and resolve Else insert clause
                if (_elseClause != null) {
                    _elseClause.resolve(db);
                }

                //check and resolve all When insert clause
                for (int i = 0, I = _insertIntoList.size(); i < I; i++) {
                    ((InsertIntoClause) _insertIntoList.get(i)).resolve(db);
                }
            }
            _resolved = true;
        }
    }

    private class ElseClause extends InsertMultipleRow {
        public ElseClause(TableIdentifier tid, List cols, List vals) {
            super(null, tid, cols, vals);
        }
    }

    private class InsertMultipleRow extends InsertIntoClause {

        public InsertMultipleRow(DMLWhenClause when, TableIdentifier tid, List cols, List vals) {
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
            DMLWhenClause when = getWhenClause();
            if (when != null) {
                when.resolve(db, new TableIdentifier[] { getSourceTableId()});
            }

            _isTargetPartOfSubQuery = _subQuery.getQueryContext().isTablePartOfSelect(
                getTargetTableId());

            resolveSelectableList(getValues(), db, getSourceTableId());
            assertRules(getSourceTable());
        }

        private boolean _isTargetPartOfSubQuery = false;
    }

    private class InsertSingleRow extends InsertIntoClause {

        public InsertSingleRow(TableIdentifier tid, List cols, List vals) {
            super(null, tid, cols, vals);
        }
        
        public InsertSingleRow(TableIdentifier tid, List cols, boolean defaultValues) {
            super(null, tid, cols, defaultValues);
        }

        public int insertRow(Database db) throws AxionException {
            addRowToTable(db, null, makeRowDecorator());
            return 1;
        }

        @Override
        public void resolve(Database db) throws AxionException {
            if (getColumnCount() > getValueCount() && getColumnCount() != 0) {
                throw new IllegalArgumentException("Number of columns and values must match.");
            }

            super.resolve(db);
            resolveSelectableList(getValues(), db, getTargetTableId());

            if (getColumnCount() < getValueCount()) {
                throw new IllegalArgumentException("Too Many Values...");
            }
        }
    }
    
    //-------------------------------------------------------------- Attributes

    public static final int WHEN_ALL = 1;
    public static final int WHEN_FIRST = 2;
    private ElseClause _elseClause;

    // default mode of when condition evaluation is ALL
    private int _evaluationMode = WHEN_ALL;
    private List _insertIntoList = new ArrayList(2);

    private boolean _resolved = false;
    private InsertSingleRow _simpleInsert;
    private transient RowDecorator _srcDec;

    private Table _source;
    private TableIdentifier _sourceId;
    private SubSelectCommand _subQuery;
}
