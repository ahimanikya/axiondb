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

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.VariableContext;
import org.axiondb.engine.tables.TableView;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.types.AnyType;

/**
 * A Sub <tt>SELECT</tt> query used for view, scalar value, from node, row list
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class SubSelectCommand extends SelectCommand implements Selectable {

    public SubSelectCommand(AxionQueryContext context) {
        super(context);
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        // If this is a scalar select then we might have parent row
        // that this select should have access to.
        if (_evaluteAsScalarValue) {
            RowIterator rowIter = getRowIterator(_dbForSubSelect, row);

            Object rval = null;
            if (rowIter.hasNext()) {
                Row rrow = rowIter.next();
                if (rowIter.hasNext()) {
                    throw new AxionException("single-row subquery returns more than one row");
                }
                if (rrow.size() > 1) {
                    throw new AxionException("too many values");
                }
                rval = rrow.get(0);
            }
            return rval;
        }

        return getRowIterator(_dbForSubSelect, row);
    }

    @Override
    public boolean execute(Database database) throws AxionException {
        throw new UnsupportedOperationException("Can't execute the sub-query directly");
    }

    @Override
    public AxionResultSet executeQuery(Database db) throws AxionException {
        throw new UnsupportedOperationException("Can't execute the sub-query directly");
    }

    @Override
    public int executeUpdate(Database database) throws AxionException {
        throw new UnsupportedOperationException("Can't execute the sub-query directly");
    }

    public String getAlias() {
        return _context.getAliasName();
    }

    public DataType getDataType() {
        return AnyType.INSTANCE;
    }

    public String getLabel() {
        return getName();
    }

    public String getName() {
        return _context.getAliasName() != null ? _context.getAliasName() : "SELECT";
    }

    /**
     * Return RowIterator that can used for other commands for sub-query.
     * 
     * @return the {@link org.axiondb.RowIterator}.
     * @throws AxionException
     */
    public RowIterator getRowIterator(Database db) throws AxionException {
        return getTableView(db, null).getRowIterator(true);
    }

    public RowIterator getRowIterator(Database db, RowDecorator rowDec) throws AxionException {
        _context.setParentRow(rowDec);
        return getTableView(db, null).getRowIterator(true);
    }

    /**
     * Return TableView a table wrapper to hold sub-query RowIterator.
     * 
     * @return the {@link org.axiondb.engine.tables.TableView}.
     * @throws AxionException
     */
    public Table getTableView(Database db, String name) throws AxionException {
        if (_view == null) {
            _view = new TableView(db, name, this);
        }
        return _view;
    }
    
    /**
     * Return TableView a table wrapper to hold sub-query RowIterator.
     * 
     * @return the {@link org.axiondb.engine.tables.TableView}.
     * @throws AxionException
     */
    public Table getTableView(Database db, String name, boolean addToDb) throws AxionException {
        if (_view == null) {
            _view = new TableView(db, name, this);
        }
        
        if(addToDb && !db.hasTable(_view.getName())) {
            db.addTable(_view);
        }
        
        return _view;
    }

    public boolean isScalarSelect() {
        return _evaluteAsScalarValue;
    }

    public boolean isCorrelated() {
        return _foundCorrelatedColumnReference;
    }

    @Override
    public RowIterator makeRowIterator(Database db, boolean readOnly) throws AxionException {
        if (_context.getRows() == null || _currentDatabase != db || (_context.getGroupByCount() > 0)
            || (_evaluteAsScalarValue && _context.isCorrelatedSubQuery())) {
            super.makeRowIterator(db, readOnly, true);
        } else {
            _context.getRows().reset();
        }
        return _context.getRows();
    }

    public void setAlias(String aliasName) {
        _context.setAliasName(aliasName);
    }

    // This is needed in order to get this Selectable to be evaluated..
    public void setDB(Database db) {
        _dbForSubSelect = db;
    }

    public void setEvaluteAsScalarValue() {
        _evaluteAsScalarValue = true;
    }

    public void setParentTables(TableIdentifier[] tables) {
        _context.setParentTables(tables);
    }

    public void setVariableContext(VariableContext context) {
    }

    @Override
    protected void buildTableList(Database db) throws AxionException {
        TableIdentifier[] t = _context.getFromArray();
        findCorrelatedColumnReference(db, t);
        _context.setCorrelatedSubQuery(isCorrelated());

        // scan the query to find outer table reference.
        if (isCorrelated()) {
            TableIdentifier[] pt = _context.getParentTables();
            int tableLen = t.length;
            if(pt != null) {
            tableLen += pt.length;
            }

            TableIdentifier[] tables = new TableIdentifier[tableLen];
            int pos = 0;

            for (int i = 0; t != null && i < t.length; i++) {
                tables[pos++] = t[i];
            }

            for (int i = 0; pt != null && i < pt.length; i++) {
                tables[pos++] = pt[i];
            }
            _context.setTables(tables);
        } else {
            _context.setParentTables(null);
            _context.setParentRow(null);
            _context.setTables(t);
        }
    }

    private boolean findCorrelatedColumnReference(Database db, TableIdentifier[] t) throws AxionException {
        // this is a lazy guess, 2nd pass may find more unresolved column
        _foundCorrelatedColumnReference = false;
        try {
            if (_context.getWhere() != null) {
                resolveSelectable(_context.getWhere(), db, t);
            }

            if (null != _context.getGroupBy()) {
                for (int i = 0, I = _context.getGroupByCount(); i < I; i++) {
                    resolveSelectable(_context.getGroupBy(i), db, t);
                }
            }
            resolveSelectable(_context.getHaving(), db, t);
            if (null != _context.getOrderBy()) {
                for (int i = 0, I = _context.getOrderByCount(); i < I; i++) {
                    OrderNode ob = _context.getOrderBy(i);
                    resolveSelectable(ob.getSelectable(), db, t);
                }
            }

        } catch (AxionException e) {
            _foundCorrelatedColumnReference = true;
        }
        return _foundCorrelatedColumnReference;
    }

    // ----------------------------------------------------------------- Members

    private Database _dbForSubSelect;
    private boolean _evaluteAsScalarValue = false;
    private boolean _foundCorrelatedColumnReference = false;
    private transient volatile TableView _view;
}
