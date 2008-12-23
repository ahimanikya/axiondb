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

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.jdbc.AxionResultSet;

/**
 * A <tt>DELETE</tt> command.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class DeleteCommand extends ChildTableUpdater {

    public DeleteCommand(String tableName, Selectable where) {
        setTable(new TableIdentifier(tableName));
        setWhere(where);
    }

    public DeleteCommand(TableIdentifier table, Selectable where) {
        setTable(table);
        setWhere(where);
    }

    public boolean execute(Database database) throws AxionException {
        executeUpdate(database);
        return false;
    }

    /** Unsupported */
    public AxionResultSet executeQuery(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use executeUpdate.");
    }

    public int executeUpdate(org.axiondb.Database db) throws AxionException {
        assertNotReadOnly(db);
        resolve(db);

        Table table = db.getTable(getTable());
        if (null == table) {
            throw new AxionException("Table " + getTable() + " not found.");
        }

        int deletecount = -1;
        if (_where == null) {
            deletecount = tryToTruncate(db);
        }

        if (deletecount == -1) {
            deletecount = 0;
            if(_dec == null) {
                _dec = makeRowDecorator(table);
            }
            RowIterator rows = getRowIterator(db, getTable(), table, getWhere(), false, _dec);
            setDeferAllConstraintIfRequired(table);
            while (rows.hasNext()) {
                Row row = rows.next();
                _dec.setRow(row);
                deleteOrSetNullChildRows(db, table, _dec);
                rows.remove();
                deletecount++;
            }
        }

        setEffectedRowCount(deletecount);
        return deletecount;
    }
    
    private int tryToTruncate(Database db) {
        TruncateCommand tc = new TruncateCommand();
        tc.setObjectName(_tableId.getTableName());
        try {
            return tc.executeUpdate(db);
        } catch (AxionException e) {
            // 
        }
        return -1;
    }

    public final TableIdentifier getTable() {
        return _tableId;
    }

    public final Selectable getWhere() {
        return _where;
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        getBindVariableVisitor().visit(getWhere());
    }

    protected void resolve(Database db) throws AxionException {
        if (!_resolved) {
            TableIdentifier[] tables = new TableIdentifier[] { getTable()};
            setWhere(resolveSelectable(getWhere(), db, tables));
            
            _resolved = true;
        }
    }
    
    private void setTable(TableIdentifier tableId) {
        _tableId = tableId;
    }
    private void setWhere(Selectable where) {
        _where = where;
    }
    
    private boolean _resolved = false;
    private TableIdentifier _tableId;
    private Selectable _where;
    private RowDecorator _dec;
}
