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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Function;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rowiterators.FilteringRowIterator;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.engine.visitors.ResolveSelectableVisitor;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.jdbc.AxionResultSet;

/**
 * Abstract base {@link AxionCommand}implementation.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public abstract class BaseAxionCommand implements AxionCommand {

    /**
     * Sets the <i>values </i> of all bind variable within this command.
     * 
     * @param index the one-based index of the variable
     * @param value the value to bind the variable to
     */
    public void bindAll(Object[] vals) throws AxionException {
        int index = 0;
        for (Iterator iter = getBindVariableIterator(); iter.hasNext(); index++) {
            BindVariable var = (BindVariable) (iter.next());
            var.setValue(vals[index]);
        }
    }

    /**
     * Clears all bind variables within this command.
     */
    public void clearBindings() throws AxionException {
        for (Iterator iter = getBindVariableIterator(); iter.hasNext();) {
            BindVariable var = (BindVariable) (iter.next());
            var.clearBoundValue();
        }
    }

    public AxionResultSet executeQuery(Database db, boolean isReadOnly) throws AxionException {
        return executeQuery(db);
    }

    public List getBindVariables() {
        if (getBindVariableVisitor() == null) {
            buildBindVariables();
        }
        return getBindVariableVisitor().getBindVariables();
    }

    public final int getEffectedRowCount() {
        return _rowCount;
    }

    public final ResultSet getResultSet() {
        return _rset;
    }

    /** Throws an {@link AxionException}if the given {@link Database}is read-only. */
    protected void assertNotReadOnly(Database db) throws AxionException {
        if (db.isReadOnly()) {
            throw new AxionException("The database is read only.");
        }
    }

    protected Object attemptToConvertValue(Object val, DataType type, ColumnIdentifier colid) throws AxionException {
        try {
            return type.convert(val);
        } catch (AxionException e) {
            throw new AxionException("Invalid value " + "\"" + val + "\"" + " for column " + colid + ", expected " + type + " : " + e.getMessage(),
                e.getVendorCode());
        }
    }

    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
    }

    protected final void createResolveSelectableVisitor(Database db) {
        if (_resolveSel == null) {
            _resolveSel = new ResolveSelectableVisitor(db);
        }
    }

    /**
     * Returns an {@link Iterator}over all my {@link BindVariable}s, in the proper
     * order. Default impl returns empty iterator.
     */
    protected Iterator getBindVariableIterator() {
        if (getBindVariableVisitor() == null) {
            buildBindVariables();
        }
        return getBindVariableVisitor().getBindVariableIterator();
    }

    protected Iterator getBindVariableIterator(Selectable sel) {
        if (getBindVariableVisitor() == null) {
            setBindVariableVisitor(new FindBindVariableVisitor());
            getBindVariableVisitor().visit(sel);
        }
        return _bvisitor.getBindVariableIterator();
    }

    protected final FindBindVariableVisitor getBindVariableVisitor() {
        return _bvisitor;
    }

    @SuppressWarnings("unchecked")
    protected List getColIdentifierList(Table table, TableIdentifier tid) throws AxionException {
        int size = table.getColumnCount();
        List colids = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            Column col = table.getColumn(i);
            colids.add(new ColumnIdentifier(tid, col.getName(), null, col.getDataType()));
        }
        return colids;
    }

    protected int getCommitSize(Database db) {
        if (_commitSize == -1) {
            int commitSize = 0;
            try {
                String size = (String) db.getGlobalVariable(Database.COMMIT_SIZE);
                commitSize = Integer.parseInt(size);
                commitSize = Math.abs(commitSize);
            } catch (Throwable e) {
            }
            _commitSize = commitSize;
        }
        return _commitSize;
    }

    protected RowIterator getRowIterator(Database db, TableIdentifier tid, Table table, Selectable whereNode, boolean readOnly, RowDecorator dec)
            throws AxionException {
        RowIterator rows = null;
        Set whereNodes = AxionQueryOptimizer.flatConditionTree(whereNode);
        Selectable searchNode = AxionQueryOptimizer.findColumnLiteralFunction(tid, table, whereNodes, true);

        // look for an index
        if (searchNode != null) {
            rows = table.getIndexedRows(searchNode, readOnly);
            if (rows != null) {
                whereNodes.remove(searchNode);
            }
        }

        if (rows == null) {
            rows = table.getRowIterator(readOnly);
        }

        if (!whereNodes.isEmpty()) {
            rows = new FilteringRowIterator(rows, dec, whereNode);
        }
        return rows;
    }

    protected final RowDecorator makeRowDecorator(Table table) {
        return table.makeRowDecorator();
    }

    protected void populateDefaultValues(Database db, Table table, TableIdentifier tableId, RowDecorator dec) throws AxionException {
        createResolveSelectableVisitor(db);
        Row row = dec.getRow();
        for (int i = 0, I = row.size(); i < I; i++) {
            Column col = table.getColumn(i);
            if (null == row.get(i) || col.isDerivedColumn()) {
                if (col.hasDefault()) {
                    Selectable sel = getCanonicalDefault(db, col.getDefault());
                    sel = _resolveSel.visit(sel, null, new TableIdentifier[] { tableId});
                    Object val = sel.evaluate(dec);

                    DataType type = col.isDerivedColumn() ? sel.getDataType() : col.getDataType();
                    val = attemptToConvertValue(val, type, null);
                    row.set(i, val);
                } else if (col.isIdentityColumn() && col.isGeneratedByDefault()) {
                    Sequence seq = table.getSequence();
                    row.set(i, seq.evaluate());
                }
            }
        }
    }

    protected boolean populateSequenceColumns(Database db, Table table, Row row) throws AxionException {
        boolean result = false;
        createResolveSelectableVisitor(db);
        for (int i = 0, size = row.size(); i < size; i++) {
            Column col = table.getColumn(i);
            if (col.isGeneratedAlways() && col.isIdentityColumn()) {
                Sequence seq = table.getSequence();
                row.set(i, seq.evaluate());
                result = true;
            }
        }
        return result;
    }

    protected void resolveGeneratedColumns(Table table, TableIdentifier tableId, List cols) throws AxionException {
        resolveGeneratedColumns(table, tableId, cols, false);
    }

    @SuppressWarnings("unchecked")
    protected void resolveGeneratedColumns(Table table, TableIdentifier tableId, List cols, boolean useDefaultValues) throws AxionException {
        boolean buildColList = cols.isEmpty();
        for (int i = 0, count = table.getColumnCount(); i < count; i++) {
            Column col = table.getColumn(i);
            ColumnIdentifier colid = new ColumnIdentifier(tableId, col.getName(), null, col.getDataType());
            if (col.isGeneratedAlways() || col.isDerivedColumn()) {
                if (cols.contains(colid) && !useDefaultValues) {
                    String msg = "Can't insert value to generated/derived column " + col.getName();
                    throw new AxionException(msg);
                }
            } else if (buildColList) {
                cols.add(colid);
            }
        }
    }

    protected Selectable resolveSelectable(Selectable sel, Database db, List selected, TableIdentifier[] tables) throws AxionException {
        createResolveSelectableVisitor(db);
        return _resolveSel.visit(sel, selected, tables);
    }

    protected Selectable resolveSelectable(Selectable sel, Database db, TableIdentifier[] tables) throws AxionException {
        createResolveSelectableVisitor(db);
        return _resolveSel.visit(sel, null, tables);
    }

    protected void resolveSelectableList(List list, Database db, TableIdentifier table) throws AxionException {
        resolveSelectableList(list, db, new TableIdentifier[] { table});
    }

    @SuppressWarnings("unchecked")
    protected void resolveSelectableList(List list, Database db, TableIdentifier[] tables) throws AxionException {
        createResolveSelectableVisitor(db);
        for (int i = 0, I = list.size(); i < I; i++) {
            list.set(i, _resolveSel.visit((Selectable) list.get(i), null, tables));
        }
    }

    protected final void setBindVariableVisitor(FindBindVariableVisitor visitor) {
        _bvisitor = visitor;
    }

    protected void setDeferAllConstraintIfRequired(Table table) {
        if (getBindVariables().size() > 0) {
            table.setDeferAllConstraints(true);
        }
    }

    /**
     * If sublasses return a number of rows effected, then upon execution, they should set
     * that number here so it can support {@link #execute}
     */
    protected final void setEffectedRowCount(int count) {
        _rowCount = count;
    }

    /**
     * If subclasses create a {@link org.axiondb.jdbc.AxionResultSet}upon execution, they
     * should set it here so that they can support {@link #execute}.
     * 
     * @see #getResultSet
     */
    protected final void setResultSet(ResultSet rset) {
        _rset = rset;
    }

    protected void updateGeneratedValues(Database db, Table table, TableIdentifier tableId, Row row) throws AxionException {
        RowDecorator dec = null;
        createResolveSelectableVisitor(db);
        for (int i = 0, size = row.size(); i < size; i++) {
            Column col = table.getColumn(i);
            if (col.isDerivedColumn()) {
                Selectable sel = getCanonicalDefault(db, col.getDefault());
                sel = _resolveSel.visit(sel, null, new TableIdentifier[] { tableId});
                if (dec == null) {
                    dec = makeRowDecorator(table);
                    dec.setRow(row);
                }
                Object val = sel.evaluate(dec);
                val = attemptToConvertValue(val, col.getDataType(), null);
                row.set(i, val);
            }
        }
    }

    private Selectable getCanonicalDefault(Database db, Selectable sel) {
        if (sel instanceof ColumnIdentifier) {
            ColumnIdentifier col = (ColumnIdentifier) sel;
            if (!(col.getTableName() != null && db.getSequence(col.getTableName()) != null)) {
                return new ColumnIdentifier(sel.getName());
            }
        } else if (sel instanceof ConcreteFunction || sel instanceof FunctionIdentifier) {
            Function fn = (Function) sel;
            FunctionIdentifier fnid = new FunctionIdentifier(fn.getName());
            for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
                fnid.addArgument(getCanonicalDefault(db, fn.getArgument(i)));
            }
            return fnid;
        }
        return sel;
    }

    private FindBindVariableVisitor _bvisitor;
    private int _commitSize = -1;
    private ResolveSelectableVisitor _resolveSel;
    private int _rowCount = -1;
    private ResultSet _rset;
}
