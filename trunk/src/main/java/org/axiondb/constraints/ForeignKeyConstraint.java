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

package org.axiondb.constraints;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.engine.SnapshotIsolationTransaction;
import org.axiondb.engine.visitors.ResolveSelectableVisitor;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;

/**
 * A FOREIGN KEY constraint
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class ForeignKeyConstraint extends BaseConstraint {

    public ForeignKeyConstraint(String name) {
        this(name, "FOREIGN KEY");
        _childColumns = new ArrayList();
        _parentColumns = new ArrayList();
    }

    public ForeignKeyConstraint(String name, String type) {
        super(name, type);
    }

    public void addColumns(List list) {
        _childColumns = list;
    }

    public void addForeignColumns(List list) {
        _parentColumns = list;
    }

    public boolean evaluate(RowEvent event) throws AxionException {
        return evaluate(event, event.getTable().makeRowDecorator());
    }
    
    public boolean evaluate(RowEvent event, RowDecorator dec) throws AxionException {
        if (null != event.getNewRow()) {
            return handleInsertOrUpdate(event, dec);
        } else if (null != event.getOldRow()) {
            return handleDelete(event, dec);
        }
        return true; // otherwise
    }

    public List getChildTableColumns() {
        return _childColumns;
    }

    public String getChildTableName() {
        return _childTableName;
    }

    public int getOnDeleteActionType() {
        return _onDeleteActionType;
    }

    public int getOnUpdateActionType() {
        return _onUpdateActionType;
    }

    public List getParentTableColumns() {
        return _parentColumns;
    }

    public String getParentTableName() {
        return _parentTableName;
    }

    // NOTE: Parent table columns has to be PK for now, we can UniqueConstraint later
    @Override
    @SuppressWarnings("unchecked")
    public void resolve(Database db, TableIdentifier table) throws AxionException {
        if (!db.hasTable(getParentTableName())) {
            throw new AxionException("Parent Table not found");
        }

        // Resolve table names
        _childTableName = table.getTableName();
        _childTable = db.getTable(_childTableName);
        if(_childTable instanceof  TransactableTable){
            _childTable = ((TransactableTable)_childTable).getTable();
        }
        _parentTable = db.getTable(getParentTableName());
        resolveColumns();

        // Vlidate column size for both parent and child table
        if (_childColumns.isEmpty()) {
            throw new AxionException("Column reference not found...");
        } else if (_childColumns.size() != _parentColumns.size()) {
            throw new AxionException("parent-child columns don't match...");
        }

        // resolve child table columns
        TableIdentifier[] tables = toArray(table);
        ResolveSelectableVisitor resolveSel = new ResolveSelectableVisitor(db);
        for (int i = 0, I =_childColumns.size(); i < I; i++) {
            _childColumns.set(i, resolveSel.visit((Selectable) _childColumns.get(i), null, tables));
        }

        // Resolve paranet table columns
        tables = toArray(new TableIdentifier(getParentTableName()));
        for (int i = 0, I =_childColumns.size(); i < I; i++) {
            _parentColumns.set(i, resolveSel.visit((Selectable) _parentColumns.get(i), null, tables));
        }
    }

    public void setChildTable(Table table) {
        _childTable = table;
    }

    public void setChildTableName(String tableName) {
        _childTableName = tableName;
    }

    public void setOnDeleteActionType(int actionType) {
        _onDeleteActionType = actionType;
    }

    public void setOnUpdateActionType(int actionType) {
        _onUpdateActionType = actionType;
    }

    public void setParentTable(Table table) {
        _parentTable = table;
    }

    public void setParentTableName(String tableName) {
        _parentTableName = tableName;
    }

    @SuppressWarnings("unchecked")
    private boolean handleDelete(RowEvent event, RowDecorator dec) throws AxionException {
        Table table = event.getTable();
        dec.setRow(event.getOldRow());

        // Check whether Child table has a row that is refering row to be deleted
        // If ON DELETE option is used we can either delete or set null the child row
        if (table.getName().equals(getParentTableName())) {
            List values = new ArrayList(_parentColumns.size());
            for (int i = 0, I = _parentColumns.size(); i < I; i++) {
                values.add(((Selectable) _parentColumns.get(i)).evaluate(dec));
            }

            Table childTable = _childTable;
            if (table instanceof TransactableTable) {
                Iterator iter = ((TransactableTable) table).getTableModificationListeners();
                while (iter.hasNext()) {
                    Object db = iter.next();
                    if (db instanceof SnapshotIsolationTransaction) {
                        childTable = ((SnapshotIsolationTransaction) db).getTable(getChildTableName());
                        break;
                    }
                }
            }

            RowIterator matching = childTable.getMatchingRows(_childColumns, values, true);
            if (matching.hasNext()) {
                if (_onDeleteActionType == CASCADE || _onDeleteActionType == SETNULL || _onDeleteActionType == SETDEFAULT) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean handleInsertOrUpdate(RowEvent event, RowDecorator dec) throws AxionException {
        Table table = event.getTable();
        dec.setRow(event.getNewRow());

        // Check whether Parent table has a row that matched the FK value
        // FKs are valid if one or more of the columns are NULL
        if (table.getName().equals(getChildTableName())) {
            List values = new ArrayList(_childColumns.size());
            if(_childTable == null && !_childColumns.isEmpty()){
                _childTable = table;
            }
            for (int i = 0, I = _childColumns.size(); i < I; i++) {
                ColumnIdentifier colid = (ColumnIdentifier) _childColumns.get(i);
                Object val = colid.evaluate(dec);
                if (val == null || val.equals(_childTable.getColumn(colid.getName()).getDefault())) {
                    return true;
                }
                values.add(val);
            }

            Table parentTable = _parentTable;
            if (table instanceof TransactableTable) {
                Iterator iter = ((TransactableTable) table).getTableModificationListeners();
                while (iter.hasNext()) {
                    Object db = iter.next();
                    if (db instanceof SnapshotIsolationTransaction) {
                        parentTable = ((SnapshotIsolationTransaction) db).getTable(getParentTableName());
                        break;
                    }
                }
            }

            RowIterator matching = parentTable.getMatchingRows(_parentColumns, values, true);
            if (matching.hasNext()) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }
    
    // TODO: We can (for sure) do better than this, I am too lazy now ;) -- Ahi
    public boolean evaluate(RowIterator oldRows, RowIterator newRows, Table table) throws AxionException {
        if (null == newRows || newRows.isEmpty()) {
            return true;
        }
        
        RowDecorator dec = table.makeRowDecorator();
        Row oldRow = null;
        newRows.reset();
        if(oldRows != null) {
            oldRows.reset();
        }
        while (newRows.hasNext()) {
            oldRow = (null != oldRows && oldRows.hasNext() ? oldRows.next() : null); 
            RowEvent event = new RowInsertedEvent(table, oldRow, newRows.next());
            if(!evaluate(event, dec)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchColumns(List tableCols, List pkCols) {
        if (tableCols.size() != pkCols.size()) {
            return false;
        }

        for (int i = 0, I = pkCols.size(); i < I;  i++) {
            ColumnIdentifier colid = (ColumnIdentifier) tableCols.get(i);
            ColumnIdentifier pkcolid = (ColumnIdentifier) pkCols.get(i);
            if (!colid.getName().equals(pkcolid.getName())) {
                return false;
            }
        }

        return true;
    }

    private void resolveColumns() throws AxionException {
        boolean foundKey = false;
        UniqueConstraint uc = null;

        // If primary key exist in parent table, choose that column as refernce column,
        // when reference columns are not specified.
        for (Iterator iter = _parentTable.getConstraints(); !foundKey && iter != null && iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof PrimaryKeyConstraint) {
                uc = (UniqueConstraint)constraint;
                foundKey = resolveColumn(uc);
            }
        }
        
        // otherwise try any Unique Constraint for a match
        for (Iterator iter = _parentTable.getConstraints(); !foundKey && iter != null && iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof UniqueConstraint) {
                uc = (UniqueConstraint)constraint;
                foundKey = resolveColumn(uc);
            }
        }

        if (!foundKey) {
            throw new AxionException("Primary/Unique Key Constraint not found for the given keys in parent table");
        } else {
            uc.addFK(getName());
        }
    }

    @SuppressWarnings("unchecked")
    private boolean resolveColumn(UniqueConstraint uc) {
        boolean foundKey = false;
        if (_parentColumns.isEmpty() && !_childColumns.isEmpty() && _childColumns.size() == uc.getSelectableCount()) {
            for (int i = 0, I = _childColumns.size(); i < I; i++) {
                _parentColumns.add(new ColumnIdentifier(((ColumnIdentifier) uc.getSelectable(i)).getName()));
            }
            foundKey = true;
        } else if (_childColumns.isEmpty() && !_parentColumns.isEmpty() && _parentColumns.size() == uc.getSelectableCount()) {
            for (int i = 0, I = _parentColumns.size(); i < I; i++) {
                _childColumns.add(new ColumnIdentifier(((ColumnIdentifier) uc.getSelectable(i)).getName()));
            }
            foundKey = true;
        } else if (_childColumns.isEmpty() && _parentColumns.isEmpty()) {
            for (int i = 0, I = uc.getSelectableCount(); i < I; i++) {
                _parentColumns.add(new ColumnIdentifier(((ColumnIdentifier) uc.getSelectable(i)).getName()));
                _childColumns.add(new ColumnIdentifier(((ColumnIdentifier) uc.getSelectable(i)).getName()));
            }
            foundKey = true;
        } else if (!_childColumns.isEmpty() && !_parentColumns.isEmpty() && _childColumns.size() == _parentColumns.size()
            && matchColumns(_parentColumns, uc.getSelectableList())) {
            foundKey = true;
        } 
        return foundKey;
    }

    public static final int CASCADE = 10;
    public static final int RESTRICT = 40;
    public static final int SETDEFAULT = 30;
    public static final int SETNULL = 20;

    private static final long serialVersionUID = -54506312013696264L;

    private List _childColumns;
    private transient Table _childTable;
    private String _childTableName;
    private int _onDeleteActionType = RESTRICT;
    private int _onUpdateActionType = RESTRICT;
    private List _parentColumns;
    private transient Table _parentTable;
    private String _parentTableName;
}

