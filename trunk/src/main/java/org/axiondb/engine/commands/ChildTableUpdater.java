/*
 * 
 * =======================================================================
 * Copyright (c) 2005-2006 Axion Development Team.  All rights reserved.
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

import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.EqualFunction;

/**
 * Update/Delete Child rows for a given relation.
 * 
 * @author Ahimanikya Satapathy
 */
public abstract class ChildTableUpdater extends BaseAxionCommand {

    protected void deleteOrSetNullChildRows(Database db, Table parentTable, RowDecorator dec) throws AxionException {
        for (Iterator iter = parentTable.getConstraints(); iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = (ForeignKeyConstraint) constraint;
                deleteOrSetNullChildRows(db, parentTable, dec, fk);
            }
        }
    }

    protected void deleteOrSetNullChildRows(Database db, Table parentTable, RowDecorator dec, ForeignKeyConstraint fk) throws AxionException {
        // Check whether Child table has a row that is refering row to be deleted
        // If ON DELETE option is used we can either delete or set null the child row
        Selectable filter = null;
        if (parentTable.getName().equals(fk.getParentTableName())) {
            List parentCols = fk.getParentTableColumns();
            List childCols = fk.getChildTableColumns();
            Table childTable = db.getTable(fk.getChildTableName());
            
            filter = buildFilter(dec, parentCols);
            RowIterator matching = getRowIterator(db, new TableIdentifier(fk.getChildTableName()), childTable, filter, false, dec);
            if (matching.hasNext()) {
                if (fk.getOnDeleteActionType() == ForeignKeyConstraint.CASCADE) {
                    deleteMatchingChildRows(matching);
                } else if (fk.getOnDeleteActionType() == ForeignKeyConstraint.SETNULL) {
                    setNullForMatchingChildRows(childCols, childTable, matching);
                }  else if (fk.getOnDeleteActionType() == ForeignKeyConstraint.SETDEFAULT) {
                    setDefaultForMatchingChildRows(childCols, childTable, matching);
                }
            }
        }
    }

    protected void updateOrSetNullChildRows(Database db, Table parentTable, Row parentOldRow, Row parentNewRow) throws AxionException {
        for (Iterator iter = parentTable.getConstraints(); iter.hasNext();) {
            Object constraint = iter.next();
            if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = (ForeignKeyConstraint) constraint;
                updateOrSetNullChildRows(db, parentTable, parentOldRow, parentNewRow, fk);
            }
        }
    }

    protected void updateOrSetNullChildRows(Database db, Table parentTable, Row parentOldRow, Row parentNewRow, ForeignKeyConstraint fk) throws AxionException {
        // Check whether Child table has a row that is refering row to be deleted
        // If ON DELETE option is used we can either delete or set null the child row
        Selectable filter = null;
        if (parentTable.getName().equals(fk.getParentTableName())) {
            RowDecorator dec = makeRowDecorator(parentTable);
            dec.setRow(parentOldRow);
            
            List parentCols = fk.getParentTableColumns();
            List childCols = fk.getChildTableColumns();
            Table childTable = db.getTable(fk.getChildTableName());
            
            filter = buildFilter(dec, parentCols);
            RowIterator matching = getRowIterator(db, new TableIdentifier(fk.getChildTableName()), childTable, filter, false, dec);
            if (matching.hasNext()) {
                if (fk.getOnDeleteActionType() == ForeignKeyConstraint.CASCADE) {
                    updateMatchingChildRows(parentTable, parentNewRow, parentCols, childCols, childTable, matching);
                } else if (fk.getOnDeleteActionType() == ForeignKeyConstraint.SETNULL) {
                    setNullForMatchingChildRows(childCols, childTable, matching);
                }  else if (fk.getOnDeleteActionType() == ForeignKeyConstraint.SETDEFAULT) {
                    setDefaultForMatchingChildRows(childCols, childTable, matching);
                }
            }
        }
    }

    private Selectable buildFilter(RowDecorator dec, List columns) throws AxionException {
        Selectable filter = null;
        for (int i = 0, I = columns.size(); i < I; i++) {
            Selectable sel = (Selectable) columns.get(i);
            Object val = sel.evaluate(dec);

            EqualFunction function = new EqualFunction();
            function.addArgument(sel);
            function.addArgument(new Literal(val));

            if (null == filter) {
                filter = function;
            } else {
                AndFunction fn = new AndFunction();
                fn.addArgument(filter);
                fn.addArgument(function);
                filter = fn;
            }
        }
        return filter;
    }
    
    private void deleteMatchingChildRows(RowIterator matching) throws AxionException {
        do {
            matching.next();
            matching.remove();
        } while (matching.hasNext());
    }

    private void setDefaultForMatchingChildRows(List childCols, Table childTable, RowIterator matching) throws AxionException {
        do {
            Row childOldrow = matching.next();
            Row newrow = new SimpleRow(childOldrow);
            for (int i = 0, I = childCols.size(); i < I; i++) {
                ColumnIdentifier colid = ((ColumnIdentifier) childCols.get(i));
                int colndx = childTable.getColumnIndex(colid.getName());
                newrow.set(colndx, childTable.getColumn(colid.getName()).getDefault());
            }
            childTable.updateRow(childOldrow, newrow);
        } while (matching.hasNext());
    }

    private void setNullForMatchingChildRows(List childCols, Table childTable, RowIterator matching) throws AxionException {
        do {
            Row childOldrow = matching.next();
            Row newrow = new SimpleRow(childOldrow);
            for (int i = 0, I = childCols.size(); i < I; i++) {
                ColumnIdentifier colid = ((ColumnIdentifier) childCols.get(i));
                int colndx = childTable.getColumnIndex(colid.getName());
                newrow.set(colndx, null);
            }
            childTable.updateRow(childOldrow, newrow);
        } while (matching.hasNext());
    }

    private void updateMatchingChildRows(Table parentTable, Row parentNewRow, List parentCols, List childCols, Table childTable, RowIterator matching) throws AxionException {
        do {
            Row childOldrow = matching.next();
            Row newrow = new SimpleRow(childOldrow);
            for (int i = 0, I = childCols.size(); i < I; i++) {
                ColumnIdentifier colid = ((ColumnIdentifier) childCols.get(i));
                ColumnIdentifier pcolid = ((ColumnIdentifier) parentCols.get(i));
                int colndx = childTable.getColumnIndex(colid.getName());
                int parentColndx = parentTable.getColumnIndex(pcolid.getName());
                newrow.set(colndx, parentNewRow.get(parentColndx));
            }
            childTable.updateRow(childOldrow, newrow);
        } while (matching.hasNext());
    }

}

