/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.engine.rowiterators.ChangingIndexedRowIterator;
import org.axiondb.engine.rowiterators.IndexNestedLoopJoinedRowIterator;
import org.axiondb.engine.rowiterators.MutableIndexedRowIterator;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.functions.EqualFunction;

/**
 * A UNIQUE constraint, which is violated when my collection of {@link Selectable}s is
 * not unique within my table.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author James Strachan
 * @author Ahimanikya Satapathy
 */
public class UniqueConstraint extends BaseSelectableBasedConstraint {

    public UniqueConstraint(String name) {
        this(name, "UNIQUE");
    }

    public UniqueConstraint(String name, String type) {
        super(name, type);
    }

    public boolean evaluate(RowEvent event) throws AxionException {
        return evaluate(event, event.getTable().makeRowDecorator(), false);
    }

    public boolean evaluate(RowEvent event, RowDecorator dec) throws AxionException {
        return evaluate(event, dec, false);
    }
    
    @SuppressWarnings("unchecked")
    public boolean evaluate(RowEvent event, RowDecorator dec, boolean wasDeferred) throws AxionException {
        if (null == event.getNewRow()) {
            return true;
        }

        Table table = event.getTable();
        dec.setRow(event.getNewRow());

        List values = new ArrayList(getSelectableCount());
        for (int i = 0, c = getSelectableCount(); i < c; i++) {
            values.add(getSelectable(i).evaluate(dec));
        }

        int acceptableRowId = -1;
        if (null != event.getOldRow()) {
            acceptableRowId = event.getOldRow().getIdentifier();
        } else if (isDeferred() || wasDeferred) {
            acceptableRowId = event.getNewRow().getIdentifier();
        }

        RowIterator matching = table.getMatchingRows(getSelectableList(), values, true);
        while (matching.hasNext()) {
            Row row = matching.next();
            if (isDeferred() && matching.hasNext()) {
                return false;
            } else if (-1 == acceptableRowId) {
                // in this case, any matching row causes failure
                return false;
            } else if (row.getIdentifier() != acceptableRowId) {
                // otherwise, check that the matching row isn't the one we're updating
                return false;
            }
        }
        return true;
    }

    public boolean evaluate(RowIterator oldRows, RowIterator newRows, Table table) throws AxionException {
        if (null == newRows || newRows.isEmpty()) {
            return true;
        }

        Selectable sel = getSelectable(0);
        if (sel instanceof ColumnIdentifier) {
            boolean update = (oldRows != null);
            String key = sel.getName();
            int colPos = table.getColumnIndex(key);
            Index index = table.getIndexForColumn(table.getColumn(key));

            MutableIndexedRowIterator currentRows = new ChangingIndexedRowIterator(index, table, new EqualFunction());
            RowIterator matching = new IndexNestedLoopJoinedRowIterator(newRows, colPos, currentRows, colPos, false, false);

            int lastId = -1;
            matching.reset();
            while (matching.hasNext()) {
                JoinedRow joinRow = (JoinedRow) matching.next();
                if (!update && !checkOtherColumns(joinRow, table)) {
                    return false;
                } else {

                    Row oldRow = joinRow.getRow(0);
                    int acceptableRowId = oldRow.getIdentifier();

                    if (-1 == acceptableRowId) {
                        // in this case, any matching row causes failure
                        return false;
                    } else if (acceptableRowId == lastId) {
                        return false;
                    } else {
                        lastId = oldRow.getIdentifier();
                    }
                }
            }
        } else {
            newRows.reset();
            RowDecorator dec = table.makeRowDecorator();
            for (RowIterator iter = newRows; iter.hasNext();) {
                Row row = iter.next();
                RowEvent event = new RowInsertedEvent(table, null, row);
                if (!evaluate(event, dec, true)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkOtherColumns(JoinedRow joinRow, Table table) throws AxionException {
        for (int i = 1, I = getSelectableCount(); i < I; i++) {
            String key = getSelectable(i).getName();
            int colPos = table.getColumnIndex(key);
            Row oldRow = joinRow.getRow(0);
            Row newRow = joinRow.getRow(1);
            if (oldRow.get(colPos).equals(newRow.get(colPos))) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public void addFK(String name) {
        if (fkSet == null) {
            fkSet = new HashSet(4);
        }
        fkSet.add(name);
    }

    public Iterator getFKs() {
        if (fkSet == null) {
            return Collections.EMPTY_LIST.iterator();
        }
        return fkSet.iterator();
    }

    private Set fkSet;
    private static final long serialVersionUID = -54506312013696264L;
}

