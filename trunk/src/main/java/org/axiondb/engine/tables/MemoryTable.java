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

package org.axiondb.engine.tables;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.AxionException;
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.event.RowInsertedEvent;

/**
 * A memory-resident {@link Table}.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class MemoryTable extends BaseTable {

    public MemoryTable(String name) {
        super(name);
    }

    public MemoryTable(String name, String type) {
        super(name);
        setType(type);
    }

    public void applyDeletes(IntCollection rowids) throws AxionException {
        applyDeletesToIndices(rowids);
        applyDeletesToRows(rowids);
    }

    public void applyInserts(RowCollection rows) throws AxionException {
        applyInsertsToIndices(rows);
        applyInsertsToRows(rows.rowIterator());
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        applyUpdatesToIndices(rows);
        applyUpdatesToRows(rows.rowIterator());
    }

    public final void freeRowId(int id) {
        if (_freeIdPos >= 0 && id == _freeIds.get(_freeIdPos)) {
            _freeIdPos--;
        } else if (_nextFreeId > _rows.size() - 1) {
            _nextFreeId--;
        }
    }

    public final int getNextRowId() {
        if (_freeIds.isEmpty() || _freeIdPos >= _freeIds.size() - 1) {
            return _nextFreeId = (_nextFreeId == -1 ? _rows.size() : _nextFreeId + 1);
        } else {
            return _freeIds.get(++_freeIdPos);
        }
    }

    public final Row getRow(int id) {
        if (id > _rows.size() - 1 || id < 0) {
            return null;
        }
        return (Row) _rows.get(id);
    }

    public final int getRowCount() {
        return _rowCount;
    }

    public void populateIndex(Index index) throws AxionException {
        for (int i = 0, I = _rows.size(); i < I; i++) {
            Row row = (Row) (_rows.get(i));
            if (row != null) {
                index.rowInserted(new RowInsertedEvent(this, null, row));
            }
        }
    }

    public void truncate() throws AxionException {
        _rows.clear();
        _freeIds.clear();
        _rowCount = 0;
        truncateIndices();
    }

    protected RowIterator getRowIterator() throws AxionException {
        return new BaseRowIterator() {
            Row _current = null;
            int _currentId = -1;
            int _currentIndex = -1;
            int _nextId = 0;
            int _nextIndex = 0;

            public Row current() {
                if (!hasCurrent()) {
                    throw new NoSuchElementException("No current row.");
                }
                return _current;
            }

            public final int currentIndex() {
                return _currentIndex;
            }

            public final boolean hasCurrent() {
                return (null != _current);
            }

            public final boolean hasNext() {
                return nextIndex() < getRowCount();
            }

            public final boolean hasPrevious() {
                return nextIndex() > 0;
            }

            public Row last() {
                if (isEmpty()) {
                    throw new IllegalStateException("No rows in table.");
                }
                _nextIndex = getRowCount();
                _nextId = _rows.size();
                previous();

                _nextIndex++;
                _nextId++;
                return current();
            }

            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next row");
                }

                do {
                    _currentId = _nextId++;
                    _current = getRow(_currentId);
                } while (null == _current);
                _currentIndex = _nextIndex;
                _nextIndex++;
                return _current;
            }

            public final int nextIndex() {
                return _nextIndex;
            }

            public Row previous() {
                if (!hasPrevious()) {
                    throw new NoSuchElementException("No previous row");
                }
                do {
                    _currentId = (--_nextId);
                    _current = getRow(_currentId);
                } while (null == _current);
                _nextIndex--;
                _currentIndex = _nextIndex;
                return _current;
            }

            public final int previousIndex() {
                return _nextIndex - 1;
            }

            public void remove() throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                deleteRow(_current);
                _nextIndex--;
                _currentIndex = -1;
            }

            public void reset() {
                _current = null;
                _nextIndex = 0;
                _currentId = -1;
                _currentIndex = -1;
                _nextId = 0;
            }

            public final void set(Row row) throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                updateRow(_current, row);
            }

            public final int size() throws AxionException {
                return getRowCount();
            }

            public String toString() {
                return "MemoryTable(" + getName() + ")";
            }
        };
    }

    private void applyDeleteToRow(int rowid) throws AxionException {
        if (rowid > _rows.size() - 1) {
            throw new AxionException("Can't delete non-existent row");
        }
        //_freeIds.add(rowid);
        _rows.set(rowid, null);
        _rowCount--;
    }

    private void applyDeletesToRows(IntCollection rowids) throws AxionException {
        for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
            applyDeleteToRow(iter.next());
        }
        _freeIds.addAll(rowids);
    }

    private void applyInsertsToRows(RowIterator rows) throws AxionException {
        for (Row row; rows.hasNext();) {
            row = rows.next();
            if (!_freeIds.isEmpty() && _freeIdPos > -1) {
                _rows.set(_freeIds.removeElementAt(0), row);
                _freeIdPos--;
            } else {
                _rows.add(row);
            }
            _rowCount++;
        }
        _nextFreeId = -1;
    }

    private void applyUpdatesToRows(RowIterator rows) throws AxionException {
        for (Row row; rows.hasNext();) {
            row = rows.next();
            if (row.getIdentifier() > _rows.size() - 1) {
                throw new AxionException("Can't update non-existent row");
            }
            _rows.set(row.getIdentifier(), row);
        }
    }
    
    private int _freeIdPos = -1;
    private ArrayIntList _freeIds = new ArrayIntList();

    private int _nextFreeId = -1;
    private int _rowCount = 0;

    private ArrayList _rows = new ArrayList();
}


