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

package org.axiondb.engine.rowiterators;

import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowSource;
import org.axiondb.Table;
import org.axiondb.engine.rows.LazyRow;

/**
 * A {@link org.axiondb.RowIterator}that creates {@link LazyRow}s based upon a list of
 * {@link Row}identifiers.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class LazyRowRowIterator extends BaseRowIterator {

    public LazyRowRowIterator(RowSource source, IntListIterator rowIdIter, int size) {
        this(source, rowIdIter, -1, null, size);
    }

    public LazyRowRowIterator(RowSource source, IntListIterator rowIdIter, int knownColumn, ListIterator valueIter, int size) {
        _source = source;
        _rowIdIter = rowIdIter;
        _knownColumnIndex = knownColumn;
        _valueIter = valueIter;
        _size = size;
    }

    public void add(Row row) {
        throw new UnsupportedOperationException("Not supported");
    }

    public Row current() {
        if (hasCurrent()) {
            return _currentRow;
        }
        throw new NoSuchElementException("No current row has been set.");
    }

    public int currentIndex() {
        return _currentIndex;
    }

    public boolean hasCurrent() {
        return (null != _currentRow);
    }

    public boolean hasNext() {
        return _rowIdIter.hasNext();
    }

    public boolean hasPrevious() {
        return _rowIdIter.hasPrevious();
    }

    public Row last() throws AxionException {
        reset();
        next(size());
        return peekPrevious();
    }

    public Row next() {
        _currentIndex = _rowIdIter.nextIndex();
        setCurrentRow(_rowIdIter.next(), (null == _valueIter ? null : _valueIter.next()));
        return _currentRow;
    }

    public int next(int count) throws AxionException {
        int lastId = -1;
        for (int i = 0; i < count; i++) {
            _currentIndex = _rowIdIter.nextIndex();
            lastId = _rowIdIter.next();
            if (_valueIter != null) {
                _valueIter.next();
            }
        }
        return lastId;
    }

    public int nextIndex() {
        return _rowIdIter.nextIndex();
    }

    public Row previous() {
        setCurrentRow(_rowIdIter.previous(), (null == _valueIter ? null : _valueIter.previous()));
        _currentIndex = _rowIdIter.nextIndex();
        return _currentRow;
    }

    public int previous(int count) throws AxionException {
        int lastId = -1;
        for (int i = 0; i < count; i++) {
            _currentIndex = _rowIdIter.previousIndex();
            lastId = _rowIdIter.previous();
            if (_valueIter != null) {
                _valueIter.previous();
            }
        }
        return lastId;
    }

    public int previousIndex() {
        return _rowIdIter.previousIndex();
    }

    public void remove() throws AxionException {
        if (!hasCurrent()) {
            throw new IllegalStateException("No current row.");
        }

        if (_source instanceof Table) {
            ((Table) _source).deleteRow(current());
            _rowIdIter.remove();
            if (_valueIter != null) {
                _valueIter.remove();
            }
            _currentRow = null;
            _currentIndex = -1;
            _size--;
        } else {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    public void reset() {
        while (_rowIdIter.hasPrevious()) {
            _rowIdIter.previous();
            if (null != _valueIter) {
                _valueIter.previous();
            }
        }
        _currentRow = null;
        _currentIndex = -1;
    }

    public void set(Row row) throws AxionException {
        if (!hasCurrent()) {
            throw new IllegalStateException("No current row.");
        }

        if (_source instanceof Table) {
            row.setIdentifier(current().getIdentifier());
            ((Table) _source).updateRow(current(), row);
            _currentRow = new LazyRow(_source, current().getIdentifier());
        } else {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    public int size() throws AxionException {
        return _size;
    }

    public String toString() {
        return "LazyRow(source=" + _source + ")";
    }

    private final void setCurrentRow(int rowid, Object value) {
        _currentRow = new LazyRow(_source, rowid, _knownColumnIndex, value);
    }

    private int _currentIndex = -1;
    private Row _currentRow = null;
    private int _knownColumnIndex = -1;
    private IntListIterator _rowIdIter = null;
    private int _size = -1;

    private RowSource _source = null;
    private ListIterator _valueIter = null;
}


