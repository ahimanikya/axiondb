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

import org.axiondb.AxionException;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;

/**
 * A {@link DelegatingRowIterator}that is wraps a {@link org.axiondb.RowIterator}from
 * some {@link Index}, and that can be {@link #reset reset}to recreate the iterator for
 * a new {@link BindVariable bound value}.
 * 
 * @version  
 * @author Amrish Lal
 * @author Ahimanikya Satapathy
 */
public class ChangingIndexedRowIterator implements MutableIndexedRowIterator {

    public ChangingIndexedRowIterator(Index index, Table table, Function fn) throws AxionException {
        _index = index;
        _table = table;
        _function = fn;
        reset();
    }

    public void add(Row row) throws AxionException {
        assertIndexSet();
        _delegate.add(row);
    }

    public Row current() {
        assertIndexSet();
        return _delegate.current();
    }

    public int currentIndex() {
        assertIndexSet();
        return _delegate.currentIndex();
    }

    public Row first() throws AxionException {
        assertIndexSet();
        return _delegate.first();
    }

    public boolean hasCurrent() {
        if (indexSet()) {
            return _delegate.hasCurrent();
        }
        return false;
    }

    public boolean hasNext() {
        if (indexSet()) {
            return _delegate.hasNext();
        }
        return false;
    }

    public boolean hasPrevious() {
        if (indexSet()) {
            return _delegate.hasPrevious();
        }
        return false;
    }

    public final boolean indexSet() {
        return (_delegate != null);
    }

    public boolean isEmpty() {
        return (!hasNext() && !hasPrevious() && !hasCurrent());
    }

    public Row last() throws AxionException {
        assertIndexSet();
        return _delegate.last();
    }

    public Row next() throws AxionException {
        assertIndexSet();
        return _delegate.next();
    }

    public int next(int count) throws AxionException {
        assertIndexSet();
        return _delegate.next(count);
    }

    public int nextIndex() {
        assertIndexSet();
        return _delegate.nextIndex();
    }

    public Row peekNext() throws AxionException {
        assertIndexSet();
        return _delegate.peekNext();
    }

    public Row peekPrevious() throws AxionException {
        assertIndexSet();
        return _delegate.peekPrevious();
    }

    public Row previous() throws AxionException {
        assertIndexSet();
        return _delegate.previous();
    }

    public int previous(int count) throws AxionException {
        assertIndexSet();
        return _delegate.previous(count);
    }

    public int previousIndex() {
        assertIndexSet();
        return _delegate.previousIndex();
    }

    public void remove() throws AxionException {
        assertIndexSet();
        _delegate.remove();
    }

    public void removeIndexKey() throws AxionException {
        _delegate = null;
    }

    public void reset() throws AxionException {
        if (indexSet()) {
            _delegate.reset();
        }
    }

    public void set(Row row) throws AxionException {
        assertIndexSet();
        _delegate.set(row);
    }

    public void setIndexKey(Object value) throws AxionException {
        _delegate = _index.getRowIterator(_table, _function, value);
        reset();
    }

    public int size() throws AxionException {
        assertIndexSet();
        return _delegate.size();
    }

    public String toString() {
        return "ChangingIndexed(index=" + _index.getName() + ";table=" + _table.getName() + ";condition=" + _function + ")";
    }

    private void assertIndexSet() {
        if (!indexSet()) {
            throw new IllegalStateException("Index not set yet");
        }
    }

    private RowIterator _delegate;
    private Function _function;
    private Index _index;
    private Table _table;
}
