/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

import java.util.NoSuchElementException;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowIterator;

/**
 * Reverse a SortedRowIterator.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class ReverseSortedRowIterator extends BaseRowIterator {

    public ReverseSortedRowIterator(RowIterator sortedIterator) throws AxionException {
        _delegate = sortedIterator;
        reset();
    }

    /** Not supported in the base implementation. */
    public void add(Row row) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public Row current() {
        if(!hasCurrent()) {
            throw new NoSuchElementException("No Current Row");
        }
        return getDelegate().current();
    }

    public int currentIndex() {
        return _currentIndex;
    }

    public Row first() throws AxionException {
        reset();
        return peekNext();
    }

    public boolean hasCurrent() {
        if(_currentIndex == -1) {
            return false;
        }
        return getDelegate().hasCurrent();
    }

    public boolean hasNext() {
        return getDelegate().hasPrevious();
    }

    public boolean hasPrevious() {
        return getDelegate().hasNext();
    }

    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    public Row last() throws AxionException {
        if (!hasNext()) {
            previous();
        }
        Row row = null;
        while (hasNext()) {
            row = next();
        }
        return row;
    }

    public Row next() throws AxionException {
        _currentIndex = _nextIndex;
        _nextIndex++;
        return getDelegate().previous();
    }

    public int nextIndex() {
        return _nextIndex;
    }

    public Row peekNext() throws AxionException {
        next();
        return previous();
    }

    public Row peekPrevious() throws AxionException {
        previous();
        return next();
    }

    public Row previous() throws AxionException {
        _nextIndex--;
        _currentIndex = _nextIndex;
        return getDelegate().next();
    }

    public int previousIndex() {
        return _nextIndex - 1;
    }

    public void remove() throws AxionException {
        getDelegate().remove();
        _currentIndex = -1;
        _nextIndex--;
    }

    public void reset() throws AxionException {
        getDelegate().reset();
        _currentIndex = -1;
        _nextIndex = 0;
        while (getDelegate().hasNext()) {
            getDelegate().next();
        }
    }

    public void set(Row row) throws AxionException {
        getDelegate().set(row);
    }

    public String toString() {
        return "ReverseSorted(" + getDelegate() + ")";
    }
    
    private RowIterator getDelegate() {
        return _delegate;
    }
    
    private int _currentIndex = -1;
    private int _nextIndex = 0;
    private RowIterator _delegate = null;

}
