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

import java.util.NoSuchElementException;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.util.ExceptionConverter;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public abstract class AbstractFilteringRowIterator extends BaseRowIterator {

    public AbstractFilteringRowIterator(RowIterator iterator) {
        _delegate = iterator;
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
        return _currentAvailable;
    }

    public boolean hasNext() {
        if (_nextAvailable) {
            return true;
        }

        try {
            return determineNextRow();
        } catch (AxionException e) {
            throw ExceptionConverter.convertToRuntimeException(e);
        }
    }

    public boolean hasPrevious() {
        if (_previousAvailable) {
            return true;
        }

        try {
            return determinePreviousRow();
        } catch (AxionException e) {
            throw ExceptionConverter.convertToRuntimeException(e);
        }
    }

    public Row next() throws AxionException {
        if (!_nextAvailable) {
            if (!determineNextRow()) {
                throw new NoSuchElementException();
            }
        }
        _currentIndex = _nextIndex;
        _nextIndex++;
        _currentRow = _nextRow;
        _currentAvailable = true;
        clearNextRow();
        return _currentRow;
    }

    public int nextIndex() {
        return _nextIndex;
    }

    public Row previous() throws AxionException {
        if (!_previousAvailable) {
            if (!determinePreviousRow()) {
                throw new NoSuchElementException();
            }
        }
        _nextIndex--;
        _currentIndex = _nextIndex;
        _currentRow = _previousRow;
        _currentAvailable = true;
        clearPreviousRow();
        return _currentRow;
    }

    public int previousIndex() {
        return (_nextIndex - 1);
    }

    public void remove() throws AxionException {
        if (!hasCurrent()) {
            throw new IllegalStateException("No current row.");
        }
        getDelegate().remove();
        _nextIndex--;
        _currentRow = null;
        _currentAvailable = false;
        _currentIndex = -1;
    }

    public void reset() throws AxionException {
        _delegate.reset();
        _previousRow = null;
        _previousAvailable = false;
        _nextRow = null;
        _nextAvailable = false;
        _nextIndex = 0;
        _currentRow = null;
        _currentAvailable = false;
        _currentIndex = -1;
    }

    public void set(Row row) throws AxionException {
        if (!hasCurrent()) {
            throw new IllegalStateException("No current row.");
        }
        getDelegate().set(row);
        _currentRow = row;
    }

    protected void clearNextRow() {
        _nextRow = null;
        _nextAvailable = false;
    }

    protected void clearPreviousRow() {
        _previousRow = null;
        _previousAvailable = false;
    }

    protected abstract boolean determineNextRow() throws AxionException;

    protected abstract boolean determinePreviousRow() throws AxionException;

    protected RowIterator getDelegate() {
        return _delegate;
    }

    protected boolean isNextAvailable() {
        return _nextAvailable;
    }

    protected boolean isPreviousAvailable() {
        return _previousAvailable;
    }

    protected void setNext(Row row) {
        _nextRow = row;
        _nextAvailable = true;
    }

    protected void setPrevious(Row row) {
        _previousRow = row;
        _previousAvailable = true;
    }

    private boolean _currentAvailable = false;
    private int _currentIndex = -1;
    private Row _currentRow = null;
    private RowIterator _delegate;
    private boolean _nextAvailable = false;
    private int _nextIndex = 0;
    private Row _nextRow = null;
    private boolean _previousAvailable = false;
    private Row _previousRow = null;
}


