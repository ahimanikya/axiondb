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

import java.util.List;
import java.util.NoSuchElementException;

import org.axiondb.Row;
import org.axiondb.RowIterator;

/**
 * A {@link RowIterator}that for a given {@link java.util.List}.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class ListRowIterator extends BaseRowIterator {

    public ListRowIterator(List list) {
        _list = list;
    }

    public Row current() {
        if (!hasCurrent()) {
            throw new NoSuchElementException("No current row.");
        }
        return _current;
    }

    public int currentIndex() {
        return _currentIndex;
    }

    public boolean hasCurrent() {
        return (null != _current);
    }

    public boolean hasNext() {
        return nextIndex() < _list.size();
    }

    public boolean hasPrevious() {
        return nextIndex() > 0;
    }

    public Row last() {
        _nextIndex = _list.size();
        _nextId = _list.size();
        previous();

        _nextIndex++;
        _nextId++;
        return current();
    }

    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No next row");
        }
        _currentId = _nextId++;
        _current = (Row) (_list.get(_currentId));
        _currentIndex = _nextIndex;
        _nextIndex++;
        return _current;
    }

    public int nextIndex() {
        return _nextIndex;
    }

    public Row previous() {
        if (!hasPrevious()) {
            throw new NoSuchElementException("No previous row");
        }

        _currentId = (--_nextId);
        _current = (Row) (_list.get(_currentId));
        _nextIndex--;
        _currentIndex = _nextIndex;
        return _current;
    }

    public int previousIndex() {
        return _nextIndex - 1;
    }

    public void reset() {
        _current = null;
        _nextIndex = 0;
        _currentIndex = -1;
        _nextId = 0;
    }

    public int size() {
        return _list.size();
    }

    private Row _current = null;
    private int _currentId = -1;
    private int _currentIndex = -1;
    private List _list = null;
    private int _nextId = 0;
    private int _nextIndex = 0;
}


