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

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowComparator;
import org.axiondb.RowIterator;

/**
 * Collates the results of two or more sorted {@link RowIterator}s according to the given
 * {@link RowComparator}. It is assumed that each iterator is already ordered (ascending)
 * according to the given {@link RowComparator}.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class CollatingRowIterator extends BaseRowIterator {

    public CollatingRowIterator(RowComparator comparator) {
        _comparator = comparator;
        _iterators = new ArrayList(3);
    }

    public void addRowIterator(RowIterator iter) throws IllegalStateException {
        assertNotStarted();
        _iterators.add(iter);
    }

    public Row current() {
        if (!hasCurrent()) {
            throw new NoSuchElementException("No current row");
        }
        return _currentRow;
    }

    public int currentIndex() {
        return _currentIndex;
    }

    public boolean hasCurrent() {
        return _hasCurrent;
    }

    public boolean hasNext() {
        start();
        return anyNextAvailable() || anyIteratorHasNext();
    }

    public boolean hasPrevious() {
        start();
        return anyPreviousAvailable() || anyIteratorHasPrevious();
    }
    
    public Row last() throws AxionException {
        if(_started) {
            clearPreviousPeeked();
            clearNextPeeked();
        }
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            ((RowIterator) (_iterators.get(i))).last();
        }
        _nextIndex = size();
        _currentIndex = _nextIndex - 1;
        _hasCurrent = true;
        return _currentRow = peekPrevious();
    }

    public Row next() throws AxionException {
        if (!hasNext()) {
            throw new NoSuchElementException("No next row");
        }
        _currentIndex = _nextIndex;
        _nextIndex++;
        _hasCurrent = true;
        return _currentRow = peekNextRow();
    }

    public int nextIndex() {
        return _nextIndex;
    }

    public Row previous() throws AxionException {
        if (!hasPrevious()) {
            throw new NoSuchElementException("No previous row");
        }
        _currentIndex = _nextIndex - 1;
        _nextIndex--;
        _hasCurrent = true;
        return _currentRow = peekPreviousRow();
    }

    public int previousIndex() {
        return _nextIndex - 1;
    }

    public void remove() throws AxionException {
        if (!hasCurrent()) {
            throw new NoSuchElementException("No current row");
        }
        RowIterator iter = getLastReturnedFrom();
        iter.remove();
        _currentRow = null;
        _currentIndex = -1;
        _hasCurrent = false;
    }

    public void reset() throws AxionException {
        start();
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            clearNextPeeked(i);
            clearPreviousPeeked(i);
            ((RowIterator) _iterators.get(i)).reset();
        }
        _currentRow = null;
        _currentIndex = -1;
        _hasCurrent = false;
        _nextIndex = 0;
        _lastReturnedFrom = -1;
    }

    public void set(Row row) throws AxionException {
        if (!hasCurrent()) {
            throw new NoSuchElementException("No current row");
        }

        RowIterator iter = getLastReturnedFrom();
        iter.set(row);
        _currentRow = row;
    }

    public String toString() {
        return "Collating(" + _iterators + ")";
    }
    
    public int size() throws AxionException {
        int size = 0;
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            size += ((RowIterator) (_iterators.get(i))).size();
        }
        return size;
    }

    private boolean anyIteratorHasNext() {
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            RowIterator it = (RowIterator) _iterators.get(i);
            if (it.hasNext()) {
                return true;
            }
        }
        return false;
    }

    private boolean anyIteratorHasPrevious() {
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            RowIterator it = (RowIterator) _iterators.get(i);
            if (it.hasPrevious()) {
                return true;
            }
        }
        return false;
    }

    private boolean anyNextAvailable() {
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            if (isValueSet(_nextValues[i])) {
                return true;
            }
        }
        return false;
    }

    private boolean anyPreviousAvailable() {
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            if (isValueSet(_previousValues[i])) {
                return true;
            }
        }
        return false;
    }

    private void assertNotStarted() throws IllegalStateException {
        if (_started) {
            throw new IllegalStateException("Already started");
        }
    }

    private void clearNextPeeked() throws AxionException {
        for (int i = 0, m = _iterators.size(); i < m; i++) {
            if (isValueSet(_nextValues[i])) {
                ((RowIterator) _iterators.get(i)).previous();
                clearNextPeeked(i);
            }
        }
        _nextAvailable = false;
    }

    private void clearNextPeeked(int i) {
        _nextValues[i] = null;
    }

    private void clearPreviousPeeked() throws AxionException {
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            if (isValueSet(_previousValues[i])) {
                ((RowIterator) _iterators.get(i)).next();
                clearPreviousPeeked(i);
            }
        }
        _previousAvailable = false;
    }

    private void clearPreviousPeeked(int i) {
        _previousValues[i] = null;
    }

    private RowIterator getLastReturnedFrom() throws IllegalStateException {
        return (RowIterator) (_iterators.get(_lastReturnedFrom));
    }

    private boolean isValueSet(Object val) {
        return (val != null);
    }

    private Row peekNextRow() throws AxionException {
        _nextAvailable = true;
        if (_previousAvailable) {
            clearPreviousPeeked();
        }

        int nextIndex = -1;
        Row nextValue = null;
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            if (_nextValues[i] == null) {
                RowIterator iter = (RowIterator) (_iterators.get(i));
                if (iter.hasNext()) {
                    // peek ahead to the next value
                    _nextValues[i] = iter.next();
                } else {
                    continue;
                }
            }
            if (-1 == nextIndex) {
                nextIndex = i;
                nextValue = _nextValues[i];
            } else if (_comparator.compare(nextValue, _nextValues[i]) > 0) {
                nextIndex = i;
                nextValue = _nextValues[i];
            }
        }

        _lastReturnedFrom = nextIndex;
        clearNextPeeked(nextIndex);
        return nextValue;
    }

    private Row peekPreviousRow() throws AxionException {
        _previousAvailable = true;
        if (_nextAvailable) {
            clearNextPeeked();
        }

        int previousIndex = -1;
        Row previousValue = null;
        for (int i = 0, I = _iterators.size(); i < I; i++) {
            if (_previousValues[i] == null) {
                RowIterator iter = (RowIterator) (_iterators.get(i));
                if (iter.hasPrevious()) {
                    // peek ahead to the previous value
                    _previousValues[i] = iter.previous();
                } else {
                    continue;
                }
            }
            if (-1 == previousIndex) {
                previousIndex = i;
                previousValue = _previousValues[i];
            } else if (_comparator.compare(previousValue, _previousValues[i]) <= 0) {
                previousIndex = i;
                previousValue = _previousValues[i];
            }
        }

        _lastReturnedFrom = previousIndex;
        clearPreviousPeeked(previousIndex);
        return previousValue;
    }

    /**
     * Initializes the collating state if it hasn't been already.
     */
    private void start() {
        if (!_started) {
            int isize = _iterators.size();
            _nextValues = new Row[isize];
            _previousValues = new Row[isize];
            _started = true;
        }
    }

    /** My {@link RowComparator}to use for collating. */
    private RowComparator _comparator;

    /** The index of {@link #_currentRow}within my iteration. */
    private int _currentIndex = -1;
    /** The last {@link Row}returned by {@link #next}or {@link #previous}. */
    private Row _currentRow;
    /** Whether or not {@link #_currentRow}has been set. */
    private boolean _hasCurrent = false;

    /** The list of {@link RowIterator}s to collate over. */
    private ArrayList _iterators;
    /** The {@link #_iterators iterator}I last returned from. */
    private int _lastReturnedFrom = -1;

    /** next {@link Row}value picked up and not used yet */
    private boolean _nextAvailable = false;
    /** The next index within my iteration. */
    private int _nextIndex = 0;
    /** next {@link Row}values peeked from my {@link #_iterators}. */
    private Row[] _nextValues;

    /** previous {@link Row}value picked up and not used yet */
    private boolean _previousAvailable = false;
    /** previous {@link Row}values peeked from my {@link #_iterators}. */
    private Row[] _previousValues;

    private boolean _started = false;
}
