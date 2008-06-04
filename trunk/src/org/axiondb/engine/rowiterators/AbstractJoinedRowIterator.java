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
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.engine.rows.JoinedRow;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.util.ExceptionConverter;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public abstract class AbstractJoinedRowIterator extends BaseRowIterator {

    public AbstractJoinedRowIterator() {
    }

    public Row current() throws NoSuchElementException {
        if (_currentSet) {
            return _current;
        } else {
            throw new NoSuchElementException();
        }
    }

    public int currentIndex() throws NoSuchElementException {
        return _currentIndex;
    }

    public boolean hasCurrent() {
        return _currentSet;
    }

    public boolean hasNext() {
        if (_nextSet) {
            return true;
        } else {
            try {
                return setNext();
            } catch (AxionException e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }
    }

    public boolean hasPrevious() {
        return nextIndex() > 0;
    }

    public Row next() throws NoSuchElementException, AxionException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            _current = _next;
            _currentSet = true;
            _currentIndex = _nextIndex;
            _nextIndex++;
            clearNext();
            return _current;
        }
    }

    public int nextIndex() {
        return _nextIndex;
    }

    public Row previous() throws NoSuchElementException, AxionException {
        if (!hasPrevious()) {
            throw new NoSuchElementException();
        } else {
            setPrevious();
            _current = _previous;
            _currentSet = true;
            _currentIndex = (_nextIndex - 1);
            _nextIndex--;
            clearPrevious();
            return _current;
        }
    }

    public int previousIndex() {
        return _nextIndex - 1;
    }

    public void reset() throws AxionException {
        _currentIterator = null;
        _left.reset();
        _nextIndex = 0;
        _nextSet = false;
        _previousSet = false;
        _currentSet = false;
        _currentIndex = -1;
        _next = null;
        _previous = null;
        _current = null;
    }

    public void setJoinCondition(Selectable joinCondition, RowDecorator decorator) {
        setJoinCondition(joinCondition);
        setRowDecorator(decorator);
    }

    public String toString() {
        String joinType = isSwapLeftAndRight() ? "right" : "left";
        joinType = isRightOuter() ? joinType + "-outer" : "inner";
        String condition = getJoinCondition() != null ? getJoinCondition().toString() : "";
        return "type=" + joinType + ";condition=" + condition;
    }

    protected abstract RowIterator generateRightRowIterator() throws AxionException;

    protected Selectable getJoinCondition() {
        return _joinCondition;
    }

    protected RowIterator getLeftRowIterator() {
        return _left;
    }

    protected int getRightSideColumnCount() {
        return _rightColumnCount;
    }

    protected RowDecorator getRowDecorator() {
        return _rowDecorator;
    }

    protected boolean isRightOuter() {
        return _rightOuter;
    }

    protected boolean isSwapLeftAndRight() {
        return _swapLeftAndRight;
    }

    protected void setLeftRowIterator(RowIterator left) {
        _left = left;
    }

    protected void setRightOuter(boolean b) {
        _rightOuter = b;
    }

    protected void setRightSideColumnCount(int rightColumnCount) {
        _rightColumnCount = rightColumnCount;
    }

    protected void setSwapLeftAndRight(boolean b) {
        _swapLeftAndRight = b;
    }

    private void clearNext() {
        _next = null;
        _nextSet = false;
    }

    private void clearPrevious() {
        _previous = null;
        _previousSet = false;
    }

    private RowIterator generateCurrentRowIterator() throws AxionException {
        RowIterator iter = new JoinRowIterator(_left.current(), generateRightRowIterator(), isSwapLeftAndRight());
        if (null != getJoinCondition()) {
            iter = new FilteringRowIterator(iter, getRowDecorator(), getJoinCondition());
        }
        if (iter.isEmpty() && isRightOuter()) {
            JoinedRow row = new JoinedRow();
            if (isSwapLeftAndRight()) {
                row.addRow(new SimpleRow(getRightSideColumnCount()));
                row.addRow(_left.current());
            } else {
                row.addRow(_left.current());
                row.addRow(new SimpleRow(getRightSideColumnCount()));
            }
            iter = new SingleRowIterator(row);
        }
        return iter;
    }

    private void setJoinCondition(Selectable joinCondition) {
        _joinCondition = joinCondition;
    }

    private boolean setNext() throws AxionException {
        if (!_left.hasCurrent()) {
            if (_left.hasNext()) {
                _left.next();
                _currentIterator = generateCurrentRowIterator();
            } else {
                return false;
            }
        }
        if (_left.currentIndex() == _left.nextIndex()) {
            _left.next();
        }
        for (;;) {
            if (_currentIterator.hasNext()) {
                setNext(_currentIterator.next());
                break;
            } else if (!_left.hasNext()) {
                clearNext();
                break;
            } else {
                _left.next();
                _currentIterator = generateCurrentRowIterator();
            }
        }
        return _nextSet;
    }

    private void setNext(Row next) {
        _next = next;
        _nextSet = true;
    }

    private boolean setPrevious() throws AxionException {
        if (_nextSet) {
            clearNext();
            setPrevious();
        }
        if (_left.currentIndex() == _left.previousIndex()) {
            _left.previous();
        }
        for (;;) {
            if (_currentIterator.hasPrevious()) {
                setPrevious(_currentIterator.previous());
                break;
            } else {
                _left.previous();
                _currentIterator = generateCurrentRowIterator();
                if (!_currentIterator.isEmpty()) {
                    _currentIterator.last();
                }
            }
        }
        return _previousSet;
    }

    private void setPrevious(Row previous) {
        _previous = previous;
        _previousSet = true;
    }

    private void setRowDecorator(RowDecorator rowDecorator) {
        _rowDecorator = rowDecorator;
    }

    private Row _current = null;
    private int _currentIndex = -1;
    private RowIterator _currentIterator = null;
    private boolean _currentSet = false;
    private Selectable _joinCondition = null;
    private RowIterator _left = null;
    private Row _next = null;
    private int _nextIndex = 0;
    private boolean _nextSet = false;
    private Row _previous = null;
    private boolean _previousSet = false;

    private int _rightColumnCount;
    private boolean _rightOuter = false;
    private RowDecorator _rowDecorator = null;
    private boolean _swapLeftAndRight = false;
}


