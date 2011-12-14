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
import org.axiondb.engine.rows.JoinedRow;

/**
 * A {@link RowIterator} that simply wraps a {@link ListIterator}.
 *
 * @version  
 * @author Rodney Waldhoff
 */
public class JoinRowIterator extends BaseRowIterator {
    public JoinRowIterator(Row row, RowIterator iterator) {
        this(row,iterator,false);
    }
    
    public JoinRowIterator(Row row, RowIterator iterator, boolean rowOnRight) {
        _row = row;
        _iterator = iterator;
        _rowOnRight = rowOnRight;
    }

    public Row current() {
        if(_iterator.hasCurrent()) {
            return makeRow(_row,_iterator.current());
        }
        throw new NoSuchElementException("No current row has been set.");
    }
    
    public boolean hasCurrent() {
        return _iterator.hasCurrent();
    }

    public boolean hasNext() {
        return _iterator.hasNext();
    }

    public boolean hasPrevious() {
        return _iterator.hasPrevious();
    }

    public Row next() throws AxionException {
        return makeRow(_row,_iterator.next());
    }

    public Row previous() throws AxionException {
        return makeRow(_row,_iterator.previous());
    }

    public int currentIndex() {
        return _iterator.currentIndex();
    }

    public int nextIndex() {
        return _iterator.nextIndex();
    }

    public int previousIndex() {
        return _iterator.previousIndex();
    }

    public void reset() throws AxionException {
        _iterator.reset();
    }

    private Row makeRow(Row fromRow, Row fromIter) {
        JoinedRow row = new JoinedRow();
        if(_rowOnRight) {
            row.addRow(fromIter);
            row.addRow(fromRow);
        } else {
            row.addRow(fromRow);
            row.addRow(fromIter);
        }
        return row;
    }

    private RowIterator _iterator = null;
    private Row _row = null;
    private boolean _rowOnRight = false;
}


