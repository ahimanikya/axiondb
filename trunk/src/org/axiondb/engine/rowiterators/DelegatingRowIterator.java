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
import org.axiondb.Row;
import org.axiondb.RowIterator;

/**
 * An abstract base {@link RowIterator}that delegates all calls to a wrapped instance.
 * 
 * @version  
 * @author Rodney Waldhoff
 */
public abstract class DelegatingRowIterator implements RowIterator {

    public DelegatingRowIterator(RowIterator iter) {
        setDelegate(iter);
    }

    public void add(Row row) throws AxionException {
        _delegate.add(row);
    }

    public Row current() {
        return _delegate.current();
    }

    public int currentIndex() {
        return _delegate.currentIndex();
    }

    public Row first() throws AxionException {
        return _delegate.first();
    }

    public boolean hasCurrent() {
        return _delegate.hasCurrent();
    }

    public boolean hasNext() {
        return _delegate.hasNext();
    }

    public boolean hasPrevious() {
        return _delegate.hasPrevious();
    }

    public boolean isEmpty() {
        return (!hasNext() && !hasPrevious() && !hasCurrent());
    }

    public Row last() throws AxionException {
        return _delegate.last();
    }

    public Row next() throws AxionException {
        return _delegate.next();
    }

    public int next(int count) throws AxionException {
        return _delegate.next(count);
    }

    public int nextIndex() {
        return _delegate.nextIndex();
    }

    public Row peekNext() throws AxionException {
        return _delegate.peekNext();
    }

    public Row peekPrevious() throws AxionException {
        return _delegate.peekPrevious();
    }

    public Row previous() throws AxionException {
        return _delegate.previous();
    }

    public int previous(int count) throws AxionException {
        return _delegate.previous(count);
    }

    public int previousIndex() {
        return _delegate.previousIndex();
    }

    public void remove() throws AxionException {
        _delegate.remove();
    }

    public void reset() throws AxionException {
        _delegate.reset();
    }

    public void set(Row row) throws AxionException {
        _delegate.set(row);
    }

    public int size() throws AxionException {
        return _delegate.size();
    }

    protected RowIterator getDelegate() {
        return _delegate;
    }

    protected void setDelegate(RowIterator delegate) {
        _delegate = delegate;
    }

    private RowIterator _delegate = null;
}
