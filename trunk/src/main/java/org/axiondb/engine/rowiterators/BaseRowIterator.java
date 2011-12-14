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

/**
 * An abstract base implementation of {@link RowIterator}.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public abstract class BaseRowIterator implements RowIterator {
    // Note: having these interface methods defined here as abstract seems to keep some
    // jdk/j2me implementations happy

    /** Not supported in the base implementation. */
    public void add(Row row) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public abstract Row current() throws NoSuchElementException;

    public abstract int currentIndex() throws NoSuchElementException;

    public Row first() throws AxionException {
        reset();
        return peekNext();
    }

    public abstract boolean hasCurrent();

    public abstract boolean hasNext();

    public abstract boolean hasPrevious();

    public boolean isEmpty() {
        return (!hasNext() && !hasPrevious());
    }

    public Row last() throws AxionException {
        if (!hasNext()) {
            previous(1);
        }

        while (hasNext()) {
            next(1);
        }
        return current();
    }

    public abstract Row next() throws NoSuchElementException, AxionException;

    public int next(int count) throws AxionException {
        for (int i = 0; i < count; i++) {
            next();
        }
        return current().getIdentifier();
    }

    public abstract int nextIndex();

    public Row peekNext() throws AxionException {
        next(1);
        return previous();
    }

    public Row peekPrevious() throws AxionException {
        previous(1);
        return next();
    }

    public abstract Row previous() throws NoSuchElementException, AxionException;

    public int previous(int count) throws AxionException {
        for (int i = 0; i < count; i++) {
            previous();
        }
        return current().getIdentifier();
    }

    public abstract int previousIndex();

    /** Not supported in the base implementation. */
    public void remove() throws AxionException {
        throw new UnsupportedOperationException();
    }

    public abstract void reset() throws AxionException;

    /** Not supported in the base implementation. */
    public void set(Row row) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public int size() throws AxionException {
        reset();
        int count = 0;
        while (hasNext()) {
            next(1);
            count++;
        }
        return count;
    }
}


