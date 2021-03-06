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
 * A {@link RowIterator}that contains no {@link Row}s.
 * 
 * @version  
 * @author Rodney Waldhoff
 */
public final class EmptyRowIterator implements RowIterator {

    public EmptyRowIterator() {
    }

    public void add(Row row) {
        throw new UnsupportedOperationException();
    }

    public Row current() {
        throw new NoSuchElementException();
    }

    public int currentIndex() {
        return -1;
    }

    public Row first() {
        throw new NoSuchElementException();
    }

    public boolean hasCurrent() {
        return false;
    }

    public boolean hasNext() {
        return false;
    }

    public boolean hasPrevious() {
        return false;
    }

    public boolean isEmpty() {
        return true;
    }

    public Row last() {
        throw new NoSuchElementException();
    }

    public Row next() {
        throw new NoSuchElementException();
    }

    public int next(int count) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public int nextIndex() {
        return 0;
    }

    public Row peekNext() {
        throw new NoSuchElementException();
    }

    public Row peekPrevious() {
        throw new NoSuchElementException();
    }

    public Row previous() {
        throw new NoSuchElementException();
    }

    public int previous(int count) throws AxionException {
        throw new UnsupportedOperationException();
    }

    public int previousIndex() {
        return -1;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void reset() {
    }

    public void set(Row row) {
        throw new UnsupportedOperationException();
    }

    public int size() throws AxionException {
        return 0;
    }

    public String toString() {
        return "EmptyRowIterator";
    }

    public static RowIterator INSTANCE = new EmptyRowIterator();
}


