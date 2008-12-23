/* 
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
package org.axiondb.engine.rowcollection;

import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.engine.rowiterators.SingleRowIterator;

/**
 * An Utility for Row Collections.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public abstract class RowCollections implements RowCollection {

    public static RowCollection singletonList(Row row) {
        return new SingletonList(row);
    }

    private static class SingletonList implements RowCollection {
        private Row _row = null;

        public boolean add(Row row) {
            throw new UnsupportedOperationException();
        }

        SingletonList(Row row) {
            _row = row;
        }

        public int size() {
            return 1;
        }

        public boolean contains(Row row) {
            return _row.equals(row);
        }

        public RowIterator rowIterator() {
            return new SingleRowIterator(_row);
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return false;
        }

        public boolean remove(Row row) {
            throw new UnsupportedOperationException();
        }
    }

}
