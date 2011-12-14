/* 
 * =======================================================================
 * Copyright (c) 2005-2006 Axion Development Team.  All rights reserved.
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
package org.axiondb;

/**
 * @version  
 * @author Ahimanikya Satapathy
 */
public interface RowCollection {

    /**
     * Ensures that this row collection contains the specified row. Returns <tt>true</tt>
     * if this row collection changed as a result of the call. (Returns <tt>false</tt>
     * if this row collection does not permit duplicates and already contains the
     * specified row.)
     * <p>
     * 
     * @param row whose presence in this row collection is to be ensured.
     * @return <tt>true</tt> if this row collection changed as a result of the call
     * @throws UnsupportedOperationException <tt>add</tt> is not supported by this row
     *         collection.
     */
    boolean add(Row row) throws AxionException;

    /**
     * Removes all of the rows from this row collection.
     * 
     * @throws UnsupportedOperationException if the <tt>clear</tt> method is not
     *         supported by this row collection.
     */
    void clear() throws AxionException;

    /**
     * Returns <tt>true</tt> if this row collection contains the specified row. More
     * formally, returns <tt>true</tt> if and only if this row collection contains at
     * least one row <tt>e</tt> such that <tt>(o==null ? e==null : row.equals(e))</tt>.
     * 
     * @param row whose presence in this row collection is to be tested.
     * @return <tt>true</tt> if this row collection contains the specified row
     */
    boolean contains(Row row) throws AxionException;

    /**
     * Returns <tt>true</tt> if this row collection contains no rows.
     * 
     * @return <tt>true</tt> if this row collection contains no rows
     */
    boolean isEmpty();

    /**
     * Returns an iterator over the rows in this row collection. There are no guarantees
     * concerning the order in which the rows are returned (unless this row collection is
     * an instance of some class that provides a guarantee).
     * 
     * @return an <tt>Iterator</tt> over the rows in this row collection
     */
    RowIterator rowIterator() throws AxionException;

    /**
     * Removes a single instance of the specified row from this row collection, if it is
     * present.
     * 
     * @param row to be removed from this row collection, if present.
     * @return <tt>true</tt> if this row collection changed as a result of the call
     * @throws UnsupportedOperationException remove is not supported by this row
     *         collection.
     */
    boolean remove(Row row) throws AxionException;

    /**
     * Returns the number of rows in this row collection.
     * 
     * @return the number of rows in this row collection
     */
    int size();

}


