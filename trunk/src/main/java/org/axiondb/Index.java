/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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

import java.io.File;

import org.axiondb.event.TableModificationListener;

/**
 * A database index. (Right now, this class assumes an Index over a single column.
 * Multipart indices will come later.)
 * <p>
 * TODO: Support expression and/or function e.g UPPER(name) <br>
 * TODO: Support for Multi column index, very useful for composite keys
 * 
 * @version  
 * @author Morgan Delagrange
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public interface Index extends TableModificationListener {

    public static final String ARRAY = "array";
    public static final String BTREE = "btree";
    public static final String DEFAULT = "default";
    public static final String TTREE = "ttree";

    void changeRowId(Table table, Row row, int oldId, int newId) throws AxionException;

    /**
     * Returns the column I index.
     */
    Column getIndexedColumn();

    IndexLoader getIndexLoader();

    /**
     * Returns a {@link RowIterator}which is inorder traversal of keys,
     * 
     * @param source table/view for which we need to get inorder traversal
     * @return
     * @throws AxionException
     */
    RowIterator getInorderRowIterator(RowSource source) throws AxionException;

    /**
     * Returns my name.
     */
    String getName();

    /**
     * Returns a {@link RowIterator}over the indexed rows, limited by the given
     * {@link ComparisonOperator}/value pair, using the default sort order.
     * 
     * @param operator the {@link ComparisonOperator}to apply
     * @param value the value to compare the indexed column to
     */
    RowIterator getRowIterator(RowSource source, Function fn, Object value) throws AxionException;

    /**
     * Returns my type.
     */
    String getType();

    /**
     * Whether or not I allow duplicate values.
     */
    boolean isUnique();

    void save(File dataDirectory) throws AxionException;

    void saveAfterTruncate(File dataDirectory) throws AxionException;

    /**
     * Returns <tt>true</tt> iff
     * {@link #getRowIterator(org.axiondb.RowSource,org.axiondb.ComparisonOperator,java.lang.Object)}
     * can support the given operator, <tt>false</tt> otherwise.
     */
    boolean supportsFunction(Function fn);

    void truncate() throws AxionException;
}
