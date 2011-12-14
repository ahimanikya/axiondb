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

package org.axiondb;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.event.TableModificationListener;

/**
 * A database table.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public interface Table extends RowSource {
    public static final String REGULAR_TABLE_TYPE = "TABLE";
    public static final String SYSTEM_TABLE_TYPE = "SYSTEM TABLE";

    /**
     * Add the given {@link Column}to this table.
     */
    void addColumn(Column col) throws AxionException;

    void addConstraint(Constraint constraint) throws AxionException;

    /**
     * Add an index, associating it with a {@link Column}, and adding it as a
     * {@link org.axiondb.TableModificationListener}to the table.
     * 
     * @see #addIndex
     * @see #addTableModificationListener
     * @see #getIndexForColumn
     * @see #isColumnIndexed
     * @see #populateIndex
     * @param index
     * @exception AxionException
     */
    void addIndex(Index index) throws AxionException;

    /**
     * Insert the given {@link Row}.
     */
    void addRow(Row row) throws AxionException;

    /**
     * Adds a listener to receive events on this table
     */
    void addTableModificationListener(TableModificationListener listener);
    
    Iterator getTableModificationListeners();

    /**
     * Remove the specified rows from this table and any associated indices. This process
     * is allowed to be destructive, the table my delete values from the given list.
     */
    void applyDeletes(IntCollection rowids) throws AxionException;

    /**
     * Insert the given rows into this table and any associated indices. This process is
     * allowed to be destructive, the table my delete rows from the given list.
     * 
     * @param rows a collection of Rows
     * @throws AxionException
     */
    void applyInserts(RowCollection rows) throws AxionException;

    /**
     * Update the given rows in this table and any associated indices. This process is
     * allowed to be destructive, the table my delete rows from the given list.
     */
    void applyUpdates(RowCollection rows) throws AxionException;

    /** Drop this table from the database. */
    void drop() throws AxionException;

    /** Un-reserve a row id. */
    void freeRowId(int id);

    /**
     * Return the {@link Column}corresponding to the given zero-based <i>index </i>.
     */
    Column getColumn(int index);

    /**
     * Return the {@link Column}for the given <i>name </i>.
     */
    Column getColumn(String name);

    /**
     * Return the number of {@link Column}s I contain.
     */
    int getColumnCount();

    /**
     * Return an readonly {@link List}over the {@link ColumnIdentifier ColumnIdentifiers}for
     * my {@link Column}s.
     */
    List getColumnIdentifiers();

    /**
     * Return the zero-based index of the {@link Column}with the given <i>name </i>.
     */
    int getColumnIndex(String name) throws AxionException;

    Iterator getConstraints();
    
    /**
     * @param readOnly when <code>true</code>, the caller does not expect to be able to
     *        modify (i.e., call {@link RowIterator#set}or {@link RowIterator#remove}on)
     *        the returned {@link RowIterator}, the returned iterator <i>may </i> be
     *        unmodifiable.
     */
    RowIterator getIndexedRows(Selectable where, boolean readOnly) throws AxionException;
    
    RowIterator getIndexedRows(RowSource source, Selectable where, boolean readOnly) throws AxionException;

    /**
     * Return the first {@link Index}that pertains to the given {@link Column}, or
     * <code>null</code> if no such {@link Index}exists.
     * 
     * @return the pertinent {@link Column}, or <code>null</code> if no such
     *         {@link Index}exists
     */
    Index getIndexForColumn(Column column);

    /** Obtain an {@link Iterator}over my indices. */
    Iterator getIndices();
    
    /**
     * Obtain an {@link RowIterator iterator}over my {@link Row}s where each
     * {@link Selectable Selectable}in the <i>selectable </i> {@link List list}
     * {@link Selectable#evaluate evaluates}to the corresponding value in the <i>value
     * </i> {@link List list}.
     * <p>
     * This is functionally similiar to executing a SELECT over this table where
     * <i>selectable[i] </i>= <i>value[i] </i> for each value of <i>i </i>. The return
     * RowIterator is not modifiable.
     */
    RowIterator getMatchingRows(List selectables, List values, boolean readOnly) throws AxionException;

    /** Get the name of this table. */
    String getName();

    /** Reserve a row id. */
    int getNextRowId();

    /**
     * Return the number of {@link Row}s I contain.
     */
    int getRowCount();

    /**
     * Obtain an {@link RowIterator iterator}over my {@link Row}s.
     * 
     * @param readOnly when <code>true</code>, the caller does not expect to be able to
     *        modify (i.e., call {@link RowIterator#set}or {@link RowIterator#remove}on)
     *        the returned {@link RowIterator}, the returned iterator <i>may </i> be
     *        unmodifiable.
     */
    RowIterator getRowIterator(boolean readOnly) throws AxionException;

    /** Get the type of this table. */
    String getType();

    /**
     * Indicate whether the {@link ColumnIdentifier}references a column in this table
     */
    boolean hasColumn(ColumnIdentifier id);

    boolean hasIndex(String name) throws AxionException;

    /**
     * Check to see if an {@link Index}exists for the given {@link Column}
     * 
     * @param column {@link Column}to check
     * @return true iff there is an existing {@link Index}for the given {@link Column}
     */
    boolean isColumnIndexed(Column column);

    /**
     * check if primary constraint exists on a column
     * 
     * @param ColumnName name of the column
     * @return if PrimaryKeyConstraint exists on the column
     */
    boolean isPrimaryKeyConstraintExists(String columnName);

    /**
     * check if unique constraint exists on a column
     * 
     * @param columnName name of the columm
     * @return true if uniqueConstraint exists on the column
     */
    boolean isUniqueConstraintExists(String columnName);

    RowDecorator makeRowDecorator();

    /** Create a {@link TransactableTable}for this table. */
    TransactableTable makeTransactableTable();
    
    /** Migrate from older version to newer version for this table*/
    void migrate() throws AxionException;

    /**
     * Populate an {@link Index}, adding my current rows to it. Does not
     * {@link #addIndex add}the index.
     * 
     * @see #addIndex
     * @param index
     * @exception AxionException
     */
    void populateIndex(Index index) throws AxionException;

    /** Notify this table that its disk-location has moved. */
    void remount(File dir, boolean dataOnly) throws AxionException;

    Constraint removeConstraint(String name);
    
    Constraint getConstraint(String name);

    /**
     * Remove an index, both from the indices and as a TableModificationListener
     * 
     * @param index
     * @exception AxionException
     */
    void removeIndex(Index index) throws AxionException;

    /**
     * Removes a listener so that it stops receiving events on this table
     */
    void removeTableModificationListener(TableModificationListener listener);
    
    void rename(String oldName, String newName) throws AxionException;

    /** The database is shutting down, shutdown this table also. */
    void shutdown() throws AxionException;

    /**
     * Unconditionally delete all rows in this table.
     * 
     * @return true if truncation succeeded; false otherwise
     */
    void truncate() throws AxionException;

    /**
     * Update the given {@link Row}.
     */
    void updateRow(Row oldrow, Row newrow) throws AxionException;
    
    /**
     * Delete the given {@link Row}.
     */
    void deleteRow(Row row) throws AxionException;
    
    void checkpoint() throws AxionException;
    
    void setSequence(Sequence seq) throws AxionException;
    
    Sequence getSequence();
    
    void setDeferAllConstraints(boolean deferAll);
    
}
