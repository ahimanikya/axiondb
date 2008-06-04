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
import java.util.List;
import java.util.Properties;

import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.functions.ConcreteFunction;

/**
 * An Axion database.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Amrish Lal
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 */
public interface Database {

    public static String COMMIT_SIZE = "COMMITSIZE";

    /** Adds a listener to receive events on this database */
    void addDatabaseModificationListener(DatabaseModificationListener l);

    /**
     * Add the given {@link Index}to this database, associated with the given table.
     */
    void addIndex(Index index, Table table) throws AxionException;

    /**
     * Add the given {@link Index}to this database, associating it with the given table
     * and (optionally) populating it.
     * 
     * @param index Index to be added and (optionally) populated
     * @param table Table to be indexed
     * @param doPopulate true if <code>index</code> should be populated by the
     *        appropriate column in <code>table</code>; false if <code>index</code>
     *        should be left as-is.
     * @throws AxionException if error occurs during addition and/or population of
     *         <code>index</code>
     */
    void addIndex(Index index, Table table, boolean doPopulate) throws AxionException;

    /**
     * Add the given {@link Table}to this database.
     */
    void addTable(Table table) throws AxionException;

    /**
     * Make sure any modified state or data has been written to disk.
     */
    void checkpoint() throws AxionException;

    void createDatabaseLink(DatabaseLink dblink) throws AxionException;

    /**
     * Create a numeric sequence
     */
    void createSequence(Sequence seq) throws AxionException;

    int defragTable(String tableName) throws AxionException;

    void dropDatabaseLink(String name) throws AxionException;

    void dropDependentExternalDBTable(List tables) throws AxionException;

    void dropDependentViews(List views) throws AxionException;

    /**
     * Drop the given {@link Index}from this database.
     */
    void dropIndex(String name) throws AxionException;
    
    /**
     * Drop the specified {@link Sequence}from this database.
     * <p>
     * Sequence name matching is case-insensitive.
     */
    void dropSequence(String name) throws AxionException;
    
    /**
     * Drop the specified {@link Table}from this database.
     * <p>
     * Table name matching is case-insensitive.
     */
    void dropTable(String name) throws AxionException;
    
    DatabaseLink getDatabaseLink(String name);
    /** Returns all listeners set to receive events on this database */
    List getDatabaseModificationListeners();

    /**
     * Get the {@link DataType}currently registered for the given name, or <tt>null</tt>.
     */
    DataType getDataType(String name);

    /**
     * Get the directory into which table information is stored, or <tt>null</tt>.
     */
    File getDBDirectory();

    List getDependentExternalDBTable(String name);

    List getDependentViews(String tableName);

    ConcreteFunction getFunction(String name);

    Object getGlobalVariable(String key);
    
    /**
     * Get the {@link IndexFactory}currently registered for the given name, or
     * <tt>null</tt>.
     */
    IndexFactory getIndexFactory(String name);
    
    /**
     * Returns the name of this <code>Database</code>.
     */
    String getName();

    /**
     * Get the specified {@link Sequence}, or <tt>null</tt> if no such sequence can be
     * found.
     * <p>
     * Sequence name matching is case-insensitive.
     */
    Sequence getSequence(String name);

    /**
     * Get the specified {@link Table}, or <tt>null</tt> if no such table can be found.
     * <p>
     * Table name matching is case-insensitive.
     */
    Table getTable(String name) throws AxionException;

    /**
     * Get the specified {@link Table}, or <tt>null</tt> if no such table can be found.
     * <p>
     * Table name matching is case-insensitive.
     */
    Table getTable(TableIdentifier table) throws AxionException;

    /**
     * Get the {@link TableFactory}currently registered for the given name, or
     * <tt>null</tt>.
     */
    TableFactory getTableFactory(String name);

    /** Get the {@link TransactionManager}for this database. */
    TransactionManager getTransactionManager();

    boolean hasDatabaseLink(String name) throws AxionException;

    /**
     * Returns <code>true</code> iff the given {@link Index}exists.
     */
    boolean hasIndex(String name) throws AxionException;

    boolean hasSequence(String name) throws AxionException;

    boolean hasTable(String name) throws AxionException;

    boolean hasTable(TableIdentifier table) throws AxionException;

    /**
     * Is this database read-only?
     */
    boolean isReadOnly();
    
    /** Migrate from older version to newer version for this database*/
    void migrate(int version) throws AxionException;

    /**
     * Notify this database that its root directory has been moved to the given location.
     * (E.g., the CD containing the data for a CD-resident database has changed drives.)
     */
    void remount(File newdir) throws AxionException;
    
    void renameTable(String oldName, String newName) throws AxionException;

    /**
     * Change table name along with dependent files attributes.
     *
     * @param oldName current table name.
     * @param newName new table name.
     * @param newTableProp Additional information to change files corresponding to the table.
     * @throws AxionException
     */
    void renameTable(String oldName, String newName, Properties newTableProp) throws AxionException;

    /**
     * Close this database and free any resources associated with it.
     */
    void shutdown() throws AxionException;

    /** Update metadata tables since this table has changed. */
    void tableAltered(Table t) throws AxionException;

}
