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

package org.axiondb.engine;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.Index;
import org.axiondb.IndexFactory;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableFactory;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.Transaction;
import org.axiondb.TransactionManager;
import org.axiondb.VariableContext;
import org.axiondb.event.ColumnEvent;
import org.axiondb.event.ConstraintEvent;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.event.RowEvent;
import org.axiondb.event.TableModificationListener;
import org.axiondb.functions.ConcreteFunction;

/**
 * A {@link Transaction}implementation that provides "snapshot isolation", which supports
 * TRANSACTION_SERIALIZABLE isolation without locking.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Amrish Lal
 * @author Dave Pekarek Krohn
 * @author Ahimanikya Satapathy
 */
public class SnapshotIsolationTransaction implements Transaction, TableModificationListener, VariableContext {

    public SnapshotIsolationTransaction(Database db) {
        _openOnTransaction = db;
    }

    public void addDatabaseModificationListener(DatabaseModificationListener l) {
    }

    public void addIndex(Index index, Table table) throws AxionException {
        _openOnTransaction.addIndex(index, table);
    }

    public void addIndex(Index index, Table table, boolean doPopulate) throws AxionException {
        _openOnTransaction.addIndex(index, table, doPopulate);
    }

    public void addTable(Table table) throws AxionException {
        _openOnTransaction.addTable(table);
    }

    public void apply() throws AxionException {
        if (STATE_COMMITTED != _state) {
            throw new AxionException("Not committed, can't apply.");
        }
        for (Iterator iter = _wrappedTables.values().iterator(); iter.hasNext();) {
            TransactableTable ttable = (TransactableTable) (iter.next());
            ttable.apply();
        }
        _state = STATE_APPLIED;
    }

    public void checkpoint() throws AxionException {
        _openOnTransaction.checkpoint();
    }

    public void columnAdded(ColumnEvent event) throws AxionException {
    }

    public void commit() throws AxionException {
        assertOpen();
        for (Iterator iter = _wrappedTables.values().iterator(); iter.hasNext();) {
            TransactableTable ttable = (TransactableTable) (iter.next());
            ttable.commit();
        }
        _contextVariables.clear();
        _state = STATE_COMMITTED;
    }

    // FIXME: There must be a better way to handle long running transaction
    public TransactableTable commit(TableIdentifier tid) throws AxionException {
        assertOpen();

        // remove the old transaction table
        if (_wrappedTables.containsKey(tid.getTableName())) {
            TransactableTable ttable = (TransactableTable) _wrappedTables.remove(tid.getTableName());
            ttable.commit();
            ttable.apply();
            _readTables.remove(tid.getTableName());
            _modifiedTables.remove(tid.getTableName());
        }

        return getWrappedTable(tid);
    }

    public void constraintAdded(ConstraintEvent event) throws AxionException {
    }

    public void constraintRemoved(ConstraintEvent event) throws AxionException {
    }

    public boolean containsKey(Object key) {
        return _contextVariables.containsKey(key);
    }

    public void createDatabaseLink(DatabaseLink server) throws AxionException {
        _openOnTransaction.createDatabaseLink(server);
    }

    public void createSequence(Sequence seq) throws AxionException {
        _openOnTransaction.createSequence(seq);
    }

    public int defragTable(String tableName) throws AxionException {
        if (_wrappedTables.containsKey(tableName)) {

            // Apply any pending transaction before renaming it.
            TransactableTable ttable = (TransactableTable) _wrappedTables.get(tableName);
            ttable.commit();
            ttable.apply();

            // remove the old table from this transaction
            _wrappedTables.remove(tableName);
            _readTables.remove(tableName);
        }

        return _openOnTransaction.defragTable(tableName);
    }

    public void dropDatabaseLink(String server) throws AxionException {
        _openOnTransaction.dropDatabaseLink(server);
    }

    public void dropDependentExternalDBTable(List tables) throws AxionException {
        _openOnTransaction.dropDependentExternalDBTable(tables);
    }

    public void dropDependentViews(List views) throws AxionException {
        _openOnTransaction.dropDependentViews(views);
    }

    public void dropIndex(String name) throws AxionException {
        _openOnTransaction.dropIndex(name);
    }

    public void dropSequence(String name) throws AxionException {
        _openOnTransaction.dropSequence(name);
    }

    public void dropTable(String name) throws AxionException {
        _openOnTransaction.dropTable(name);
        if (_wrappedTables.containsKey(name)) {
            _wrappedTables.remove(name);
            _readTables.remove(name);
        }
    }

    public Object get(Object key) {
        return _contextVariables.get(key);
    }

    public DatabaseLink getDatabaseLink(String name) {
        return _openOnTransaction.getDatabaseLink(name);
    }

    public List getDatabaseModificationListeners() {
        return null;
    }

    public DataType getDataType(String name) {
        return _openOnTransaction.getDataType(name);
    }

    public File getDBDirectory() {
        return _openOnTransaction.getDBDirectory();
    }

    public List getDependentExternalDBTable(String name) {
        return _openOnTransaction.getDependentExternalDBTable(name);
    }

    public List getDependentViews(String tableName) {
        return _openOnTransaction.getDependentViews(tableName);
    }

    public ConcreteFunction getFunction(String name) {
        return _openOnTransaction.getFunction(name);
    }

    public Object getGlobalVariable(String key) {
        return _openOnTransaction.getGlobalVariable(key);
    }

    public IndexFactory getIndexFactory(String name) {
        return _openOnTransaction.getIndexFactory(name);
    }

    public Set getModifiedTables() {
        return _modifiedTables;
    }

    public String getName() {
        return _openOnTransaction.getName();
    }

    public Database getOpenOnTransaction() {
        return _openOnTransaction;
    }

    public Set getReadTables() {
        return _readTables;
    }

    public Sequence getSequence(String name) {
        return _openOnTransaction.getSequence(name);
    }

    public int getState() {
        return _state;
    }

    public Table getTable(String name) throws AxionException {
        return getWrappedTable(new TableIdentifier(name));
    }

    public Table getTable(TableIdentifier table) throws AxionException {
        return getWrappedTable(table);
    }

    public TableFactory getTableFactory(String name) {
        return _openOnTransaction.getTableFactory(name);
    }

    public TransactionManager getTransactionManager() {
        return _openOnTransaction.getTransactionManager();
    }

    public boolean hasDatabaseLink(String name) throws AxionException {
        return _openOnTransaction.hasDatabaseLink(name);
    }

    public boolean hasIndex(String name) throws AxionException {
        return _openOnTransaction.hasIndex(name);
    }

    public boolean hasSequence(String name) throws AxionException {
        return _openOnTransaction.hasSequence(name);
    }

    public boolean hasTable(String name) throws AxionException {
        return _openOnTransaction.hasTable(name);
    }

    public boolean hasTable(TableIdentifier table) throws AxionException {
        return _openOnTransaction.hasTable(table);
    }

    public boolean isReadOnly() {
        return _openOnTransaction.isReadOnly();
    }

    public void migrate(int version) throws AxionException {
        _openOnTransaction.migrate(version);
    }

    @SuppressWarnings("unchecked")
    public void put(Object key, Object value) {
        _contextVariables.put(key, value);
    }

    public void remount(File newdir) throws AxionException {
        _openOnTransaction.remount(newdir);
    }

    public void remove(Object key) {
        _contextVariables.remove(key);
    }

    public void renameTable(String oldName, String newName) throws AxionException {
        renameTable(oldName, newName, null);
    }

    public void renameTable(String oldName, String newName, Properties newTableProp) throws AxionException {
        if (_wrappedTables.containsKey(oldName)) {

            // Apply any pending transaction before renaming it.
            TransactableTable ttable = (TransactableTable) _wrappedTables.get(oldName);
            ttable.commit();
            ttable.apply();

            // remove the old table from this transaction
            _wrappedTables.remove(oldName);
            _readTables.remove(oldName);
        }

        // rename the table to the newName
        _openOnTransaction.renameTable(oldName, newName, newTableProp);
    }

    public void rollback() throws AxionException {
        assertOpen();
        for (Iterator iter = _wrappedTables.values().iterator(); iter.hasNext();) {
            TransactableTable ttable = (TransactableTable) (iter.next());
            ttable.rollback();
        }
        _contextVariables.clear();
        _state = STATE_ABORTED;
    }

    @SuppressWarnings("unchecked")
    public void rowDeleted(RowEvent event) throws AxionException {
        _modifiedTables.add(event.getTable().getName());
    }

    @SuppressWarnings("unchecked")
    public void rowInserted(RowEvent event) throws AxionException {
        _modifiedTables.add(event.getTable().getName());
    }

    @SuppressWarnings("unchecked")
    public void rowUpdated(RowEvent event) throws AxionException {
        _modifiedTables.add(event.getTable().getName());
    }

    public void shutdown() throws AxionException {
        _openOnTransaction.shutdown();
    }

    public void tableAltered(Table table) throws AxionException {
        _openOnTransaction.tableAltered(table);
    }

    private void assertOpen() throws AxionException {
        if (STATE_OPEN != _state) {
            throw new AxionException("Already committed or rolled back.");
        }
    }

    @SuppressWarnings("unchecked")
    private TransactableTable getWrappedTable(TableIdentifier id) throws AxionException {
        TransactableTable ttable = (TransactableTable) (_wrappedTables.get(id.getTableName()));
        if (null == ttable) {
            Table table = _openOnTransaction.getTable(id);
            if (null == table) {
                return null;
            }

            ttable = table.makeTransactableTable();
            ttable.addTableModificationListener(this);
            _wrappedTables.put(id.getTableName(), ttable);
            _readTables.add(ttable.getName());
        }
        return ttable;
    }

    private Map _contextVariables = new HashMap();
    private Set _modifiedTables = new HashSet();
    private Database _openOnTransaction;
    private Set _readTables = new HashSet();
    private int _state = STATE_OPEN;
    private Map _wrappedTables = new HashMap();

}
