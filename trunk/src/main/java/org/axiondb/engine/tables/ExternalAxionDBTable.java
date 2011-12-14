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
package org.axiondb.engine.tables;


import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.ExternalTable;
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TransactableTable;
import org.axiondb.engine.Databases;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.event.BaseTableModificationPublisher;
import org.axiondb.event.ColumnEvent;
import org.axiondb.jdbc.ConnectionFactory;

/**
 * Axion external table implementation of ExternalTable interface.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 * @see org.axiondb.DatabaseLink
 * @see org.axiondb.engine.tables.ExternalDatabaseTable
 */
public class ExternalAxionDBTable extends BaseTableModificationPublisher implements ExternalTable {

    private static Logger _log = Logger.getLogger(ExternalAxionDBTable.class.getName());

    /** Set of recognized keys for organization properties */
    private static final Set PROPERTY_KEYS = new HashSet(6);

    /** Set of required keys for organization properties */
    private static final Set REQUIRED_KEYS = new HashSet(1);

    static {
        // Build set of recognized property keys for external db tables.
        PROPERTY_KEYS.add(PROP_DB);
        PROPERTY_KEYS.add(PROP_REMOTETABLE);
        PROPERTY_KEYS.add(PROP_VENDOR);

        // Build set of required property keys for external db tables.
        REQUIRED_KEYS.add(PROP_DB);
    }

    public ExternalAxionDBTable(String name, Database db) {
        _name = name;
        _type = EXTERNAL_DB_TABLE_TYPE;
        _db = db;
    }

    public void addColumn(Column col) throws AxionException {
        _cols.add(col);
        publishEvent(new ColumnEvent(this, col));
    }

    public void addConstraint(Constraint constraint) throws AxionException {
    }

    public void addIndex(Index index) throws AxionException {
        _remoteTable.addIndex(index);
    }

    public void addRow(Row row) throws AxionException {
        _remoteTable.addRow(row);
    }

    public void applyDeletes(IntCollection rowIds) throws AxionException {
        _remoteTable.applyDeletes(rowIds);
    }

    public void applyInserts(RowCollection rows) throws AxionException {
        _remoteTable.applyInserts(rows);
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        _remoteTable.applyUpdates(rows);
    }

    public void checkpoint() throws AxionException {
    }

    public void deleteRow(Row row) throws AxionException {
        _remoteTable.deleteRow(row);
    }

    public void drop() throws AxionException {
        if(_remoteTable != null) {
            _remoteTable.drop();
            cleanUp();
        }
    }

    public void freeRowId(int id) {
        _remoteTable.freeRowId(id);
    }

    public Column getColumn(int index) {
        return _remoteTable.getColumn(index);
    }

    public Column getColumn(String name) {
        return _remoteTable.getColumn(name);
    }

    public int getColumnCount() {
        return _remoteTable.getColumnCount();
    }

    public List getColumnIdentifiers() {
        return _remoteTable.getColumnIdentifiers();
    }

    public int getColumnIndex(String name) throws AxionException {
        return _remoteTable.getColumnIndex(name);
    }

    public Iterator getConstraints() {
        return _remoteTable.getConstraints();
    }
    
    public String getDBLinkName() {
        return _dblink;
    }

    public RowIterator getIndexedRows(RowSource source, Selectable node, boolean readOnly) throws AxionException {
        return _remoteTable.getIndexedRows(source, node, readOnly);
    }

    public RowIterator getIndexedRows(Selectable node, boolean readOnly) throws AxionException {
        return _remoteTable.getIndexedRows(node, readOnly);
    }

    public Index getIndexForColumn(Column column) {
        return _remoteTable.getIndexForColumn(column);
    }

    public Iterator getIndices() {
        return _remoteTable.getIndices();
    }

    public RowIterator getMatchingRows(List selectables, List values, boolean readOnly) throws AxionException {
        return _remoteTable.getMatchingRows(selectables, values, readOnly);
    }

    public String getName() {
        return _name;
    }

    public int getNextRowId() {
        return _remoteTable.getNextRowId();
    }

    public Row getRow(int id) throws AxionException {
        return _remoteTable.getRow(id);
    }

    public int getRowCount() {
        return _remoteTable.getRowCount();
    }

    public RowIterator getRowIterator(boolean readOnly) throws AxionException {
        return _remoteTable.getRowIterator(readOnly);
    }

    public Sequence getSequence() {
        return _remoteTable.getSequence();
    }

    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    public String getType() {
        return _type;
    }

    public boolean hasColumn(ColumnIdentifier id) {
        boolean result = false;
        String tableName = id.getTableName();
        if (tableName == null || tableName.equals(getName())) {
            result = (getColumn(id.getName()) != null);
        }
        return result;
    }

    public boolean hasIndex(String name) throws AxionException {
        return _remoteTable.hasIndex(name);
    }

    public boolean isColumnIndexed(Column column) {
        return _remoteTable.isColumnIndexed(column);
    }

    public boolean isPrimaryKeyConstraintExists(String columnName) {
        return _remoteTable.isPrimaryKeyConstraintExists(columnName);
    }

    public boolean isUniqueConstraintExists(String columnName) {
        return _remoteTable.isUniqueConstraintExists(columnName);
    }

    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new ExternalDatabaseTableOrganizationContext();
        context.readOrSetDefaultProperties(props);
        context.updateProperties();
        return true;
    }

    public RowDecorator makeRowDecorator() {
        return _remoteTable.makeRowDecorator();
    }

    public TransactableTable makeTransactableTable() {
        return new TransactableTableImpl(this);
    }

    public void migrate() throws AxionException {
    }

    public void populateIndex(Index index) throws AxionException {
        _remoteTable.populateIndex(index);
    }

    public void remount() throws AxionException {
    }

    public void remount(File dir, boolean datafilesonly) throws AxionException {
    }

    public Constraint removeConstraint(String name) {
        return _remoteTable.removeConstraint(name);
    }
    
    public Constraint getConstraint(String name) {
        return _remoteTable.getConstraint(name);
    }

    public void removeIndex(Index index) throws AxionException {
        _remoteTable.removeIndex(index);
    }

    public void rename(String oldName, String newName) throws AxionException {
        _remoteTable.rename(oldName, newName);
    }

    public void setSequence(Sequence seq) throws AxionException {
    }

    public void shutdown() throws AxionException {
        if(_remoteTable !=null){
        _remoteTable.shutdown();
        }
        cleanUp();
    }

    @Override
    public String toString() {
        return getName();
    }

    public void truncate() throws AxionException {
        _remoteTable.truncate();
    }

    public void updateRow(Row oldrow, Row newrow) throws AxionException {
        _remoteTable.updateRow(oldrow, newrow);
    }

    private void assertColumns() throws AxionException {
        Iterator iter = _cols.iterator();
        while (iter.hasNext()) {
            Column col = (Column) iter.next();
            if (!hasColumn(new ColumnIdentifier(null, col.getName(), null, col.getDataType()))) {
                throw new AxionException("Column " + col + "does not exist in remote table" + _remoteTableName);
            }
        }
    }

    private void cleanUp() throws AxionException {
        _remoteDb = null;
        _remoteTable = null;
    }

    private void setUp(DatabaseLink server) throws AxionException {
        if(_remoteDb == null) {
            String url = server.getJdbcUrl();
            String name;
            File path = null;
            String prefixStripped = url.substring(ConnectionFactory.URL_PREFIX.length());
            int colon = prefixStripped.indexOf(":");
            if(colon == -1 || (prefixStripped.length()-1 == colon)) {
                name = prefixStripped;
            } else {
                name = prefixStripped.substring(0,colon);
                path = new File(prefixStripped.substring(colon+1));
            }
            
            _remoteDb = Databases.getOrCreateDatabase(name, path);
            _remoteTable = _remoteDb.getTable(_remoteTableName);
        }
        
        if(_remoteTable == null)  {
            throw new AxionException("Initialization error for remote table " + getName());
        }
    }
    

    private class ExternalDatabaseTableOrganizationContext extends BaseTableOrganizationContext {

        public Set getPropertyKeys() {
            Set baseKeys = getBasePropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);

            return keys;
        }

        public Set getRequiredPropertyKeys() {
            Set baseRequiredKeys = getBaseRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);

            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            // Validate all supplied property keys to ensure they are recognized.
            super.assertValidPropertyKeys(props);

            _dblink = props.getProperty(PROP_DB);
            if (_dblink == null || _dblink.trim().length() == 0 || !_db.hasDatabaseLink(_dblink)) {
                throw new AxionException("Please provide a valid server name");
            }

            DatabaseLink server = _db.getDatabaseLink(_dblink);
            if (server == null) {
                throw new AxionException("Database link " + _dblink + " does not exist.");
            }

            _remoteTableName = props.getProperty(PROP_REMOTETABLE);
            _remoteTableName = (_remoteTableName != null) ? _remoteTableName : getName();

            setUp(server);
            assertColumns();

            _log.log(Level.SEVERE,"External DB Table " + _remoteTableName + " created.");
        }

        public void updateProperties() {
            super.updateProperties();

            setProperty(PROP_DB, _dblink);
            setProperty(PROP_REMOTETABLE, _remoteTableName);
            setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_REMOTE);
            setProperty(PROP_VENDOR, "AXION");
        }
    }

    private List _cols = new ArrayList();

    private Database _db;
    private String _dblink;

    private String _name = null;

    private Database _remoteDb;
    private Table _remoteTable;
    private String _remoteTableName;

    private String _type = null;

    private ExternalDatabaseTableOrganizationContext context;

}
