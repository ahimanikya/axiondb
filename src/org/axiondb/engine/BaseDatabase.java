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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.DataTypeFactory;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.FunctionFactory;
import org.axiondb.Index;
import org.axiondb.IndexFactory;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableFactory;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactionManager;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.commands.SubSelectCommand;
import org.axiondb.engine.indexes.BaseBTreeIndex;
import org.axiondb.engine.metaupdaters.AxionColumnsMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionConstraintsMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionDBLinksMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionSequencesMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionTablePropertiesMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionTablesMetaTableUpdater;
import org.axiondb.engine.metaupdaters.AxionTypesMetaTableUpdater;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.BaseFlatfileTable;
import org.axiondb.engine.tables.ExternalAxionDBTable;
import org.axiondb.engine.tables.ExternalDatabaseTable;
import org.axiondb.engine.tables.TableView;
import org.axiondb.event.DatabaseLinkEvent;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.event.DatabaseModifiedEvent;
import org.axiondb.event.DatabaseSequenceEvent;
import org.axiondb.event.DatabaseTypeEvent;
import org.axiondb.event.SequenceModificationListener;
import org.axiondb.event.TableModificationListener;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.types.BooleanType;
import org.axiondb.types.DateType;
import org.axiondb.types.IntegerType;
import org.axiondb.types.LOBType;
import org.axiondb.types.ShortType;
import org.axiondb.types.StringType;
import org.axiondb.types.TimeType;
import org.axiondb.types.TimestampType;

/**
 * Abstract base {@link Database}implementation.
 *
 * @version
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Morgan Delagrange
 * @author James Strachan
 * @author Amrish Lal
 * @author Rahul Dwivedi
 * @author Dave Pekarek Krohn
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 */
public abstract class BaseDatabase implements Database {
    
    public BaseDatabase(String name) {
        _name = name;
    }
    
    @SuppressWarnings("unchecked")
    public void addDatabaseModificationListener(DatabaseModificationListener l) {
        _listeners.add(l);
    }
    
    public void addIndex(Index index, Table table) throws AxionException {
        addIndex(index, table, false);
    }
    
    @SuppressWarnings("unchecked")
    public void addIndex(Index index, Table table, boolean doPopulate) throws AxionException {
        if (_indices.containsKey(index.getName())) {
            throw new AxionException("An index named " + index.getName() + " already exists");
        }
        
        Column indexedColumn = index.getIndexedColumn();
        if (table.isColumnIndexed(indexedColumn)) {
            Iterator i = table.getIndices();
            while (i.hasNext()) {
                Index existing = (Index) i.next();
                if (existing.getIndexedColumn().equals(indexedColumn) && index.getClass() == existing.getClass()) {
                    throw new AxionException("Column " + indexedColumn + " is already indexed " + "by an existing index, " + existing.getName()
                            + ", of the same type");
                }
            }
        }
        
        table.addIndex(index);
        if (doPopulate) {
            try {
                table.populateIndex(index);
            } catch (AxionException e) {
                try {
                    table.removeIndex(index);
                } catch (AxionException ignore) {
                    // Don't throw this exception; throw the initial one that caused us to
                    // rollback the index.
                }
                throw e;
            }
        }
        
        _indices.put(index.getName(), new Object[] { index, table});
        addIndexMetaEntry(index, table);
    }
    
    @SuppressWarnings("unchecked")
    public void addTable(Table t) throws AxionException {
        if (t != null) {
            String name = t.getName();
            if (_tables.containsKey(name)) {
                throw new AxionException("A table named " + name + " already exists.");
            }
            
            _tables.put(name, t);
            t.addTableModificationListener((TableModificationListener) _colUpd);
            t.addTableModificationListener(_pkfkUpd);
            Iterator i = getDatabaseModificationListeners().iterator();
            while (i.hasNext()) {
                DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
                cur.tableAdded(new DatabaseModifiedEvent(t));
            }
        }
    }
    
    public void checkpoint() throws AxionException {
        for (Iterator tables = _tables.values().iterator(); tables.hasNext();) {
            Table table = (Table) (tables.next());
            table.checkpoint();
        }
    }
    
    @SuppressWarnings("unchecked")
    public void createDatabaseLink(DatabaseLink dblink) throws AxionException {
        String upName = dblink.getName().toUpperCase();
        _databaseLink.put(upName, dblink);
        
        DatabaseLinkEvent e = new DatabaseLinkEvent(dblink);
        Iterator i = getDatabaseModificationListeners().iterator();
        while (i.hasNext()) {
            DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
            cur.serverAdded(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public void createSequence(Sequence seq) throws AxionException {
        if (seq != null) {
            DatabaseSequenceEvent e = new DatabaseSequenceEvent(seq);
            Iterator i = getDatabaseModificationListeners().iterator();
            while (i.hasNext()) {
                DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
                cur.sequenceAdded(e);
            }
            _sequences.put(seq.getName(), seq);
            seq.addSequenceModificationListener((SequenceModificationListener) _seqUpd);
        }
    }
    
    public void defrag() throws AxionException {
        throw new UnsupportedOperationException("Defrag not supported in Memory DB.");
    }
    
    public int defragTable(String tableName) throws AxionException {
        throw new UnsupportedOperationException("Defrag not supported in Memory DB.");
    }
    
    public void dropDatabaseLink(String name) throws AxionException {
        String upName = name.toUpperCase();
        DatabaseLink dblink = (DatabaseLink) (_databaseLink.remove(upName));
        if (null == dblink) {
            throw new AxionException("No Database Link Server " + upName + " found");
        }
        
        DatabaseLinkEvent e = new DatabaseLinkEvent(dblink);
        Iterator i = getDatabaseModificationListeners().iterator();
        while (i.hasNext()) {
            DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
            cur.serverDropped(e);
        }
    }
    
    public void dropDependentExternalDBTable(List tables) throws AxionException {
        Iterator i = tables.iterator();
        while (i.hasNext()) {
            Table t = (Table) (i.next());
            dropTable(t.getName());
            
            List v = getDependentExternalDBTable(t.getName());
            if (!v.isEmpty()) {
                dropDependentExternalDBTable(v);
            }
        }
    }
    
    public void dropDependentViews(List views) throws AxionException {
        Iterator i = views.iterator();
        while (i.hasNext()) {
            Table t = (Table) (i.next());
            dropTable(t.getName());
            
            List v = getDependentViews(t.getName());
            if (!v.isEmpty()) {
                dropDependentViews(v);
            }
        }
    }
    
    public void dropIndex(String name) throws AxionException {
        String upName = name.toUpperCase();
        Object[] pair = (Object[]) (_indices.remove(upName));
        if (null == pair) {
            throw new AxionException("No index " + upName + " found.");
        }
        
        Index index = (Index) (pair[0]);
        Table table = (Table) (pair[1]);
        table.removeIndex(index);
        removeIndexMetaEntry(index);
    }
    
    public void dropSequence(String name) throws AxionException {
        String upName = name.toUpperCase();
        Sequence seq = (Sequence) (_sequences.remove(upName));
        if (null == seq) {
            throw new AxionException("No sequence " + upName + " found");
        }
        
        DatabaseSequenceEvent e = new DatabaseSequenceEvent(seq);
        Iterator i = getDatabaseModificationListeners().iterator();
        while (i.hasNext()) {
            DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
            cur.sequenceDropped(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public void dropTable(String name) throws AxionException {
        String upName = name.toUpperCase();
        if (_tables.containsKey(upName)) {
            Table table = (Table) (_tables.remove(upName));
            
            // Copying to local List to avoid ConcurrentModificationException
            Iterator i = table.getIndices();
            List indexesToDrop = new ArrayList();
            while (i.hasNext()) {
                indexesToDrop.add(i.next());
            }
            
            i = indexesToDrop.iterator();
            while (i.hasNext()) {
                dropIndex(((Index) i.next()).getName());
            }
            
            i = getDatabaseModificationListeners().iterator();
            while (i.hasNext()) {
                DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
                cur.tableDropped(new DatabaseModifiedEvent(table));
            }
            
            table.drop();
        } else {
            throw new AxionException("No table " + upName + " found");
        }
    }
    
    public DatabaseLink getDatabaseLink(String name) {
        return (DatabaseLink) _databaseLink.get(name.toUpperCase());
    }
    
    public List getDatabaseModificationListeners() {
        return _listeners;
    }
    
    public DataType getDataType(String name) {
        DataType type = (DataType) (_dataTypes.get(name.toUpperCase()));
        if (type instanceof LOBType) {
            type = ((DataTypeFactory) type).makeNewInstance();
        }
        return type;
    }
    
    // --------------------------------------------------------------- Protected
    
    @SuppressWarnings("unchecked")
    public List getDependentExternalDBTable(String name) {
        String upName = name.toUpperCase();
        Iterator i = getTables();
        List tables = new ArrayList();
        while (i.hasNext()) {
            Table t = (Table) (i.next());
            if (t instanceof ExternalDatabaseTable) {
                ExternalDatabaseTable et = (ExternalDatabaseTable) t;
                if (upName.equals(et.getDBLinkName())) {
                    tables.add(et);
                }
            } else if (t instanceof ExternalAxionDBTable) {
                ExternalAxionDBTable et = (ExternalAxionDBTable) t;
                if (upName.equals(et.getDBLinkName())) {
                    tables.add(et);
                }
            }
        }
        return tables;
    }
    
    @SuppressWarnings("unchecked")
    public List getDependentViews(String tableName) {
        String upName = tableName.toUpperCase();
        Iterator i = getTables();
        List views = new ArrayList();
        while (i.hasNext()) {
            Table t = (Table) (i.next());
            if (t instanceof TableView) {
                Iterator i2 = ((TableView) t).getTables();
                String tName;
                while (i2.hasNext()) {
                    tName = ((TableIdentifier) i2.next()).getTableName();
                    if (upName.equals(tName)) {
                        views.add(t);
                        break;
                    }
                }
            }
        }
        return views;
    }
    
    public ConcreteFunction getFunction(String name) {
        FunctionFactory factory = (FunctionFactory) (_functions.get(name.toUpperCase()));
        if (null != factory) {
            return factory.makeNewInstance();
        }
        return null;
    }
    
    public Object getGlobalVariable(String key) {
        return _globalVariables.get(key.toUpperCase());
    }
    
    public IndexFactory getIndexFactory(String name) {
        return (IndexFactory) (_indexTypes.get(name.toUpperCase()));
    }
    
    public String getName() {
        return _name;
    }
    
    public Sequence getSequence(String name) {
        return (Sequence) _sequences.get(name.toUpperCase());
    }
    
    public Table getTable(String name) throws AxionException {
        String upName = name.toUpperCase();
        return (Table) (_tables.get(upName));
    }
    
    public Table getTable(TableIdentifier table) throws AxionException {
        return (Table) (_tables.get(table.getTableName()));
    }
    
    public TableFactory getTableFactory(String name) {
        return (TableFactory) (_tableTypes.get(name.toUpperCase()));
    }
    
    public TransactionManager getTransactionManager() {
        return _transactionManager;
    }
    
    // ----------------------------------------------------------------- Private
    
    public boolean hasDatabaseLink(String name) throws AxionException {
        return getDatabaseLink(name) != null;
    }
    
    public boolean hasIndex(String name) throws AxionException {
        return _indices.containsKey(name);
    }
    
    public boolean hasSequence(String name) throws AxionException {
        return getSequence(name) != null;
    }
    
    public boolean hasTable(String name) throws AxionException {
        return getTable(name) != null;
    }
    
    public boolean hasTable(TableIdentifier id) throws AxionException {
        return getTable(id) != null;
    }
    
    public boolean isReadOnly() {
        return _readOnly;
    }
    
    /** Migrate from older version to newer version for this database */
    public void migrate(int version) throws AxionException {
    }
    
    public void remount(File newdir) throws AxionException {
        for (Iterator tables = _tables.values().iterator(); tables.hasNext();) {
            Table table = (Table) (tables.next());
            table.remount(new File(newdir, table.getName()), false);
        }
    }
    
    public void removeDatabaseModificationListener(DatabaseModificationListener l) {
        _listeners.remove(l);
    }
    
    @SuppressWarnings("unchecked")
    public void renameTable(String oldName, String newName, Properties newTableProp) throws AxionException {
        String upOldName = oldName.toUpperCase();
        String upNewName = newName.toUpperCase();
        if (_tables.containsKey(upOldName)) {
            Table table = (Table) (_tables.remove(upOldName));
            // Note: this does not handle index migration
            
            // Initiate table drop event
            Iterator i = getDatabaseModificationListeners().iterator();
            while (i.hasNext()) {
                DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
                cur.tableDropped(new DatabaseModifiedEvent(table));
            }
            
            if (table instanceof BaseFlatfileTable){
                ((BaseFlatfileTable)table).rename(upOldName, upNewName, newTableProp);
            } else {
                table.rename(upOldName, upNewName);
            }
            
            _tables.put(upNewName, table);
            
            // table add event
            table.addTableModificationListener((TableModificationListener) _colUpd);
            i = getDatabaseModificationListeners().iterator();
            while (i.hasNext()) {
                DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
                cur.tableAdded(new DatabaseModifiedEvent(table));
            }
        } else {
            throw new AxionException("No table " + upOldName + " found");
        }
    }
    
    public void renameTable(String oldName, String newName) throws AxionException {
        renameTable(oldName, newName, null);
    }
    
    // to set the db for subselect this fun
    public Selectable resolveSelectSelectable(SubSelectCommand select, TableIdentifier[] tables) {
        select.setDB(this);
        select.setParentTables(tables);
        return select;
    }
    
    public void shutdown() throws AxionException {
        checkpoint();
        for (Iterator tables = _tables.values().iterator(); tables.hasNext();) {
            Table table = (Table) (tables.next());
            table.shutdown();
        }
        Databases.forgetDatabase(getName());
    }
    
    // -------------------------------------------------------------- Attributes
    
    public void tableAltered(Table t) throws AxionException {
        Iterator i = getDatabaseModificationListeners().iterator();
        while (i.hasNext()) {
            DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
            DatabaseModifiedEvent e = new DatabaseModifiedEvent(t);
            cur.tableDropped(e);
            cur.tableAdded(e);
        }
    }
    
    /**
     * Should get called by subclasses in constructors
     */
    protected void createMetaDataTables() throws AxionException {
        Table columns = null;
        {
            columns = createSystemTable("AXION_COLUMNS");
            columns.addColumn(new Column("TABLE_CAT", new StringType()));
            columns.addColumn(new Column("TABLE_SCHEM", new StringType()));
            columns.addColumn(new Column("TABLE_NAME", new StringType()));
            columns.addColumn(new Column("COLUMN_NAME", new StringType()));
            columns.addColumn(new Column("DATA_TYPE", new ShortType()));
            columns.addColumn(new Column("TYPE_NAME", new StringType()));
            columns.addColumn(new Column("COLUMN_SIZE", new IntegerType()));
            columns.addColumn(new Column("BUFFER_LENGTH", new IntegerType()));
            columns.addColumn(new Column("DECIMAL_DIGITS", new IntegerType()));
            columns.addColumn(new Column("NUM_PREC_RADIX", new IntegerType()));
            columns.addColumn(new Column("NULLABLE", new IntegerType()));
            columns.addColumn(new Column("REMARKS", new StringType()));
            columns.addColumn(new Column("COLUMN_DEF", new StringType()));
            columns.addColumn(new Column("SQL_DATA_TYPE", new IntegerType()));
            columns.addColumn(new Column("SQL_DATETIME_SUB", new IntegerType()));
            columns.addColumn(new Column("CHAR_OCTET_LENGTH", new IntegerType()));
            columns.addColumn(new Column("ORDINAL_POSITION", new IntegerType()));
            columns.addColumn(new Column("IS_NULLABLE", new StringType()));
            columns.addColumn(new Column("SCOPE_CATALOG", new StringType()));
            columns.addColumn(new Column("SCOPE_SCHEMA", new StringType()));
            columns.addColumn(new Column("SCOPE_TABLE", new StringType()));
            columns.addColumn(new Column("SOURCE_DATA_TYPE", new ShortType()));
        }
        addDatabaseModificationListener(_colUpd);
        addTable(columns);
        
        Table tables = null;
        AxionTablesMetaTableUpdater updTables = new AxionTablesMetaTableUpdater(this);
        {
            tables = createSystemTable("AXION_TABLES");
            tables.addColumn(new Column("TABLE_CAT", new StringType()));
            tables.addColumn(new Column("TABLE_SCHEM", new StringType()));
            tables.addColumn(new Column("TABLE_NAME", new StringType()));
            tables.addColumn(new Column("TABLE_TYPE", new StringType()));
            tables.addColumn(new Column("REMARKS", new StringType()));
            // bootstrap AXION_COLUMNS into AXION_TABLES
            Row row = updTables.createRowForAddedTable(columns);
            tables.addRow(row);
        }
        addDatabaseModificationListener(updTables);
        addTable(tables);
        
        {
            Table tableTypes = createSystemTable("AXION_TABLE_TYPES");
            tableTypes.addColumn(new Column("TABLE_TYPE", new StringType()));
            String[] types = new String[] { Table.REGULAR_TABLE_TYPE, Table.SYSTEM_TABLE_TYPE};
            for (int i = 0; i < types.length; i++) {
                SimpleRow row = new SimpleRow(1);
                row.set(0, types[i]);
                tableTypes.addRow(row);
            }
            addTable(tableTypes);
        }
        
        {
            Table catalogs = createSystemTable("AXION_CATALOGS");
            catalogs.addColumn(new Column("TABLE_CAT", new StringType()));
            {
                SimpleRow row = new SimpleRow(1);
                row.set(0, "");
                catalogs.addRow(row);
            }
            addTable(catalogs);
        }
        
        {
            Table schemata = createSystemTable("AXION_SCHEMATA");
            schemata.addColumn(new Column("TABLE_CAT", new StringType()));
            schemata.addColumn(new Column("TABLE_SCHEM", new StringType()));
            {
                SimpleRow row = new SimpleRow(2);
                row.set(0, "");
                row.set(1, "");
                schemata.addRow(row);
            }
            addTable(schemata);
        }
        
        {
            // FIXME: these are a bit hacked
            Table types = createSystemTable("AXION_TYPES");
            types.addColumn(new Column("TYPE_NAME", new StringType()));
            types.addColumn(new Column("DATA_TYPE", new ShortType()));
            types.addColumn(new Column("PRECISION", new IntegerType()));
            types.addColumn(new Column("LITERAL_PREFIX", new StringType()));
            types.addColumn(new Column("LITERAL_SUFFIX", new StringType()));
            types.addColumn(new Column("CREATE_PARAMS", new StringType()));
            types.addColumn(new Column("NULLABLE", new IntegerType()));
            types.addColumn(new Column("CASE_SENSITIVE", new BooleanType()));
            types.addColumn(new Column("SEARCHABLE", new ShortType()));
            types.addColumn(new Column("UNSIGNED_ATTRIBUTE", new BooleanType()));
            types.addColumn(new Column("FIXED_PREC_SCALE", new BooleanType()));
            types.addColumn(new Column("AUTO_INCREMENT", new BooleanType()));
            types.addColumn(new Column("LOCAL_TYPE_NAME", new StringType()));
            types.addColumn(new Column("MINIMUM_SCALE", new ShortType()));
            types.addColumn(new Column("MAXIMUM_SCALE", new ShortType()));
            types.addColumn(new Column("SQL_DATA_TYPE", new IntegerType()));
            types.addColumn(new Column("SQL_DATETIME_SUB", new IntegerType()));
            types.addColumn(new Column("NUM_PREC_RADIX", new IntegerType()));
            addTable(types);
            addDatabaseModificationListener(new AxionTypesMetaTableUpdater(this));
        }
        
        {
            Table seqTable = createSystemTable("AXION_SEQUENCES");
            seqTable.addColumn(new Column("SEQUENCE_NAME", new StringType()));
            seqTable.addColumn(new Column("SEQUENCE_VALUE", new IntegerType()));
            addTable(seqTable);
            addDatabaseModificationListener(_seqUpd);
        }
        
        // Add AXION_TABLE_PROPERTIES to hold values of external table properties
        {
            Table tableProps = createSystemTable("AXION_TABLE_PROPERTIES");
            tableProps.addColumn(new Column("TABLE_NAME", new StringType()));
            tableProps.addColumn(new Column("PROPERTY_NAME", new StringType()));
            tableProps.addColumn(new Column("PROPERTY_VALUE", new StringType()));
            addTable(tableProps);
            addDatabaseModificationListener(new AxionTablePropertiesMetaTableUpdater(this));
        }
        
        // Add AXION_DB_LINKS to hold references to external database servers
        {
            Table tableLinks = createSystemTable(BaseDatabase.SYSTABLE_DB_LINKS);
            tableLinks.addColumn(new Column("LINK_NAME", new StringType()));
            tableLinks.addColumn(new Column("LINK_URL", new StringType()));
            tableLinks.addColumn(new Column("LINK_USERNAME", new StringType()));
            addTable(tableLinks);
            addDatabaseModificationListener(new AxionDBLinksMetaTableUpdater(this));
        }
        
        // Add AXION_INDICES to hold information on indexes
        {
            Table tableIndices = createSystemTable(BaseDatabase.SYSTABLE_INDEX_INFO);
            tableIndices.addColumn(new Column("TABLE_CAT", new StringType()));
            tableIndices.addColumn(new Column("TABLE_SCHEM", new StringType()));
            tableIndices.addColumn(new Column("TABLE_NAME", new StringType()));
            tableIndices.addColumn(new Column("NON_UNIQUE", new BooleanType()));
            tableIndices.addColumn(new Column("INDEX_QUALIFIER", new StringType()));
            tableIndices.addColumn(new Column("INDEX_NAME", new StringType()));
            tableIndices.addColumn(new Column("TYPE", new ShortType()));
            tableIndices.addColumn(new Column("ORDINAL_POSITION", new ShortType()));
            tableIndices.addColumn(new Column("COLUMN_NAME", new StringType()));
            tableIndices.addColumn(new Column("ASC_OR_DESC", new StringType()));
            tableIndices.addColumn(new Column("CARDINALITY", new IntegerType()));
            tableIndices.addColumn(new Column("PAGES", new IntegerType()));
            tableIndices.addColumn(new Column("FILTER_CONDITION", new StringType()));
            tableIndices.addColumn(new Column("INDEX_TYPE", new StringType()));
            addTable(tableIndices);
        }
        
        //Add AXION_KEYS to hold PK and FK Data for Tables (Imported and Exported Keys)
        {
            Table tableOfPkFk = createSystemTable("AXION_KEYS");
            tableOfPkFk.addColumn(new Column("KEY_SEQ", new ShortType()));
            tableOfPkFk.addColumn(new Column("PKTABLE_CAT", new StringType()));
            tableOfPkFk.addColumn(new Column("PKTABLE_SCHEMA", new StringType()));
            tableOfPkFk.addColumn(new Column("PKTABLE_NAME", new StringType()));
            tableOfPkFk.addColumn(new Column("PK_NAME", new StringType()));
            tableOfPkFk.addColumn(new Column("PKCOLUMN_NAME", new StringType()));
            tableOfPkFk.addColumn(new Column("FKTABLE_CAT", new StringType()));
            tableOfPkFk.addColumn(new Column("FKTABLE_SCHEMA", new StringType()));            
            tableOfPkFk.addColumn(new Column("FKTABLE_NAME", new StringType()));
            tableOfPkFk.addColumn(new Column("FK_NAME", new StringType()));
            tableOfPkFk.addColumn(new Column("FKCOLUMN_NAME", new StringType()));            
            tableOfPkFk.addColumn(new Column("UPDATE_RULE", new ShortType()));
            tableOfPkFk.addColumn(new Column("DELETE_RULE", new ShortType()));
            tableOfPkFk.addColumn(new Column("DEFERRABILITY", new ShortType()));
            addTable(tableOfPkFk);
            addDatabaseModificationListener(new AxionConstraintsMetaTableUpdater(this));
        }    }
    
    protected abstract Table createSystemTable(String name);
    
    protected int getSequenceCount() {
        return _sequences.size();
    }
    
    protected Iterator getSequences() {
        return _sequences.values().iterator();
    }
    
    protected Iterator getTables() {
        return _tables.values().iterator();
    }
    
    @SuppressWarnings("unchecked")
    protected void loadProperties(Properties props) throws AxionException {
        Enumeration keys = props.propertyNames();
        while (keys.hasMoreElements()) {
            String key = (String) (keys.nextElement());
            if (key.startsWith("type.")) {
                addDataType(key.substring("type.".length()), props.getProperty(key));
            } else if (key.startsWith("function.")) {
                addFunction(key.substring("function.".length()), props.getProperty(key));
            } else if (key.startsWith("index.")) {
                addIndexType(key.substring("index.".length()), props.getProperty(key));
            } else if (key.startsWith("table.")) {
                addTableType(key.substring("table.".length()), props.getProperty(key));
            } else if (key.equals("readonly") || key.equals("database.readonly")) {
                String val = props.getProperty(key);
                if ("yes".equalsIgnoreCase(val) || "true".equalsIgnoreCase(val) || "on".equalsIgnoreCase(val)) {
                    _readOnly = true;
                } else {
                    _readOnly = false;
                }
            } else if (key.startsWith("database.timezone")) {
                TimestampType.setTimeZone(props.getProperty(key));
                DateType.setTimeZone(props.getProperty(key));
                TimeType.setTimeZone(props.getProperty(key));
            } else if (key.startsWith("database.")) {
                _globalVariables.put(key.substring("database.".length()).toUpperCase(), props.getProperty(key));
            } else {
                _log.log(Level.WARNING, "Unrecognized property \"" + key + "\".");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private void addDataType(String typename, DataTypeFactory factory) throws AxionException {
        assertNotNull(typename, factory);
        if (null != _dataTypes.get(typename.toUpperCase())) {
            throw new AxionException("A type named \"" + typename + "\" already exists (" + _dataTypes.get(typename.toUpperCase()) + ")");
        }
        _log.log(Level.FINE,"Adding type \"" + typename + "\" (" + factory + ").");
        DataType type = factory.makeNewInstance();
        typename = typename.toUpperCase();
        _dataTypes.put(typename, type);
        DatabaseTypeEvent e = new DatabaseTypeEvent(typename, type);
        Iterator i = getDatabaseModificationListeners().iterator();
        while (i.hasNext()) {
            DatabaseModificationListener cur = (DatabaseModificationListener) i.next();
            cur.typeAdded(e);
        }
    }
    
    private void addDataType(String typename, String factoryclassname) throws AxionException {
        assertNotNull(typename, factoryclassname);
        try {
            DataTypeFactory factory = (DataTypeFactory) (getInstanceForClassName(factoryclassname));
            addDataType(typename, factory);
        } catch (ClassCastException e) {
            throw new AxionException("Expected DataType for \"" + factoryclassname + "\".");
        }
    }
    
    @SuppressWarnings("unchecked")
    private void addFunction(String fnname, FunctionFactory factory) throws AxionException {
        assertNotNull(fnname, factory);
        if (null != _functions.get(fnname.toUpperCase())) {
            throw new AxionException("A function named \"" + fnname + "\" already exists (" + _functions.get(fnname.toUpperCase()) + ")");
        }
        _log.log(Level.FINE,"Adding function \"" + fnname + "\" (" + factory + ").");
        _functions.put(fnname.toUpperCase(), factory);
    }
    
    private void addFunction(String fnname, String factoryclassname) throws AxionException {
        assertNotNull(fnname, factoryclassname);
        try {
            FunctionFactory factory = (FunctionFactory) (getInstanceForClassName(factoryclassname));
            addFunction(fnname, factory);
        } catch (ClassCastException e) {
            throw new AxionException("Expected FunctionFactory for \"" + factoryclassname + "\".");
        }
    }
    
    /**
     * Adds index metadata information to system table for display in console and
     * retrieval via JDBC DatabaseMetaData class.
     *
     * @param index Index to add to system table
     * @param table table associated with <code>index</code>
     */
    private void addIndexMetaEntry(Index index, Table table) throws AxionException {
        // FIXME: TYPE, ASC_OR_DESC, CARDINALITY, and PAGES need to be revisited as
        // placeholder values are returned.
        Row row = new SimpleRow(14);
        row.set(0, null);
        row.set(1, null);
        row.set(2, table.getName());
        row.set(3, Boolean.valueOf(index.isUnique()));
        row.set(4, null);
        row.set(5, index.getName());
        // FIXME: Clarify index types and how they map to those indicated in
        // DatabaseMetaData
        row.set(6, new Short(DatabaseMetaData.tableIndexOther));
        row.set(7, Short.valueOf("1"));
        row.set(8, index.getIndexedColumn().getName());
        // Determine sort order if any and set accordingly
        row.set(9, null);
        // Determine number of unique values in index
        row.set(10, new Short(Short.MAX_VALUE));
        // Determine number of pages used for current index
        row.set(11, new Short(Short.MAX_VALUE));
        row.set(12, null);
        // This column is Axion-specific and indicates the basic indexing strategy
        row.set(13, (index instanceof BaseBTreeIndex) ? "BTREE" : "ARRAY");
        
        this.getTable(SYSTABLE_INDEX_INFO).addRow(row);
    }
    
    @SuppressWarnings("unchecked")
    private void addIndexType(String typename, IndexFactory factory) throws AxionException {
        assertNotNull(typename, factory);
        if (null != _indexTypes.get(typename.toUpperCase())) {
            throw new AxionException("An index type named \"" + typename + "\" already exists (" + _indexTypes.get(typename.toUpperCase()) + ")");
        }
        _log.log(Level.FINE,"Adding index type \"" + typename + "\" (" + factory + ").");
        _indexTypes.put(typename.toUpperCase(), factory);
    }
    
    private void addIndexType(String typename, String factoryclassname) throws AxionException {
        assertNotNull(typename, factoryclassname);
        try {
            IndexFactory factory = (IndexFactory) (getInstanceForClassName(factoryclassname));
            addIndexType(typename, factory);
        } catch (ClassCastException e) {
            throw new AxionException("Expected IndexFactory for \"" + factoryclassname + "\".");
        }
    }
    
    private void addTableType(String typename, String factoryclassname) throws AxionException {
        if (null == typename || null == factoryclassname) {
            throw new AxionException("Neither argument can be null.");
        }
        try {
            TableFactory factory = (TableFactory) (getInstanceForClassName(factoryclassname));
            addTableType(typename, factory);
        } catch (ClassCastException e) {
            throw new AxionException("Expected IndexFactory for \"" + factoryclassname + "\".");
        }
    }
    
    @SuppressWarnings("unchecked")
    private void addTableType(String typename, TableFactory factory) throws AxionException {
        if (null == typename || null == factory) {
            throw new AxionException("Neither argument can be null.");
        }
        if (null != _tableTypes.get(typename.toUpperCase())) {
            throw new AxionException("An table type named \"" + typename + "\" already exists (" + _tableTypes.get(typename.toUpperCase()) + ")");
        }
        _log.log(Level.FINE,"Adding table type \"" + typename + "\" (" + factory + ").");
        _tableTypes.put(typename.toUpperCase(), factory);
    }
    
    private void assertNotNull(Object obj1, Object obj2) throws AxionException {
        if (null == obj1 || null == obj2) {
            throw new AxionException("Neither argument can be null.");
        }
    }
    
    private Object getInstanceForClassName(String classname) throws AxionException {
        try {
            Class clazz = Class.forName(classname);
            return clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new AxionException("Class \"" + classname + "\" not found.");
        } catch (InstantiationException e) {
            throw new AxionException("Unable to instantiate class \"" + classname + "\" via a no-arg constructor.");
        } catch (IllegalAccessException e) {
            throw new AxionException("IllegalAccessException trying to instantiate class \"" + classname + "\" via a no-arg constructor.");
        }
    }
    
    /**
     * @param index
     */
    private void removeIndexMetaEntry(Index index) throws AxionException {
        FunctionIdentifier fnIndexName = new FunctionIdentifier("=");
        fnIndexName.addArgument(new ColumnIdentifier("INDEX_NAME"));
        fnIndexName.addArgument(new Literal(index.getName()));
        
        DeleteCommand cmd = new DeleteCommand(SYSTABLE_INDEX_INFO, fnIndexName);
        try {
            cmd.execute(this);
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to remove mention of index in system tables", ex);
        }
    }
    
    /** Callers should treat the returned Properties as immutable. */
    protected static synchronized Properties getBaseProperties() {
        if (null == _props) {
            _props = new Properties();
            InputStream in = null;
            try {
                in = getBasePropertyStream();
                if (null != in) {
                    _props.load(in);
                } else {
                    _log.log(Level.WARNING,"Could not find axiondb.properties on the classpath.");
                }
            } catch (Exception e) {
                _log.log(Level.SEVERE,"Exception while base properties", e); // PROPOGATE UP!?!
            } finally {
                try {
                    in.close();
                } catch (Exception e) {
                }
            }
        }
        return _props;
    }
    
    private static InputStream getBasePropertyStream() {
        InputStream in = getBasePropertyStreamFromProperty();
        if (null == in) {
            in = getBasePropertyStreamFromClassLoader();
        }
        if (null == in) {
            in = getBasePropertyStreamFromContextClassLoader();
        }
        return in;
    }
    
    private static InputStream getBasePropertyStreamFromClassLoader() {
        try {
            return getPropertyStream(BaseDatabase.class.getClassLoader());
        } catch (Exception e) {
            return null;
        }
    }
    
    private static InputStream getBasePropertyStreamFromContextClassLoader() {
        try {
            return getPropertyStream(Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            return null;
        }
    }
    
    private static InputStream getBasePropertyStreamFromProperty() {
        InputStream in = null;
        String propfile = null;
        try {
            propfile = System.getProperty("org.axiondb.engine.BaseDatabase.properties");
        } catch (Throwable t) {
            propfile = null;
        }
        if (null != propfile) {
            try {
                File file = new File(propfile);
                if (file.exists()) {
                    in = new FileInputStream(file);
                }
            } catch (IOException e) {
                in = null;
            }
        }
        return in;
    }
    
    private static InputStream getPropertyStream(ClassLoader classLoader) {
        InputStream in = null;
        if (null != classLoader) {
            in = classLoader.getResourceAsStream("org/axiondb/axiondb.properties");
            if (null == in) {
                in = classLoader.getResourceAsStream("axiondb.properties");
            }
        }
        return in;
    }
    
    public static final String SYSTABLE_DB_LINKS = "AXION_DB_LINKS";
    public static final String SYSTABLE_INDEX_INFO = "AXION_INDEX_INFO";
    private static Logger _log = Logger.getLogger(BaseDatabase.class.getName());
    private static Properties _props;
    
    private Map _databaseLink = new HashMap();
    private Map _dataTypes = new HashMap();
    private Map _functions = new HashMap();
    private Map _globalVariables = new HashMap();
    private Map _indexTypes = new HashMap();
    private Map _indices = new HashMap();
    private List _listeners = new ArrayList();
    private String _name;
    private boolean _readOnly = false;
    private Map _sequences = new HashMap();
    
    private DatabaseModificationListener _colUpd = new AxionColumnsMetaTableUpdater(this);
    private DatabaseModificationListener _seqUpd = new AxionSequencesMetaTableUpdater(this);
    private TableModificationListener _pkfkUpd = new AxionConstraintsMetaTableUpdater(this);
    
    private Map _tables = new HashMap();
    private Map _tableTypes = new HashMap();
    private TransactionManager _transactionManager = new TransactionManagerImpl(this);
}
