/*
 * 
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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
package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.IndexFactory;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TransactableTable;
import org.axiondb.constraints.BaseSelectableBasedConstraint;
import org.axiondb.constraints.CheckConstraint;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.engine.tables.BaseFlatfileTable;
import org.axiondb.engine.tables.ExternalDatabaseTable;
import org.axiondb.engine.visitors.TableColumnsUsedInFunctionVisitor;
import org.axiondb.io.FileUtil;
import org.axiondb.jdbc.AxionResultSet;

/**
 * A <tt>ALTER TABLE tableName ADD | DROP |ALTER column definition <tt> command.
 *
 * <p> NOTE: Identity column can't be altered.
 * <p> NOTE: Renaming column require cascade to drop constraints and indexes and then rename
 * <p> TODO: ADD [COLUMN] .... [BEFORE <existingcolumn>]
 *
 * @version  
 * @author Ahimanikya Satapathy
 */
public class AlterTableCommand extends BaseAxionCommand {

    public AlterTableCommand(String theTableName, boolean cascade) {
        _tableName = theTableName;
        _cascade = cascade;
        _createTempTableCmd = new CreateTableCommand(generateName());
    }

    @SuppressWarnings("unchecked")
    public void addChildCommand(AxionCommand cmd) {
        _childCommands.add(cmd);
    }

    public void addColumn(String name, String type, String precision, String scale, Selectable defaultValue, String generated) {
        _createTempTableCmd.addColumn(name, type, precision, scale, defaultValue, generated);
    }

    public void alterColumn(String name, String newName, Selectable newDefault, Boolean dropDefault) {
        _createTempTableCmd.alterColumn(name, newName, newDefault, dropDefault);
        _alterColumn = (newName == null ? null : name);
    }

    public void dropColumn(String colName) {
        _createTempTableCmd.excludeColumn(colName);
    }

    public boolean execute(Database db) throws AxionException {
        executeUpdate(db);
        return false;
    }

    /** Unsupported */
    public AxionResultSet executeQuery(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use execute.");
    }

    public int executeUpdate(Database db) throws AxionException {
        assertNotReadOnly(db);

        Table table = db.getTable(_tableName);
        if (null == table) {
            throw new AxionException("Table " + _tableName + " not found.");
        }

        List depedentViews = db.getDependentViews(_tableName);
        if (depedentViews.size() > 0) {
            if (_cascade) {
                db.dropDependentViews(depedentViews);
            } else {
                throw new AxionException("Can't Atlter Table/View: " + _tableName + " has reference in another View...");
            }
        }

        if (_newTableName != null) {
            db.renameTable(_tableName, _newTableName);
            setEffectedRowCount(0);
            return 0;
        }

        // create temp table using "create-as-subquery" command
        _createTempTableCmd.setSourceTable(table);

        // 2. copy old table index and constraint information
        List indexesToAdd = checkAndMigrateIndexes(db, table);
        List constraintToAdd = checkAndMigrateConstraints(table);

        handleExternalTable(table);
        _createTempTableCmd.execute(db);

        // 3. drop old table
        table.shutdown();
        db.dropTable(_tableName);

        // 4. rename new table to old table
        if (_origFlatFileDir != null){
            Properties prop = new Properties();
            prop.put(BaseFlatfileTable.PARAM_KEY_FILE_DIR, _origFlatFileDir);
            prop.put(BaseFlatfileTable.PARAM_KEY_FILE_NAME, _origFlatFileName);
            prop.put(BaseFlatfileTable.PARAM_KEY_TEMP_FILE_NAME, _tempFileName);
            db.renameTable(_createTempTableCmd.getObjectName(), _tableName, prop);
        } else {
            db.renameTable(_createTempTableCmd.getObjectName(), _tableName);
        }


        table = db.getTable(_tableName);

        // 5. restore and populate index and constraint for the new table
        for (Iterator iter = indexesToAdd.iterator(); iter.hasNext();) {
            db.addIndex(((Index) iter.next()), table, true);
        }

        for (Iterator iter = constraintToAdd.iterator(); iter.hasNext();) {
            table.addConstraint((Constraint) iter.next());
        }

        for (Iterator iter = _childCommands.iterator(); iter.hasNext();) {
            AxionCommand cmd = (AxionCommand) (iter.next());
            cmd.execute(db);
        }

        int rowcount = table.getRowCount();
        setEffectedRowCount(rowcount);
        return rowcount;
    }

    public void setRenameTo(String newName) {
        _newTableName = newName;
    }

    private List checkAndMigrateConstraints(Table table) throws AxionException {
        // if column name has been changed/droped, check whether oldname has references
        List constraintToAdd = new ArrayList();
        for (Iterator iter = table.getConstraints(); iter.hasNext();) {
            Constraint constraint = (Constraint) iter.next();
            if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = ((ForeignKeyConstraint) constraint);
                List cols = Collections.EMPTY_LIST;
                if (_tableName.equals(fk.getParentTableName())) {
                    cols = fk.getParentTableColumns();
                } else if (_tableName.equals(fk.getChildTableName())) {
                    cols = fk.getChildTableColumns();
                }
                checkColumnReference(constraintToAdd, fk, cols);

            } else if (constraint instanceof BaseSelectableBasedConstraint) {
                BaseSelectableBasedConstraint c = (BaseSelectableBasedConstraint) constraint;
                for (int k = 0, K = c.getSelectableCount(); k < K; k++) {
                    checkColumnReference(table, constraintToAdd, c, c.getSelectable(k));
                }

            } else if (constraint instanceof CheckConstraint) {
                checkColumnReference(table, constraintToAdd, constraint, ((CheckConstraint) constraint).getCondition());
            } else if (_alterColumn != null && !_cascade) {
                throw new AxionException("Can't drop/alter Column: " + constraint.getName() + " might have reference ");
            }
        }
        return constraintToAdd;
    }

    @SuppressWarnings("unchecked")
    private List checkAndMigrateIndexes(Database db, Table table) throws AxionException {
        List indexesToAdd = new ArrayList();
        for (Iterator iter = table.getIndices(); iter.hasNext();) {
            Index i = (Index) iter.next();
            Column col = i.getIndexedColumn();
            if (_createTempTableCmd.isColumnEexcluded(col.getName()) && !_cascade) {
                throw new AxionException("Can't drop Column: Index exist for " + col.getName());
            } else if (col.getName().equals(_alterColumn) && !_cascade) {
                throw new AxionException("Can't alter Column: Index exist for " + col.getName());
            } else if (!_createTempTableCmd.isColumnEexcluded(col.getName()) && !col.getName().equals(_alterColumn)) {
                IndexFactory factory = db.getIndexFactory(i.getType());
                Index index = factory.makeNewInstance(i.getName(), col, i.isUnique(), db.getDBDirectory() == null);
                indexesToAdd.add(index);
            }
        }
        return indexesToAdd;
    }

    private void checkColumnReference(List constraintToAdd, Constraint c, Collection cols) throws AxionException {
        for (Iterator i = cols.iterator(); i.hasNext();) {
            checkColumnReference(constraintToAdd, c, (ColumnIdentifier) i.next());
        }
    }

    @SuppressWarnings("unchecked")
    private void checkColumnReference(List constraintToAdd, Constraint c, ColumnIdentifier col) throws AxionException {
        if (_createTempTableCmd.isColumnEexcluded(col.getName()) && !_cascade) {
            throw new AxionException("Can't drop Column: " + col.getName() + " used in constraint " + c.getName());
        } else if (col.getName().equals(_alterColumn) && !_cascade) {
            throw new AxionException("Can't alter Column: " + col.getName() + " used in constraint " + c.getName());
        } else if(!_createTempTableCmd.isColumnEexcluded(col.getName()) && !col.getName().equals(_alterColumn)) {
            constraintToAdd.add(c);
        }
    }

    private void checkColumnReference(Table table, List constraintToAdd, Constraint c, Selectable sel) throws AxionException {
        if (sel instanceof ColumnIdentifier) {
            checkColumnReference(constraintToAdd, c, (ColumnIdentifier) sel);
        } else if (sel instanceof Function) {
            TableColumnsUsedInFunctionVisitor visitor = new TableColumnsUsedInFunctionVisitor();
            visitor.visit((Function) sel, table);
            checkColumnReference(constraintToAdd, c, visitor.getColumnsUsedInFunction());
        }
    }

    private void handleExternalTable(Table table) {
        String origDataFilePath = null;
        String dirPath = null;

        // Do not try to defragmet external database tables.
        if (table instanceof ExternalDatabaseTable){
            return;
        }

        while (table instanceof TransactableTable) {
            table = ((TransactableTable) table).getTable();
        }

        // Carry the table properties for external table
        if (table instanceof ExternalTable) {
            Properties prop = new Properties();
            prop.putAll(((ExternalTable) table).getTableProperties());
            origDataFilePath = prop.getProperty(BaseFlatfileTable.PROP_FILENAME);
            if (origDataFilePath != null){
                dirPath = FileUtil.getDirecoryFromPath(origDataFilePath);
                if (dirPath != null){
                    prop.put(BaseFlatfileTable.PROP_FILENAME, dirPath + _createTempTableCmd.getObjectName() );
                    _origFlatFileName = origDataFilePath.substring(dirPath.length());
                    _origFlatFileDir = dirPath;
                    _tempFileName = _createTempTableCmd.getObjectName();
                }else{
                    prop.remove(BaseFlatfileTable.PROP_FILENAME); // clear file
                }
            }

            // Override to TRUE if property is present.
            if ((_newTableName == null) && (prop.get(ExternalTable.PROP_CREATE_IF_NOT_EXIST) != null)){
                prop.put(ExternalTable.PROP_CREATE_IF_NOT_EXIST, "TRUE");
            }
            _createTempTableCmd.setProperties(prop);
            _createTempTableCmd.setType("EXTERNAL");
        }
    }

    private static int _idCounter = 0;

    private static String generateName() {
        return "TEMP_" + Long.toHexString(System.currentTimeMillis()).toUpperCase() + _idCounter++;
    }

    private String _alterColumn;
    private boolean _cascade = false;
    private List _childCommands = new ArrayList(4);
    private CreateTableCommand _createTempTableCmd;
    private String _newTableName;
    private String _tableName;
    private String _origFlatFileName;
    private String _origFlatFileDir;
    private String _tempFileName;
}
