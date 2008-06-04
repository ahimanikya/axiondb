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

package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Selectable;
import org.axiondb.SelectableBasedConstraint;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableFactory;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.tables.ExternalTableFactory;

/**
 * A <code>CREATE [<i>TYPE</i>] TABLE</code> command.
 * 
 * @version  
 * @author Chuck Burdick
 * @author James Strachan
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class CreateTableCommand extends CreateCommand {

    public CreateTableCommand() {
    }

    public CreateTableCommand(String tableName) {
        setObjectName(tableName);
    }

    @SuppressWarnings("unchecked")
    public void addChildCommand(AxionCommand cmd) {
        _childCommands.add(cmd);
    }

    public void addColumn(String name, String datatypename) {
        addColumn(name, datatypename, null, null, null, null);
    }

    public void addColumn(String name, String datatypename, String precision) {
        addColumn(name, datatypename, precision, null, null, null);
    }

    public void addColumn(String name, String datatypename, String precision, String scale, Object defaultValue) {
        addColumn(name, datatypename, precision, scale, defaultValue, null);
    }

    @SuppressWarnings("unchecked")
    public void addColumn(String name, String datatypename, String precision, String scale, Object defaultValue, String generated) {
        _columnNames.add(name);
        _dataTypes.add(datatypename);
        _defaults.add(defaultValue);
        _columnSize.add(precision);
        _columnScale.add(scale);
        _generated.add(generated);
    }
    
    @SuppressWarnings("unchecked")
    public void alterColumn(String name, String newName, Selectable newDefault, Boolean dropDefault) {
        _alterColumnNames.add(name);
        _newColumnNames.add(newName);
        _newDefaults.add(newDefault);
        _dropdefaults.add(dropDefault);
    }

    @SuppressWarnings("unchecked")
    public void excludeColumn(String colName) {
        _excludeSourceColumns.add(_columnsAreCaseSensitive ? colName: colName.toUpperCase());
    }

    public boolean isColumnEexcluded(String colName) {
        return _excludeSourceColumns.contains(_columnsAreCaseSensitive ? colName: colName.toUpperCase());
    }

    public boolean execute(Database db) throws AxionException {
        synchronized (CreateTableCommand.class) {
            updateColumnNames();
            assertNotReadOnly(db);
            if (!db.hasTable(getObjectName())) {

                _columnsAreCaseSensitive = Boolean.valueOf((String) _tableOrganization.get(ExternalTable.COLUMNS_ARE_CASE_SENSITIVE)).booleanValue();

                // Handle CREATE TABLE AS
                buildColumnsFromSourceTable(db);
                if (_sourceTable != null && _subQuery == null) {
                    buildSubQuery();
                }

                buildColumns(db);
                applyAlterColumn();

                if (_sourceTable != null && _excludeSourceColumns.size() == _sourceTable.getColumnCount()) {
                    throw new AxionException("Can't drop last column " + _excludeSourceColumns);
                }

                TableFactory factory = db.getTableFactory(_type);
                if (factory == null) {
                    throw new AxionException("Unknown table type (" + _type + ")");
                }

                Table table = null;
                if (factory instanceof ExternalTableFactory) {
                    table = ((ExternalTableFactory) factory).createTable(db, getObjectName(), _tableOrganization, _columns);
                } else {
                    table = factory.createTable(db, getObjectName());
                    Iterator iter = _columns.iterator();
                    while (iter.hasNext()) {
                        table.addColumn((Column) iter.next());
                    }
                }

                db.addTable(table);
                try {
                    for (Iterator iter = _childCommands.iterator(); iter.hasNext();) {
                        AxionCommand cmd = (AxionCommand) (iter.next());
                        cmd.execute(db);
                    }

                    // persist sequences if any for the identity column.
                    if (_sequence != null) {
                        table.setSequence(_sequence);
                    }

                    if (_createTableWithData) {
                        populateData(db, table);
                    }
                } catch (AxionException e) {
                    db.dropTable(getObjectName());
                    throw e;
                }

            } else if (!isIfNotExists()) {
                throw new AxionException("A table/view named \"" + getObjectName().toUpperCase() + "\" already exists.");
            }
        }
        return false;
    }

    public AxionCommand getChildCommand(int i) {
        return (AxionCommand) (_childCommands.get(i));
    }

    public int getChildCommandCount() {
        return _childCommands.size();
    }

    @SuppressWarnings("unchecked")
    public List getColumnNames() {
        return Collections.unmodifiableList(_columnNames);
    }

    public String getType() {
        return _type;
    }

    public void setProperties(Properties prop) {
        _tableOrganization = prop;
    }

    public void setSourceTable(Table table) {
        _sourceTable = table;
    }

    public void setSubQuery(SubSelectCommand subQuery) {
        _subQuery = subQuery;
    }

    public void setType(String type) {
        _type = type;
    }

    public void setCreateTableWithData(boolean createTableWithData) {
        _createTableWithData = createTableWithData;
    }

    @SuppressWarnings("unchecked")
    private void updateColumnNames(){
        _columnsAreCaseSensitive = Boolean.valueOf((String) _tableOrganization.get(ExternalTable.COLUMNS_ARE_CASE_SENSITIVE)).booleanValue();
        if ((!_columnsAreCaseSensitive)){
            if ((_columnNames != null) && (_columnNames.size() > 0)){
                List tmpList = this._columnNames;
                _columnNames = new ArrayList(tmpList.size());
                int size = tmpList.size(); 
                for (int i =0; i < size; i++){
                    _columnNames.add(((String) tmpList.get(i)).toUpperCase());
                }                
            }    
            
            if ((this._childCommands != null) && (this._childCommands.size() > 0)){
                List tempConstraints = this._childCommands; 
                this._childCommands = new ArrayList(tempConstraints.size());
                int size = tempConstraints.size();
                AddConstraintCommand ac = null;
                
                for (int i =0; i < size; i++){
                    ac = (AddConstraintCommand) tempConstraints.get(i);
                    Constraint constraint = ac.getConstraint();
                    SelectableBasedConstraint sbConstraint = null;
                    Selectable sel = null;
                    
                    if (constraint instanceof SelectableBasedConstraint){
                        sbConstraint = (SelectableBasedConstraint) constraint;
                        sel = sbConstraint.getSelectable(0);
                        
                        if (sel instanceof ColumnIdentifier){
                            ((ColumnIdentifier)sel).setName(sel.getName().toUpperCase());
                        }
                    }
                    this._childCommands.add(ac);                        
                }
            }            
        }
    }
    
    @SuppressWarnings("unchecked")
    private void buildColumns(Database db) throws AxionException {
        Set existingNames = new HashSet(_columnNames.size());
        for (int i = 0, I = _columnNames.size(); i < I; i++) {
            String typeStr = (String) _dataTypes.get(i);
            String generated = (String) _generated.get(i);

            // If the column is derived then set the column to StringType...
            if (typeStr == null && generated != null && generated.equals(Column.GENERATED_ALWAYS)) {
                typeStr = "string";
            }

            DataType type = db.getDataType(typeStr);
            if (null == type) {
                try {
                    type = (DataType) (Class.forName((String) (_dataTypes.get(i))).newInstance());
                } catch (Exception e) {
                    type = null;
                }
            } else {
                // Clone this instance of datatype so that its precision and scale can be
                // customized
                type = type.makeNewInstance();
            }

            if (null == type) {
                throw new AxionException("Type " + _dataTypes.get(i) + " not recognized.");
            }

            String normalizedColumnName = _columnsAreCaseSensitive ? (String) _columnNames.get(i) : ((String) _columnNames.get(i)).toUpperCase();
            if (existingNames.contains(normalizedColumnName)) {
                throw new AxionException("Duplicate column name " + _columnNames.get(i) + ".");
            }
            existingNames.add(normalizedColumnName);

            Selectable defaultValue = null;
            CreateSequenceCommand createSeqCmd = null;
            Object def = _defaults.get(i);
            if (def instanceof Selectable) {
                defaultValue = (Selectable) def;
            } else if (def instanceof CreateSequenceCommand) {
                if (_sequence == null) {
                    createSeqCmd = (CreateSequenceCommand) def;
                    _sequence = createSeqCmd.createSequence(db);
                } else {
                    throw new AxionException("Can't have more than one Identity columns");
                }
            }

            String sizeSpec = (String) _columnSize.get(i);
            if (null != sizeSpec && type instanceof DataType.NonFixedPrecision) {
                try {
                    Integer size = new Integer(sizeSpec);
                    ((DataType.NonFixedPrecision) type).setPrecision(size.intValue());
                } catch (NumberFormatException e) {
                    // ignore, use default
                }

                String scaleSpec = (String) _columnScale.get(i);
                if (null != scaleSpec && type instanceof DataType.ExactNumeric) {
                    try {
                        Integer scale = new Integer(scaleSpec);
                        ((DataType.ExactNumeric) type).setScale(scale.intValue());
                    } catch (NumberFormatException e) {
                        // ignore, use default
                    }
                }
            }

            Column col = new Column((String) _columnNames.get(i), type, defaultValue);            
            col.setSqlType((String) _dataTypes.get(i));

            if (createSeqCmd != null) {
                col.setIdentityType(createSeqCmd.getIdentityType());
            }

            if (generated != null && generated.equals(Column.GENERATED_ALWAYS)) {
                col.setGeneratedColType(generated);
            }

            _columns.add(col);
        }
    }

    @SuppressWarnings("unchecked")
    private void applyAlterColumn() throws AxionException {
        TableIdentifier trgtTid = new TableIdentifier(getObjectName());
        for (int i = 0, I = _columns.size(), K =_alterColumnNames.size(); i < I && K > 0; i++) {
            Column col = (Column) _columns.get(i);
            if (!col.isDerivedColumn() && !col.isIdentityColumn()) {
                for (int j = 0, J = _alterColumnNames.size();  j <  J; j++) {
                    if (col.getName().equals(_alterColumnNames.get(j))) {
                        String newColName = _newColumnNames.get(j) == null ? col.getName() : (String) _newColumnNames.get(j);
                        Selectable newDefault = _newDefaults.get(j) == null ? col.getDefault() : (Selectable) _newDefaults.get(j);
                        newDefault = _dropdefaults.get(j) == null ? newDefault : null;
                        Column newCol = new Column(newColName, col.getDataType(), newDefault);
                        newCol.setGeneratedColType(col.getGeneratedColType());
                        newCol.setIdentityType(col.getIdentityType());
                        newCol.setSqlType(col.getSqlType());
                        _alterColumnNames.remove(j);

                        _columns.set(i, newCol);
                        ColumnIdentifier colid = new ColumnIdentifier(trgtTid, col.getName(), null, col.getDataType());
                        ColumnIdentifier newcolid = new ColumnIdentifier(trgtTid, newCol.getName(), null, newCol.getDataType());
                        _colIdsForInsert.set(_colIdsForInsert.indexOf(colid), newcolid);
                    }
                }
            } else {
                throw new AxionException("Can't rename Generated columns");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void buildColumnsFromSourceTable(Database db) throws AxionException {
        // Create table from view, used in CREATE TABLE AS VIEW command
        if (_subQuery != null) {
            _sourceTable = _subQuery.getTableView(db, null);
        }

        // resolve columns to be droped, used in ALTER TABLE DROP COLUMN
        if (_sourceTable != null) {
            Iterator iter = _excludeSourceColumns.iterator();
            while (iter.hasNext()) {
                String columnName = (String) iter.next();
                Column col = _sourceTable.getColumn(columnName);
                if (col == null) {
                    throw new AxionException("Column not found " + columnName);
                }
            }

            // check for duplicate column name
            iter = _columnNames.iterator();
            while (iter.hasNext()) {
                String columnName = (String) iter.next();
                Column col = _sourceTable.getColumn(columnName);
                if (col != null) {
                    throw new AxionException("Column Already Exists " + columnName);
                }
            }

            // Add column definitions from the source Table/View to new table
            TableIdentifier trgtTid = new TableIdentifier(getObjectName());
            for (int i = 0, count = _sourceTable.getColumnCount(); i < count; i++) {
                Column col = _sourceTable.getColumn(i);
                if (!_excludeSourceColumns.contains(_columnsAreCaseSensitive ? col.getName() : col.getName().toUpperCase())) {
                    _columns.add(col);

                    if (!col.isDerivedColumn()) {
                        ColumnIdentifier colid = new ColumnIdentifier(trgtTid, col.getName(), null, col.getDataType());
                        _colIdsForInsert.add(colid);
                    }
                }
            }
        }
    }

    private void buildSubQuery() {
        AxionQueryContext ctx = new AxionQueryContext();
        TableIdentifier srctid = new TableIdentifier(_sourceTable.getName());
        ctx.addFrom(srctid);
        Iterator iter = _columns.iterator();
        while (iter.hasNext()) {
            Column col = (Column) iter.next();
            if (!col.isDerivedColumn()) {
                ColumnIdentifier colid = new ColumnIdentifier(srctid, col.getName(), null, col.getDataType());
                ctx.addSelect(colid);
            }
        }

        _subQuery = new SubSelectCommand(ctx);
    }

    private void populateData(Database db, Table table) throws AxionException {
        if (_sourceTable != null) {
            // prepare insert select command and add as a child
            TableIdentifier tid = new TableIdentifier(table.getName());
            InsertCommand insertSelectCmd = new InsertCommand(tid, _colIdsForInsert, _subQuery);
            insertSelectCmd.execute(db);
        }
    }

    private List _childCommands = new ArrayList();
    private List _colIdsForInsert = new ArrayList();
    private List _columnNames = new ArrayList();
    private List _columns = new ArrayList();
    private List _columnScale = new ArrayList();
    private Sequence _sequence;

    private List _alterColumnNames = new ArrayList();
    private List _newColumnNames = new ArrayList();
    private List _newDefaults = new ArrayList();
    private List _dropdefaults = new ArrayList();

    private List _columnSize = new ArrayList(); // precision
    private List _dataTypes = new ArrayList();
    private List _defaults = new ArrayList();
    private List _generated = new ArrayList();
    private Set _excludeSourceColumns = new HashSet();
    private Table _sourceTable;
    private SubSelectCommand _subQuery;
    private Properties _tableOrganization = new Properties();
    private String _type;
    private boolean _createTableWithData = true;
    private boolean _columnsAreCaseSensitive = false;
}
