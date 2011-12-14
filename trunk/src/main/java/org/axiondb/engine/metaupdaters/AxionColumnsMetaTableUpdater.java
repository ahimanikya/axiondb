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

package org.axiondb.engine.metaupdaters;

import java.sql.DatabaseMetaData;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.Function;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.Selectable;
import org.axiondb.SelectableBasedConstraint;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.commands.UpdateCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.BaseDatabaseModificationListener;
import org.axiondb.event.ColumnEvent;
import org.axiondb.event.ConstraintEvent;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.event.DatabaseModifiedEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.TableModificationListener;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.util.ValuePool;

/**
 * Updates the <code>AXION_TABLES</code> meta table
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy 
 */
public class AxionColumnsMetaTableUpdater extends BaseDatabaseModificationListener 
        implements DatabaseModificationListener, TableModificationListener {
    private static Logger _log = Logger.getLogger(AxionColumnsMetaTableUpdater.class.getName());
    private Database _db = null;

    public AxionColumnsMetaTableUpdater(Database db) {
        _db = db;
    }

    public void tableAdded(DatabaseModifiedEvent e) {
        try {
            for(int i = 0, I = e.getTable().getColumnCount(); i < I; i++) {
                Column col = e.getTable().getColumn(i);
                Row row = createRowForColumnAdded(e.getTable(), col);
                _db.getTable("AXION_COLUMNS").addRow(row);
            }
        } catch (AxionException ex) {
            _log.log(Level.SEVERE, "Unable to mention table in system tables", ex);
        }
    }

    public void tableDropped(DatabaseModifiedEvent e) {
        Function where = makeEqualFunction(new ColumnIdentifier("TABLE_NAME"),new Literal(e.getTable().getName()));
        AxionCommand cmd = new DeleteCommand("AXION_COLUMNS", where);
        try {
            cmd.execute(_db);
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to remove mention of table in system tables", ex);
        }
    }

    //===================================== TableModificationListener Interface

    public void columnAdded(ColumnEvent e) throws AxionException {
        Row row = createRowForColumnAdded(e.getTable(), e.getColumn());
        _db.getTable("AXION_COLUMNS").addRow(row);
    }

    public void rowInserted(RowEvent event) throws AxionException {
    }

    public void rowDeleted(RowEvent event) throws AxionException {
    }

    public void rowUpdated(RowEvent event) throws AxionException {
    }

    public void constraintAdded(ConstraintEvent event) throws AxionException {
    }

    public void constraintRemoved(ConstraintEvent event) throws AxionException {
    }

    public void updateNullableStatus(ConstraintEvent event, boolean changeNullableTo) {
        Constraint c = event.getConstraint();
        if(c instanceof NotNullConstraint || c instanceof PrimaryKeyConstraint) {
            SelectableBasedConstraint nn = (SelectableBasedConstraint)c;
            for(int i = 0; i < nn.getSelectableCount(); i++) {
                try {
                    AxionCommand cmd = createUpdateNullableCmd(
                        event.getTable(),
                        nn.getSelectable(i).getLabel(),
                        changeNullableTo);
                    cmd.execute(_db);
                } catch (AxionException ex) {
                    _log.log(Level.SEVERE,"Unable to mark nullable status in system tables", ex);
                }
            }
        }
    }

    protected Row createRowForColumnAdded(Table t, Column col) throws AxionException {
        boolean isnullable = isNullable(t,col.getName());
        Integer nullableInt = ValuePool.getInt(isnullable ? 
                                          DatabaseMetaData.columnNullable :
                                          DatabaseMetaData.columnNoNulls);
        String nullableString = isnullable ? "YES" : "NO";

        Short typeVal = new Short((short)col.getDataType().getJdbcType());
        Integer size = ValuePool.getInt(col.getDataType().getPrecision());
        Integer scale = ValuePool.getInt(col.getDataType().getScale());;
        Integer radix = ValuePool.getInt(col.getDataType().getPrecisionRadix());
        Selectable selDef = col.getDefault();
        String colDef = selDef != null ? selDef.toString(): null;
        
        boolean isAutoIncr = col.isIdentityColumn();
        
        String autoIncrStr = isAutoIncr ? "YES" : "NO";        
                
        String sqlType = col.getSqlType();
        if(sqlType == null){
            sqlType = col.getDataType().getTypeName().toUpperCase();
        } else {
            sqlType = sqlType.toUpperCase();
        }
        int ord = t.getColumnIndex(col.getName());

        SimpleRow row = new SimpleRow(23);
        row.set(0, "");                             // table_cat
        row.set(1, "");                             // table schem
        row.set(2, t.getName());                    // table_name
        row.set(3, col.getName().toUpperCase());    // column_name
        row.set(4, typeVal);                        // data_type
        row.set(5, sqlType);                        // type_name
        row.set(6, size);                           // column_size
        row.set(7, null);                           // buffer_length (unused)
        row.set(8, scale);                          // decimal_digits
        row.set(9, radix);                          // num_prec_radix
        row.set(10, nullableInt);                   // nullable
        row.set(11, null);                          // remarks
        row.set(12, colDef);                        // column_def
        row.set(13, null);                          // sql_data_type (unused)
        row.set(14, null);                          // sql_datetime_sub (unused)
        row.set(15, null);                          // char_octet_length
        row.set(16, ValuePool.getInt(ord+1));       // ordinal_position
        row.set(17, nullableString);                // is_nullable
        row.set(18, null);                          // scope_catalog
        row.set(19, null);                          // scope_schema
        row.set(20, null);                          // scope_table
        row.set(21, null);                          // source_data_type
        row.set(22, autoIncrStr);                   // IS_AUTOINCREMENT
        
        return row;
    }

    private UpdateCommand createUpdateNullableCmd(Table t, String colName, boolean isnullable) {
        Integer nullableInt = ValuePool.getInt(isnullable ? 
                                          DatabaseMetaData.columnNullable :
                                          DatabaseMetaData.columnNoNulls);
        String nullableString = isnullable ? "YES" : "NO";

        Selectable tableMatch = makeEqualFunction(new ColumnIdentifier("TABLE_NAME"),new Literal(t.getName()));
        
        Selectable colMatch = makeEqualFunction(new ColumnIdentifier("COLUMN_NAME"),new Literal(colName.toUpperCase()));

        FunctionIdentifier where = new FunctionIdentifier("and");
        where.addArgument(tableMatch);
        where.addArgument(colMatch);

        UpdateCommand cmd = new UpdateCommand();
        cmd.setTable(new TableIdentifier("AXION_COLUMNS"));
        cmd.addColumn(new ColumnIdentifier("NULLABLE"));
        cmd.addValue(new Literal(nullableInt));
        cmd.addColumn(new ColumnIdentifier("IS_NULLABLE"));
        cmd.addValue(new Literal(nullableString));
        cmd.setWhere(where);
        return cmd;
    }

    private boolean isNullable(Table table, String column) {
        // FIXME: this is a little hack       
        for(Iterator iter = table.getConstraints(); iter.hasNext(); ) {
            Constraint c = (Constraint)iter.next();
            if(c instanceof NotNullConstraint || c instanceof PrimaryKeyConstraint) {
                SelectableBasedConstraint nn = (SelectableBasedConstraint)c;
                for(int i=0;i<nn.getSelectableCount();i++) {
                    if(column.equals(nn.getSelectable(i).getLabel())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }    
    
    private FunctionIdentifier makeEqualFunction(Selectable left, Selectable right) {
        FunctionIdentifier function = new FunctionIdentifier("=");
        function.addArgument(left);
        function.addArgument(right);
        return function;
    }
}

