/*
 * 
 * =======================================================================
 * Copyright (c) 2004-2005 Axion Development Team.  All rights reserved.
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
package org.axiondb.engine.visitors;

import java.util.ArrayList;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.SequenceEvaluator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.VariableContext;
import org.axiondb.engine.commands.SelectCommand;
import org.axiondb.engine.commands.SubSelectCommand;
import org.axiondb.functions.CastAsFunction;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.util.Utils;

/**
 * Resolves a (@link Selectable) for a given (@link Database)
 * 
 * @author Ahimanikya Satapathy
 */
public class ResolveSelectableVisitor {

    private Database _db = null;

    public ResolveSelectableVisitor(Database db) {
        _db = db;
    }

    public Selectable visit(ColumnIdentifier column, List selected, TableIdentifier[] tables) throws AxionException {
        Selectable result = column;
        boolean found = false;

        if (selected != null && !selected.isEmpty()) {
            String aliasName = column.getName();
            
            for(int i = 0, I = selected.size(); i < I; i++) {
                Selectable sel = (Selectable) selected.get(i);
                if (sel instanceof ColumnIdentifier && (aliasName.equals(sel.getAlias()) || (aliasName.equals(sel.getName())))) {
                    // if alias is qualified by tableName, then check for a match
                    ColumnIdentifier colid = (ColumnIdentifier) sel;
                    if (matchTableName(column, colid)) {
                        // In case of Insert Select same column with diffrent alias
                        // is used in select cluase
                        String resolvedAlias = colid.getAlias();
                        String colAlias = column.getAlias();
                        if(resolvedAlias != null && colAlias != null && !resolvedAlias.equals(colAlias)) {
                            column.setTableIdentifier(colid.getTableIdentifier());
                            column.setDataType(colid.getDataType());
                            column.setName(column.getName());
                        } else {
                            column = colid;
                        }
                        break;
                    }
                } else if (aliasName.equals(sel.getAlias())) {
                    // assumption: sel has been resolved before
                    result = sel;
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            if (isAlreadyResolved(column)) {
                result = column;
            } else if (identifiesSequence(column)) {
                // Note: sequenceName.NEXTVAL, parser create a ColumnIdentifier
                // and sets table name as sequenceName and column name as NEXTVAL
                Sequence seq = _db.getSequence(column.getTableName());
                result = new SequenceEvaluator(seq, column.getName());
            } else {
                result = resolveTrueColumn(column, tables);
            }
        }

        return setVariableContext(result);
    }

    public Selectable visit(FunctionIdentifier fn, List selected, TableIdentifier[] tables) throws AxionException {
        ConcreteFunction cfn = _db.getFunction(fn.getName());
        if (null == cfn) {
            throw new AxionException("No function matching " + fn.getName());
        }

        // special handling for cast as, we want to get the user supplied
        // literal data type then change the data type of literal based on
        // what we get from the database
        boolean isCastAsFunction = (cfn instanceof CastAsFunction) ? true : false;
        if (isCastAsFunction) {
            List args = new ArrayList(2);
            Literal literal = (Literal) fn.getArgument(1);

            DataType targetType = _db.getDataType(literal.getName());
            
            if (Utils.isPrecisionRequired(targetType.getJdbcType())){
            	targetType = targetType.makeNewInstance();
            }
            
            if (targetType != null) {
                if (targetType instanceof DataType.NonFixedPrecision) {
                    if (targetType instanceof DataType.ExactNumeric) {
                        Literal scaleVal = (Literal) fn.getArgument(3);
                        int scale = Integer.parseInt(scaleVal.evaluate().toString());
                        ((DataType.ExactNumeric) targetType).setScale(scale);
                    }

                    Literal precisionVal = (Literal) fn.getArgument(2);
                    int precision = Integer.parseInt(precisionVal.evaluate().toString());
                    ((DataType.NonFixedPrecision) targetType).setPrecision(precision);
                }
                literal.setDataType(targetType);
            } else {
                throw new AxionException("invalid data type: " + literal.getName());
            }

            args.add(fn.getArgument(0));
            args.add(fn.getArgument(1));

            String alias = fn.getAlias();
            fn = new FunctionIdentifier(fn.getName(), args);
            fn.setAlias(alias);
        }

        for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
            cfn.addArgument(visit(fn.getArgument(i), selected, tables));
        }

        if (!cfn.isValid()) {
            throw new AxionException("Function " + cfn + " isn't valid.");
        }
        cfn.setAlias(fn.getAlias());

        return setVariableContext(cfn);
    }

    /**
     * "Resolve" the given {@link Selectable}relative to the given list of
     * {@link TableIdentifier tables}, converting aliased or relative references into
     * absolute ones.
     */
    public Selectable visit(Selectable selectable, List selected, TableIdentifier[] tables) throws AxionException {
        Selectable result = selectable;

        if (selectable instanceof ColumnIdentifier) {
            result = visit((ColumnIdentifier) selectable, selected, tables);
        } else if (selectable instanceof FunctionIdentifier) {
            result = visit((FunctionIdentifier) selectable, selected, tables);
        } else if (selectable instanceof ConcreteFunction) {
            result = selectable;
        } else if (selectable instanceof BindVariable) {
            result = selectable;
        } else if (selectable instanceof Literal) {
            result = selectable;
        } else if (selectable instanceof SelectCommand) {
            result = visit((SubSelectCommand) selectable, tables);
        } else if (null != selectable) {
            throw new AxionException("Couldn't resolve Selectable " + selectable);
        }

        return setVariableContext(result);
    }

    public Selectable visit(SubSelectCommand select, TableIdentifier[] tables) throws AxionException {
        select.setDB(_db);
        select.setParentTables(tables);
        return setVariableContext(select);
    }

    private boolean identifiesSequence(ColumnIdentifier column) {
        return column.getTableName() != null && _db.getSequence(column.getTableName()) != null;
    }

    private boolean isAlreadyResolved(ColumnIdentifier column) {
        return column.getTableName() != null && column.getTableAlias() != null && column.getDataType() != null;
    }

    private boolean matchTableName(ColumnIdentifier alias, ColumnIdentifier colid) {
        String aliasTableName = alias.getTableName();
        String tableName = colid.getTableName();
        String aliasName = colid.getTableAlias();

        if (aliasTableName == null) { // No qualifire used
            return true;
        } else if (tableName != null && aliasTableName.equals(tableName)) {
            return true;
        } else if (tableName != null && aliasTableName.equals(aliasName)) {
            return true;
        }
        return false;
    }

    private Selectable resolveTrueColumn(ColumnIdentifier column, TableIdentifier[] tables) throws AxionException {
        for (int i = 0; null != tables && i < tables.length; i++) {
            if (!_db.hasTable(tables[i])) {
                throw new AxionException(42704);
            }

            if (resolveTrueColumnForTable(column, tables[i])) {
                return column;
            }
        }
        throw new AxionException(42703);
    }

    private boolean resolveTrueColumnForTable(ColumnIdentifier column, TableIdentifier tableId) throws AxionException {
        if (null != tableId.getTableAlias() && tableId.getTableAlias().equals(column.getTableName())) {
            // if the column's table name is the table's alias name,
            // the column belongs to this table
            column.setTableIdentifier(tableId);
            return isStar(column) ? true : resolveDataType(column, _db.getTable(tableId));
        } else if (tableId.getTableName().equals(column.getTableName())) {
            // else if the column's table name is the table's name
            // the column belongs to this table
            column.setTableIdentifier(tableId);
            return isStar(column) ? true : resolveDataType(column, _db.getTable(tableId));
        } else if (null == column.getTableName()) {
            // if the column has no table name
            if (isStar(column)) {
                return true;
            } else if (resolveDataType(column, _db.getTable(tableId))) {
                column.setTableIdentifier(tableId);
                return true;
            }
        }
        return false;
    }

    private boolean resolveDataType(ColumnIdentifier column, Table table) throws AxionException {
        if (table.hasColumn(column)) {
            Column col = table.getColumn(column.getName());
            column.setDataType(col.getDataType());
            return true;
        }
        return false;
    }

    private boolean isStar(ColumnIdentifier column) {
        if ("*".equals(column.getName())) {
            // if the column is "*", we're done
            return true;
        } else {
            return false;
        }
    }

    private Selectable setVariableContext(Selectable sel) {
        if (sel != null && _db instanceof VariableContext) {
            sel.setVariableContext((VariableContext) _db);
        }
        return sel;
    }
}
