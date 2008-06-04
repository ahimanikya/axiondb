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
package org.axiondb.engine.commands;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.FromNode;
import org.axiondb.Function;
import org.axiondb.FunctionFactory;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.visitors.FlattenWhereNodeVisitor;
import org.axiondb.engine.visitors.ReferencesOtherTablesWhereNodeVisitor;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.ComparisonFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.IsNotNullFunction;
import org.axiondb.functions.IsNullFunction;

/**
 * Axion Query Optimizer
 * 
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 * @author Ritesh Adval
 */
public class AxionQueryOptimizer {

    // TODO: Removal of unnecessary parentheses in WHERE Node:
    // given: ((a AND b) AND c OR (((a AND b) AND (c AND d))))
    // production: (a AND b AND c) OR (a AND b AND c AND d)

    // TODO: Constant folding in WHERE Node:
    // given: (a<b AND b=c) AND a=5
    // production: b>5 AND b=c AND a=5

    // TODO: Constant condition removal (needed because of constant folding):
    // given: (B>=5 AND B=5) OR (B=6 AND 5=5) OR (B=7 AND 5=6)
    // production: B=5 OR B=6

    // TODO: Apply Demorgan's law to simply the expression/where node tree.

    // TODO : Implement normalize() that will flip the comparison functions, if required.
    /**
     * Compose back the decomposed condition into a single condition tree.
     * 
     * @param conditions decomposed condition set
     * @return Single condition tree composed with AndFunction
     */
    public static Selectable createOneRootFunction(Set conditions) {
        if (conditions == null || conditions.size() == 0) {
            return null;
        } else if (conditions.size() == 1) {
            return (Selectable) conditions.iterator().next();
        } else {
            // "AND"ing all condition nodes to create one root "AND" function
            AndFunction previousAnd = new AndFunction();
            for (Iterator iter = conditions.iterator(); iter.hasNext();) {
                Selectable function = (Selectable) iter.next();
                if (previousAnd.getArgumentCount() != 2) {
                    previousAnd.addArgument(function);
                } else {
                    AndFunction andFunction = new AndFunction();
                    andFunction.addArgument(previousAnd);
                    andFunction.addArgument(function);
                    previousAnd = andFunction;
                }
            }
            return previousAnd;
        }
    }

    /**
     * Decomose a where/join condition into three part: (1) column-column comparision
     * function (2) column-literal conditions. column-literal conditions can be applied
     * earlier at table level than at join level. (3) and all remaining conditions: any
     * other function, these will be applied after join.
     * 
     * @param flattenConditionSet Flatten Condition Set, created from the where/join
     *        condition, or combined where and join condition set
     * @return Array of Set 0: column-column set, 1: column-literal set, 2: other
     */
    @SuppressWarnings("unchecked")
    public static Set[] decomposeWhereConditionNodes(Set flattenConditionSet, boolean isAllInnerJoin) {
        Set columnColumnConditons = new LinkedHashSet();
        Set columnLiteralConditions = new LinkedHashSet();
        Set remaingConditionNodes = new LinkedHashSet();

        for (Iterator it = flattenConditionSet.iterator(); it.hasNext();) {
            Object condition = it.next();
            if (condition instanceof ComparisonFunction) {
                ComparisonFunction fn = (ComparisonFunction) condition;
                if (fn.isColumnColumn()) {
                    columnColumnConditons.add(fn);
                } else if (fn.isColumnLiteral()) {
                    columnLiteralConditions.add(fn);
                } else {
                    remaingConditionNodes.add(fn);
                }
            } else if (isAllInnerJoin && (condition instanceof IsNotNullFunction || condition instanceof IsNullFunction)) {
                columnLiteralConditions.add(condition);
            } else {
                remaingConditionNodes.add(condition);
            }
        }
        return new Set[] { columnColumnConditons, columnLiteralConditions, remaingConditionNodes};
    }

    public static boolean hasTableReference(ComparisonFunction fn, TableIdentifier tid) {
        return getColumnRefersTable(fn, tid) != null;
    }

    public static Selectable getColumnRefersTable(ComparisonFunction fn, TableIdentifier tid) {
        Selectable searchColumn = null;
        Selectable leftColumn = fn.getArgument(0);
        Selectable rightColumn = fn.getArgument(1);

        if (hasColumnForTable(leftColumn, tid)) {
            searchColumn = leftColumn;
        } else if (hasColumnForTable(rightColumn, tid)) {
            searchColumn = rightColumn;
        }
        return searchColumn;
    }
    
    private static boolean hasColumnForTable(Selectable column, TableIdentifier tid) {
        if (column instanceof ColumnIdentifier) {
            ColumnIdentifier col = (ColumnIdentifier) column;
            if (tid.equals(col.getTableIdentifier())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Decomposes the given {@link WhereNode}into a {@link Set}of nodes that were
     * originally joined by ANDs, and adds to this set predicates that are implied by the
     * original tree (for example, given <tt>A = 1</tt> and <tt>A = B</tt>, we can
     * infer <tt>B = 1</tt>.)
     * 
     * @param flattenConditions
     * @return derived condition set
     * @throws AxionException
     */
    public static Set deriveTableFilter(Set flattenConditions, boolean isAllInnerJoin) throws AxionException {
        Set[] splitConditions = decomposeWhereConditionNodes(flattenConditions, isAllInnerJoin);
        Set columnColumns = splitConditions[0];
        Set columnLiterals = splitConditions[1];

        for (Iterator iter = columnColumns.iterator(); iter.hasNext();) {
            Function columnColumn = (Function) (iter.next());
            if (columnColumn instanceof EqualFunction) {
                for (Iterator aiter = columnLiterals.iterator(); aiter.hasNext();) {
                    Function columnLiteral = (Function) (aiter.next());
                    FunctionFactory fnFactory = (FunctionFactory) columnLiteral;
                    Function derivedFunction = fnFactory.makeNewInstance();
                    addDerivedFunction(flattenConditions, columnColumn, columnLiteral, derivedFunction);
                }
            }
        }
        return flattenConditions;
    }

    /**
     * Find column-literal comparision function for a given table. This function then can
     * be applied first to restrict the number of rows returned by an iterator. We do this
     * at the index level also. Give preference to EqualFunction
     * 
     * @param tid TableIdentifier
     * @param conditions decomposed condition set
     * @return column-literal function if found, null if not found
     */
    public static Function findColumnLiteralFunction(TableIdentifier tid, Table table, Set conditions, boolean mustCheckForIndex) {
        Function result = null;
        for (Iterator it = conditions.iterator(); it.hasNext();) {
            Selectable condition = (Selectable) it.next();
            Function fn = isColumnIndexed(tid, table, condition, mustCheckForIndex);
            if (fn != null) {
                if (fn instanceof EqualFunction) {
                    return fn;
                }
                if (result == null) {
                    result = fn;
                }
            }
        }
        return result;
    }
    
    public static Function findColumnLiteralEqualFunction(TableIdentifier tid, Table table, Set conditions, boolean mustCheckForIndex) {
        Function result = null;
        for (Iterator it = conditions.iterator(); it.hasNext();) {
            Selectable condition = (Selectable) it.next();
            Function fn = isColumnIndexed(tid, table, condition, mustCheckForIndex);
            if (fn != null && fn instanceof EqualFunction) {
                return fn;
            }
        }
        return result;
    }

    public static Function isColumnIndexed(TableIdentifier tid, Table table, Selectable condition, boolean mustCheckForIndex) {
        if (condition instanceof ComparisonFunction) {
            ComparisonFunction fn = (ComparisonFunction) condition;
            if (fn.isColumnLiteral() && onlyReferencesTable(tid, fn)) {
                Selectable searchColumn = getColumnRefersTable(fn, tid);
                if (isColumnIndexed(mustCheckForIndex, table, searchColumn)) {
                    return fn;
                }
            }
        } else if (condition instanceof IsNotNullFunction || condition instanceof IsNullFunction) {
            Function fn = (Function) condition;
            if (onlyReferencesTable(tid, fn)) {
                Selectable searchColumn = fn.getArgument(0);
                if (isColumnIndexed(mustCheckForIndex, table, searchColumn)) {
                    return fn;
                }
            }
        }
        return null;
    }

    private static boolean isColumnIndexed(boolean mustCheckForIndex, Table table, Selectable searchColumn) {
        if (mustCheckForIndex) { // if one column is indexed
            if (searchColumn != null && table.isColumnIndexed(table.getColumn(searchColumn.getName()))) {
                return true;
            }
        }
        return !mustCheckForIndex;
    }

    // If we have a column-column equal function for the given table, then give prefernce
    // to that as that will be best fit for the join condition.
    public static ComparisonFunction findFirstColumnColumnComparisonFunction(Set columnColumnConditions, TableIdentifier tid, Table table,
            boolean mustCheckForIndex) throws AxionException {

        ComparisonFunction result = null;
        for (Iterator iter = columnColumnConditions.iterator(); iter.hasNext();) {
            Object condition = iter.next();
            if (condition instanceof ComparisonFunction) {
                ComparisonFunction fn = (ComparisonFunction) condition;
                if (isColumnColumnComparisonFunction(fn, columnColumnConditions, tid, table, mustCheckForIndex)) {
                    if (fn instanceof EqualFunction) {
                        return fn;
                    } else if (result == null) {
                        result = fn;
                    }
                }
            }
        }
        return result;
    }

    public static EqualFunction findFirstEqualFunction(Set columnColumnConditions, TableIdentifier tid, Table table, boolean mustCheckForIndex)
            throws AxionException {

        for (Iterator iter = columnColumnConditions.iterator(); iter.hasNext();) {
            Object condition = iter.next();
            if (condition instanceof EqualFunction) {
                EqualFunction fn = (EqualFunction) condition;
                if (isColumnColumnComparisonFunction(fn, columnColumnConditions, tid, table, mustCheckForIndex)) {
                    return fn;
                }
            }
        }
        return null;
    }

    /**
     * Flatten the given condition tree into an ANDed set
     * 
     * @param conditionTree condition tree
     * @return flat set of functions which are anded together
     */
    public static Set flatConditionTree(Selectable conditionTree) {
        if (null == conditionTree) {
            return new LinkedHashSet(1);
        }
        return new FlattenWhereNodeVisitor().getNodes(conditionTree);
    }
    
    /**
     * Check if the given table is the only table refernce in the condition
     * 
     * @param table TableIdentifier
     * @param conditionNode
     * @return true if match, otherwise false.
     */
    public static boolean onlyReferencesTable(TableIdentifier table, Selectable conditionNode) {
        ReferencesOtherTablesWhereNodeVisitor v = new ReferencesOtherTablesWhereNodeVisitor(table);
        v.visit(conditionNode);
        return v.getResult();
    }

    @SuppressWarnings("unchecked")
    private static void addDerivedFunction(Set flattenConditions, Function columnColumn, Function columnLiteral, Function derivedFunction) {
        if (columnLiteral.getArgumentCount() == 2) {
            if (columnColumn.getArgument(0).equals(columnLiteral.getArgument(0))) {
                derivedFunction.addArgument(columnColumn.getArgument(1));
                derivedFunction.addArgument(columnLiteral.getArgument(1));
            } else if (columnColumn.getArgument(0).equals(columnLiteral.getArgument(1))) {
                derivedFunction.addArgument(columnLiteral.getArgument(0));
                derivedFunction.addArgument(columnColumn.getArgument(1));
            } else if (columnColumn.getArgument(1).equals(columnLiteral.getArgument(0))) {
                derivedFunction.addArgument(columnColumn.getArgument(0));
                derivedFunction.addArgument(columnLiteral.getArgument(1));
            } else if (columnColumn.getArgument(1).equals(columnLiteral.getArgument(1))) {
                derivedFunction.addArgument(columnLiteral.getArgument(0));
                derivedFunction.addArgument(columnColumn.getArgument(0));
            }
        } else {
            if (columnColumn.getArgument(0).equals(columnLiteral.getArgument(0))) {
                derivedFunction.addArgument(columnColumn.getArgument(1));
            } else if (columnColumn.getArgument(1).equals(columnLiteral.getArgument(0))) {
                derivedFunction.addArgument(columnColumn.getArgument(0));
            }
        }

        if (derivedFunction.getArgumentCount() > 0) {
            flattenConditions.add(derivedFunction);
        }
    }

    private static boolean isColumnColumnComparisonFunction(ComparisonFunction fn, Set columnColumnConditions, TableIdentifier tid, Table table,
            boolean mustCheckForIndex) {

        if (fn.isColumnColumn()) {
            if (mustCheckForIndex) { // if one column is indexed
                Selectable searchColumn = getColumnRefersTable(fn, tid);
                if (searchColumn != null && table.isColumnIndexed(table.getColumn(searchColumn.getName()))) {
                    return true;
                }
            } else if (hasTableReference(fn, tid)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAllInnerJoin(Object node) {
        FromNode from = (FromNode) node;
        if (from.isInnerJoin()) {
            if (from.getRight() instanceof FromNode && !isAllInnerJoin(from.getRight())) {
                return false;
            }

            if (from.getLeft() instanceof FromNode && !isAllInnerJoin(from.getLeft())) {
                return false;
            }
            return true;
        }
        return false;
    }

}
