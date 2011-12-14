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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.FromNode;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.engine.rowiterators.ChangingIndexedRowIterator;
import org.axiondb.engine.rowiterators.DistinctRowIterator;
import org.axiondb.engine.rowiterators.FilteringChangingIndexedRowIterator;
import org.axiondb.engine.rowiterators.FilteringRowIterator;
import org.axiondb.engine.rowiterators.GroupedRowIterator;
import org.axiondb.engine.rowiterators.IndexNestedLoopJoinedRowIterator;
import org.axiondb.engine.rowiterators.LimitingRowIterator;
import org.axiondb.engine.rowiterators.ListRowIterator;
import org.axiondb.engine.rowiterators.MutableIndexedRowIterator;
import org.axiondb.engine.rowiterators.NestedLoopJoinedRowIterator;
import org.axiondb.engine.rowiterators.ReverseSortedRowIterator;
import org.axiondb.engine.rowiterators.SingleRowIterator;
import org.axiondb.engine.rowiterators.SortedRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.ExternalDatabaseTable;
import org.axiondb.engine.tables.TableView;
import org.axiondb.functions.ComparisonFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.util.ValuePool;

/**
 * Query Planner used in select and sub-select Query. I use Rule-based analysis and
 * decision-making – rules expressing proven, efficient statement execution methods
 * determine how operations and their attributes are built and combined.
 * <p>
 * One of the important components is the Query Optimizer. Its goal, for every SQL
 * statement, is to answer this question: What is the quickest way to execute the
 * statement, to produce exactly the data requested by the statement in the shortest
 * possible time?
 * <p>
 * The query processor makes extensive use of a relational algebra tree representation to
 * model and manipulate SQL queries. These trees can be thought of as a series of pipes,
 * valves, and other components through which data flows, entering through the bottom from
 * one or more tables, and leaving through the top as a result set. At various points
 * within the tree, the data are operated on as needed to produce the desired result. Each
 * operation is represented as a node in the tree. Nodes may have one or more expressions
 * associated with them to specify columns, conditions, and calculations associated with
 * the operation. In Axion in most cases these trees are represented as RowIterators.
 * <p>
 * Some of the operators that may be present in the tree are:
 * <li>Restrict – reduces the number of output rows by eliminating those that fail to
 * satisfy some condition applied to the input. Restrict operators appear in the tree from
 * WHERE clauses and JOINs. example: FilteringRowIterator
 * <li>Join – combines two input tables into a single output table that contains some
 * combination of rows from the inputs. Joins appear in the tree from the use of FROM
 * clauses and from JOIN clauses. example: NestedLoopJoinRowIterator.
 * <li>Sort – changes the ordering of rows in an input table to produce an output table
 * in the desired order. example: SortedRowIterator
 * <li>Table – represents a Table scan or an Index scan, reading data from a given table
 * by either its default index (table scan) or a specific index (index scan). Leaf nodes
 * of the tree are always references to database tables.
 * <p>
 * Current Axion SQL parser does not build a formal query tree, instead it builds a
 * AxionQueryContext object, which hold the query tree, that includes selectables, where
 * node, from node, oder by and group by node. FromNode is nested and represet a from
 * tree, the subqueries are nested in the context object. Then I apply rule bases analysis
 * to optimize the query.
 * <p>
 * The Axion query optimizer divides the optimization process into several phases. Some of
 * the phases deal with very significant rule based cost factors and are straightforward
 * to understand. Each phase, listed below, addresses a specific type of optimization:
 * <p>
 * <li>Pushing restrictions as far down the tree as possible
 * <li>Derive restrictions
 * <li>Using indexes for restrictions
 * <li>Join optimization
 * <li>Sort optimization
 * <li>Using index for NULL Check
 * <li>Count Rows Optimization
 * <p>
 * <b>Pushing Restrict Operations Close to the Data Origin: </b> This optimization stage
 * consists of moving restrict operators as far down the query tree as possible. This
 * reduces the number of tuples moving up the tree for further processing and minimizes
 * the amount of data handled. When restrict operations on a join node cannot be moved
 * below the join node, they are set as join conditions. When multiple predicates are
 * moved down the tree to the same relative position, they are reassembled into a single
 * Restrict operation. Consider the SQL clause:
 * <p>
 * <code>
 *                 SELECT EMP.NAME FROM EMP, DEPT 
 *                        WHERE EMP.SAL &gt; 4000 
 *                        AND EMP.SAL &lt;= 6000 
 *                        AND EMP.DEPTNO = DEPT.DEPTNO
 * </code>
 * <p>
 * The optimizer apply restrictions <code> EMP.SAL > 4000 </code> and
 * <code> EMP.SAL <= 6000 </code> are moved down the tree, below the join node, since they
 * apply to a single table. The restriction EMP.DEPTNO = DEPT.DEPTNO, applying to two
 * tables, stays in the join node.
 * <p>
 * <b>Derive restrictions: </b> This optimization stage consists of deriving restrict
 * operators based on the current current restriction and join condition. Consider the
 * following SQL clause:
 * <p>
 * <code>
 *          SELECT EMP.NAME FROM EMP, DEPT 
 *              WHERE EMP.DEPTNO = DEPT.DEPTNO AND EMP.DEPTNO = 10
 * </code>
 * <p>
 * In this stage, the Optimizer can derive <code> DEPT.DEPTNO = 10 </code> and push that
 * down the tree, below the join node, since they apply to a single table. The restriction
 * EMP.DEPTNO = DEPT.DEPTNO, applying to two tables, stays in the join node.
 * <p>
 * <b>Using Indexes for Restrictions: </b> This optimization phase consists of recognizing
 * those cases where an existing index can be used to evaluate a restriction and
 * converting a table scan into an index bracket or set of contiguous index entries. An
 * index bracket is extremely effective in limiting the number of rows that must be
 * processed. Consider the following SQL clause:
 * <p>
 * <code>
 *          SELECT EMP.NAME FROM EMP WHERE EMP.SAL > 6000 AND EMP.DEPTNO = 10
 * </code>
 * <p>
 * Instead of a separate restrict operation, the output tree uses the index EmpIdx on the
 * DEPTNO column table EMP to evaluate the restriction DEPTNO = 10.
 * <p>
 * <b>Choosing the Join Algorithm: </b> Currently Axion support Augmented nested loop
 * (ANL) or Index Nested Loop join and Nested loop join.
 * <p>
 * The <b>Index Nested Loop Join or Augmented Nested Loop Join (ANL) </b> is by far the
 * most common join method and is the classic Axion join method. An augmented nested loop
 * join is performed by doing a scan over the left subtree and for each row in it,
 * performing an index bracket scan on a portion of the right subtree. The right subtree
 * is read as many times as there are rows in the left subtree. To be a candidate for an
 * ANL join, the subtree pair for a join node must meet the following criteria:
 * <p>
 * <li>There must be an index(es) defined on the join column(s) for the table in the
 * right subtree.
 * <li>No other scan on that index has already been set.
 * <p>
 * When there is an index defined on the left subtree’s table instead of on the right, the
 * optimizer swaps the subtrees to make an ANL join possible. When neither subtree’s table
 * has an index defined on the join column, the optimizer creats a dynamic index on one of
 * the subtree.
 * <p>
 * A <b>Nested Loop Join </b> is performed by doing a scan over the left subtree and for
 * each row in it performing a full scan of the right subtree. This is the default join
 * algorithm, which can be used for any join. However, it is usually less efficient than
 * the other methods. Usually, either an existing index, or a dynamic index, used in an
 * ANL join, will cost much less. Occasionally, when subtree cardinalities are very low
 * (possibly because of index bracketing), nested loop will be the method with the least
 * cost.
 * <p>
 * <b>Count Rows Optimization: </b> If we are counting the leaf nodes, axion will use
 * table row count instead of table scan. If index was used to scan the table, count will
 * use the index count. e.g <code>select count(*) from emp</code> will get the row count
 * from table emp, instead of making a full table scan, but
 * <code>select count(*) from emp where emp.id > 100 </code> require a full table scan
 * unless optimizer can take advantage of index, if index is scanned then, index count
 * will be used.
 * <p>
 * <b>Sort Optimization: </b> The optimizer/planner performs two separate optimizations
 * designed to avoid sort operations whenever possible: eliminating redundant sorts and
 * converting table scans followed by sorts into index bracket scans.
 * <p>
 * <li><b>Eliminating Redundant Sorts: </b> The optimizer/planner checks whether the
 * query tree contains unnecessary sort nodes. For example, when an SQL statement contains
 * both a GROUP BY and an ORDER BY clause that refers to the same column, at most one sort
 * is needed. A sort node is also redundant when the immediate descendant node of the sort
 * node is an index bracket scan on the sort column. That is, the sort is redundant when
 * the data input to the sort was read using an index with the needed sort order.
 * <p>
 * <li><b>Converting Table Scans to Index Bracket Scans: </b> When a leaf node of a
 * subtree is a table scan, the optimizer/planner checks whether any indexes that exist on
 * the table match the sort column(s). If so, the optimizer converts the table scan to the
 * index bracket scan and removes the sort node.
 * 
 * @version  
 * @author Morgan Delagrange
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Amrish Lal
 * @author Dave Pekarek Krohn
 * @author Rahul Dwivedi
 * @author Ahimanikya Satapathy
 * @author Ritesh Adval
 */
public class AxionQueryPlanner {

    // TODO: Take advantage of Index for MIN/MAX - The first and last values represent MIN
    // and MAX values respectively for ascending indexes.

    // TODO: Take advantage of Index for LIKE operator.

    // TODO: Reducing Multi-Block queries to Single Block - Folding correlated sub-query
    // to outer query where possible or use SemiJoin like technique for optimizing
    // Muti-Block queries. See: http://www-db.stanford.edu/~widom/cs346/chaudhuri.pdf

    // TODO: Handle situation where we have multiple index available for the join condition;
    // once we have multi column index this will make sense.

    // TODO: Use Index and Cardinal statistics to estimate cost, we could maintain system
    // table for this. Generate multiple join path and based on the cost choose the best
    // fit.

    // TODO: Use System-R Join Optimization ?

    public AxionQueryPlanner(AxionQueryContext context) {
        _context = context;
    }

    public Map getColumnIdToFieldMap() {
        return _colIdToFieldMap;
    }

    public RowIterator getPlanNodeRowIterator() {
        return _planNode.getRowIterator();
    }

    /**
     * Makes appropriate {@link RowIterator}for the current query/subquery.
     * 
     * @param db the current database.
     * @param readOnly when <code>true</code>, the caller does not expect to be able to
     *        modify (i.e., call {@link RowIterator#set}or {@link RowIterator#remove}on)
     *        the returned {@link RowIterator}, the returned iterator <i>may </i> be
     *        unmodifiable.
     * @return returns the top most RowIterator that may wrap other underlying
     *         RowIterator based on the specific query.
     */
    public RowIterator makeRowIterator(Database db, boolean readOnly) throws AxionException {
        if (db.isReadOnly()) {
            readOnly = true;
        }

        RowIterator rows = null;
        AxionQueryContext context = getContext();
        _literals = context.createLiteralList();
        _parentRow = context.getParentRow();

        try {
            if (context.getTableCount() == 0) { // *** NO from node *** //
                rows = processQueryWithNoTable(context);
            } else if (context.getTableCount() == 1) { // *** Only one table *** //
                rows = processQueryForSingleTable(db, readOnly, context);
            } else { // *** Traditional or ANSI Join *** //
                rows = processQuery(db, readOnly, context);
            }

            rows = makeGroupedRowIterator(db, rows, context, getColumnIdToFieldMap());
            if (context.foundAggregateFunction()) {
                resolveSelectedAfterGrouping(context, getColumnIdToFieldMap());
            }

            rows = makeDistinctRowIterator(rows, context);
            rows = makeOrderedRowIterator(db, rows, getColumnIdToFieldMap(), context);
            rows = makeLimitingRowIterator(rows, context, getColumnIdToFieldMap());

        } finally {
            dropViewIfSubQuery(context.getFrom(), db);
        }
        return rows;
    }

    private void addExplainRow(RowIterator rows) {
        if (_context.isExplain()) {
            _planNode.addExplainRow(rows.toString());
        }
    }

    private Map clearAndGetColumnIdToFieldMap() {
        _colIdToFieldMap.clear();
        return _colIdToFieldMap;
    }

    // Note: Assume index available for the given column of a table so create a new Index
    // Caller need to make sure no existing available index for this column
    private RowIterator createDynamicIndex(Database db, Table table, ColumnIdentifier columnId) throws AxionException {
        Column column = table.getColumn(columnId.getName());
        if (table.getIndexForColumn(column) == null) {
            Index index = db.getIndexFactory("default").makeNewSystemInstance(table, column, db.getDBDirectory() == null);
            db.addIndex(index, table, true);
            if (table instanceof ExternalDatabaseTable){
                // External DB Table's indices are different and are not maintained by Axion.
                index = table.getIndexForColumn(column);
            }
            return new ChangingIndexedRowIterator(index, table, new EqualFunction());
        }
        return null;
    }

    private void createDynamicIndex(FromNode from, JoinCondition joinCondition, QueryPlannerJoinContext joinContext, Database db)
            throws AxionException {
        // If either left and right are physical tables, then create index with the
        // following exception
        // Rule1: If right outer join , I will not create index on right table
        // Rule2: If left outer join , I will not create index on left table
        if (!from.isRightJoin() && from.getRight() instanceof TableIdentifier && !joinContext.swapRightToLeft()) {
            TableIdentifier rtid = (TableIdentifier) from.getRight();
            Table rightTable = db.getTable(rtid);
            if (!isTableView(rightTable)) {
                createDynamicIndexOnRightTable(from, rtid, joinCondition, joinContext, db, rightTable);
            }
        } else if (!from.isLeftJoin() && from.getLeft() instanceof TableIdentifier) {
            TableIdentifier ltid = (TableIdentifier) from.getLeft();
            Table leftTable = db.getTable(ltid);
            if (!isTableView(leftTable)) {
                createDynamicIndexOnLeftTable(from, ltid, joinCondition, joinContext, db, leftTable);
            }
        }
    }

    private void createDynamicIndexOnLeftTable(FromNode from, TableIdentifier ltid, JoinCondition joinCondition, QueryPlannerJoinContext joinContext,
            Database db, Table table) throws AxionException {

        EqualFunction fn = AxionQueryOptimizer.findFirstEqualFunction(joinCondition.getNodes(), ltid, table, false);
        if (fn != null && !fn.getArgument(0).equals(fn.getArgument(1))) {
            // Assume that left argument of function is from left table
            joinContext.setLeftTableKey((ColumnIdentifier) fn.getArgument(0));
            joinContext.setRightTablekey((ColumnIdentifier) fn.getArgument(1));

            // Left argument of function is not from left table so flip it
            if (!ltid.equals(joinContext.getLeftTableKey().getTableIdentifier())) {
                joinContext.flipKey();
            }

            // Now create index
            ColumnIdentifier columnId = joinContext.getLeftTableKey();
            RowIterator leftIter = createDynamicIndex(db, table, columnId);
            if (leftIter != null) {
                addExplainRow(leftIter);
                joinContext.setLeftIterator(leftIter);
                swapKeyForSortingIfUSedInOrderByOrGroupBy(joinContext.getLeftTableKey(), joinContext.getRightTablekey());
                joinContext.setRowsAreSortedByJoinKey(true);
                joinCondition.getNodes().remove(fn);
                pushUnwantedConditions(from, joinCondition, ltid);
            }
        }
    }

    private void createDynamicIndexOnRightTable(FromNode from, TableIdentifier rtid, JoinCondition joinCondition,
            QueryPlannerJoinContext joinContext, Database db, Table table) throws AxionException {

        EqualFunction fn = AxionQueryOptimizer.findFirstEqualFunction(joinCondition.getNodes(), rtid, table, false);
        if (fn != null && !fn.getArgument(0).equals(fn.getArgument(1))) {
            // Assume that right argument of function is from right table
            joinContext.setLeftTableKey((ColumnIdentifier) fn.getArgument(0));
            joinContext.setRightTablekey((ColumnIdentifier) fn.getArgument(1));

            // Right argument of function is not from right table so flip it
            if (!rtid.equals(joinContext.getRightTablekey().getTableIdentifier())) {
                joinContext.flipKey();
            }

            // Now create index
            ColumnIdentifier columnId = joinContext.getRightTablekey();
            RowIterator rightIter = createDynamicIndex(db, table, columnId);
            if (rightIter != null) {
                addExplainRow(rightIter);
                joinContext.setRightIterator(rightIter);
                swapKeyForSortingIfUSedInOrderByOrGroupBy(joinContext.getRightTablekey(), joinContext.getLeftTableKey());
                joinContext.setRowsAreSortedByJoinKey(true);
                joinCondition.getNodes().remove(fn);
                pushUnwantedConditions(from, joinCondition, rtid);
            }
        }
    }

    private void dropViewIfSubQuery(FromNode from, Database db) throws AxionException {
        if (from == null) {
            return;
        }
        dropViewIfSubQuery(from.getLeft(), db);
        dropViewIfSubQuery(from.getRight(), db);
    }

    private void dropViewIfSubQuery(Object child, Database db) throws AxionException {
        if (child instanceof TableIdentifier) {
            TableIdentifier tid = (TableIdentifier) child;
            Table view = db.getTable(tid);
            if (view instanceof TransactableTableImpl) {
                view = ((TransactableTableImpl) view).getTable();
            }
            if (view instanceof TableView) {
                if (((TableView) view).getType().equals(TableView.SUBQUERY)) {
                    db.dropTable(tid.getTableName());
                }
            }
        } else if (child instanceof FromNode) {
            dropViewIfSubQuery((FromNode) child, db);
        }
    }

    private AxionQueryContext getContext() {
        return _context;
    }

    private RowIterator getIndexedRows(Set conditions, ColumnIdentifier cid, Table table, boolean readOnly) throws AxionException {
        RowIterator rows = null;
        Function fn = AxionQueryOptimizer.findColumnLiteralFunction(cid.getTableIdentifier(), table, conditions, true);

        if (fn != null) {
            rows = table.getIndexedRows(fn, readOnly);
        }
        if (rows == null) {
            rows = table.getIndexedRows(cid, readOnly);
        }
        return rows;
    }

    private RowIterator getIndexedRowsIfSortingRequired(RowIterator rows, AxionQueryContext context, Table table, boolean readOnly, Set conditions)
            throws AxionException {
        // Attempt to get Index based row iterator, if group column is from this table
        if (null == rows && context.getGroupByCount() == 1) {
            ColumnIdentifier grpCol = (ColumnIdentifier) context.getGroupBy(0);
            rows = getIndexedRows(conditions, grpCol, table, readOnly);
            if (rows != null) {
                setIndexUsedForGroupBy(true);
                addExplainRow(rows);
            }
        }

        // Attempt to get Index based row iterator, if order column is from this table
        if (null == rows && context.getOrderByCount() == 1) {
            OrderNode node = context.getOrderBy(0);
            Selectable ordCol = node.getSelectable();
            if (ordCol instanceof ColumnIdentifier) {
                rows = getIndexedRows(conditions, (ColumnIdentifier) ordCol, table, readOnly);
                if (rows != null) {
                    setSkipSorting(true);
                    addExplainRow(rows);
                }
            }
        }
        return rows;
    }

    private RowIterator getRowIteratorFromTable(Database db, TableIdentifier tid, Table table, RowIterator rows, boolean readOnly, Set conditions)
            throws AxionException {
        if (rows != null) {
            return rows;
        }

        Function fn = AxionQueryOptimizer.findColumnLiteralFunction(tid, table, conditions, true);

        // First try to apply column literal equal function or other comparison
        // function, this is very fast in case of comparison function this will result in
        // subset which contains less rows which improves performance
        if (fn != null) {
            // ...then try to find an index for this node.
            rows = table.getIndexedRows(fn, readOnly);
            if (rows != null) {
                conditions.remove(fn);
                addExplainRow(rows);
            }
        }

        // If we still don't have a RowIterator for this table, then we'll use a full
        // table scan.
        if (null == rows) {
            rows = table.getRowIterator(readOnly);
            addExplainRow(rows);
        }
        return rows;
    }

    private boolean wasSortedWhileGrouping(List groupByColumns, List orderByColumns, boolean isDescending) throws AxionException {
        int groupByCount = groupByColumns.size();
        int orderByCount = orderByColumns.size();

        if (groupByCount == 0 || groupByCount < orderByCount) {
            return false;
        }

        if (wasIndexUsedForGroupBy() && isDescending) {
            _doReverseSorting = true;
        }

        if (!groupByColumns.equals(orderByColumns)) {
            return false;
        }
        return true;
    }

    /**
     * Detemines whether we can eliminate sorting by checking whether or not the
     * RowIterrator has been already sorted.
     * <p>
     * If ORDER BY has less or equal number of nodes (say n) as GROUP BY nodes (say m)
     * then, then we eliminate sorting if they order node(n) matches first n nodes of
     * group by nodes. If one or more ORDER BY nodes order is descending then, we can
     * eliminate sorting only if GroupedRowIterator did not use index based row iterators.
     */
    @SuppressWarnings("unchecked")
    private boolean isSortingRequired(AxionQueryContext context) throws AxionException {
        int orderByCount = context.getOrderByCount();
        int groupByCount = context.getGroupByCount();

        if (orderByCount == 0) {
            _doReverseSorting = false;
            return false;
        }

        List orderByColumns = new ArrayList();
        boolean isDescending = true;
        for (int i = 0; i < orderByCount; i++) {
            OrderNode node = context.getOrderBy(i);
            isDescending = (node.isDescending() && isDescending) ? true : false;
            orderByColumns.add(node.getSelectable());
        }

        if (skipSorting() && isDescending) {
            _doReverseSorting = true;
        }

        List groupByColumns = Collections.EMPTY_LIST;
        if (groupByCount > orderByCount) {
            groupByColumns = new ArrayList(context.getGroupBy().subList(0, context.getOrderByCount()));
        } else if (groupByCount > 0) {
            groupByColumns = context.getGroupBy();
        }

        if (skipSorting() || wasSortedWhileGrouping(groupByColumns, orderByColumns, isDescending)) {
            return false;
        }

        return true;
    }

    private boolean isTableView(Table table) {
        if (table instanceof TransactableTableImpl) {
            table = ((TransactableTableImpl) table).getTable();
        }

        if (table instanceof TableView) {
            return true;
        }
        return false;
    }

    private RowIterator makeChangingIndexedRowIterator(Table table, Index index) throws AxionException {
        RowIterator iter = new ChangingIndexedRowIterator(index, table, new EqualFunction());
        addExplainRow(iter);
        return iter;
    }

    private RowIterator makeDistinctRowIterator(RowIterator rows, AxionQueryContext context) {
        // Apply distinct, if needed
        if (context.getDistinct()) {
            rows = new DistinctRowIterator(rows, getColumnIdToFieldMap(), context.getSelected());
            addExplainRow(rows);
        }
        return rows;
    }

    private RowIterator makeFilteringRowIterator(TableIdentifier tid, Table table, RowIterator rows, AxionQueryContext context, Set conditions)
            throws AxionException {
        // Apply any unapplied whereNodesForTable.
        Iterator tableFilterIt = conditions.iterator();
        while (tableFilterIt.hasNext()) {
            Map localmap = new HashMap();
            List columns = new ArrayList();
            populateColumnList(columns, tid, table, context);
            populateColumnIdToFieldMap(columns, localmap);
            Selectable node = (Selectable) (tableFilterIt.next());
            // If the node only references this table...
            if (AxionQueryOptimizer.onlyReferencesTable(tid, node)) {
                if (rows instanceof MutableIndexedRowIterator) {
                    rows = new FilteringChangingIndexedRowIterator((MutableIndexedRowIterator) rows, new RowDecorator(localmap), node);
                } else {
                    rows = new FilteringRowIterator(rows, new RowDecorator(localmap), node);
                }
                tableFilterIt.remove();
                addExplainRow(rows);
            }
        }
        return (rows);
    }

    // Take advantage of index in group by if group by used on a single column and single
    // table.
    private RowIterator makeGroupedIndexBasedRowIterator(boolean readOnly, AxionQueryContext context, Table table) throws AxionException {
        ColumnIdentifier cid = (ColumnIdentifier) context.getGroupBy(0);
        RowIterator rows = table.getIndexedRows(cid, readOnly);
        if (rows != null) {
            setIndexUsedForGroupBy(true);
            addExplainRow(rows);
            rows = new GroupedRowIterator(false, rows, getColumnIdToFieldMap(), context.getGroupBy(), context.getSelect(), context.getHaving(),
                context.getWhere(), context.getOrderBy());
            addExplainRow(rows);
        }
        return rows;
    }

    // If we have any join and single column is used for group by then, we check if the
    // Index based Joined RowIterator key of the table is matching with group by column,
    // and has been scanned using index on the group by column.
    private RowIterator makeGroupedRowIterator(Database db, RowIterator rows, AxionQueryContext context, Map colIdToFieldMap) throws AxionException {
        // Apply group by if any
        RowIterator grpRows = null;
        if (context.foundAggregateFunction()) {
            // Check if the rows has been already sorted for the group by column
            if (wasIndexUsedForGroupBy()) {
                grpRows = new GroupedRowIterator(false, rows, colIdToFieldMap, context.getGroupBy(), context.getSelect(), context.getHaving(), null,
                    context.getOrderBy());
                addExplainRow(grpRows);
                return grpRows;
            } else {
                grpRows = new GroupedRowIterator(rows, colIdToFieldMap, context.getGroupBy(), context.getSelect(), context.getHaving(),
                    context.getOrderBy());
                addExplainRow(grpRows);
                return grpRows;
            }
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private void makeIndexNestedLoopJoinedRowIterator(FromNode from, JoinCondition joinCondition, QueryPlannerJoinContext joinContext)
            throws AxionException {
        IndexNestedLoopJoinedRowIterator joinedRowIterator = null;
        if (joinContext.getRightIterator() instanceof MutableIndexedRowIterator && !from.isRightJoin() && !joinContext.swapRightToLeft()) {
            joinedRowIterator = new IndexNestedLoopJoinedRowIterator(joinContext.getLeftIterator(), joinContext.getLeftColumnPosition(),
                (MutableIndexedRowIterator) (joinContext.getRightIterator()), joinContext.getRightColumnCount(), !from.isInnerJoin(),
                from.isRightJoin());
        } else if (joinContext.getLeftIterator() instanceof MutableIndexedRowIterator && !from.isLeftJoin()) {
            joinedRowIterator = new IndexNestedLoopJoinedRowIterator(joinContext.getRightIterator(), joinContext.getRightColumnPosition(),
                (MutableIndexedRowIterator) (joinContext.getLeftIterator()), joinContext.getLeftColumnCount(), !from.isInnerJoin(), true);
        }

        if (joinedRowIterator != null) {
            if (!joinCondition.isEmpty()) {
                joinedRowIterator.setJoinCondition(joinCondition.stitchAll(), new RowDecorator(joinContext.getColumnIdToFieldMap()));
                _unappliedWhereNodes.removeAll(joinCondition.getNodes());
            }
            joinContext.setRowIterator(joinedRowIterator);
            addExplainRow(joinedRowIterator);
        }
    }

    private void makeLeftChangingIndexedRowIterator(FromNode from, JoinCondition joinCondition, Database db, QueryPlannerJoinContext joinContext)
            throws AxionException {

        if (joinContext.getRightIterator() == null && from.getLeft() instanceof TableIdentifier) {
            TableIdentifier ltid = (TableIdentifier) from.getLeft();
            Table table = db.getTable(ltid);
            ComparisonFunction fn = AxionQueryOptimizer.findFirstColumnColumnComparisonFunction(joinCondition.getNodes(), ltid, table, true);
            if (fn != null && !fn.getArgument(0).equals(fn.getArgument(1))) {
                // Assume that left argument of function is from left table
                joinContext.setLeftTableKey((ColumnIdentifier) fn.getArgument(0));
                joinContext.setRightTablekey((ColumnIdentifier) fn.getArgument(1));

                // If left argument of function is not from left table then flip it
                if (!ltid.equals(joinContext.getLeftTableKey().getTableIdentifier())) {
                    joinContext.flipKey();
                }

                // Check if we can use index
                if (joinContext.getLeftTableKey() != null && !from.isLeftJoin()) {
                    Index index = table.getIndexForColumn(table.getColumn(joinContext.getLeftTableKey().getName()));
                    joinContext.setLeftIterator(makeChangingIndexedRowIterator(table, index));

                    if (fn instanceof EqualFunction) {
                        swapKeyForSortingIfUSedInOrderByOrGroupBy(joinContext.getLeftTableKey(), joinContext.getRightTablekey());
                        joinContext.setRowsAreSortedByJoinKey(true);
                    }
                    joinCondition.getNodes().remove(fn);
                    pushUnwantedConditions(from, joinCondition, ltid);
                } else {
                    joinContext.setLeftTableKey(null);
                    joinContext.setRightTablekey(null);
                }
            }
        }
    }

    private void makeLeftRowIterator(FromNode from, JoinCondition joinCondition, Database db, QueryPlannerJoinContext joinContext,
            ColumnIdentifier lcol, AxionQueryContext context, boolean readOnly) throws AxionException {

        Object leftChild = from.getLeft();
        if (leftChild instanceof FromNode) {
            QueryPlannerJoinContext nestedJoinContext = processFromTree((FromNode) leftChild, db, context, readOnly);
            joinContext.setLeftIterator(nestedJoinContext.getRowIterator());
            joinContext.addColumnIdentifiers(nestedJoinContext.getColumnIdentifiers());
            joinContext.setLeftColumnCount(nestedJoinContext.getTotalColumnCount());
            
            if (lcol != null) {
                Integer colPos = (Integer) nestedJoinContext.getColumnIdToFieldMap().get(lcol.getCanonicalIdentifier());
                if (colPos != null) {
                    joinContext.setLeftColumnPosition(colPos.intValue());
                }
            }
        } else {
            TableIdentifier tid = (TableIdentifier) leftChild;
            Table left = db.getTable(tid);
            if (lcol != null) {
                joinContext.setLeftColumnPosition(left.getColumnIndex(lcol.getName()));
            }

            RowIterator rows = joinContext.getLeftIterator();
            if (joinContext.isRowsAreSortedByJoinKey()) {
                rows = getIndexedRowsIfSortingRequired(rows, context, left, readOnly, _unappliedTableFilters);
            }
            rows = getRowIteratorFromTable(db, tid, left, rows, readOnly, _unappliedTableFilters);
            rows = makeFilteringRowIterator(tid, left, rows, context, _unappliedTableFilters);

            populateColumnList(joinContext.getColumnIdentifiers(), tid, left, context);
            joinContext.setLeftIterator(rows);
            joinContext.setLeftColumnCount(left.getColumnCount());

            pushUnwantedConditions(from, joinCondition, tid);
        }
    }

    @SuppressWarnings("unchecked")
    private void pushUnwantedConditions(FromNode from, JoinCondition joinCondition, TableIdentifier tid) {
        if (from.isInnerJoin() && !joinCondition.isEmpty()) {
            Set conditions = new LinkedHashSet();
            for (Iterator iter = joinCondition.getNodes().iterator(); iter.hasNext();) {
                ComparisonFunction fn = (ComparisonFunction) iter.next();
                if (!AxionQueryOptimizer.hasTableReference(fn, tid)) {
                    _unappliedWhereNodes.add(fn);
                } else {
                    conditions.add(fn);
                }
            }
            joinCondition.setNodes(conditions);
        }
    }

    private RowIterator makeLimitingRowIterator(RowIterator rows, AxionQueryContext context, Map colIdToFieldMap) throws AxionException {
        // If there's a limit, apply it
        if (null != context.getLimit() || null != context.getOffset()) {
            rows = new LimitingRowIterator(rows, context.getLimit(), context.getOffset());
            addExplainRow(rows);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private RowIterator makeLiteralRowIterator(List columns) throws AxionException {
        RowIterator literaliter = null;

        if (null != _literals || _parentRow != null) {
            int ltRowSize = ((null != _literals) ? _literals.size() : 0);
            ltRowSize += ((null != _parentRow) ? _parentRow.getRow().size() : 0);
            Row litrow = new SimpleRow(ltRowSize);

            Iterator iter;
            int pos = 0;
            if (null != _literals) {
                iter = _literals.iterator();
                for (int i = 0; iter.hasNext(); i++) {
                    Literal literal = (Literal) iter.next();
                    columns.add(literal);
                    litrow.set(pos++, literal);
                }
            }

            if (null != _parentRow) {
                iter = _parentRow.getSelectableIterator();
                for (int i = 0; iter.hasNext(); i++) {
                    ColumnIdentifier parentCol = (ColumnIdentifier) iter.next();
                    // Give preference to the inner select if the name exists
                    // in both inner and outer select
                    if (!columns.contains(parentCol)) {
                        columns.add(parentCol);
                        litrow.set(pos++, _parentRow.get(parentCol));
                    }
                }
            }

            literaliter = new SingleRowIterator(litrow);
            addExplainRow(literaliter);

            // Set to null, so that they are processed only once.
            _literals = null;
            _parentRow = null;
        }
        return literaliter;
    }

    @SuppressWarnings("unchecked")
    private void makeNestedLoopJoinedIterator(FromNode from, JoinCondition joinCondition, QueryPlannerJoinContext joinContext) throws AxionException {
        // NOTE: Join is carried out using nested loop algorithm; hence, in case of
        // of right outer join, we make the right table as the outer table of
        // the nested loop algorithm. Note that no change is made to _colIdToFieldmap.

        NestedLoopJoinedRowIterator joinedRowIter = null;
        if (!from.isRightJoin() && !joinContext.swapRightToLeft()) {
            joinedRowIter = new NestedLoopJoinedRowIterator(joinContext.getLeftIterator(), joinContext.getRightIterator(),
                joinContext.getRightColumnCount(), !from.isInnerJoin(), from.isRightJoin());
        } else {
            joinedRowIter = new NestedLoopJoinedRowIterator(joinContext.getRightIterator(), joinContext.getLeftIterator(),
                joinContext.getLeftColumnCount(), !from.isInnerJoin(), true);
        }

        // Note: if RootJoinCondition is null or empty, it will be a Cross Product join.
        if (!joinCondition.isEmpty()) {
            joinedRowIter.setJoinCondition(joinCondition.stitchAll(), new RowDecorator(joinContext.getColumnIdToFieldMap()));
            _unappliedWhereNodes.removeAll(joinCondition.getNodes());
        }
        joinContext.setRowIterator(joinedRowIter);
        addExplainRow(joinedRowIter);
    }

    private RowIterator makeOrderedIndexBasedRowIterator(boolean readOnly, AxionQueryContext context, Table table) throws AxionException {
        // Check if we can use index for order by; only supported for single table
        RowIterator orderedRows = null;
        OrderNode node = context.getOrderBy(0);
        ColumnIdentifier cid = null;
        Selectable where = context.getWhere();

        // Index keep data row pointer is ascending order
        if (node.getSelectable() instanceof ColumnIdentifier) {
            cid = (ColumnIdentifier) node.getSelectable();
            Set conditions = AxionQueryOptimizer.flatConditionTree(where);
            orderedRows = getIndexedRows(conditions, cid, table, readOnly);
        }

        if (orderedRows != null) {
            setSkipSorting(true);
            addExplainRow(orderedRows);
            if (where != null) {
                orderedRows = new FilteringRowIterator(orderedRows, new RowDecorator(getColumnIdToFieldMap()), where);
                addExplainRow(orderedRows);
            }
        }
        return orderedRows;
    }

    private RowIterator makeOrderedRowIterator(Database db, RowIterator rows, Map colIdToFieldMap, AxionQueryContext context) throws AxionException {
        if (isSortingRequired(context)) {
            rows = new SortedRowIterator.MergeSort(rows, context.getOrderBy(), new RowDecorator(colIdToFieldMap));
            addExplainRow(rows);
        } else if (_doReverseSorting) {
            rows = new ReverseSortedRowIterator(rows);
            addExplainRow(rows);
        }
        return rows;
    }

    private void makeRightChangingIndexedRowIterator(FromNode from, JoinCondition joinCondition, Database db, QueryPlannerJoinContext joinContext)
            throws AxionException {

        if (joinContext.getLeftIterator() == null && from.getRight() instanceof TableIdentifier) {
            TableIdentifier rtid = (TableIdentifier) from.getRight();
            Table table = db.getTable(rtid);
            ComparisonFunction fn = AxionQueryOptimizer.findFirstColumnColumnComparisonFunction(joinCondition.getNodes(), rtid, table, true);
            if (fn != null && !fn.getArgument(0).equals(fn.getArgument(1))) {
                // Assume that right argument of function is from right table
                joinContext.setLeftTableKey((ColumnIdentifier) fn.getArgument(0));
                joinContext.setRightTablekey((ColumnIdentifier) fn.getArgument(1));

                // Right argument of function is not from right table so flip it
                if (!rtid.equals(joinContext.getRightTablekey().getTableIdentifier())) {
                    joinContext.flipKey();
                }

                // Check in we can use index
                if (joinContext.getRightTablekey() != null && !from.isRightJoin()) {
                    Index index = table.getIndexForColumn(table.getColumn(joinContext.getRightTablekey().getName()));
                    joinContext.setRightIterator(makeChangingIndexedRowIterator(table, index));
                    if (fn instanceof EqualFunction) {
                        swapKeyForSortingIfUSedInOrderByOrGroupBy(joinContext.getRightTablekey(), joinContext.getLeftTableKey());
                        joinContext.setRowsAreSortedByJoinKey(true);
                    }
                    joinCondition.getNodes().remove(fn);
                    pushUnwantedConditions(from, joinCondition, rtid);
                } else {
                    joinContext.setLeftTableKey(null);
                    joinContext.setRightTablekey(null);
                }
            }
        }
    }

    private void makeRightRowIterator(FromNode from, JoinCondition joinCondition, Database db, QueryPlannerJoinContext joinContext,
            ColumnIdentifier rcol, AxionQueryContext context, boolean readOnly) throws AxionException {
        Object rightChild = from.getRight();
        if (rightChild instanceof FromNode) {
            QueryPlannerJoinContext nestedJoinContext = processFromTree((FromNode) rightChild, db, context, readOnly);
            joinContext.setRightIterator(nestedJoinContext.getRowIterator());
            joinContext.addColumnIdentifiers(nestedJoinContext.getColumnIdentifiers());
            joinContext.setRightColumnCount(nestedJoinContext.getTotalColumnCount());
            
            if (rcol != null) {
                Integer colPos = (Integer) nestedJoinContext.getColumnIdToFieldMap().get(rcol.getCanonicalIdentifier());
                if (colPos != null) {
                    joinContext.setRightColumnPosition(colPos.intValue());
                }
            }
        } else {
            TableIdentifier tid = (TableIdentifier) rightChild;
            Table right = db.getTable(tid);
            if (rcol != null) {
                joinContext.setRightColumnPosition(right.getColumnIndex(rcol.getName()));
            }

            RowIterator rows = joinContext.getRightIterator();
            if (joinContext.isRowsAreSortedByJoinKey()) {
                rows = getIndexedRowsIfSortingRequired(rows, context, right, readOnly, _unappliedTableFilters);
            }
            rows = getRowIteratorFromTable(db, tid, right, rows, readOnly, _unappliedTableFilters);
            rows = makeFilteringRowIterator(tid, right, rows, context, _unappliedTableFilters);

            populateColumnList(joinContext.getColumnIdentifiers(), tid, right, context);
            joinContext.setRightIterator(rows);
            joinContext.setRightColumnCount(right.getColumnCount());

            pushUnwantedConditions(from, joinCondition, tid);
        }
    }

    @SuppressWarnings("unchecked")
    private void populateColumnIdToFieldMap(List columnList, Map colIdToFieldMap) {
        for (int i = 0, I = columnList.size(); i < I; i++) {
            colIdToFieldMap.put(columnList.get(i), ValuePool.getInt(i));
        }
    }

    @SuppressWarnings("unchecked")
    private void populateColumnList(List colList, TableIdentifier tableIdent, Table table, AxionQueryContext context) throws AxionException {
        for (int j = 0, J = table.getColumnCount(); j < J; j++) {
            ColumnIdentifier id = null;
            // Determine which selected column id matches, if any
            for (int k = 0, K = context.getSelectCount(); k < K; k++) {
                Selectable sel = context.getSelect(k);
                if (sel instanceof ColumnIdentifier) {
                    ColumnIdentifier cSel = (ColumnIdentifier) sel;
                    if (tableIdent.equals(cSel.getTableIdentifier()) && cSel.getName().equals(table.getColumn(j).getName())) {
                        id = cSel.getCanonicalIdentifier();
                        break;
                    }
                }
            }

            if (null == id) {
                id = new ColumnIdentifier(tableIdent, table.getColumn(j).getName());
            }
            colList.add(id);
        }
    }

    private QueryPlannerJoinContext processFromTree(FromNode from, Database db, AxionQueryContext context, boolean readOnly) throws AxionException {

        QueryPlannerJoinContext joinContext = new QueryPlannerJoinContext();

        // Process join on and where conditions
        JoinCondition joinCondition = processJoinConditionTree(from);

        // NO Join, may be a correlated-subquery or right nested join
        if (!from.hasRight()) {
            makeLeftRowIterator(from, joinCondition, db, joinContext, joinContext.getLeftTableKey(), context, readOnly);
            joinContext.setRowIterator(joinContext.getLeftIterator());
            return joinContext;
        }

        if (!findFilterOnIndexColumnForRightTable(from, db, joinContext, joinCondition)) {
            // If index found for the join column in right table create
            // ChangingIndexedRowIterator on right table
            makeRightChangingIndexedRowIterator(from, joinCondition, db, joinContext);
        }

        // If index found for the join column in left table create
        // ChangingIndexedRowIterator on the left table
        makeLeftChangingIndexedRowIterator(from, joinCondition, db, joinContext);

        // If no index found then we may create dynamic index
        if (from.getTableCount() > 1 && joinContext.getLeftIterator() == null && joinContext.getRightIterator() == null) {
            createDynamicIndex(from, joinCondition, joinContext, db);
        }
        
        // Get RowIterator from left subtree.
        makeLeftRowIterator(from, joinCondition, db, joinContext, joinContext.getLeftTableKey(), context, readOnly);
        joinContext.setIteratorCount(joinContext.getIteratorCount() + 1);

        // Get RowIterator from right subtree.
        makeRightRowIterator(from, joinCondition, db, joinContext, joinContext.getRightTablekey(), context, readOnly);
        joinContext.setIteratorCount(joinContext.getIteratorCount() + 1);

        // If index found or created for the outer table then use
        // IndexNestedLoopRowIterator.
        makeIndexNestedLoopJoinedRowIterator(from, joinCondition, joinContext);
        // Else use NestedLoopJoinedIterator.
        if (joinContext.getRowIterator() == null) {
            makeNestedLoopJoinedIterator(from, joinCondition, joinContext);
        }
        return joinContext;
    }
    
    private boolean findFilterOnIndexColumnForRightTable(FromNode from, Database db, QueryPlannerJoinContext joinContext, JoinCondition joinCondition)
            throws AxionException {
        if (!_isAllInnerJoin) {
            return false;
        }

        Function fn = null;
        if (from.isInnerJoin() && from.getRight() instanceof TableIdentifier) {
            TableIdentifier rtid = (TableIdentifier) from.getRight();
            Table rightTable = db.getTable(rtid);
            fn = AxionQueryOptimizer.findColumnLiteralEqualFunction(rtid, rightTable, _unappliedTableFilters, true);
            if (fn != null) {
                fn = AxionQueryOptimizer.findFirstColumnColumnComparisonFunction(joinCondition.getNodes(), rtid, rightTable, false);
                if (fn != null) {
                    // Assume that right argument of function is from right table
                    joinContext.setLeftTableKey((ColumnIdentifier) fn.getArgument(0));
                    joinContext.setRightTablekey((ColumnIdentifier) fn.getArgument(1));

                    // Right argument of function is not from right table so flip it
                    if (!rtid.equals(joinContext.getRightTablekey().getTableIdentifier())) {
                        joinContext.flipKey();
                    }
                    joinContext.setSwapRightToLeft(true);
                    pushUnwantedConditions(from, joinCondition, rtid);
                }
            }
        }
        return (fn != null ? true : false);
    }

    @SuppressWarnings("unchecked")
    private JoinCondition processJoinConditionTree(FromNode from) throws AxionException {
        // ANSI Join Syntax: if join type is inner join we can derive condition from where
        // node for other table based on the join condition
        Set joinConditions = AxionQueryOptimizer.flatConditionTree(from.getCondition());
        if (from.isInnerJoin()) {
            Set joinAndWhereConditions = new LinkedHashSet();
            joinAndWhereConditions.addAll(joinConditions);
            joinAndWhereConditions.addAll(_unappliedWhereNodes);
            Set conditions = AxionQueryOptimizer.deriveTableFilter(joinAndWhereConditions, _isAllInnerJoin);
            Set[] splitConditions = AxionQueryOptimizer.decomposeWhereConditionNodes(conditions, _isAllInnerJoin);
            _unappliedTableFilters.addAll(splitConditions[1]);
            _unappliedWhereNodes = splitConditions[2];

            return new JoinCondition(splitConditions[0]);
        } else {
            return new JoinCondition(joinConditions);
        }
    }

    @SuppressWarnings("unchecked")
    private RowIterator processQuery(Database db, boolean readOnly, AxionQueryContext context) throws AxionException {
        _unappliedWhereNodes = AxionQueryOptimizer.flatConditionTree(context.getWhere());
        _isAllInnerJoin = AxionQueryOptimizer.isAllInnerJoin(context.getFrom());
        
        QueryPlannerJoinContext rootJoinContext = processFromTree(context.getFrom(), db, context, readOnly);

        // Get _rowIterator for literals (if any) in the select list
        RowIterator literaliter = makeLiteralRowIterator(rootJoinContext.getColumnIdentifiers());
        if (literaliter != null) {
            rootJoinContext.setIteratorCount(rootJoinContext.getIteratorCount() + 1);
            int literaliterRowCount = literaliter.first().size();
            RowIterator cpjRows = new NestedLoopJoinedRowIterator(rootJoinContext.getRowIterator(), literaliter, literaliterRowCount);
            rootJoinContext.setRowIterator(cpjRows);
        }
        RowIterator rows = rootJoinContext.getRowIterator();

        Map colIdToFieldMap = clearAndGetColumnIdToFieldMap();
        populateColumnIdToFieldMap(rootJoinContext.getColumnIdentifiers(), colIdToFieldMap);

        // And apply any remaining where nodes to the join.
        if (!_unappliedWhereNodes.isEmpty() || !_unappliedTableFilters.isEmpty()) {
            _unappliedWhereNodes.addAll(_unappliedTableFilters);
            Selectable unappliedWhere = AxionQueryOptimizer.createOneRootFunction(_unappliedWhereNodes);
            rows = new FilteringRowIterator(rows, new RowDecorator(colIdToFieldMap), unappliedWhere);
            addExplainRow(rows);
        }
        return rows;
    }

    private RowIterator processQueryForSingleTable(Database db, boolean readOnly, AxionQueryContext context) throws AxionException {
        Map colIdToFieldMap = clearAndGetColumnIdToFieldMap();
        Table table = db.getTable(context.getTables(0));
        List columnList = new ArrayList();
        populateColumnList(columnList, context.getTables(0), table, context);
        RowIterator literaliter = makeLiteralRowIterator(columnList);
        populateColumnIdToFieldMap(columnList, colIdToFieldMap);

        RowIterator rows = null;
        if (context.getOrderByCount() == 1 && context.getGroupByCount() < 1) {
            rows = makeOrderedIndexBasedRowIterator(readOnly, context, table);
        } else if (context.getGroupByCount() == 1) {
            rows = makeGroupedIndexBasedRowIterator(readOnly, context, table);
        }

        if (rows == null) {
            _unappliedWhereNodes = AxionQueryOptimizer.flatConditionTree(context.getWhere());
            rows = getRowIteratorFromTable(db, context.getTables(0), table, rows, readOnly, _unappliedWhereNodes);
        }

        // Add RowIterator for literals (if any) in the select list
        if (literaliter != null) {
            int literaliterRowCount = literaliter.first().size();
            rows = new NestedLoopJoinedRowIterator(rows, literaliter, literaliterRowCount);
            addExplainRow(rows);
        }

        if (_unappliedWhereNodes != null && !_unappliedWhereNodes.isEmpty()) {
            Selectable unappliedWhere = AxionQueryOptimizer.createOneRootFunction(_unappliedWhereNodes);
            rows = new FilteringRowIterator(rows, new RowDecorator(colIdToFieldMap), unappliedWhere);
            addExplainRow(rows);
        }
        return rows;
    }

    private RowIterator processQueryWithNoTable(AxionQueryContext context) throws AxionException {
        RowIterator rows;
        Map colIdToFieldMap = clearAndGetColumnIdToFieldMap();
        List columnList = new ArrayList();
        rows = makeLiteralRowIterator(columnList);
        populateColumnIdToFieldMap(columnList, colIdToFieldMap);
        if (rows == null) {
            rows = new SingleRowIterator(new SimpleRow(0));
        }
        if (context.getWhere() != null) {
            rows = new FilteringRowIterator(rows, new RowDecorator(colIdToFieldMap), context.getWhere());
        }
        return rows;
    }

    private void resolveOrderByAfterApplyingGroupBy(AxionQueryContext context, Selectable oldSel, Selectable newSel) {
        Selectable temp = null;
        for (int i = 0, I = context.getOrderByCount(); i < I; i++) {
            temp = context.getOrderBy(i).getSelectable();
            if (oldSel.equals(temp)) {
                context.getOrderBy(i).setSelectable(newSel);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void resolveSelectedAfterGrouping(AxionQueryContext context, Map colIdToFieldMap) throws AxionException {
        colIdToFieldMap.clear();
        Selectable[] newSelected = new Selectable[context.getSelectCount()];

        for (int i = 0, I = context.getSelectCount(); i < I; i++) {
            Selectable sel = context.getSelect(i);
            TableIdentifier tid = null;

            if (sel instanceof ColumnIdentifier) {
                tid = ((ColumnIdentifier) sel).getTableIdentifier();
            } else {
                tid = new TableIdentifier(context.getAliasName());
            }
            newSelected[i] = new ColumnIdentifier(tid, sel.getLabel(), null, sel.getDataType());

            if (context.getGroupByCount() > 0) {
                resolveOrderByAfterApplyingGroupBy(context, sel, newSelected[i]);
            }
            colIdToFieldMap.put(newSelected[i], ValuePool.getInt(i));
        }

        context.setSelected(newSelected);
        context.setResolvedSelect(Arrays.asList(newSelected));
    }

    private boolean setIndexUsedForGroupBy(boolean indexUsed) {
        return _wasIndexUsedForGroupBy = indexUsed;
    }

    private void setSkipSorting(boolean skipSorting) {
        _skipSorting = skipSorting;
    }

    private boolean skipSorting() {
        return _skipSorting;
    }

    // Swap the key , in case index is available for the other table we will make use of
    // index for order by and group by and avoid extra sorting. Note that we don't swap
    // key for index scan for non-equal function
    private void swapKeyForSortingIfUSedInOrderByOrGroupBy(ColumnIdentifier current, ColumnIdentifier swapWith) {
        AxionQueryContext context = getContext();
        if (context.getGroupByCount() == 1) {
            ColumnIdentifier grpCol = (ColumnIdentifier) context.getGroupBy(0);
            if (grpCol.equals(current)) {
                context.setGroupBy(0, swapWith);
            }
        }

        // Attempt to get Index based row iterator, if order column is from this table
        if (context.getOrderByCount() == 1) {
            // List OrdList = context.getOrderBy();
            OrderNode node = context.getOrderBy(0);
            Selectable ordCol = node.getSelectable();
            if (ordCol instanceof ColumnIdentifier && ordCol.equals(current)) {
                node.setSelectable(swapWith);
            }
        }
    }

    private boolean wasIndexUsedForGroupBy() {
        return _wasIndexUsedForGroupBy;
    }

    // Note: For now its only used in Explain command and Junit test cases.
    private class AxionQueryPlanNode {
        List _explainRows = new ArrayList(2);

        @SuppressWarnings("unchecked")
        public void addExplainRow(String value) {
            Row row = new SimpleRow(1);
            row.set(0, value);
            _explainRows.add(row);
        }

        public RowIterator getRowIterator() {
            return new ListRowIterator(_explainRows);
        }
    }

    class JoinCondition {
        private Set _joinConditions;

        // Conditions which specifies join between two tables.
        public JoinCondition(Set jConditions) {
            _joinConditions = jConditions;
        }

        public Set getNodes() {
            return _joinConditions;
        }

        public void setNodes(Set conditions) {
            _joinConditions = conditions;
        }

        public boolean isEmpty() {
            return _joinConditions == null || _joinConditions.isEmpty();
        }

        public Selectable stitchAll() {
            return AxionQueryOptimizer.createOneRootFunction(_joinConditions);
        }

        @Override
        public String toString() {
            return "JoinCondition[" + _joinConditions.toString() + "]";
        }
    }

    class QueryPlannerJoinContext {
        private List _columns = new ArrayList();
        private int _iteratorCount = 0;
        private int _leftColumnCount = -1;
        private int _leftColumnPosition = -1;

        private RowIterator _leftIterator;
        private ColumnIdentifier _leftTableKey;
        private int _rightColumnCount = -1;
        private int _rightColumnPosition = -1;
        private RowIterator _rightIterator;
        private ColumnIdentifier _rightTablekey;
        private RowIterator _rowIterator;

        private boolean _rowsAreSortedByJoinKey = false;
        private boolean _swapRightToLeft = false;

        /**
         * Default constructor
         */
        public QueryPlannerJoinContext() {
        }

        @SuppressWarnings("unchecked")
        public void addColumnIdentifiers(List columns) {
            _columns.addAll(columns);
        }

        public void flipKey() {
            ColumnIdentifier temp = _rightTablekey;
            _rightTablekey = _leftTableKey;
            _leftTableKey = temp;
        }

        public List getColumnIdentifiers() {
            return _columns;
        }

        @SuppressWarnings("unchecked")
        public Map getColumnIdToFieldMap() {
            int size = _columns.size();
            Map colIdToFieldMap = new HashMap(size);
            for (int i = 0; i < size; i++) {
                colIdToFieldMap.put(_columns.get(i), ValuePool.getInt(i));
            }
            return colIdToFieldMap;
        }

        /**
         * @return Returns the _iteratorCount.
         */
        public int getIteratorCount() {
            return _iteratorCount;
        }

        /**
         * @return Returns the _leftColumnCount.
         */
        public int getLeftColumnCount() {
            return _leftColumnCount;
        }

        /**
         * @return Returns the _leftColumnPosition.
         */
        public int getLeftColumnPosition() {
            return _leftColumnPosition;
        }

        /**
         * @return Returns the _leftIterator.
         */
        public RowIterator getLeftIterator() {
            return _leftIterator;
        }

        /**
         * @return Returns the _leftTableKey.
         */
        public ColumnIdentifier getLeftTableKey() {
            return _leftTableKey;
        }

        /**
         * @return Returns the _rightColumnCount.
         */
        public int getRightColumnCount() {
            return _rightColumnCount;
        }

        /**
         * @return Returns the _rightColumnPosition.
         */
        public int getRightColumnPosition() {
            return _rightColumnPosition;
        }

        /**
         * @return Returns the _rightIterator.
         */
        public RowIterator getRightIterator() {
            return _rightIterator;
        }

        /**
         * @return Returns the _rightTablekey.
         */
        public ColumnIdentifier getRightTablekey() {
            return _rightTablekey;
        }

        /**
         * @return Returns the _rowIterator.
         */
        public RowIterator getRowIterator() {
            return _rowIterator;
        }

        public int getTotalColumnCount() {
            return _leftColumnCount + _rightColumnCount;
        }

        /**
         * @param _iteratorCount The _iteratorCount to set.
         */
        public void setIteratorCount(int iteratorCount) {
            _iteratorCount = iteratorCount;
        }

        /**
         * @param _leftColumnCount The _leftColumnCount to set.
         */
        public void setLeftColumnCount(int leftColumnCount) {
            _leftColumnCount = leftColumnCount;
        }

        /**
         * @param _leftColumnPosition The _leftColumnPosition to set.
         */
        public void setLeftColumnPosition(int leftColumnPosition) {
            _leftColumnPosition = leftColumnPosition;
        }

        /**
         * @param _leftIterator The _leftIterator to set.
         */
        public void setLeftIterator(RowIterator leftIterator) {
            _leftIterator = leftIterator;
        }

        /**
         * @param _leftTableKey The _leftTableKey to set.
         */
        public void setLeftTableKey(ColumnIdentifier leftTableKey) {
            _leftTableKey = leftTableKey;
        }

        /**
         * @param _rightColumnCount The _rightColumnCount to set.
         */
        public void setRightColumnCount(int rightColumnCount) {
            _rightColumnCount = rightColumnCount;
        }

        /**
         * @param _rightColumnPosition The _rightColumnPosition to set.
         */
        public void setRightColumnPosition(int rightColumnPosition) {
            _rightColumnPosition = rightColumnPosition;
        }

        /**
         * @param _rightIterator The _rightIterator to set.
         */
        public void setRightIterator(RowIterator rightIterator) {
            _rightIterator = rightIterator;
        }

        /**
         * @param _rightTablekey The _rightTablekey to set.
         */
        public void setRightTablekey(ColumnIdentifier rightTablekey) {
            _rightTablekey = rightTablekey;
        }

        /**
         * @param _rowIterator The _rowIterator to set.
         */
        public void setRowIterator(RowIterator rowIterator) {
            _rowIterator = rowIterator;
        }

        /**
         * @return true if Rows are sorted by left key
         */
        public boolean isRowsAreSortedByJoinKey() {
            return _rowsAreSortedByJoinKey;
        }

        /**
         * @param areSortedByLeftKey
         */
        public void setRowsAreSortedByJoinKey(boolean areSortedByJoinKey) {
            _rowsAreSortedByJoinKey = areSortedByJoinKey;
        }
        
        public boolean swapRightToLeft() {
            return _swapRightToLeft;
        }

        public void setSwapRightToLeft(boolean swapRightToLeft) {
            _swapRightToLeft = swapRightToLeft;
        }
        
        @Override
        public String toString() {
            StringBuffer buf = new StringBuffer(20);
            buf.append("JoinContext{");
            buf.append("\niteratorCount=").append(_iteratorCount);
            buf.append("\nrowIterator=").append(_rowIterator);

            buf.append("\n\nleftColumnCount=").append(_leftColumnCount);
            buf.append("\nleftColumnPosition=").append(_leftColumnPosition);
            buf.append("\nleftIterator=").append(_leftIterator);
            buf.append("\nleftTableKey=").append(_leftTableKey);

            buf.append("\n\nrightColumnCount=").append(_rightColumnCount);
            buf.append("\nrightColumnPosition=").append(_rightColumnPosition);
            buf.append("\nrightIterator=").append(_rightIterator);
            buf.append("\nrightTablekey=").append(_rightTablekey);

            buf.append("\n\nrowsAreSortedByJoinKey=").append(_rowsAreSortedByJoinKey);
            buf.append("}");
            return buf.toString();
        }
    }

    private Map _colIdToFieldMap = new HashMap();
    private AxionQueryContext _context;
    private List _literals;
    private RowDecorator _parentRow;
    private Set _unappliedWhereNodes = new LinkedHashSet();
    private Set _unappliedTableFilters = new LinkedHashSet();
    private boolean _isAllInnerJoin = false;    

    private AxionQueryPlanNode _planNode = new AxionQueryPlanNode();
    private boolean _skipSorting = false;
    private boolean _doReverseSorting = false;
    private boolean _wasIndexUsedForGroupBy = false;

}
