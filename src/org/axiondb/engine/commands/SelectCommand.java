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

package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.OrderNode;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.rowiterators.RowIteratorRowDecoratorIterator;
import org.axiondb.engine.visitors.AmbiguousColumnReferenceVisitor;
import org.axiondb.engine.visitors.AssertGroupByRulesVisitor;
import org.axiondb.engine.visitors.FindAggregateFunctionVisitor;
import org.axiondb.engine.visitors.FindBindVariableVisitor;
import org.axiondb.engine.visitors.ResolveFromNodeVisitor;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.types.StringType;

/**
 * A <tt>SELECT</tt> query.
 * 
 * @version  
 * @author Morgan Delagrange
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Amrish Lal
 * @author Dave Pekarek Krohn
 * @author Rahul Dwivedi
 * @author Ahimanikya Satapathy
 */
public class SelectCommand extends BaseAxionCommand {
    //------------------------------------------------------------ Constructors

    public SelectCommand(AxionQueryContext context) {
        _context = context;
        _planner = new AxionQueryPlanner(context);
    }

    public boolean execute(Database database) throws AxionException {
        setResultSet(executeQuery(database));
        return (getResultSet() != null);
    }

    //-------------------------------------------------- Command Implementation

    /**
     * Execute this command, returning a {@link java.sql.ResultSet}.
     * 
     * @return the {@link java.sql.ResultSet}generated by this command.
     * @throws AxionException
     */
    public AxionResultSet executeQuery(Database db) throws AxionException {
        return executeQuery(db, true);
    }
    
    @Override
    public AxionResultSet executeQuery(Database db, boolean isReadOnly) throws AxionException {
        RowIterator rows = makeRowIterator(db, isReadOnly);
        if (_context.isExplain()) {
            return makeExplainResultSet(rows);
        }
        return new AxionResultSet(new RowIteratorRowDecoratorIterator(rows, new RowDecorator(_planner.getColumnIdToFieldMap())),
            _context.getSelected(), null);
    }

    /** Unsupported */
    public int executeUpdate(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use executeQuery, not executeUpdate.");
    }

    public Map getColumnIdToFieldMap() {
        return _planner.getColumnIdToFieldMap();
    }

    public AxionQueryContext getQueryContext() {
        return _context;
    }

    @Override
    public String toString() {
        return _context.toString();
    }

    protected void buildTableList(Database db) throws AxionException {
        _context.setTables(_context.getFromArray());
    }

    @Override
    protected void buildBindVariables() {
        setBindVariableVisitor(new FindBindVariableVisitor());
        getBindVariableVisitor().visit(this);
    }

    public RowIterator makeRowIterator(Database db, boolean isReadOnly) throws AxionException {
        return makeRowIterator(db, isReadOnly, false);
    }
    
    public RowIterator makeRowIterator(Database db, boolean isReadOnly, boolean refresh) throws AxionException {
        resolve(db);
        if (refresh || _currentDatabase != db || _context.foundAggregateFunction() || (_context.getGroupByCount() > 0)) {
            _context.setSelected(generateSelectArrayForResultSet());
            _context.setRows(_planner.makeRowIterator(db, isReadOnly));
        } else {
            _context.getRows().reset();
        }
        _currentDatabase = db;

        return _context.getRows();
    }

    @SuppressWarnings("unchecked")
    protected void resolve(Database db) throws AxionException {

        if (!_context.isResolved()) {

            // resolve from node for any sub-select
            ResolveFromNodeVisitor fnVisitor = new ResolveFromNodeVisitor();
            fnVisitor.resolveFromNode(_context.getFrom(), db);
            buildTableList(db); // Sub-Select will inlude tables from outer select

            // resolve SELECT part
            List tempList = new ArrayList();
            for (int i = 0, I = _context.getSelectCount(); i < I; i++) {
                Selectable selectable = resolveSelectable(_context.getSelect(i), db, tempList, _context.getTables());
                _context.setSelect(i, selectable);
                tempList.add(selectable);
            }
            tempList = null;

            // resolve for "*"
            Selectable sel = _context.getSelect(0);
            if (sel instanceof ColumnIdentifier && "*".equals(((ColumnIdentifier) sel).getName())) {
                addColumnIdentifiersForStar(db, _context.getResolvedSelect(), _context.getTables(), (ColumnIdentifier) sel);
            } else {
                _context.addAllSelectToResolvedSelect();
            }

            // resolve FROM part
            fnVisitor.resolveFromNode(_context.getFrom(), db, _context.getResolvedSelect());

            // resolve WHERE part
            _context.setWhere(resolveSelectable(_context.getWhere(), db, _context.getResolvedSelect(), _context.getTables()));

            FindAggregateFunctionVisitor findAggr = new FindAggregateFunctionVisitor();
            findAggr.visit(_context.getWhere()); // check for aggregate functions in where
                                                 // clause
            if (findAggr.foundAggregateFunction()) {
                throw new AxionException("group function is not allowed here");
            }

            // resolve Group BY part
            if (!_context.getGroupBy().isEmpty()) {
                ArrayList temp = new ArrayList();
                for (int i = 0, I = _context.getGroupByCount(); i < I; i++) {
                    Selectable gp = _context.getGroupBy(i);
                    Selectable gp1 = resolveSelectable(gp, db, _context.getResolvedSelect(), _context.getTables());
                    temp.add(gp1);
                }
                _context.setGroupBy(temp);
            }
            AssertGroupByRulesVisitor gpVisitor = new AssertGroupByRulesVisitor();
            boolean foundScalar = gpVisitor.visit(_context.getSelect(), _context.getGroupBy());
            _context.setFoundAggregateFunction(!foundScalar);

            // resolve HAVING part
            gpVisitor = new AssertGroupByRulesVisitor();
            _context.setHaving(resolveSelectable(_context.getHaving(), db, _context.getResolvedSelect(), _context.getTables()));
            gpVisitor.visit(Collections.singletonList(_context.getHaving()), _context.getGroupBy());
            
            // resolve ORDER BY part
            if (!_context.getOrderBy().isEmpty()) {
                for (int i = 0, I = _context.getOrderByCount(); i < I; i++) {
                    OrderNode ob = _context.getOrderBy(i);
                    ob.setSelectable(resolveSelectable(ob.getSelectable(), db, _context.getResolvedSelect(), _context.getTables()));
                }
            }
            
            if(!_context.getOrderBy().isEmpty()) {
                AmbiguousColumnReferenceVisitor ambiguityCheck = new AmbiguousColumnReferenceVisitor();
                ambiguityCheck.visit(_context.getSelect(), _context.getOrderBy());
            }

            _context.setResolved(true);
        }
    }

    @SuppressWarnings("unchecked")
    private void addColumnIdentifiersForStar(Database db, List list, TableIdentifier[] tables, ColumnIdentifier colid) throws AxionException {

        if (null == colid.getTableName()) {
            for (int j = 0, J =_context.getFromCount(); j < J; j++) {
                list.addAll(getColIdentifierList(db.getTable(tables[j]), tables[j]));
            }
        } else {
            TableIdentifier tid = colid.getTableIdentifier();
            list.addAll(getColIdentifierList(db.getTable(tid), tid));
        }
    }

    @SuppressWarnings("unchecked")
    private Selectable[] generateSelectArrayForResultSet() throws AxionException {
        List list = _context.getResolvedSelect();
        return (Selectable[]) (list.toArray(new Selectable[list.size()]));
    }

    @SuppressWarnings("unchecked")
    private AxionResultSet makeExplainResultSet(RowIterator iter) {
        Map map = new HashMap();
        Selectable resultcolumn = new ColumnIdentifier(null, "EXPLANATION", null, StringType.instance());
        map.put(resultcolumn, new Integer(0));
        RowIterator rowIter = _planner.getPlanNodeRowIterator();
        return new AxionResultSet(new RowIteratorRowDecoratorIterator(rowIter, new RowDecorator(map)), new Selectable[] { resultcolumn}, null);
    }

    //----------------------------------------------------------------- Members
    protected AxionQueryContext _context;
    protected Database _currentDatabase;
    protected AxionQueryPlanner _planner;

}