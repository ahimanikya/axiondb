/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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
import java.util.Iterator;
import java.util.List;

import org.axiondb.FromNode;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;

/**
 * AxionQueryContext holds metadata for the Query or Sub-Query.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class AxionQueryContext {

    public AxionQueryContext() {
    }

    @SuppressWarnings("unchecked")
    public void addAllSelectToResolvedSelect() {
        if(_resolvedSelect == null) {
            _resolvedSelect = new ArrayList(_select.size());
        }
        _resolvedSelect.addAll(_select);
    }

    /**
     * Adds a {@link TableIdentifier}to the list of tables being selected from.
     * 
     * @param table a {@link TableIdentifier}
     * @throws IllegalStateException if I have already been resolved
     */
    public void addFrom(TableIdentifier table) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        if (_from == null) {
            _from = new FromNode();
            _from.setType(FromNode.TYPE_SINGLE);
        }

        if (_from.getLeft() == null) {
            _from.setLeft(table);
        } else {
            _from.setRight(table);
            _from.setType(FromNode.TYPE_INNER);
        } 
    }

    /**
     * Appends an {@link OrderNode}to the order by clause for this query
     * 
     * @param orderby an {@link OrderNode}to append
     * @throws IllegalStateException if I have already been resolved
     */
    @SuppressWarnings("unchecked")
    public void addOrderBy(OrderNode orderby) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        if(_orderBy == null) {
            _orderBy = new ArrayList(4);
        }
        _orderBy.add(orderby);
    }

    /**
     * Adds a {@link Selectable}to the list of items being selected.
     * 
     * @param column the {@link Selectable}to add
     * @throws IllegalStateException if I have already been resolved
     */
    @SuppressWarnings("unchecked")
    public void addSelect(Selectable column) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _select.add(column);
    }

    public boolean foundAggregateFunction() {
        return _foundAggregateFunction;
    }

    public String getAliasName() {
        return _aliasName;
    }

    /**
     * Indicates if the {@link java.sql.ResultSet}generated from this object will contain
     * distinct tuples.
     * 
     * @return <code>true</code> for distinct tuples
     */
    public boolean getDistinct() {
        return _distinct;
    }

    /**
     * Gets the root {@link FromNode}for the select statement.
     */
    public FromNode getFrom() {
        return _from;
    }

    /**
     * Gets the <i>i </i> <sup>th </sup> table being selected. Clients should treat the
     * returned value as immutable.
     * 
     * @param i the zero-based index
     */
    public TableIdentifier getFrom(int i) {
        TableIdentifier[] tableIDs = _from.toTableArray();
        return (tableIDs[i]);
    }

    public TableIdentifier[] getFromArray() {
        if (_from != null) {
            return (_from.toTableArray());
        }
        return (null);
    }

    /**
     * Gets the number of tables being from.
     */
    public int getFromCount() {
        return _from.getTableCount();
    }

    /**
     * Gets Selectable in Group by clause.
     */
    @SuppressWarnings("unchecked")
    public List getGroupBy() {
        if (_groupBy != null) {
            return Collections.unmodifiableList(_groupBy);
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Gets Selectable in Group by clause. Clients should treat the returned value as
     * immutable.
     * 
     * @param i the zero-based index
     */
    public Selectable getGroupBy(int i) {
        if (_groupBy != null) {
            return (Selectable) (_groupBy.get(i));
        } 
        return null;
    }

    /**
     * Gets the number of {@link Slectable}s group by in my query.
     */
    public int getGroupByCount() {
        return (null == _groupBy ? 0 : _groupBy.size());
    }

    public Literal getLimit() {
        return _limit;
    }

    public Literal getOffset() {
        return _offset;
    }

    /**
     * Gets the List of {@link OrderNode}in my order by clause.
     */
    @SuppressWarnings("unchecked")
    public List getOrderBy() {
        if (_orderBy != null) {
            return Collections.unmodifiableList(_orderBy);
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Gets the <i>i </i> <sup>th </sup> {@link OrderNode}in my order by clause. Clients
     * should treat the returned value as immutable.
     * 
     * @param i the zero-based index
     */
    public OrderNode getOrderBy(int i) {
        if(_orderBy != null) {
            return (OrderNode) (_orderBy.get(i));
        }
        return null;
    }

    /**
     * Gets the number of {@link OrderNode}s in my query.
     */
    public int getOrderByCount() {
        return (null == _orderBy ? 0 : _orderBy.size());
    }

    public RowDecorator getParentRow() {
        return _parentRow;
    }

    public TableIdentifier[] getParentTables() {
        return _parentTables;
    }

    public List getResolvedSelect() {
        if(_resolvedSelect == null) {
            _resolvedSelect = new ArrayList();
        }
        return _resolvedSelect;
    }

    public RowIterator getRows() {
        return _rows;
    }

    public List getSelect() {
        return _select;
    }

    /**
     * Gets the <i>i </i> <sup>th </sup> {@link Selectable}being selected. Clients should
     * treat the returned value as immutable.
     * 
     * @param i the zero-based index
     */
    public Selectable getSelect(int i) {
        return (Selectable) (_select.get(i));
    }

    /**
     * Gets the number of {@link Selectable}s being selected.
     */
    public int getSelectCount() {
        return _select.size();
    }

    public Selectable[] getSelected() {
        return _selected;
    }

    public TableIdentifier[] getTables() {
        return _tables;
    }
    
    public TableIdentifier getTables(int i) {
        return _tables[i];
    }
    
    public int getTableCount() {
        return (null == _tables ? 0 : _tables.length);
    }

    /**
     * Returns the {@link Selectable where tree}for this query. Clients should treat the
     * returned value as immutable.
     * 
     * @return the {@link Selectable where tree}for this query, or <tt>null</tt>.
     */
    public Selectable getWhere() {
        return _where;
    }
    
    public Selectable getHaving() {
        return _having;
    }

    public boolean isCorrelatedSubQuery() {
        return _isCorrelatedSubQuery;
    }

    public boolean isExplain() {
        return _explain;
    }
    
    public boolean isResolved() {
        return _resolved;
    }

    public boolean isTablePartOfSelect(TableIdentifier tid) {
        for (int i = 0; i < _tables.length; i++) {
            if (tid.equals(_tables[i])) {
                return true;
            }
        }
        return false;
    }

    public void setAliasName(String name) {
        _aliasName = name;
    }
    
    public void setCorrelatedSubQuery(boolean isCorrelatedSubQuery) {
        _isCorrelatedSubQuery = isCorrelatedSubQuery;
    }

    /**
     * Determines if the {@link java.sql.ResultSet}generated from this object will
     * contain distinct tuples (default is false).
     * 
     * @param distinct true for distinct tuples
     */
    public void setDistinct(boolean distinct) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _distinct = distinct;
    }

    public void setExplain(boolean explain) {
        _explain = explain;
    }

    public void setFoundAggregateFunction(boolean found) {
        _foundAggregateFunction = found;
    }

    /**
     * Sets the root {@link FromNode}for the select statement.
     */
    public void setFrom(FromNode from) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _from = from;
    }

    /**
     * Sets the group by clause for this query.
     * 
     * @param groupby a {@link List}of {@link Selectable}s.
     * @throws IllegalStateException if I have already been resolved
     */
    public void setGroupBy(List groupby) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _groupBy = groupby;
    }
    
    @SuppressWarnings("unchecked")
    void setGroupBy(int i, Selectable column) {
        if(_groupBy == null) {
            throw new IllegalArgumentException("GroupBy List is Empty");
        }
        _groupBy.set(i, column);
    }

    public void setLimit(Literal limit) {
        _limit = limit;
    }

    public void setOffset(Literal offset) {
        _offset = offset;
    }

    /**
     * Sets the order by clause for this query.
     * 
     * @param orderby a {@link List}of {@link OrderNode}s.
     * @throws IllegalStateException if I have already been resolved
     */
    public void setOrderBy(List orderby) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _orderBy = orderby;
    }

    public void setParentRow(RowDecorator row) {
        _parentRow = row;
    }

    public void setParentTables(TableIdentifier[] tables) {
        _parentTables = tables;
    }

    public void setResolved(boolean resolved) {
        _resolved = resolved;
    }

    public void setResolvedSelect(List select) {
        _resolvedSelect = select;
    }

    public void setRows(RowIterator rows) {
        _rows = rows;
    }

    /**
     * Sets the <i>i </i> <sup>th </sup> {@link Selectable}being selected.
     * 
     * @param i the zero-based index
     * @param sel the new {@link Selectable}
     * @throws IllegalStateException if I have already been resolved
     */
    @SuppressWarnings("unchecked")
    public void setSelect(int i, Selectable sel) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _select.set(i, sel);
    }

    public void setSelect(List columns) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _select = columns;
    }

    public void setSelected(Selectable[] selected) {
        _selected = selected;
    }

    public void setTables(TableIdentifier[] tables) {
        _tables = tables;
    }

    /**
     * Sets the {@link Selectable where tree}for this query.
     * 
     * @param where a boolean valued {@link Selectable}
     * @throws IllegalStateException if I have already been resolved
     */
    public void setWhere(Selectable where) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _where = where;
    }
    
    public void setHaving(Selectable having) {
        if (_resolved) {
            throw new IllegalStateException("Already resolved.");
        }
        _having = having;
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("SELECT ");
        if (_distinct) {
            buf.append("DISTINCT ");
        }
        {
            Iterator iter = _select.iterator();
            while (iter.hasNext()) {
                buf.append(iter.next());
                if (iter.hasNext()) {
                    buf.append(", ");
                }
            }
        }
        if (null != _from) {
            buf.append(" FROM ");
            buf.append(_from);
        }

        if (null != _where) {
            buf.append(" WHERE ");
            buf.append(_where);
        }
        if (null != _groupBy && !_groupBy.isEmpty()) {
            buf.append(" GROUP BY ");
            {
                Iterator iter = _groupBy.iterator();
                while (iter.hasNext()) {
                    buf.append(iter.next());
                    if (iter.hasNext()) {
                        buf.append(", ");
                    }
                }
            }
        }
        
        if (null != _having) {
            buf.append(" HAVING ");
            buf.append(_having);
        }
        
        if (null != _orderBy && !_orderBy.isEmpty()) {
            buf.append(" ORDER BY ");
            {
                Iterator iter = _orderBy.iterator();
                while (iter.hasNext()) {
                    buf.append(iter.next());
                    if (iter.hasNext()) {
                        buf.append(", ");
                    }
                }
            }
        }
        return buf.toString();
    }
    
    @SuppressWarnings("unchecked")
    public List createLiteralList() {
        List literals = null;
        for (int i = 0, I = getSelectCount(); i < I; i++) {
            if(getSelect(i) instanceof Literal) {
                if(null == literals) {
                    literals = new ArrayList();
                }
                literals.add(getSelect(i));
            }
        }
        return literals;
    }
    
    private String _aliasName;
    private boolean _distinct = false;

    private boolean _explain = false;
    private boolean _foundAggregateFunction = false;
    private FromNode _from;
    private List _groupBy;
    private Selectable _having;
    private boolean _isCorrelatedSubQuery;
    private Literal _limit;

    private Literal _offset;
    private List _orderBy;
    private RowDecorator _parentRow;
    private TableIdentifier[] _parentTables;
    private boolean _resolved = false;
    private List _resolvedSelect;
    private RowIterator _rows;

    private List _select = new ArrayList();
    private Selectable[] _selected;
    private TableIdentifier[] _tables;
    private Selectable _where;
}
