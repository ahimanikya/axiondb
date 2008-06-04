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

package org.axiondb;

import org.axiondb.types.AnyType;

/**
 * An identifier for a column.
 * <p>
 * Column names and aliases always stored (and returned) in upper case.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class ColumnIdentifier extends BaseSelectable implements Selectable {

    /**
     * @param column the local name of my column
     */
    public ColumnIdentifier(String column) {
        this(null, column);
    }

    /**
     * @param table my table, which may be <code>null</code>
     * @param column my column
     */
    public ColumnIdentifier(TableIdentifier table, String columnName) {
        this(table, columnName, null);
    }

    /**
     * @param table my table, which may be <code>null</code>
     * @param column my column
     * @param columnAlias the alias for my column, which may be <code>null</code>
     */
    public ColumnIdentifier(TableIdentifier table, String columnName, String columnAlias) {
        this(table, columnName, columnAlias, null);
    }

    /**
     * @param table my table, which may be <code>null</code>
     * @param column my column
     * @param columnAlias the alias for my column, which may be <code>null</code>
     * @param type the {@link DataType}of my column, which may be <code>null</code>
     */
    public ColumnIdentifier(TableIdentifier table, String columnName, String columnAlias, DataType type) {
        setName(columnName);

        if (null == _table) {
            _table = table;
        }

        setAlias(columnAlias);
        _type = (null == type ? AnyType.INSTANCE : type);
    }

    /**
     * Returns <code>true</code> iff <i>otherobject </i> is a {@link ColumnIdentifier}
     * whose name, table identifier, and alias are equal to mine.
     */
    @Override
    public boolean equals(Object otherobject) {
        if (this == otherobject || (_canonicalForm != null && _canonicalForm == otherobject)) {
            return true;
        }
        
        if (otherobject instanceof ColumnIdentifier) {
            ColumnIdentifier that = (ColumnIdentifier) otherobject;
            String thisName = getName();
            String thatName = that.getName();
            String thisAlias = getAlias();
            String thatAlias = that.getAlias();
            return ((null == thisName ? null == thatName : thisName.equals(thatName))
                && (null == _table ? null == that._table : _table.equals(that._table)) 
                && (null == thisAlias ? null == thatAlias : thisAlias.equals(thatAlias)));
        }
        return false;
    }

    /**
     * Returns the value of the column I identify within the given <i>row </i>.
     */
    public Object evaluate(RowDecorator row) throws AxionException {
        if (null == row) {
            throw new AxionException("Expected non-null RowDecorator here.");
        }
        return row.get(this);
    }

    public ColumnIdentifier getCanonicalIdentifier() {
        ColumnIdentifier cid = _canonicalForm;
        if (null == cid) {
            cid = new ColumnIdentifier(_table, getName(), null, _type);
            _canonicalForm = cid;
        }
        return cid;
    }

    /**
     * Returns my {@link DataType}, if any.
     */
    public final DataType getDataType() {
        return _type;
    }

    /**
     * Returns the alias name of my table or null. Unlike
     * <code>{@link #getTableIdentifier getTableIdentifier()}.{@link TableIdentifier#getTableAlias getTableAlias()}</code>
     * this method will return <code>null</code> when I don't have a table identifier.
     */
    public final String getTableAlias() {
        return (null == _table ? null : _table.getTableAlias());
    }

    /**
     * Returns my table identifier, if any.
     */
    public final TableIdentifier getTableIdentifier() {
        return _table;
    }

    /**
     * Returns the name of my table or null. Unlike
     * <code>{@link #getTableIdentifier getTableIdentifier()}.{@link TableIdentifier#getTableName getTableName()}</code>
     * this method will return <code>null</code> when I don't have a table identifier.
     */
    public final String getTableName() {
        return (null == _table ? null : _table.getTableName());
    }

    /**
     * Returns a hash code in keeping with the standard {@link Object#equals equals}/
     * {@link Object#hashCode hashCode}contract.
     */
    @Override
    public int hashCode() {
        int hashCode = _hash;
        if (hashCode == 0) {
            String name = getName();
            String alias = getAlias();
            if (null != name) {
                hashCode = name.hashCode();
            }
            if (null != alias) {
                hashCode ^= alias.hashCode();
            }
            if (null != _table) {
                hashCode ^= _table.hashCode() << 4;
            }
            _hash = hashCode;

        }
        return hashCode;
    }

    /**
     * Sets my {@link DataType}, if any.
     */
    public void setDataType(DataType type) {
        _type = (null == type ? AnyType.INSTANCE : type);
        clearCanonicalForm();
    }

    /**
     * Sets the name of this column, and the name of my table if the given name includes "
     * <code>.</code>".
     */
    @Override
    public void setName(String column) {
        if (column != null) {
            int pivot = column.indexOf(".");
            if (pivot != -1) {
                setTableIdentifier(new TableIdentifier(column.substring(0, pivot)));
                column = column.substring(pivot + 1);
            }
            super.setName(column);
        }
        clearCanonicalForm();
    }

    /**
     * Sets my table identifier, if any.
     */
    public void setTableIdentifier(TableIdentifier table) {
        _table = table;
        _hash = 0;
        clearCanonicalForm();
    }

    /**
     * Returns a <code>String</code> representation of me, suitable for debugging
     * output.
     */
    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        if (getTableIdentifier() != null) {
            result.append("(");
            result.append(getTableIdentifier().toString());
            result.append(").");
        }
        result.append(getName());
        if (null != getAlias()) {
            result.append(" AS ");
            result.append(getAlias());
        }
        return result.toString();
    }

    private final void clearCanonicalForm() {
        _canonicalForm = null;
    }

    private ColumnIdentifier _canonicalForm;

    /** My {@link TableIdentifier}, if any. */
    private TableIdentifier _table;
    /** My {@link DataType}, if any. */
    private DataType _type;
    
    private static final long serialVersionUID = -5021851410960110853L;
}
