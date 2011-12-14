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

package org.axiondb.engine.indexes;

import java.io.File;
import java.io.IOException;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.Function;
import org.axiondb.IndexLoader;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Table;
import org.axiondb.engine.ObjectBTreeIndexLoader;
import org.axiondb.engine.rowiterators.EmptyRowIterator;
import org.axiondb.engine.rowiterators.LazyRowRowIterator;
import org.axiondb.event.RowEvent;
import org.axiondb.event.TableModificationListener;
import org.axiondb.functions.ComparisonFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanFunction;
import org.axiondb.functions.GreaterThanOrEqualFunction;
import org.axiondb.functions.IsNotNullFunction;
import org.axiondb.functions.IsNullFunction;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.functions.LessThanOrEqualFunction;
import org.axiondb.util.NullObject;
import org.axiondb.util.ObjectBTree;

/**
 * A {@link BaseBTreeIndex B-Tree index}over <code>Object</code> keys.
 * 
 * @version  
 * @author Dave Pekarek Krohn
 * @author Ritesh Adval
 * @author Charles Ye
 */
public class ObjectBTreeIndex extends BaseBTreeIndex implements TableModificationListener {

    public ObjectBTreeIndex(String name, Column column, boolean unique) throws AxionException {
        this(name, column, unique, null);
    }

    public ObjectBTreeIndex(String name, Column column, boolean unique, File dataDirectory) throws AxionException {
        this(name, column, unique, 1000, dataDirectory);
    }

    private ObjectBTreeIndex(String name, Column column, boolean unique, int minimizationFactor, File dataDirectory) throws AxionException {
        super(name, column, unique);
        try {
            _dataDirectory = dataDirectory;
            _minimizationFactor = minimizationFactor;
            _tree = createTree(_dataDirectory, getName(), _minimizationFactor, getDataType());
        } catch (IOException e) {
            throw new AxionException("Unable to create index file", e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to create index file", e);
        }
    }

    public final void changeRowId(Table table, Row row, int oldId, int newId) throws AxionException {
        try {
            int colnum = table.getColumnIndex(getIndexedColumn().getName());
            Object key = row.get(colnum);
            _tree.replaceId(key, oldId, newId);
        } catch (IOException e) {
            throw new AxionException("Unable to change row id", e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to change row id", e);
        }
    }

    public final ObjectBTree getBTree() {
        return _tree;
    }

    public IndexLoader getIndexLoader() {
        return LOADER;
    }

    public final RowIterator getInorderRowIterator(RowSource source) throws AxionException {
        IntListIterator resultIds = null;
        try {
            resultIds = _tree.inorderIterator();
        } catch (IOException e) {
            throw new AxionException("Unable to retrieve values from index" + getName(), e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to retrieve values from index" + getName(), e);
        }

        return new LazyRowRowIterator(source, resultIds, _tree.size());
    }

    public final RowIterator getRowIterator(RowSource source, Function function, Object value) throws AxionException {
        IntListIterator resultIds = null;
        try {
            if (function instanceof ComparisonFunction) {
                Object convertedValue = getIndexedColumn().getDataType().convert(value);

                if (null == convertedValue) {
                    // null fails all comparisions I support
                    return EmptyRowIterator.INSTANCE;
                }

                if (function instanceof EqualFunction) {
                    if (!isUnique()) {
                        resultIds = _tree.getAll(convertedValue);
                    } else {
                        Integer result = _tree.get(convertedValue);
                        if (result == null) {
                            return EmptyRowIterator.INSTANCE;
                        } else {
                            ArrayIntList ids = new ArrayIntList(1);
                            ids.add(result.intValue());
                            return new LazyRowRowIterator(source, ids.listIterator(), 1);
                        }
                    }
                } else if (function instanceof LessThanFunction) {
                    resultIds = _tree.getAllTo(convertedValue);
                } else if (function instanceof LessThanOrEqualFunction) {
                    resultIds = _tree.getAllTo(getIndexedColumn().getDataType().successor(convertedValue));
                } else if (function instanceof GreaterThanFunction) {
                    resultIds = _tree.getAllFrom(getIndexedColumn().getDataType().successor(convertedValue));
                } else if (function instanceof GreaterThanOrEqualFunction) {
                    resultIds = _tree.getAllFrom(convertedValue);
                } else {
                    throw new AxionException("Unsupported function " + function);
                }
            } else if (function instanceof IsNotNullFunction) {
                resultIds = _tree.getAllExcludingNull();
            } else if (function instanceof IsNullFunction) {
                value = (value == null) ? getNullKey() : value;
                resultIds = _tree.getAll(value);
            } else {
                throw new AxionException("Unsupported function " + function);
            }
        } catch (IOException e) {
            throw new AxionException("Unable to retrieve values from index" + getName(), e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to retrieve values from index" + getName(), e);
        }

        // return new LazyRowRowIterator(source, resultIds);

        // FIXME: There should be a better way to fix concurent modification issue
        ArrayIntList ids = new ArrayIntList();
        while (resultIds.hasNext()) {
            ids.add(resultIds.next());
        }
        return new LazyRowRowIterator(source, ids.listIterator(), ids.size());
    }

    public final void rowDeleted(RowEvent event) throws AxionException {
        String colName = getIndexedColumn().getName();
        int colIndex = event.getTable().getColumnIndex(colName);
        Object key = event.getOldRow().get(colIndex);
        int rowid = event.getOldRow().getIdentifier();
        key = (key == null) ? getNullKey() : key;
        try {
            _tree.delete(key, rowid);
        } catch (IOException e) {
            throw new AxionException("Unable to delete from index " + getName(), e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to delete from index " + getName(), e);
        }
    }

    // TABLE MODIFICATION LISTENER
    public final void rowInserted(RowEvent event) throws AxionException {
        String colName = getIndexedColumn().getName();
        int colIndex = event.getTable().getColumnIndex(colName);
        Object value = event.getNewRow().get(colIndex);
        value = (value == null) ? getNullKey() : value;
        try {
            _tree.insert(value, event.getNewRow().getIdentifier());
        } catch (IOException e) {
            throw new AxionException("Unable to insert into index " + getName(), e);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to insert into index " + getName(), e);
        }
    }

    public final void rowUpdated(RowEvent event) throws AxionException {
        rowDeleted(event);
        rowInserted(event);
    }

    public void truncate() throws AxionException {
        _tree.truncate();
    }

    protected ObjectBTree createTree(File dataDirectory, String name, int minimizationFactor, DataType dataType) throws IOException,
            ClassNotFoundException {
        return new ObjectBTree(dataDirectory, name, minimizationFactor, dataType);
    }

    protected Object getNullKey() {
        return NullObject.INSTANCE;
    }
    
    private static final IndexLoader LOADER = new ObjectBTreeIndexLoader();

    protected int _minimizationFactor;
    private File _dataDirectory;
    private ObjectBTree _tree = null;
}
