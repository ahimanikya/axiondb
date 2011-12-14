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
import org.axiondb.engine.IntBTreeIndexLoader;
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
import org.axiondb.util.IntBTree;
import org.axiondb.util.NullObject;

/**
 * A {@link BaseBTreeIndex B-Tree index}over integer keys.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 * @author Ritesh Adval
 * @author Charles Ye
 */
public class IntBTreeIndex extends BaseBTreeIndex implements TableModificationListener {

    public IntBTreeIndex(String name, Column column, boolean unique) throws AxionException {
        this(name, column, unique, null);
    }

    public IntBTreeIndex(String name, Column column, boolean unique, File dataDirectory) throws AxionException {
        super(name, column, unique);
        try {
            _dataDirectory = dataDirectory;
            _tree = new IntBTree(_dataDirectory, getName(), 1000);
        } catch (Exception e) {
            String msg = "Unable to create index file for " + getName() + " due to IOException";
            throw new AxionException(msg, e);
        }
    }

    public void changeRowId(Table table, Row row, int oldId, int newId) throws AxionException {
        try {
            int colnum = table.getColumnIndex(getIndexedColumn().getName());
            Integer key = (Integer) row.get(colnum);
            _tree.replaceId(key.intValue(), oldId, newId);
        } catch (Exception e) {
            String msg = "Unable to change row id in index " + getName() + " due to IOException";
            throw new AxionException(msg, e);
        }
    }

    public IntBTree getBTree() {
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
            String msg = "Unable to retrieve values from index" + getName();
            throw new AxionException(msg, e);
        } catch (ClassNotFoundException e) {
            String msg = "Unable to retrieve values from index" + getName();
            throw new AxionException(msg, e);
        }

        return new LazyRowRowIterator(source, resultIds, _tree.size());
    }

    public RowIterator getRowIterator(RowSource source, Function function, Object value) throws AxionException {
        IntListIterator resultIds = null;
        try {
            if (function instanceof ComparisonFunction) {
                DataType type = getIndexedColumn().getDataType();

                Object convertedValue = type.convert(value);
                if (null == convertedValue) {
                    // null fails all comparisions I support
                    return EmptyRowIterator.INSTANCE;
                }
                int iVal = type.toInt(convertedValue);
                if (function instanceof EqualFunction) {
                    if (!isUnique()) {
                        resultIds = _tree.getAll(iVal);
                    } else {
                        Integer result = _tree.get(iVal);
                        if (result == null) {
                            return EmptyRowIterator.INSTANCE;
                        } else {
                            ArrayIntList ids = new ArrayIntList(1);
                            ids.add(result.intValue());
                            return new LazyRowRowIterator(source, ids.listIterator(), 1);
                        }
                    }
                } else if (function instanceof LessThanFunction) {
                    resultIds = _tree.getAllTo(iVal);
                } else if (function instanceof LessThanOrEqualFunction) {
                    int iSuccessor = getSuccessor(type, convertedValue);
                    resultIds = _tree.getAllTo(iSuccessor);
                } else if (function instanceof GreaterThanFunction) {

                    int iSuccessor = getSuccessor(type, convertedValue);

                    // NOTE: _tree.valueIterator returns a continuation rather than
                    // enumerating all elements of the RowIterator first. This is slightly
                    // slower than the getAllFrom for small tables, but faster and less
                    // memory consumptive memory for large tables, especially when we
                    // rarely visit the tail of the result set. This also postpones
                    // loading the index nodes until the data is actually read (in
                    // constrast, getAllFrom(<some small value>) will load all or nearly
                    // all nodes. For optimal performance it may be best to determine how
                    // large the index is and use that to figure out which
                    // approach--enumeration or continuation--is most appropriate for the
                    // given query.

                    // resultIds = _tree.getAllFrom(iSuccessor);
                    resultIds = _tree.valueIteratorGreaterThanOrEqualTo(iSuccessor);

                } else if (function instanceof GreaterThanOrEqualFunction) {
                    // NOTE: see note above.
                    // resultIds = _tree.getAllFrom(iVal);
                    resultIds = _tree.valueIteratorGreaterThanOrEqualTo(iVal);

                } else {
                    throw new AxionException("Unsupported function " + function);
                }
            } else if (function instanceof IsNotNullFunction) {
                resultIds = _tree.getAllExcludingNull();
            } else if (function instanceof IsNullFunction) {
                resultIds = _tree.getAll(NullObject.INSTANCE.intValue());
            } else {
                throw new AxionException("Unsupported function " + function);
            }
        } catch (Exception e) {
            String msg = "Unable to retrieve values from index " + getName() + " due to IOException";
            throw new AxionException(msg, e);
        }
        //return new LazyRowRowIterator(source, resultIds);

        // FIXME: There should be a better way to fix concurent modification issue
        ArrayIntList ids = new ArrayIntList();
        while (resultIds.hasNext()) {
            ids.add(resultIds.next());
        }
        return new LazyRowRowIterator(source, ids.listIterator(), ids.size());
    }

    public void rowDeleted(RowEvent event) throws AxionException {
        String colName = getIndexedColumn().getName();
        int colIndex = event.getTable().getColumnIndex(colName);
        Integer key = (Integer) event.getOldRow().get(colIndex);
        int rowid = event.getOldRow().getIdentifier();
        int intKey = (key == null) ? NullObject.INSTANCE.intValue() : key.intValue();
        try {
            _tree.delete(intKey, rowid);
        } catch (Exception e) {
            String msg = "Unable to delete from index " + getName() + " due to " + e.getMessage();
            throw new AxionException(msg, e);
        }
    }

    // TABLE MODIFICATION LISTENER
    public void rowInserted(RowEvent event) throws AxionException {
        String colName = getIndexedColumn().getName();
        int colIndex = event.getTable().getColumnIndex(colName);
        Integer value = (Integer) event.getNewRow().get(colIndex);
        int intValue = (value == null) ? NullObject.INSTANCE.intValue() : value.intValue();

        try {
            _tree.insert(intValue, event.getNewRow().getIdentifier());
        } catch (Exception e) {
            String msg = "Unable to insert into index " + getName() + " due to IOException";
            throw new AxionException(msg, e);
        }
    }

    public void rowUpdated(RowEvent event) throws AxionException {
        rowDeleted(event);
        rowInserted(event);
    }

    public void truncate() throws AxionException {
        _tree.truncate();
    }

    private int getSuccessor(DataType type, Object convertedValue) throws AxionException {
        return type.toInt(type.successor(convertedValue));
    }
    
    private static final IndexLoader LOADER = new IntBTreeIndexLoader();

    private File _dataDirectory;
    private IntBTree _tree = null;
}
