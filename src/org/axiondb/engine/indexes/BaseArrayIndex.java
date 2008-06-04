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
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Table;
import org.axiondb.engine.rowiterators.EmptyRowIterator;
import org.axiondb.engine.rowiterators.LazyRowRowIterator;
import org.axiondb.event.RowEvent;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanFunction;
import org.axiondb.functions.GreaterThanOrEqualFunction;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.functions.LessThanOrEqualFunction;

/**
 * Abstract base implemenation for {@link Index indices}that maintain an in-memory,
 * sorted array of key values (and their associated row identifiers). This type of index
 * is fast to read, relatively slow to write and somewhat memory expensive when very
 * large.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ritesh Adval
 */
public abstract class BaseArrayIndex extends BaseIndex implements Index {

    public BaseArrayIndex(String name, Column column, boolean unique) {
        super(name, column, unique);
    }

    public BaseArrayIndex(String name, Column column, boolean unique, IntList values) {
        super(name, column, unique);
        _rowIds = values;
    }

    public void changeRowId(Table table, Row row, int oldId, int newId) throws AxionException {
        int colnum = table.getColumnIndex(getIndexedColumn().getName());
        Object key = row.get(colnum);
        if (null == key) {
            // null values aren't indexed
        } else {
            int index = find(key, true);
            for (int i = index, I = _rowIds.size(); i < I; i++) {
                if (oldId == _rowIds.get(i)) {
                    _rowIds.set(i, newId);
                    break;
                }
            }
        }
    }
    public abstract IndexLoader getIndexLoader();

    public RowIterator getInorderRowIterator(RowSource source) throws AxionException {
        int minindex = 0;
        int maxindex = _rowIds.size();

        return new LazyRowRowIterator(source, _rowIds.subList(minindex, maxindex).listIterator(),
            source.getColumnIndex(getIndexedColumn().getName()), getKeyList(minindex, maxindex).listIterator(), maxindex);
    }

    public abstract List getKeyList();

    public RowIterator getRowIterator(RowSource source, Function fn, Object value) throws AxionException {
        Object convertedValue = getIndexedColumn().getDataType().convert(value);

        if (null == convertedValue) {
            // null fails all comparisions I support
            return EmptyRowIterator.INSTANCE;
        }

        int minindex = 0;
        int maxindex = _rowIds.size();

        if (fn instanceof EqualFunction) {
            minindex = find(convertedValue, true);
            if (minindex >= 0) {
                if (!isUnique()) {
                    maxindex = find(getIndexedColumn().getDataType().successor(convertedValue), false);
                } else {
                    maxindex = minindex + 1;
                }
            } else {
                maxindex = -1;
            }
        } else if (fn instanceof GreaterThanFunction) {
            minindex = find(getIndexedColumn().getDataType().successor(convertedValue), false);
        } else if (fn instanceof GreaterThanOrEqualFunction) {
            minindex = find(convertedValue, false);
        } else if (fn instanceof LessThanFunction) {
            maxindex = find(convertedValue, false);
        } else if (fn instanceof LessThanOrEqualFunction) {
            maxindex = find(getIndexedColumn().getDataType().successor(convertedValue), false);
        } else {
            throw new AxionException("Unsupported function" + fn);
        }

        if (minindex < 0 || minindex >= _rowIds.size() || maxindex <= 0 || minindex == maxindex) {
            return EmptyRowIterator.INSTANCE;
        } 
          
        //FIXME: There should be a better way to fix concurent modification issue
        IntListIterator resultIds = _rowIds.subList(minindex, maxindex).listIterator();
        ArrayIntList ids = new ArrayIntList();
        while (resultIds.hasNext()) {
            ids.add(resultIds.next());
        }
        ListIterator keyResult = getKeyList(minindex, maxindex).listIterator();
        ArrayList keys = new ArrayList();
        while (keyResult.hasNext()) {
            keys.add(keyResult.next());
        }

        return new LazyRowRowIterator(source, ids.listIterator(), source.getColumnIndex(getIndexedColumn().getName()), keys.listIterator(), ids.size());
    }

    public String getType() {
        return Index.ARRAY;
    }

    public void rowDeleted(RowEvent event) throws AxionException {
        int colnum = event.getTable().getColumnIndex(getIndexedColumn().getName());
        Object key = event.getOldRow().get(colnum);
        if (null == key) {
            // null values aren't indexed
        } else {
            if (isUnique()) {
                // if we're unique, just remove the entry at key
                int index = removeKey(key);
                if (-1 != index) {
                    _rowIds.removeElementAt(index);
                }
            } else {
                // if we're not unique, scroll thru to find the right row to remove
                int index = find(key, true);
                if (-1 != index) {
                    while (_rowIds.get(index) != event.getOldRow().getIdentifier()) {
                        index++;
                    }
                    _rowIds.removeElementAt(index);
                    removeKeyAt(index);
                }
            }
        }
    }

    public void rowInserted(RowEvent event) throws AxionException {
        int colnum = event.getTable().getColumnIndex(getIndexedColumn().getName());
        Object key = event.getNewRow().get(colnum);
        if (null == key) {
            // null values aren't indexed
        } else {
            int index = insertKey(key);
            _rowIds.add(index, event.getNewRow().getIdentifier());
        }
    }

    public void rowUpdated(RowEvent event) throws AxionException {
        int colnum = event.getTable().getColumnIndex(getIndexedColumn().getName());
        Object newkey = event.getNewRow().get(colnum);
        Object oldkey = event.getOldRow().get(colnum);
        if (null == newkey ? null == oldkey : newkey.equals(oldkey)) {
            return;
        }
        rowDeleted(event);
        rowInserted(event);
    }

    public void save(File dataDirectory) throws AxionException {
        getIndexLoader().saveIndex(this, dataDirectory);
    }

    public void saveAfterTruncate(File dataDirectory) throws AxionException {
        getIndexLoader().saveIndexAfterTruncate(this, dataDirectory);
    }

    public boolean supportsFunction(Function fn) {
        if (fn instanceof EqualFunction) {
            if (isUnique()) {
                return true;
            }
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else if (fn instanceof GreaterThanFunction) {
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else if (fn instanceof GreaterThanOrEqualFunction) {
            return true;
        } else if (fn instanceof LessThanFunction) {
            return true;
        } else if (fn instanceof LessThanOrEqualFunction) {
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else {
            return false;
        }
    }

    public void truncate() throws AxionException {
        if (_rowIds != null) {
            _rowIds.clear();
        }
    }

    protected abstract int find(Object value, boolean required);

    protected abstract List getKeyList(int minIndex, int maxIndex);

    protected IntList getValueList() {
        return _rowIds;
    }

    protected abstract int insertKey(Object value) throws AxionException;

    protected abstract int removeKey(Object value) throws AxionException;

    protected abstract void removeKeyAt(int index) throws AxionException;

    private IntList _rowIds = new ArrayIntList();
}
