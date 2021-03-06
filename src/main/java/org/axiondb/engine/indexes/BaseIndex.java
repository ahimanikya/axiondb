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
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.event.BaseTableModificationListener;
import org.axiondb.event.TableModificationListener;

/**
 * Abstract base implementation of {@link Index}.
 * 
 * @version  
 * @author Chuck Burdick
 */
public abstract class BaseIndex extends BaseTableModificationListener implements Index, TableModificationListener {

    public BaseIndex(String name, Column column, boolean unique) {
        _name = name;
        _col = column;
        _isUnique = unique;
        _dataType = _col.getDataType();
    }

    public Column getIndexedColumn() {
        return _col;
    }

    public abstract IndexLoader getIndexLoader();

    public abstract RowIterator getInorderRowIterator(RowSource source) throws AxionException;

    public String getName() {
        return _name;
    }

    public abstract RowIterator getRowIterator(RowSource source, Function fn, Object value) throws AxionException;

    public boolean isUnique() {
        return _isUnique;
    }

    public abstract void save(File dataDirectory) throws AxionException;

    public abstract void saveAfterTruncate(File dataDirectory) throws AxionException;

    public abstract boolean supportsFunction(Function fn);

    public abstract void truncate() throws AxionException;

    protected Comparator getComparator() {
        return _dataType;
    }

    protected DataType getDataType() {
        return _dataType;
    }
    
    private Column _col = null;
    private DataType _dataType = null;
    private boolean _isUnique = false;

    private String _name = null;
}
