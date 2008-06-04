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

package org.axiondb;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link org.axiondb.Row}with meta-information. (Note that we've intentionally not
 * implemented <code>Row</code> here. <code>Row</code> and <code>RowDecorator</code>
 * have different contracts. A reference to a <code>Row</code> is somewhat
 * persistent--it can be added to a Collection, for example. A {@link RowDecorator}
 * changes all the time.)
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class RowDecorator {
    public RowDecorator(Map selectableToFieldMap) {
        _fieldMap = selectableToFieldMap;
    }

    /** Returns the value of the specified column. */
    public Object get(ColumnIdentifier colid) throws AxionException {
        Integer index = (Integer) (_fieldMap.get(getCanonicalForm(colid)));
        if (null == index) {
            throw new AxionException("Field " + colid + " not found.", 42703);
        }
        Object obj = _row.get(index.intValue());
        return colid.getDataType().convert(obj);
    }
    
    /** Gets the {@link Row}I'm currently decorating. */
    public Row getRow() {
        return _row;
    }

    public int getRowIndex() throws AxionException {
        if (_rowndx == -1) {
            throw new AxionException("Row index not available.");
        }
        return _rowndx;
    }

    /** Gets the selectable To Field Iterator */
    public Iterator getSelectableIterator() {
        return Collections.unmodifiableSet(_fieldMap.keySet()).iterator();
    }

    /** Sets the {@link Row}I'm currently decorating. */
    public void setRow(int rowndx, Row row) {
        _rowndx = rowndx;
        _row = row;
    }

    /** Sets the {@link Row}I'm currently decorating. */
    public void setRow(Row row) {
        setRow(-1, row);
    }
    
    private ColumnIdentifier getCanonicalForm(ColumnIdentifier colid) {
        if (null == colid) {
            return null;
        }
        return colid.getCanonicalIdentifier();
    }

    private Map _fieldMap;
    private Row _row;
    private int _rowndx = -1;
}


