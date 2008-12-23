/*
 * 
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.rows;

import org.axiondb.Row;

/**
 * A {@link Row}wrapper for sub-query view.
 * <p>
 * RowView is (typically) a subset of a Row. e.g.
 * <p>
 * Row = {ID, NAME, ADD, TEL} RowView = {ID, ADD} colIndex[]={0,2}
 * <p>
 * ColumnIndex tells which columns of Row are included in the RowView. For functions
 * however, there is no column in row that can be pointed to. So for functions, we'll use
 * a placeholder where evaluated values will be held in an evaluatedRow, that will have
 * not null value for evaluated columns
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class RowView extends BaseRow implements Row {

    public RowView(Row row, int id, int[] colIndex) {
        super.setIdentifier(id);
        _row = row;
        _evaluatedRow = new SimpleRow(colIndex.length);
        _columnIndex = colIndex;
    }

    /**
     * Get the value of field <i>i </i>. Note that the index is zero-based.
     */
    public Object get(int i) {
        if (_columnIndex[i] < 0) {
            // get the evaluated value
            return _evaluatedRow.get(i);
        }
        return _row.get(_columnIndex[i]);
    }

    /**
     * Set the value of field <i>i </i> to <i>val </i>. Note that the index is zero-based.
     */
    public void set(int i, Object val) {
        if (_columnIndex[i] < 0) {
            // set the evaluated value
            _evaluatedRow.set(i, val);
        } else {
            _row.set(_columnIndex[i], val);
        }
        _hash = 0;
    }

    /**
     * Return the number of fields I contain.
     */
    public final int size() {
        return _columnIndex.length;
    }

    private Row _row;
    private Row _evaluatedRow;
    private int[] _columnIndex;
}
