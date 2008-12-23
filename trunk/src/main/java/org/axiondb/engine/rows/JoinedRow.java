/*
 * 
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

import java.util.ArrayList;

import org.axiondb.Row;

/**
 * A {@link Row} composed of zero or more <code>Row</code>s, joined together
 * end-to-end.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class JoinedRow extends BaseRow {
    public JoinedRow() {
        _rows = new ArrayList(2);
    }

    public void addRow(Row row) {
        _size += row.size();
        _rows.add(row);
        _hash = 0;
    }

    public Object get(int i) {
        int n = i;
        for (int j = 0, J = _rows.size(); j < J; j++) {
            Row row = (Row) (_rows.get(j));
            int size = row.size();
            if (n < size) {
                return row.get(n);
            }
            n -= size;
        }
        throw new IndexOutOfBoundsException(i + " >= " + size());
    }

    public void set(int i, Object val) {
        int n = i;
        for (int j = 0, J = _rows.size(); j < J; j++) {
            Row row = (Row) (_rows.get(j));
            int size = row.size();
            if (n < size) {
                row.set(n, val);
                _hash = 0;
                return;
            }
            n -= size;
        }
        throw new IndexOutOfBoundsException(i + " >= " + size());
    }

    public final int size() {
        return _size;
    }

    // if LOJ left row iter index is "0" and right row iter index is "1"
    // if ROJ left row iter index is "1" and right row iter index ia "0"
    public Row getRow(int i) {
        return (Row) _rows.get(i);
    }

    private ArrayList _rows;
    private int _size = 0;
}

