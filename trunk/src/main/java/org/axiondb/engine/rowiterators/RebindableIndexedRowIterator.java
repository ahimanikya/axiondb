/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.rowiterators;

import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;

/**
 * A {@link DelegatingRowIterator} that is wraps
 * a {@link org.axiondb.RowIterator} from some {@link Index},
 * and that can be {@link #reset reset} to recreate the iterator
 * for a new {@link BindVariable bound value}.
 * 
 * @see org.axiondb.engine.BaseTable#getIndexedRows
 * @see org.axiondb.Index
 * @see org.axiondb.BindVariable
 * 
 * @version  
 * @author Rodney Waldhoff
 */
public class RebindableIndexedRowIterator extends DelegatingRowIterator {
    public RebindableIndexedRowIterator(Index index, RowSource table, Function fn, BindVariable bvar) throws AxionException {
        super(getRowIterator(index,table,fn,bvar));
        _index = index;
        _table = table;
        _function = fn;
        _bindVar = bvar;
    }

    public void reset() throws AxionException {
        setDelegate(getRowIterator(_index,_table,_function,_bindVar));
    }

    private static RowIterator getRowIterator(Index index, RowSource table, Function fn, BindVariable bvar) throws AxionException {
        return index.getRowIterator(table,fn,bvar.evaluate());
    }
    
    private Index _index;
    private RowSource _table;
    private Function _function;
    private BindVariable _bindVar;
}

