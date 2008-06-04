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

package org.axiondb.engine.rowiterators;

import org.axiondb.AxionException;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;

/**
 * A {@link DelegatingRowIterator} that only returns {@link Row}s
 * that match a given {@link WhereNode}.
 *
 * @version  
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class FilteringRowIterator extends AbstractAcceptingRowIterator {

    public FilteringRowIterator(RowIterator iterator, RowDecorator decorator, Selectable where) {
        super(iterator);
        _decorator = decorator;
        _where = where;
    }
    
    public String getShortName() {
        return "Filtering";
    }

    public String toString() {
        return getShortName() + "(" + _where  + ")";
    }
    
    protected boolean acceptable(int rowindex, Row row) throws AxionException {
        // ISO/IEC 9075-2:2003, Section 7.8, General Rule 2 - filter is applied if condition
        // evaluates to true; null evaluation thus maps to false.
        Boolean result = (Boolean) _where.evaluate(decorate(rowindex,row)); 
        return (result == null) ? false : result.booleanValue();
    }

    private RowDecorator decorate(int rowindex, Row row) {
        _decorator.setRow(rowindex,row);
        return _decorator;
    }
    
    private RowDecorator _decorator = null;
    private Selectable _where = null;
}


