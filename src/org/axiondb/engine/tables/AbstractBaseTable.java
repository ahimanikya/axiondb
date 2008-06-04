/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.tables;

import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.Constraint;
import org.axiondb.ConstraintViolationException;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.engine.rowiterators.FilteringRowIterator;
import org.axiondb.event.BaseTableModificationPublisher;
import org.axiondb.event.RowEvent;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.EqualFunction;

/**
 * An abstract implementation of {@link Table}, code common between TransactableTableImpl
 * and BaseTable
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public abstract class AbstractBaseTable extends BaseTableModificationPublisher implements Table {

    public RowIterator getMatchingRows(List selectables, List values, boolean readOnly) throws AxionException {
        if (null == selectables || selectables.isEmpty()) {
            return getRowIterator(readOnly);
        }

        RowIterator baseIterator = null;
        Selectable filter = null;
        for (int i = 0, I = selectables.size(); i < I; i++) {
            Selectable sel = (Selectable) selectables.get(i);
            Object val = values.get(i);

            EqualFunction function = new EqualFunction();
            function.addArgument(sel);
            function.addArgument(new Literal(val));

            if (null == baseIterator) {
                baseIterator = getIndexedRows(function, readOnly);
                if (baseIterator != null) {
                    function = null;
                }
            }

            if (function != null) {
                if (null == filter) {
                    filter = function;
                } else {
                    AndFunction fn = new AndFunction();
                    fn.addArgument(filter);
                    fn.addArgument(function);
                    filter = fn;
                }
            }
        }

        if (null == baseIterator) {
            baseIterator = getRowIterator(readOnly);
        }

        if (null != filter) {
            return new FilteringRowIterator(baseIterator, makeRowDecorator(), filter);
        }
        return baseIterator;
    }
    
    public void migrate() throws AxionException{
    }

    protected void checkConstraints(RowEvent event, RowDecorator dec) throws AxionException {
        if(isDeferAll()) {
            return;
        }
        checkConstraints(event, false, dec);
    }

    protected void checkConstraints(RowEvent event, boolean deferred, RowDecorator dec) throws AxionException {
        for (Iterator iter = getConstraints(); iter.hasNext();) {
            Constraint c = (Constraint) iter.next();
            if (c.isDeferred() == deferred) {
                if (!c.evaluate(event, dec)) {
                    throw new ConstraintViolationException(c);
                }
            }
        }
    }
    
    protected void checkConstraints(RowIterator oldRows, RowIterator newRows) throws AxionException {
        for (Iterator iter = getConstraints(); iter.hasNext();) {
            Constraint c = (Constraint) iter.next();
            if (!c.evaluate(oldRows, newRows, this)) {
                throw new ConstraintViolationException(c);
            }
        }
    }

    protected boolean hasDeferredConstraint() {
        if(isDeferAll()) {
            return true;
        }
        
        for (Iterator iter = getConstraints(); iter.hasNext();) {
            Constraint c = (Constraint) iter.next();
            if (c.isDeferred()) {
                return true;
            }
        }
        return false;
    }
    
    protected boolean isDeferAll() {
        return false;
    }

    
}
