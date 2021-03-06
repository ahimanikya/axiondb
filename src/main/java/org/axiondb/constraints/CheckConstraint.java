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

package org.axiondb.constraints;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.visitors.ResolveSelectableVisitor;
import org.axiondb.event.RowEvent;

/**
 * A CHECK constraint, which is violated whenever the given
 * {@link #setCondition condition}is violated.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Amrish Lal
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */
public class CheckConstraint extends BaseConstraint {
    public CheckConstraint(String name) {
        super(name, "CHECK");
    }

    public void setCondition(Selectable where) {
        _condition = where;
    }

    public Selectable getCondition() {
        return _condition;
    }

    @Override
    public void resolve(Database db, TableIdentifier table) throws AxionException {
        ResolveSelectableVisitor resolveSel = new ResolveSelectableVisitor(db);
        _condition = resolveSel.visit(_condition, null, toArray(table));
    }

    public boolean evaluate(RowEvent event) throws AxionException {
        return evaluate(event, event.getTable().makeRowDecorator());
    }
    
    public boolean evaluate(RowEvent event, RowDecorator dec) throws AxionException {
        Row row = event.getNewRow();
        return ((null == row) ? true : check(row, dec));
    }
    
    public boolean evaluate(RowIterator oldRows, RowIterator newRows, Table table) throws AxionException {
        if (null == newRows || newRows.isEmpty()) {
            return true;
        }
        
        RowDecorator dec = table.makeRowDecorator();
        while (newRows.hasNext()) {
            if(!check(newRows.next(), dec)) {
                return false;
            }
        }
        return true;
    }

    private boolean check(Row row, RowDecorator dec) throws AxionException {
        dec.setRow(row);

        // ISO/IEC 9075-2:2003, Section 4.17 - a constraint is satisfied if and only if the
        // specified search condition is not FALSE (that is, evaluates to TRUE or null). 
        Boolean result = (Boolean) _condition.evaluate(dec);
        return (result == null) ? true : result.booleanValue();
    }

    private Selectable _condition;
    private static final long serialVersionUID = 6322188199298311548L;
}


