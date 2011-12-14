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

package org.axiondb.engine.visitors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.axiondb.BindVariable;
import org.axiondb.FromNode;
import org.axiondb.Function;
import org.axiondb.Selectable;
import org.axiondb.engine.commands.AxionQueryContext;
import org.axiondb.engine.commands.InsertIntoClause;
import org.axiondb.engine.commands.SelectCommand;
import org.axiondb.engine.commands.UpdateCommand;
import org.axiondb.engine.commands.UpsertCommand;

/**
 * Returns a set of BindVarible used in a Selectable and in various commands.
 * <p>
 * Note: Almost all Commands could use BindVariables, we need to find out what is the ANSI
 * and/or JDBC standards are for this. This almost allow us to simulate Dynamic SQL, e.g.
 * if we can use prepare statement for things like column name, table properties, default
 * value etc in CreateCommand
 * 
 * @author Ahimanikya Satapathy
 */
public class FindBindVariableVisitor {

    private List bvUsedInSelectable = new ArrayList(10);
    private volatile ListIterator bvUsedInSelectableIter;

    public List getBindVariables() {
        return bvUsedInSelectable;
    }
    
    public Iterator getBindVariableIterator() {
        if(bvUsedInSelectableIter == null) {
            return bvUsedInSelectableIter = bvUsedInSelectable.listIterator();
        } else {
            while(bvUsedInSelectableIter.hasPrevious()) {
                bvUsedInSelectableIter.previous();
            }
            return bvUsedInSelectableIter;
        }
    }
    
    @SuppressWarnings("unchecked")
    public void visit(Selectable sel) {
        if (sel instanceof BindVariable) {
            bvUsedInSelectable.add(sel);
        } else if (sel instanceof Function) {
            visit((Function) sel);
        } else if (sel instanceof SelectCommand) {
            visit((SelectCommand) sel);
        }
    }

    public void visit(FromNode frmNd) {
        Object left = frmNd.getLeft();
        Object right = frmNd.getRight();

        if (left instanceof FromNode){
            visit((FromNode)left);
        }

        if (right instanceof FromNode){
            visit((FromNode)right);
        }

        Selectable cond = frmNd.getCondition();
        if (cond != null){
            visit(cond);
        }
    }

    public void visit(Function fn) {
        for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
            visit(fn.getArgument(i));
        }
    }

    public void visit(SelectCommand select) {
        AxionQueryContext context = select.getQueryContext();
        for (int i = 0, I = context.getSelectCount(); i < I; i++) {
            visit(context.getSelect(i));
        }
        if (context.getFrom() != null){
            visit(context.getFrom());
        }

        visit(context.getWhere());
        visit(context.getLimit());
        visit(context.getOffset());
    }

    public void visit(InsertIntoClause insertInto) {
        if(insertInto.getWhenClause() != null) {
            visit(insertInto.getWhenClause().getCondition());
        }
        
        for (Iterator iter = insertInto.getValueIterator(); iter.hasNext();) {
            visit((Selectable) iter.next());
        }
    }

    public void visit(UpsertCommand upsert) {
        if (upsert.getUsingSubSelectCommand() != null) {
            visit((SelectCommand) upsert.getUsingSubSelectCommand());
        }
        visit(upsert.getCondition());
        for (Iterator iter = upsert.getUpdateValueIterator(); iter.hasNext();) {
            visit((Selectable) iter.next());
        }

        for (Iterator iter = upsert.getInsertValueIterator(); iter.hasNext();) {
            visit((Selectable) iter.next());
        }

        InsertIntoClause exceptionWhen = upsert.getExceptionWhenClause();
        if (exceptionWhen != null) {
            visit(exceptionWhen);
        }
    }

    public void visit(UpdateCommand update) {
        for (Iterator iter = update.getValueIterator(); iter.hasNext();) {
            visit((Selectable) iter.next());
        }
        AxionQueryContext context = update.getQueryContext();
        if (context != null) {
            if (context.getFrom() != null) {
                visit(context.getFrom());
            }
        }
        visit(update.getWhere());

        InsertIntoClause exceptionWhen = update.getExceptionWhenClause();
        if (exceptionWhen != null) {
            visit(exceptionWhen);
        }
    }
}
