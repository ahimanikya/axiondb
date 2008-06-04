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
import java.util.List;

import org.axiondb.ColumnIdentifier;
import org.axiondb.FromNode;
import org.axiondb.Function;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;

/**
 * Masks Selectables For given Tables, used to mask seletable while resolving condition in
 * FromNode scope.
 * 
 * @author Ahimanikya Satapathy
 */
public class MaskSelectablesForTablesVisitor {

    public List maskAliasListForTables(FromNode from, List selected) {
        if (selected == null) {
            return null;
        }

        List aliasList = new ArrayList();
        TableIdentifier[] tables = from.toTableArray();
        for(int i = 0, I = selected.size(); i < I; i++) {
            Selectable sel = (Selectable) selected.get(i);
            if (visit(sel, tables)) {
                aliasList.add(sel);
            }
        }
        return aliasList;
    }

    public boolean visit(ColumnIdentifier col, TableIdentifier[] tables) {
        for (int i = 0; i < tables.length; i++) {
            if (!"*".equals(col.getName()) && col.getTableName().equals(tables[i].getTableName())) {
                return true;
            }
        }
        return false;
    }

    public boolean visit(Function fn, TableIdentifier[] tables) {
        for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
            if (!visit(fn.getArgument(i), tables)) {
                return false;
            }
        }
        return true;
    }

    public boolean visit(Selectable sel, TableIdentifier[] tables) {
        if (sel instanceof ColumnIdentifier) {
            if (!visit((ColumnIdentifier) sel, tables)) {
                return false;
            }
        }
        if (sel instanceof Function) {
            if (!visit((Function) sel, tables)) {
                return false;
            }
        }
        return true;
    }
}
