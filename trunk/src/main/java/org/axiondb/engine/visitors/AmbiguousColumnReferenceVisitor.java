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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.engine.commands.SubSelectCommand;
import org.axiondb.functions.ConcreteFunction;

/**
 * Assert Ambiguous Column Reference.
 * 
 * @author Ahimanikya Satapathy
 */
public class AmbiguousColumnReferenceVisitor  {

    public void visit(List selectList, List referenceColumns) throws AxionException {
        for(int i = 0, I = referenceColumns.size(); i < I; i++) {
            String refCol = getResolvedColumnName(referenceColumns.get(i));
            Set colidset = new HashSet();
            for (int j = 0, J = selectList.size(); j < J; j++) {
                Object col = selectList.get(j);
                String colname = getResolvedColumnName(col);

                if (colidset.contains(colname) && !(col instanceof Literal)) {
                    throw new AxionException(42705);
                }
                if(refCol.equals(colname)) {
                    colidset.add(colname);
                }
            }
        }
    }

    private String getResolvedColumnName(Object obj) {
        String colName = null;
        if (obj instanceof ColumnIdentifier) {
            ColumnIdentifier col = (ColumnIdentifier) obj;
            colName = col.getAlias();
            if (colName == null) {
                colName = col.getName();
            }
        } else if (obj instanceof ConcreteFunction) {
            ConcreteFunction fn = (ConcreteFunction) obj;
            colName = fn.getLabel();
        } else if (obj instanceof SubSelectCommand) {
            SubSelectCommand sq = (SubSelectCommand) obj;
            colName = sq.getName();
        } else if (obj instanceof Literal) {
            Literal lit = (Literal) obj;
            colName = lit.getName();
        }else if (obj instanceof OrderNode) {
            colName = getResolvedColumnName(((OrderNode)obj).getSelectable());
        }
        return colName;
    }

}
