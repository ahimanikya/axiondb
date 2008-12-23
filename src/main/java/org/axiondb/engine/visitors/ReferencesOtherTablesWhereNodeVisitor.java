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

package org.axiondb.engine.visitors;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Function;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.SelectableVisitor;

/**
 * Return true if reference a table other than the given one, false otherwise.
 * 
 * @author Rodney Waldhoff
 */
public class ReferencesOtherTablesWhereNodeVisitor implements SelectableVisitor {
    public ReferencesOtherTablesWhereNodeVisitor(TableIdentifier id) {
        _id = id;
    }

    public boolean getResult() {
        return _result;
    }

    protected boolean hasResult() {
        return _hasResult;
    }

    public void visit(Selectable node) {
        if (!_hasResult) {
            if (node instanceof ColumnIdentifier) {
                ColumnIdentifier aid = (ColumnIdentifier) (node);
                if (!(_id.equals(aid.getTableIdentifier()))) {
                    _result = false;
                    _hasResult = true;
                }
            } else if (node instanceof Function) {
                Function fn = (Function) node;
                for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
                    if (_hasResult) {
                        break;
                    }
                    visit(fn.getArgument(i));
                }
            }
        }
    }

    private boolean _hasResult = false;
    private boolean _result = true;
    private TableIdentifier _id;

}
