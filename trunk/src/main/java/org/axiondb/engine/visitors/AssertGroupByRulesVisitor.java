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

import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Selectable;
import org.axiondb.SelectableVisitor;
import org.axiondb.functions.ScalarFunction;

/**
 * Assert general rules for Group By for Select Command.
 * 
 * @author Ahimanikya Satapathy
 */
public class AssertGroupByRulesVisitor implements SelectableVisitor {

    public boolean visit(List selectList, List groupByNodes) throws AxionException {
        _groupByNodes = groupByNodes;

        if(groupByNodes != null && !groupByNodes.isEmpty()) {
            AmbiguousColumnReferenceVisitor ambiguityCheck = new AmbiguousColumnReferenceVisitor();
            ambiguityCheck.visit(selectList, groupByNodes);
        }
        
        for(int i = 0, I = selectList.size(); i < I; i++) {
            Selectable sel = (Selectable) selectList.get(i);
            FindAggregateFunctionVisitor findAggr = new FindAggregateFunctionVisitor();
            findAggr.visit(sel); // check for aggregate functions
            if (findAggr.foundAggregateFunction()) {
                _foundAggregateFn = true;
                continue;
            }
            if (_groupByNodes == null || _groupByNodes.isEmpty()) {
                _foundScalar = true;
            } else {
                visit(sel);
            }
        }

        if (_foundScalar && _foundAggregateFn) {
            throw new AxionException("Can't select both scalar values and aggregate functions.");
        }

        if (_foundScalar && _groupByNodes != null && !_groupByNodes.isEmpty()) {
            throw new AxionException("Invalid Group By Expression...");
        }
        return _foundScalar;
    }

    // Allow Scalar function on the group by columns
    public void visit(Selectable select) {
        if (select instanceof ScalarFunction) {
            ScalarFunction fn = (ScalarFunction) select;
            for (int i = 0, I = fn.getArgumentCount(); i < I; i++) {
                Selectable fnArg = fn.getArgument(i);
                visit(fnArg);
            }
        } else if (select instanceof ColumnIdentifier) {
            // column is not part of GroupBy column list
            if (!_groupByNodes.contains(select)) {
                _foundScalar = true;
            }
        }
    }

    private boolean _foundAggregateFn = false;
    private boolean _foundScalar = false;
    private List _groupByNodes;
}
