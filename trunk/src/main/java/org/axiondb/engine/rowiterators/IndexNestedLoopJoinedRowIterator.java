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

package org.axiondb.engine.rowiterators;

import org.axiondb.AxionException;
import org.axiondb.RowIterator;

/**
 * The Index Nested Loop Join or Augmented Nested Loop Join (ANL) is by far the most
 * common join method and is the classic Axion join method. An augmented nested loop join
 * is performed by doing a scan over the left subtree and for each row in it, performing
 * an index bracket scan on a portion of the right subtree. The right subtree is read as
 * many times as there are rows in the left subtree. To be a candidate for an ANL join,
 * the subtree pair for a join node must meet the following criteria:
 * <p>
 * <li>There must be an index(es) defined on the join column(s) for the table in the
 * right subtree.
 * <li>No other scan on that index has already been set.
 * <p>
 * When there is an index defined on the left subtree’s table instead of on the right, the
 * optimizer swaps the subtrees to make an ANL join possible. When neither subtree’s table
 * has an index defined on the join column, the optimizer creats a dynamic index on one of
 * the subtree.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class IndexNestedLoopJoinedRowIterator extends AbstractJoinedRowIterator {
    public IndexNestedLoopJoinedRowIterator(RowIterator left, int leftJoinColumn, MutableIndexedRowIterator rightIndex, int rightColumnCount,
            boolean rightOuter, boolean swapLeftAndRight) throws AxionException {
        setLeftRowIterator(left);
        _joinColumnInLeft = leftJoinColumn;
        _rightIndexRowIterator = rightIndex;
        setRightSideColumnCount(rightColumnCount);
        setRightOuter(rightOuter);
        setSwapLeftAndRight(swapLeftAndRight);
    }

    protected RowIterator generateRightRowIterator() throws AxionException {
        _rightIndexRowIterator.setIndexKey(getLeftRowIterator().current().get(_joinColumnInLeft));
        return _rightIndexRowIterator;
    }

    public String toString() {
        return "IndexNestedLoop(" + super.toString() + ")";
    }

    private MutableIndexedRowIterator _rightIndexRowIterator = null;
    private int _joinColumnInLeft;
}


