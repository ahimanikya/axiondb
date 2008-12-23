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
 * A Nested Loop Join is performed by doing a scan over the left subtree and for each row
 * in it performing a full scan of the right subtree. This is the default join algorithm,
 * which can be used for any join. However, it is usually less efficient than the other
 * methods. Usually, either an existing index, or a dynamic index, used in an ANL join,
 * will cost much less. Occasionally, when subtree cardinalities are very low (possibly
 * because of index bracketing), nested loop will be the method with the least cost.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class NestedLoopJoinedRowIterator extends AbstractJoinedRowIterator {

    public NestedLoopJoinedRowIterator(RowIterator left, RowIterator right, int rightColumnCount, boolean rightOuter, boolean swapLeftAndRight)
            throws AxionException {
        setLeftRowIterator(left);
        _rightRowIterator = right;
        setRightSideColumnCount(rightColumnCount);
        setRightOuter(rightOuter);
        setSwapLeftAndRight(swapLeftAndRight);
    }
    
    public NestedLoopJoinedRowIterator(RowIterator left, RowIterator right, int rightColumnCount) throws AxionException {
        this(left,right,rightColumnCount,false,false);
    }

    protected RowIterator generateRightRowIterator() throws AxionException {
        _rightRowIterator.reset();
        return _rightRowIterator;
    }
    
    public void reset() throws AxionException {
        _rightRowIterator.reset();
        super.reset();
    }

    public String toString() {
        return "NestedLoop(" + super.toString() + ")";
    }

    private RowIterator _rightRowIterator = null;
}


