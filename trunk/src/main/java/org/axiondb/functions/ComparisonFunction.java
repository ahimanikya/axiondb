/*
 * 
 * =======================================================================
 * Copyright (c) 2003-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.functions;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.DataType;
import org.axiondb.Literal;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.types.BooleanType;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public abstract class ComparisonFunction extends BaseFunction implements ScalarFunction {
    public ComparisonFunction(String name) {
        super(name);
    }

    public final DataType getDataType() {
        return BOOLEAN_TYPE;
    }

    public final boolean isValid() {
        return getArgumentCount() == 2;
    }

    public final Object evaluate(RowDecorator row) throws AxionException {
        Selectable selLeft = getArgument(0);
        Object valLeft = selLeft.evaluate(row);
        DataType typeLeft = selLeft.getDataType();
        Selectable selRight = getArgument(1);
        Object valRight = selRight.evaluate(row);
        	
        try {
        	valRight = typeLeft.convert(valRight);
        }catch(AxionException ex){
        	return Boolean.FALSE;
        }

        if (null == valLeft || null == valRight) {
            return null;
        } else if (compare(typeLeft.compare(valLeft, valRight))) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    protected abstract boolean compare(int value);

    protected abstract ComparisonFunction makeFlipFunction();

    public ComparisonFunction flip() {
        ComparisonFunction fn = makeFlipFunction();
        fn.setName("FLIPPED_" + this.getName());
        fn.setAlias(this.getAlias());
        fn.addArgument(this.getArgument(0));
        fn.addArgument(this.getArgument(1));
        return fn;
    }

    public boolean isColumnColumn() {
        return (getArgumentCount() == 2) && getArgument(0) instanceof ColumnIdentifier && getArgument(1) instanceof ColumnIdentifier;
    }

    public boolean isColumnLiteral() {
        return (getArgumentCount() == 2)
            && ((getArgument(0) instanceof ColumnIdentifier && getArgument(1) instanceof Literal) || (getArgument(1) instanceof ColumnIdentifier && getArgument(0) instanceof Literal));
    }
    
    public abstract String getOperatorString();

    private static final DataType BOOLEAN_TYPE = new BooleanType();

}
