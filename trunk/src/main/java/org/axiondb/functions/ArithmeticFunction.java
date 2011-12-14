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

package org.axiondb.functions;

import java.math.BigDecimal;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.types.BigDecimalType;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Jonathan Giron 
 */
public abstract class ArithmeticFunction extends BaseFunction implements ScalarFunction {
    public ArithmeticFunction(String name) {
        super(name);
    }

    public final DataType getDataType() {
        return (_datatype == null) ? new BigDecimalType() : _datatype;
    }

    public final Object evaluate(RowDecorator row) throws AxionException {
        BigDecimal result = null;
        for (int i = 0, I = getArgumentCount(); i < I; i++) {
            Selectable sel = getArgument(i);
            Object val = sel.evaluate(row);
            if (null == val) {
                return null;
            }
            
            BigDecimal bdval = sel.getDataType().toBigDecimal(val);
            if (null == result) {
                result = bdval;
            } else {
                result = operate(result, bdval);
            }
        }
        return result;
    }

    public final boolean isValid() {
        return (getArgumentCount() != 0);
    }

    protected abstract BigDecimal operate(BigDecimal left, BigDecimal right) throws AxionException;

    protected void setDataType(DataType newType) {
        _datatype = newType;
    }
    
    private DataType _datatype = null;
}
