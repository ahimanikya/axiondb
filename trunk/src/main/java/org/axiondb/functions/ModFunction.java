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
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.types.BigDecimalType;

/**
 * @version  
 * @author Sudhendra Seshachala
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class ModFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public ModFunction() {
        super("MOD");
    }

    public ConcreteFunction makeNewInstance() {
        return new ModFunction();
    }

    /** {@link BigDecimalType} */
    public DataType getDataType() {
        return (_datatype == null) ? new BigDecimalType() : _datatype;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        Object rawDividend = getArgument(0).evaluate(row);
        Object rawDivisor = getArgument(1).evaluate(row);

        if (null == rawDividend || null == rawDivisor) {
            return null;
        }

        // Per ISO/ANSI spec, dividend and divisor must be an exact numeric value with
        // scale 0. We accept numeric values with fractional components, but they will be
        // truncated.
        BigDecimal dividend = getArgument(0).getDataType().toBigDecimal(rawDividend).setScale(0, BigDecimal.ROUND_DOWN);
        BigDecimal divisor = getArgument(1).getDataType().toBigDecimal(rawDivisor).setScale(0, BigDecimal.ROUND_DOWN);

        BigDecimal result = null;
        try {
            BigDecimal divideResult = dividend.divide(divisor, 0, BigDecimal.ROUND_DOWN);
            result = dividend.subtract(divisor.multiply(divideResult));
        } catch (ArithmeticException e) {
            throw new AxionException(22012);
        }

        // Per ISO/ANSI spec, return datatype must match that of the divisor. If the
        // divisor is a non-exact numeric type, we should return an appropriately sized
        // BigDecimal with scale 0.
        DataType divisorDatatype = getArgument(1).getDataType();
        _datatype = (divisor instanceof DataType.ExactNumeric) ? divisorDatatype : getEquivalentExactNumericType(divisorDatatype);

        return _datatype.convert(result);
    }

    private DataType getEquivalentExactNumericType(DataType datatype) {
        return new BigDecimalType(datatype.getPrecision(), 0);
    }

    public boolean isValid() {
        return (getArgumentCount() == 2);
    }

    private DataType _datatype = null;
}
