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

import java.math.BigDecimal;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecoratorIterator;
import org.axiondb.Selectable;
import org.axiondb.types.BigDecimalType;

/**
 * @version  
 * @author Sanjeeth Duvuru
 * @author Ahimanikya Satapathy
 */
public class AverageFunction extends BaseAggregateFunction implements AggregateFunction, FunctionFactory {

    public AverageFunction() {
        super("AVG");
    }

    public ConcreteFunction makeNewInstance() {
        return new AverageFunction();
    }

    public final DataType getDataType() {
        return _targetType == null ? BIG_DECIMAL_TYPE : _targetType;
    }

    public final Object evaluate(RowDecoratorIterator rows) throws AxionException {
        Selectable sel = getArgument(0);
        _targetType = sel.getDataType();
        setDistinct();

        double rowCount = 0;
        BigDecimal sum = null;
        while (rows.hasNext()) {
            Object val = _targetType.convert(sel.evaluate(rows.next()));
            if (null != val && acceptable(val)) {
                rowCount++;
                sum = evaluate(sum, _targetType.toBigDecimal(val));
            }
        }

        if (null != sum) {
            return sum.divide(new BigDecimal(rowCount), BigDecimal.ROUND_HALF_UP);
        }
        return null;
    }

    protected BigDecimal evaluate(BigDecimal src, BigDecimal val) {
        return null == src ? val : src.add(val);
    }

    private DataType _targetType = null;
    private static final DataType BIG_DECIMAL_TYPE = new BigDecimalType();
}
