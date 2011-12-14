/*
 * 
 * =======================================================================
 * Copyright (c) 2004-2005 Axion Development Team.  All rights reserved.
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
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.Selectable;
import org.axiondb.types.BooleanType;
import org.axiondb.types.StringType;
import org.axiondb.util.DateTimeUtils;

/**
 * Function to test whether the given String expression is in the same format as the given
 * date format expression.
 * 
 * @author Jonathan Giron
 * @version  
 */
public class IsValidDateTimeFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    public IsValidDateTimeFunction() {
        super("ISVALIDDATETIME");
    }

    public DataType getDataType() {
        return BOOLEAN_TYPE;
    }

    public boolean isValid() {
        return getArgumentCount() == 2;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        String dateStr = null;
        String formatStr = null;

        // Get 'format-literal'
        Selectable sel = getArgument(1);
        Object arg2 = sel.evaluate(row);
        if (null == arg2) {
            throw new AxionException(22004);
        } else {
            formatStr = (String) ARG2_TYPE.convert(arg2);
        }

        // Get 'date-string'
        sel = getArgument(0);
        Object arg1 = sel.evaluate(row);

        if (null == arg1) {
            return null;
        } else {
            dateStr = (String) ARG1_TYPE.convert(arg1);
        }

        try {
            DateTimeUtils.convertToTimestamp(dateStr, formatStr);
            return Boolean.TRUE;
        } catch (AxionException e) {
            // Rethrow AxionException if formatStr is invalid (SQLSTATE 22007).
            if ("22007".equals(e.getSQLState())) {
                throw e;
            }
            return Boolean.FALSE;
        }
    }

    public ConcreteFunction makeNewInstance() {
        return new IsValidDateTimeFunction();
    }

    private static final DataType BOOLEAN_TYPE = new BooleanType();
    private static final DataType ARG1_TYPE = new StringType();
    private static final DataType ARG2_TYPE = new StringType();
}
