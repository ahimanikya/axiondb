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

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.types.StringType;
import org.axiondb.types.TimestampType;
import org.axiondb.util.DateTimeUtils;

/**
 * Syntax: CharToDate( date-string, 'format-literal' )
 * 
 * @version  
 * @author Jonathan Giron
 */
public class CharToDateFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public CharToDateFunction() {
        super("CHARTODATE");
    }

    public ConcreteFunction makeNewInstance() {
        return new CharToDateFunction();
    }

    /** {@link DataType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    /**
     * Gets String value representing the timestamp/date value in the given format.
     * 
     * @param row
     * @return String representation of the timestamp/date value
     */
    public Object evaluate(RowDecorator row) throws AxionException {
        String dateStr = null;
        String formatStr = null;

        // Get 'format-literal'
        Object arg2 = getArgument(1).evaluate(row);
        if (null == arg2) {
            throw new AxionException("CharToDate cannot accept a null format string.");
        } else {
            formatStr = (String) STRING_TYPE.convert(arg2);
        }

        // Get 'date-string'
        Object arg1 = getArgument(0).evaluate(row);
        if (null == arg1) {
            return null;
        } else {
            dateStr = (String) STRING_TYPE.convert(arg1);
        } 

        return DateTimeUtils.convertToTimestamp(dateStr, formatStr);
    }

    public boolean isValid() {
        return getArgumentCount() >= 2;
    }

    private static final DataType RETURN_TYPE = new TimestampType();
    private static final DataType STRING_TYPE = new StringType();
}
