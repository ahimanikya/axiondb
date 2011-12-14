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
package org.axiondb.functions;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.types.CharacterType;
import org.axiondb.types.IntegerType;
import org.axiondb.types.StringType;

/**
 * TRIM ( [LEADING| TRAILING| BOTH] [ trim-character ] FROM ] trim-source) function
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class TrimFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public TrimFunction() {
        super("TRIM");
    }

    public ConcreteFunction makeNewInstance() {
        return new TrimFunction();
    }

    /** {@link StringType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        char trimChar = ' '; // default trim char
        if (getArgumentCount() == 3) {
            Object trimCharObj = getArgument(2).evaluate(row);
            String trimCharStr = (String) CHAR_TYPE.convert(trimCharObj) ;
            if (trimCharStr != null) {
                trimChar = trimCharStr.charAt(0);
            }
        }

        Object trimSourceObj = getArgument(1).evaluate(row);
        if ((trimSourceObj == null) || ("".equals(trimSourceObj))){
            return trimSourceObj;
        }
        
        String trimSourceStr = (String) RETURN_TYPE.convert(trimSourceObj);
        
        Object trimTypeObj = getArgument(0).evaluate(row);
        int type = ((Integer) INT_TYPE.convert(trimTypeObj)).intValue();

        if (type == 1 || type == 3) { // Leading or Both
            int trimSourceLen = trimSourceStr.length();
            int i = 0;
            for (; i < trimSourceLen && trimSourceStr.charAt(i) == trimChar; i++);
            trimSourceStr = ((i == 0) ? trimSourceStr : trimSourceStr.substring(i));
        }

        if (type == 2 || type == 3) { // Trailing or Both
            int trimSourceLen = trimSourceStr.length();
            int i, len = trimSourceLen - 1;
            for (i=len; i > 0 && trimSourceStr.charAt(i) == trimChar; i--);
            trimSourceStr = (i == len) ? trimSourceStr : trimSourceStr.substring(0, i + 1);
        }

        return trimSourceStr;
    }

    public boolean isValid() {
        return (getArgumentCount() >= 1 && getArgumentCount() < 4);
    }

    private static final DataType RETURN_TYPE = new StringType();
    private static final DataType CHAR_TYPE = new CharacterType(1);
    private static final DataType INT_TYPE = new IntegerType();
}
