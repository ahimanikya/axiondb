/*
 * 
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

/**
 * @version  
 * @author Sudhendra Seshachala
 * @author Ahimanikya Satapathy
 */
public class LTrimFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public LTrimFunction() {
        super("LTRIM");
    }

    public ConcreteFunction makeNewInstance() {
        return new LTrimFunction();
    }

    /** {@link StringType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        char trimChar = ' '; // default trim char
        if (getArgumentCount() == 2) {
            Object trimCharObj = getArgument(1).evaluate(row);
            String trimCharStr = (String) RETURN_TYPE.convert(trimCharObj) ;
            if (trimCharStr != null && trimCharStr.length() > 0) {
                trimChar = trimCharStr.charAt(0);
            }
        }

        Object trimSourceObj = getArgument(0).evaluate(row);
        if ((trimSourceObj == null) || ("".equals(trimSourceObj))){
            return trimSourceObj;
        }
        String trimSourceStr = (String) RETURN_TYPE.convert(trimSourceObj);
        int trimSourceLen = trimSourceStr.length();
        int i = 0;
        for (; i < trimSourceLen && trimSourceStr.charAt(i) == trimChar; i++);
        trimSourceStr = ((i == 0) ? trimSourceStr : trimSourceStr.substring(i));

        return trimSourceStr;
    }

    public boolean isValid() {
        return (getArgumentCount() >= 1 && getArgumentCount() < 3);
    }

    private static final DataType RETURN_TYPE = new StringType();
}
