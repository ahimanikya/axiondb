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

/**
 * @version  
 * @author Sanjeeth Duvuru
 * @author Ahimanikya Satapathy
 */
public class ReplaceFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public ReplaceFunction() {
        super("REPLACE");
    }

    public ConcreteFunction makeNewInstance() {
        return new ReplaceFunction();
    }

    /** {@link StringType} */
    public DataType getDataType() {
        return STRING_TYPE;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        Object val = getArgument(0).evaluate(row);        
        Object val1 = getArgument(1).evaluate(row);

        if ((val == null) || (val1 == null)){
            return val;
        }

        String s1 = (String) STRING_TYPE.convert(val);
        String s2 = (String) STRING_TYPE.convert(val1);
        int i2 = s2.length();
        
        if (getArgumentCount() == 3) {
            Object val2 = getArgument(2).evaluate(row);
            String s3 = (String) STRING_TYPE.convert(val2);
            return replacedString(s1, s2, s3, i2);
        } else {
            return replacedString(s1, s2, "", i2);
        }
    }

    private String replacedString(String s1, String s2, String s3, int i2) {
        int i0 = s1.indexOf(s2);
        if (i0 != -1) {
            s1 = s1.substring(0, i0) + s3 + s1.substring(i0 + i2);
            s1 = replacedString(s1, s2, s3, i2);
        }
        return s1;
    }

    public boolean isValid() {
        return (getArgumentCount() >= 2 && getArgumentCount() < 4);
    }

    private static final DataType STRING_TYPE = new StringType();
}
