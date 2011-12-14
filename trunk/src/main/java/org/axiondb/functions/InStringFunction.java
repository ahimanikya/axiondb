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
import org.axiondb.types.IntegerType;
import org.axiondb.types.StringType;
import org.axiondb.util.ValuePool;

/**
 * INSTR(str,substr): Returns the position of the first occurrence of substring substr in
 * string str.
 * 
 * @version  
 * @author Sanjeeth Duvuru
 * @author Ahimanikya Satapathy
 */
public class InStringFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    public InStringFunction() {
        super("INSTRING");
    }

    public ConcreteFunction makeNewInstance() {
        return new InStringFunction();
    }

    /** {@link StringType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        int pos = -1;
        String str = (String) getArgument(0).evaluate(row);
        String substr = (String) getArgument(1).evaluate(row);
        
        if(str == null || substr == null) {
            return ValuePool.getInt(pos);
        }
        
        if (getArgumentCount() == 3) {
            Object val3 = getArgument(2).evaluate(row);
            if (val3!= null) {
                Integer startPos = (Integer) (ARG_TYPE.convert(val3));
                if (startPos.intValue() >= 0) {
                    pos = str.indexOf(substr, startPos.intValue());
                } else {
                    int fromIdx = str.length() - Math.abs(startPos.intValue());
                    pos = str.lastIndexOf(substr, fromIdx);
                }
            } else {
                throw new AxionException("Value " + val3 + " cannot be converted to a " + ARG_TYPE);
            }
        } else{
            pos = str.indexOf(substr);
        }
        return ValuePool.getInt(pos);
    }

    public boolean isValid() {
        return (getArgumentCount() >= 2 && getArgumentCount() < 4);
    }

    private static final DataType RETURN_TYPE = new StringType();
    private static final DataType ARG_TYPE = new IntegerType();
}
