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
import org.axiondb.Selectable;
import org.axiondb.types.IntegerType;
import org.axiondb.types.StringType;

/**
 * Syntax: SUBSTRING(str, m [,n]) -- m is one(1) based index.)
 * 
 * @version  
 * @author Sudhendra Seshachala
 * @author Ahimanikya Satapathy
 * @author Rupesh Ramachandran
 */
public class SubstringFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public SubstringFunction() {
        super("SUBSTRING");
    }

    public ConcreteFunction makeNewInstance() {
        return new SubstringFunction();
    }

    /** {@link StringType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    /**
     * Returns substring of str, beginning at character m, n characters long.
     * <p>
     * <li>If m is 0, it is treated as 1. If m is positive, it counts from the beginning
     * of str to find the first character. If m is negative, it counts backwards from the
     * end of str.
     * <li>If n is omitted, it returns all characters to the end of str. If n is less
     * than 1, a null is returned.
     * <li>Floating-point numbers passed as arguments to substr are automatically
     * converted to integers.
     * <li>If m greater than length of str, return null. If m+n is bigger than length of
     * str, ignore n. if str is null, return null.
     */
    public Object evaluate(RowDecorator row) throws AxionException {
        int m = 0, n = 0;

        Object sel = getArgument(0).evaluate(row);
        String str = (String) RETURN_TYPE.convert(sel);
        // if current str is null, then return is also null
        if (str == null) {
            return null;
        }
        int strLen = str.length();

        // Get 'm'
        Selectable sel1 = getArgument(1);
        Object val1 = sel1.evaluate(row);
        Integer int1 = (Integer) (INT_TYPE.convert(val1));
        m = int1.intValue();
        if (m < 0) {
            m = strLen + m; // if -ve, count backwards from strLen
        } else if (m != 0) {
            m = m - 1; // if +ve, convert to 0-based index
        }

        // if m == 0, then leave as 0 (1 in 0-based index)
        // if m is too big, return null else IndexOutOfBound
        if (m > strLen) {
            return null;
        }

        // Get 'n'
        if (getArgumentCount() == 3) {
            Selectable sel2 = getArgument(2);
            Object val2 = sel2.evaluate(row);

            Integer int2 = (Integer) (INT_TYPE.convert(val2));
            n = int2.intValue();
            if (n < 1) {
                return null;
            }

            // if n is too big, ignore it
            // otherwise IndexOutOfBound exception
            if (n > (strLen - m)) {
                n = strLen - m;
            }
        }

        if (getArgumentCount() == 3) {
            return str.substring(m, n + m);
        } else {
            return str.substring(m);
        }
    }

    public boolean isValid() {
        return (getArgumentCount() >= 2);
    }

    private static final DataType RETURN_TYPE = new StringType();
    private static final DataType INT_TYPE = new IntegerType();

}
