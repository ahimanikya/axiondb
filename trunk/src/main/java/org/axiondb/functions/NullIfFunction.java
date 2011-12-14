/*
 /*  tag
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
import org.axiondb.types.AnyType;

/**
 * The NULLIF function returns a null value if the arguments are equal, otherwise it
 * returns the value of the first argument.
 * <p>
 * The result of using NULLIF(e1,e2) is the same as using the expression
 * <code>CASE WHEN e1=e2 THEN NULL ELSE e1 END</code>
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class NullIfFunction extends BaseFunction implements ScalarFunction, FunctionFactory {
    public NullIfFunction() {
        super("NULLIF");
    }

    public ConcreteFunction makeNewInstance() {
        return new NullIfFunction();
    }

    /** {@link AnyType} */
    public DataType getDataType() {
        return _returnType;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        Selectable sel1 = getArgument(0);
        EqualFunction fn = new EqualFunction();
        fn.addArgument(sel1);
        fn.addArgument(getArgument(1));

        Boolean result = (Boolean) fn.evaluate(row); 
        if (result == null || !result.booleanValue()) {
            // Must evaluate row with sel1 before getting datatype of sel1.
            Object val = sel1.evaluate(row);
            _returnType = sel1.getDataType();
            return val;
        }
        return null;
    }

    public boolean isValid() {
        return (getArgumentCount() == 2);
    }

    private DataType _returnType = AnyType.INSTANCE;
}
