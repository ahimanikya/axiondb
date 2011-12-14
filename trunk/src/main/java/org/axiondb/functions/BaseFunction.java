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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.axiondb.BaseSelectable;
import org.axiondb.DataType;
import org.axiondb.Selectable;
import org.axiondb.VariableContext;

/**
 * An abstract base {@link ConcreteFunction}implementation.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public abstract class BaseFunction extends BaseSelectable implements ConcreteFunction {

    public BaseFunction(String name) {
        setName(name);
    }

    public BaseFunction(String name, List args) {
        setName(name);
        _args = args;
    }

    public void addArgument(Selectable arg) {
        _args.add(arg);
    }

    public Selectable getArgument(int i) {
        return (Selectable) (_args.get(i));
    }

    public int getArgumentCount() {
        return _args.size();
    }

    public abstract DataType getDataType();

    public abstract boolean isValid();

    public void setArgument(int i, Selectable arg) {
        _args.set(i, arg);
    }

    public void setVariableContext(VariableContext context) {
        for (Iterator iter = _args.iterator(); iter.hasNext();) {
            Selectable sel = (Selectable) (iter.next());
            if (null != sel) {
                sel.setVariableContext(context);
            }
        }
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(getName());
        if (!_args.isEmpty()) {
            buf.append("(");
            for (Iterator iter = _args.iterator(); iter.hasNext();) {
                buf.append(iter.next());
                if (iter.hasNext()) {
                    buf.append(",");
                }
            }
            buf.append(")");
        }

        if (null != getAlias()) {
            buf.append(" AS ");
            buf.append(getAlias());
        }
        return buf.toString();
    }
    
    private List _args = new ArrayList(2);
}
