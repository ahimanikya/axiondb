/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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

package org.axiondb;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class SequenceEvaluator implements Selectable {
    public static final String CURRVAL = "CURRVAL";
    public static final String NEXTVAL = "NEXTVAL";

    public SequenceEvaluator(Sequence seq, String method) throws AxionException {
        _seq = seq;
        _method = method;
        if (NEXTVAL.equalsIgnoreCase(method)) {
            _nextval = true;
        } else if (CURRVAL.equalsIgnoreCase(method)) {
            _nextval = false;
        } else {
            throw new AxionException("Pseudocolumn " + method + " not recognized.");
        }
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        if (_nextval) {
            Object val = _seq.evaluate();
            _ctx.put(_seq, val);
            return val;
        }

        if (_ctx.containsKey(_seq)) {
            return _ctx.get(_seq);
        }

        throw new AxionException("Sequence " + getName() + " not yet evaluated in this context.");
    }

    public String getAlias() {
        if (_alias != null) {
            return _alias;
        }
        return _seq.getName();
    }

    public void setAlias(String alias) {
        _alias = alias;
    }

    public DataType getDataType() {
        return _seq.getDataType();
    }

    public String getLabel() {
        if (_alias != null) {
            return _alias;
        }
        return _seq.getName() + "_" + _method;
    }

    public String getName() {
        return _seq.getName();
    }

    public void setVariableContext(VariableContext ctx) {
        _ctx = ctx;
    }
    
    private String _method;
    private String _alias;
    private VariableContext _ctx;
    private boolean _nextval;
    private Sequence _seq;
}
