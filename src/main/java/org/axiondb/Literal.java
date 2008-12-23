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

import org.axiondb.types.AnyType;

/**
 * A {@link DataType typed}literal value.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class Literal extends BaseSelectable implements Selectable {

    public Literal(Object value) {
        this(value, AnyType.INSTANCE);
    }

    public Literal(Object value, DataType type) {
        _value = value;
        _type = type;
    }

    protected Literal(DataType type) {
        this(null, type);
    }

    /**
     * Returns <code>true</code> iff <i>otherobject </i> is a {@link Literal}whose name
     * are equal to mine.
     */
    @Override
    public boolean equals(Object otherobject) {
        if (this == otherobject) {
            return true;
        }
        
        if (otherobject instanceof Literal) {
            Literal that = (Literal) otherobject;
            return getName().equals(that.getName());
        }
        return false;
    }

    public Object evaluate() throws AxionException {
        if (null == _value && null == _type) {
            return _value;
        } else if (null == _type) {
            return _value;
        } else {
            // store the converted value, so we may not have to convert again.
            if (!_evaluated) {
                _value = _type.convert(_value);
                _evaluated = true;
            }
            return _value;
        }
    }

    /**
     * @param row is ignored and may be null.
     * @see #evaluate
     */
    public final Object evaluate(RowDecorator row) throws AxionException {
        return evaluate();
    }

    public DataType getDataType() {
        return _type;
    }

    /**
     * Returns a hash code in keeping with the standard {@link Object#equals equals}/
     * {@link Object#hashCode hashCode}contract.
     */
    @Override
    public int hashCode() {
        int hashCode = _hash;
        if (hashCode == 0) {
            hashCode = getName().hashCode();
            _hash = hashCode;
        }
        return hashCode;
    }

    public void setDataType(DataType type) {
        _type = type;
        _hash = 0;
    }
    
    /**
     * Returns my Literal name.
     */
    @Override
    public String getLabel() {
        return getName();
    }
    
    /**
     * Returns the name of Literal, if any.
     */
    @Override
    public String getName() {
        if (getAlias() != null) {
            return getAlias();
        }
        return String.valueOf(_value);
    }

    /**
     * Returns a <code>String</code> representation of me, suitable for debugging
     * output.
     */
    @Override
    public String toString() {
        return getName();
    }

    private boolean _evaluated = false;
    private DataType _type;
    protected Object _value;
}
