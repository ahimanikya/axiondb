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

package org.axiondb.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType} representing a single-precision floating-point value.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class FloatType extends BaseNumberDataType {

    public FloatType() {
    }

    public int getJdbcType() {
        return java.sql.Types.FLOAT;
    }

    public String getPreferredValueClassName() {
        return "java.lang.Float";
    }
    
    public String getTypeName() {
        return "FLOAT";
    }

    public int getPrecision() {
        return String.valueOf(Float.MAX_VALUE).length();
    }

    public int getScale() {
        // NOTE: Per ANSI SQL-2003 standard approximate datatypes like float do not have
        // a scale value - only a precision value.
        return 0;
    }

    public int getColumnDisplaySize() {
        return String.valueOf(Float.MAX_VALUE).length();
    }

    /**
     * Returns <code>"float"</code>
     * 
     * @return <code>"float"</code>
     */
    public String toString() {
        return "float";
    }

    /**
     * Returns a <tt>Float</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        double doubleValue = 0.0;
        if (value instanceof Float) {
            float floatValue = ((Float) value).floatValue();
            if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
                throw new AxionException(22003);
            }            
            return value;
        } else if (value instanceof Number) {
            doubleValue = ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                doubleValue = new BigDecimal(value.toString().trim()).doubleValue();
            } catch (NumberFormatException e) {
                throw new AxionException(22018);
            }
        } else {
            return super.convert(value);
        }
        
        assertValueInRange(doubleValue);
        return new Float(doubleValue);
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        float value = in.readFloat();
        if (Float.MIN_VALUE == value) {
            if (!in.readBoolean()) {
                return null;
            }
        }
        return new Float(value);
    }

    /** <code>false</code> */
    public boolean supportsSuccessor() {
        return true;
    }

    public Object successor(Object value) throws IllegalArgumentException {
        float v = ((Float) value).floatValue();
        if (v == Float.MAX_VALUE) {
            return value;
        }
        
        int ieee754Bits = Float.floatToIntBits(v);
        int sign = (ieee754Bits & SIGN_MASK);
        boolean isNegative = (SIGN_NEGATIVE == sign);
        
        int accumExpMantissa = (ieee754Bits & EXP_MANTISSA_MASK);
        if (isNegative) {
            if (0x0L != accumExpMantissa) {
                accumExpMantissa -= 1;
            } else {
                sign = 0x0;
                accumExpMantissa = 0x0;
            }
        } else {
            if (0x0L != accumExpMantissa) {
                accumExpMantissa += 1;
            } else {
                accumExpMantissa = 1;
            }
        }
        
        return new Float(Float.intBitsToFloat(sign | accumExpMantissa));        
    }

    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeFloat(Float.MIN_VALUE);
            out.writeBoolean(false);
        } else {
            try {
                float val = ((Float) (convert(value))).floatValue();
                out.writeFloat(val);
                if (Float.MIN_VALUE == val) {
                    out.writeBoolean(true);
                }
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }

        }
    }

    public DataType makeNewInstance() {
        return new FloatType();
    }

    public int compare(Object a, Object b) {
        float pa = ((Number) a).floatValue();
        float pb = ((Number) b).floatValue();
        return (pa < pb) ? -1 : ((pa == pb) ? 0 : 1);
    }

    protected Comparator getComparator() {
        return this;
    }

    private void assertValueInRange(double doubleValue) throws AxionException {
        if (doubleValue > Float.MAX_VALUE || doubleValue < -Float.MAX_VALUE) {
            throw new AxionException(22003);
        }
    }

    private static final long serialVersionUID = -831981915887585231L;

    private static final int SIGN_MASK = 0x80000000;
    private static final int SIGN_NEGATIVE = SIGN_MASK;
    private static final int EXP_MANTISSA_MASK = 0x7fffffff;
}


