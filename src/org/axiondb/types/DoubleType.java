/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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
 * A {@link DataType}representing a double value.
 * 
 * @version  
 * @author Jonathan Giron
 */
public class DoubleType extends BaseNumberDataType {

    public DoubleType() {
    }

    public int getJdbcType() {
        return java.sql.Types.DOUBLE;
    }

    public String getPreferredValueClassName() {
        return "java.lang.Double";
    }

    public int getPrecision() {
        return String.valueOf(Double.MAX_VALUE).length();
    }

    public int getPrecisionRadix() {
        return 10;
    }

    public int getScale() {
        // NOTE: Per ANSI SQL-2003 standard approximate data types like double do not have
        // a scale value - only a precision value.
        return 0;
    }

    public int getColumnDisplaySize() {
        return String.valueOf(Double.MAX_VALUE).length();
    }

    /**
     * Returns <code>"double"</code>
     * 
     * @return <code>"double"</code>
     */
    public String toString() {
        return "double";
    }

    /**
     * Returns a <tt>Double</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        BigDecimal bdValue = null;
        if (value instanceof Double) {
            double doubleValue = ((Number) value).doubleValue();
            if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
                throw new AxionException(22003);
            }
            return value;
        } else if (value instanceof BigDecimal) {
            bdValue = (BigDecimal) value;
        } else if (value instanceof Number) {
            return new Double(((Number) value).doubleValue());
        } else if (value instanceof String) {
            try {
                bdValue = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException e) {
                throw new AxionException(22018);
            }
        } else {
            return super.convert(value);
        }
        
        assertValueInRange(bdValue);
        return new Double(bdValue.doubleValue());
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        double value = in.readDouble();
        if (Double.MIN_VALUE == value) {
            if (!in.readBoolean()) {
                return null;
            }
        }
        return new Double(value);
    }

    /** <code>true</code> */
    public boolean supportsSuccessor() {
        return true;
    }

    public Object successor(Object value) throws IllegalArgumentException {
        Double obj = (Double) value;
        double v = obj.doubleValue();
        if (Double.MAX_VALUE == v) {
            return value;
        }

        long ieee754Bits = Double.doubleToLongBits(v);
        long sign = (ieee754Bits & SIGN_MASK);
        boolean isNegative = (SIGN_NEGATIVE == sign);
        
        long accumExpMantissa = (ieee754Bits & EXP_MANTISSA_MASK);
        if (isNegative) {
            if (0x0L != accumExpMantissa) {
                accumExpMantissa -= 1L;
            } else {
                sign = 0x0L;
                accumExpMantissa = 0x0L;
            }
        } else {
            if (0x0L != accumExpMantissa) {
                accumExpMantissa += 1L;
            } else {
                accumExpMantissa = 1L;
            }
        }
        return new Double(Double.longBitsToDouble(sign | accumExpMantissa));
    }

    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeDouble(Double.MIN_VALUE);
            out.writeBoolean(false);
        } else {
            try {
                double val = ((Double) (convert(value))).doubleValue();
                out.writeDouble(val);
                if (Double.MIN_VALUE == val) {
                    out.writeBoolean(true);
                }
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }

        }
    }

    public DataType makeNewInstance() {
        return new DoubleType();
    }

    public int compare(Object a, Object b) {
        double pa = ((Number) a).doubleValue();
        double pb = ((Number) b).doubleValue();
        return (pa < pb) ? -1 : ((pa == pb) ? 0 : 1);
    }

    protected Comparator getComparator() {
        return this;
    }

    private void assertValueInRange(BigDecimal bdValue) throws AxionException {
        long expMantissa = Double.doubleToLongBits(bdValue.doubleValue()) & EXP_MANTISSA_MASK;
        if (expMantissa > EXP_MANTISSA_MAX) {
            throw new AxionException(22003);
        }
    }
    
    private static final long serialVersionUID  = -831981915887585231L;

    private static final long EXP_MANTISSA_MASK = 0x7FFFFFFFFFFFFFFFL;
    private static final long EXP_MANTISSA_MAX  = 0x7FEFFFFFFFFFFFFFL;
    private static final long SIGN_MASK         = 0x8000000000000000L;
    private static final long SIGN_NEGATIVE     = SIGN_MASK;    
}

