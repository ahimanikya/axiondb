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
import java.math.BigInteger;
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing an number value.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class BigDecimalType extends BaseNumberDataType implements DataType.ExactNumeric {
    public static final int ROUNDING_RULE = BigDecimal.ROUND_HALF_UP;

    public static final int DEFAULT_PRECISION = 22;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_PRECISION = 38;

    public BigDecimalType() {
        this(DEFAULT_PRECISION, 0);
    }

    public BigDecimalType(int scale) {
        this(DEFAULT_PRECISION, scale);
    }

    public BigDecimalType(int precision, int scale) {
        _precision = precision;
        _scale = scale;
    }

    public BigDecimalType(BigDecimal result) {
        _scale = result.scale();
        
        int valueLength = result.toString().length();
        valueLength -= (_scale != 0) ? 1 : 0; // account for decimal point if present
        valueLength -= (result.signum() < 0) ? 1 : 0; // account for sign if present

        int unscaledPrecision = result.unscaledValue().abs().toString().length(); 
        if (unscaledPrecision == _scale) {
            _precision = _scale;
        } else {
            _precision = Math.max(valueLength, unscaledPrecision);
        }
    }
    
    public String getTypeName() {
        return "NUMERIC";
    }

    public int getColumnDisplaySize() {
        // # significant digits + decimal point (if any) + sign
        return _precision + ((_scale == 0) ? 0 : 1) + 1;
    }

    public int getPrecision() {
        return _precision;
    }

    public int getJdbcType() {
        return java.sql.Types.NUMERIC;
    }

    public String getPreferredValueClassName() {
        return "java.math.BigDecimal";
    }

    public int getScale() {
        return _scale;
    }

    public void setPrecision(int newPrecision) {
        _precision = newPrecision;
    }

    public void setScale(int newScale) {
        _scale = newScale;
    }

    /**
     * Returns <code>"BigDecimal"</code>
     * 
     * @return <code>"BigDecimal"</code>
     */
    public String toString() {
        StringBuffer buf = new StringBuffer(25);
        buf.append("numeric").append("(").append(_precision).append(",").append(_scale).append(")");
        return buf.toString();
    }

    public boolean accepts(Object value) {
        if (value instanceof BigDecimal) {
            return true;
        } else {
            return super.accepts(value);
        }
    }

    public boolean requiresRounding(BigDecimal value) {
        return _scale >= 0 && value.scale() != _scale;
    }

    /**
     * Returns a <tt>BigDecimal</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        if (value == null) {
            return value;
        }

        BigDecimal toreturn = null;
        if (value instanceof BigDecimal) {
            toreturn = (BigDecimal) (value);
        } else if (value instanceof BigInteger) {
            toreturn = new BigDecimal((BigInteger) value);
        } else if (value instanceof Double || value instanceof Float) {
            try {
                toreturn = new BigDecimal(String.valueOf(value));
            } catch (NumberFormatException e) {
                throw new AxionException(22003);
            }
        } else if (value instanceof Number) {
            toreturn = BigDecimal.valueOf(((Number) value).longValue());
        } else if (value instanceof String) {
            try {
                toreturn = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException e) {
                throw new AxionException(22018);
            }
        } else {
            toreturn = (BigDecimal) (super.convert(value));
        }
        
        if (null != toreturn) {
            try {
                if (requiresRounding(toreturn)) {
                    toreturn = toreturn.setScale(_scale, ROUNDING_RULE);
                }
                assertValueNotOutOfRange(toreturn);
            } catch (ArithmeticException e) {
                throw new AxionException("BigDecimal " + toreturn + " has scale " + toreturn.scale() + ", can't convert to scale " + _scale + ": "
                    + e.getMessage());
            }
        }
        return toreturn;
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        String str = in.readUTF();
        if (NULL_BIGDEC.equals(str)) {
            return null;
        }
        BigInteger value = new BigInteger(str, TOSTRING_RADIX);
        int scale = in.readInt();
        return new BigDecimal(value, scale);
    }

    /** <code>false</code> */
    public boolean supportsSuccessor() {
        return true;
    }

    public Object successor(Object value) throws IllegalArgumentException {
        double v = ((BigDecimal) value).doubleValue();

        return new BigDecimal(v + 1);
    }

    public void write(Object value, DataOutput out) throws IOException {
        try {
            BigDecimal towrite = (BigDecimal) (convert(value));
            if (null == towrite) {
                out.writeUTF(NULL_BIGDEC);
            } else {
                out.writeUTF(towrite.unscaledValue().toString(TOSTRING_RADIX));
                out.writeInt(towrite.scale());
            }
        } catch (AxionException e) {
            throw new IOException(e.getMessage());
        }
    }

    public DataType makeNewInstance() {
        return makeNewInstance(DEFAULT_PRECISION, 0);
    }

    public int compare(Object a, Object b) {
        BigDecimal bda = null;
        try {
            bda = (BigDecimal) convert(a);
        } catch (AxionException e) {
            throw new ClassCastException("cannot convert " + a.toString() + " to BigDecimal for comparison");
        }

        BigDecimal bdb = null;
        try {
            bdb = (BigDecimal) convert(b);
        } catch (AxionException e) {
            throw new ClassCastException("cannot convert " + b.toString() + " to BigDecimal for comparison");
        }

        return bda.compareTo(bdb);
    }

    public DataType.ExactNumeric makeNewInstance(int newPrecision, int newScale) {
        return new BigDecimalType(newPrecision, newScale);
    }

    protected Comparator getComparator() {
        return this;
    }

    private int getPrecision(BigDecimal value) {
        return value.unscaledValue().abs().toString().length();
    }

    private void assertValueNotOutOfRange(BigDecimal value) throws AxionException {
        int wholeNumberPrecision = Math.max(0, getPrecision(value) - value.scale());
        if ((wholeNumberPrecision != 0) && wholeNumberPrecision > (_precision - _scale)) {
            throw new AxionException(22003);
        }
    }

    private int _precision = DEFAULT_PRECISION;
    private int _scale = DEFAULT_SCALE;

    private static final String NULL_BIGDEC = " ";

    private static final int TOSTRING_RADIX = Character.MAX_RADIX;

    private static final long serialVersionUID = -8555010408278020956L;
}
