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
import java.sql.Timestamp;
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing a long value.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class BigIntType extends BaseNumberDataType {

    public BigIntType() {
    }

    public int getJdbcType() {
        return java.sql.Types.BIGINT;
    }

    public String getPreferredValueClassName() {
        return "java.lang.Long";
    }

    public String getTypeName() {
        return "BIGINT";
    }
    
    public int getPrecision() {
        return String.valueOf(Long.MAX_VALUE).length();
    }

    public int getColumnDisplaySize() {
        return String.valueOf(Long.MIN_VALUE).length();
    }

    public String toString() {
        return "bigint";
    }

    /**
     * Returns <code>true</code> iff <i>value </i> is <code>String</code> that can be
     * {@link #convert converted}without exception, <code>null</code>, or a
     * {@link Number Number}.
     */
    public boolean accepts(Object value) {
        if (value instanceof Timestamp) {
            return true;
        }
        return super.accepts(value);
    }

    /**
     * Returns an <tt>Long</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        BigDecimal rawValue = null;
        
        if (value instanceof Long) {
            return value;
        } else if (value instanceof BigDecimal) {
            rawValue = (BigDecimal) value;
        } else if (value instanceof Number) {
            return new Long(((Number) value).longValue());
        } else if (value instanceof String) {
            try {
                rawValue = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException e) {
                throw new AxionException(22018);
            }
        } else if (value instanceof Timestamp) {
            return new Long(((Timestamp) value).getTime());
        } else {
            return super.convert(value);
        }
        
        assertValueInRange(rawValue);
        return new Long(rawValue.longValue());
    }

    public Object successor(Object value) throws IllegalArgumentException {
        long v = ((Long) value).longValue();
        if (v == Long.MAX_VALUE) {
            return value;
        }
        return new Long(++v);
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        long value = in.readLong();
        if (Long.MIN_VALUE == value) {
            if (!in.readBoolean()) {
                return null;
            }
        }
        return new Long(value);
    }

    /**
     * Writes the given <i>value </i> to the given <code>DataOutput</code>.
     * <code>Null</code> values are written as <code>Long.MIN_VALUE</code>,
     * <code>false</code>.<code>Long.MIN_VALUE</code> values are written as
     * <code>Long.MIN_VALUE</code>,<code>true</code>. All other values are written
     * directly.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}
     */
    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeLong(Long.MIN_VALUE);
            out.writeBoolean(false);
        } else {
            long val;
            try {
                val = ((Long) (convert(value))).longValue();
                out.writeLong(val);
                if (Long.MIN_VALUE == val) {
                    out.writeBoolean(true);
                }
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    public DataType makeNewInstance() {
        return new BigIntType();
    }

    public int compare(Object a, Object b) {
        long pa = ((Number) a).longValue();
        long pb = ((Number) b).longValue();
        return (pa < pb) ? -1 : ((pa == pb) ? 0 : 1);
    }

    protected Comparator getComparator() {
        return this;
    }

    private void assertValueInRange(BigDecimal bdValue) throws AxionException {
        if (bdValue.compareTo(MIN_BD) < 0 || bdValue.compareTo(MAX_BD) > 0) {
            throw new AxionException(22003);
        }
    }

    private static final BigDecimal MAX_BD = new BigDecimal(Long.MAX_VALUE);
    private static final BigDecimal MIN_BD = new BigDecimal(Long.MIN_VALUE);
    private static final long serialVersionUID = 249194398145634251L;
}


