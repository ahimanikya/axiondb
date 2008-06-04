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
import java.sql.Types;
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing a byte value.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class ByteType extends BaseNumberDataType {

    public ByteType() {
    }

    /** @return {@link Types#TINYINT} */
    public int getJdbcType() {
        return Types.TINYINT;
    }

    public int getColumnDisplaySize() {
        return String.valueOf(Byte.MIN_VALUE).length();
    }

    public String getPreferredValueClassName() {
        return "java.lang.Byte";
    }

    public int getPrecision() {
        return String.valueOf(Byte.MAX_VALUE).length();
    }

    public String toString() {
        return "byte";
    }

    /**
     * Returns an <tt>Byte</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        short rawValue = (short) 0;
        
        if (value instanceof Byte) {
            return value;
        } else if (value instanceof Number) {
            rawValue = ((Number) value).shortValue();
        } else if (value instanceof String) {
            try {
                rawValue = new BigDecimal(value.toString().trim()).shortValue();
            } catch (NumberFormatException e) {
                throw new AxionException(22018);
            }
        } else {
            return super.convert(value);
        }
        
        assertValueInRange(rawValue);
        return new Byte((byte) rawValue);
    }

    public Object successor(Object value) throws IllegalArgumentException {
        byte v = ((Byte) value).byteValue();
        if (v == Byte.MAX_VALUE) {
            return value;
        }
        return new Byte(++v);
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        byte value = in.readByte();
        if (Byte.MIN_VALUE == value) {
            if (!in.readBoolean()) {
                return null;
            }
        }
        return new Byte(value);
    }

    /**
     * Writes the given <i>value </i> to the given <code>DataOutput</code>.
     * <code>Null</code> values are written as <code>Byte.MIN_VALUE</code>,
     * <code>false</code>.<code>Byte.MIN_VALUE</code> values are written as
     * <code>Byte.MIN_VALUE</code>,<code>true</code>. All other values are written
     * directly.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}
     */
    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeByte(Byte.MIN_VALUE);
            out.writeBoolean(false);
        } else {
            try {
                byte val = ((Byte) (convert(value))).byteValue();
                out.writeByte(val);
                if (Byte.MIN_VALUE == val) {
                    out.writeBoolean(true);
                }
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    public DataType makeNewInstance() {
        return new ByteType();
    }
    
    public int compare(Object a, Object b) {
        byte pa = ((Number) a).byteValue();
        byte pb = ((Number) b).byteValue();
        return (pa - pb);
    }

    protected Comparator getComparator() {
        return this;
    }

    private void assertValueInRange(short rawValue) throws AxionException {
        if (rawValue < Byte.MIN_VALUE || rawValue > Byte.MAX_VALUE) {
            throw new AxionException(22003);
        }
    }
    
    private static final long serialVersionUID = -8198635981830444420L;
}


