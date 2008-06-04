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
import java.io.Serializable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;
import java.util.Comparator;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing a {@link BinaryArray}value.
 * 
 * @version  
 * @author Rahul Dwivedi
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class VarBinaryType extends BaseDataType implements Serializable, Comparator, 
        DataType.NonFixedPrecision {
    
    public VarBinaryType() {
        this(1);
    }

    /** Creates a new instance of VarBinaryType */
    public VarBinaryType(int length) {
        setLength(length);
    }

    public void setLength(int length) {
        this._length = length;

    }

    public int getColumnDisplaySize() {
        return Integer.MAX_VALUE;
    }

    public int getPrecision() {
        return _length;
    }

    /**
     * Return <code>true</code> if a field of my type can be assigned the given non-
     * <code>null</code> <i>value </i>, <code>false</code> otherwise.
     * 
     * @param value non- <code>null</code> value
     */
    public boolean accepts(Object value) {
        return true;
    }

    public boolean supportsSuccessor() {
        return true;
    }

    public Object successor(Object value) throws IllegalArgumentException {
        byte[] val = (byte[]) value;
        if (val.length == 0) {
            return new byte[] { Byte.MIN_VALUE };
        }
        byte last = val[val.length - 1];
        if (last == Byte.MAX_VALUE) {
            byte[] newval = new byte[val.length + 1];
            System.arraycopy(val, 0, newval, 0, val.length);
            newval[val.length] = Byte.MIN_VALUE;
            return newval;
        }
        byte[] newval = new byte[val.length];
        System.arraycopy(val, 0, newval, 0, val.length - 1);
        newval[val.length - 1] = (byte) (last + 1);
        return newval;
    }

    /**
     * Converts an {@link #accepts acceptable}value to one of the appropriate type.
     */
    public Object convert(Object value) throws AxionException {
        if (value instanceof byte[]) {
            if (((byte[]) value).length <= _length) {
                return (byte[]) value;
            } 
            throw new AxionException(22001);
        } else if (null == value) {
            return null;
        } else if (value instanceof Blob){
            try {
                return convert(((Blob)value).getBytes(1, (int)((Blob)value).length()));
            } catch (Exception e) {
                throw new AxionException(e);
            }
        } else if (value instanceof Clob){
            try {
                return convert(((Clob)value).getSubString(1, (int)((Clob)value).length()));
            } catch (Exception e) {
                throw new AxionException(e);
            }
        } else {
            return String.valueOf(value).getBytes();
        }
    }

    public byte[] toByteArray(Object value) throws AxionException {
        return (byte[]) convert(value);
    }

    /**
     * Returns {@link java.sql.Types#VARBINARY}.
     */
    public int getJdbcType() {
        return Types.VARBINARY;
    }

    public DataType makeNewInstance() {
        return new VarBinaryType();
    }

    /**
     * Instantiate an object of my type from the given {@link DataInput}. The next
     * sequence of bytes to be read from the <code>DataInput</code> will have been
     * written by {@link #write}.
     * 
     * @param in DataInput from which to read data 
     * @throws IOException if error occurs during read
     */
    public Object read(DataInput in) throws IOException {
        int length = in.readInt();
        if (-1 == length) {
            return null;
        } else if (length > _length) {
            throw new IOException("Record length exceeds length for this binary type instance.");
        }
        
        byte[] data = new byte[length];
        in.readFully(data);
        return data;
    }

    /**
     * Write an object of my type to the given {@link DataOutput}.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}to this
     *        <code>DataType</code>
     * @param out DataOutput to receive data
     * @throws IOException if error occurs while writing value, or if <code>value</code> is invalid 
     * for this type
     */
    public void write(Object value, DataOutput out) throws IOException {
        try {
            byte[] val = (byte[]) convert(value);
            if (null == val) {
                out.writeInt(-1);
            } else {
                out.writeInt(val.length);
                out.write(val);
            }
        } catch (AxionException e) {
            throw new IOException(e.getMessage());
        }
    }

    public String toString(Object value) {
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        return null == value ? null : value.toString();
    }

    public Comparator getComparator() {
        return this;
    }

    public int compare(Object a, Object b) throws ClassCastException {
        return compare((byte[]) a, (byte[]) b);
    }

    public void setPrecision(int newSize) {
        _length = newSize;
    }

    private int compare(byte[] left, byte[] right) {
        for (int i = 0; i < left.length; i++) {
            if (i >= right.length) {
                return 1;
            } else if (left[i] < right[i]) {
                return -1;
            } else if (left[i] > right[i]) {
                return 1;
            }
        }
        return left.length < right.length ? -1 : 0;
    }

    private int _length = 0;
    private static final long serialVersionUID = -7647413688800437403L;
}
