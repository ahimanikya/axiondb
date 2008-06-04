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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * Generic implemention of {@link DataType}, for use by {@link org.axiondb.BindVariable}.
 *
 * @version  
 * @author Rodney Waldhoff
 */
public class AnyType extends BaseDataType {
    public DataType makeNewInstance() {
        return new AnyType();
    }
    
    public boolean accepts(Object value) {
        return true;
    }
    
    public Object convert(Object value) {
        return value;
    }
    
    public Object read(DataInput in) throws IOException {
        throw new IOException("This type is not meant to be saved.");
    }
    
    public void write(Object value, DataOutput out) throws IOException {
        throw new IOException("This type is not meant to be saved.");
    }
    
    public int getColumnDisplaySize() {
        return 0;
    }
        
    public int getJdbcType() {
        return java.sql.Types.OTHER;
    }
    
    public String getPreferredValueClassName() {
        return "java.lang.String";
    }

    public boolean supportsSuccessor() {
        return false;
    }    

    public Object successor(Object value) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    protected Number toNumber(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Number) {
            return (Number)value;
        } else {
            return new Integer(toInt(value));
        }
    }

    public boolean toBoolean(Object value) throws AxionException {
        if(value instanceof Boolean) {
            return ((Boolean)value).booleanValue();
        } else if("true".equals(toString(value))) {
            return true;
        } else if("false".equals(toString(value))) {
            return false;
        } else {
            throw new AxionException("Can't convert " + value + " to a boolean value, expected 'true' or 'false'.", 22018);
        }
    }

    public byte toByte(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).byteValue();
        }
        try {
            return Byte.parseByte(String.valueOf(value));
        } catch(NumberFormatException e) {
            throw new AxionException("Can't convert " + value + " to byte.", 22018);
        }
    }

    public byte[] toByteArray(Object value) throws AxionException {
        if(value instanceof byte[]) {
            return ((byte[])value);
        }
        
        try {
            return toString(value).getBytes();
        } catch (RuntimeException e) {
            throw new AxionException("Can't convert " + value + " to byte array.", 22018);            
        }
    }
    
    public double toDouble(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).doubleValue();
        }
        
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch(NumberFormatException e) {
            throw new AxionException("Can't convert " + value + " to double.", 22018);
        }
    }

    public float toFloat(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).floatValue();
        }

        try {
            return Float.parseFloat(String.valueOf(value));
        } catch(NumberFormatException e) {
            throw new AxionException("Can't convert " + value + " to float.", 22018);
        }
    }

    public int toInt(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).intValue();
        }

        try {
            return Integer.parseInt(String.valueOf(value));
        } catch(NumberFormatException e) {
            throw new AxionException("Can't convert " + value + " to int.", 22018);
        }
    }

    public long toLong(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).longValue();
        } else if(value instanceof Date) {
            return ((Date)value).getTime();
        } else if(value instanceof Time) {
            return ((Time)value).getTime();
        } else if(value instanceof Timestamp) {
            return ((Timestamp)value).getTime();
        } else {
            try {
                return Long.parseLong(String.valueOf(value));
            } catch(NumberFormatException e) {
                throw new AxionException("Can't convert " + value + " to long.", 22018);
            }
        }
    }

    public short toShort(Object value) throws AxionException {
        if(value instanceof Number) {
            return ((Number)value).shortValue();
        }
        
        try {
            return Short.parseShort(String.valueOf(value));
        } catch(NumberFormatException e) {
            throw new AxionException("Can't convert " + value + " to short.", 22018);
        }
    }

    public String toString(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof String) {
            return (String)value;
        } else {
            return String.valueOf(value);
        }
    }

    public Date toDate(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Date) {
            return (Date)value;
        } else if(value instanceof Number) {
            return new Date(((Number)value).longValue());
        } else {
            throw new AxionException("Can't convert " + value + " to Time.", 22018); // should be smarter
        }
    }

    public Time toTime(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Time) {
            return (Time)value;
        } else if(value instanceof Number) {
            return new Time(((Number)value).longValue());
        } else {
            throw new AxionException("Can't convert " + value + " to Time.", 22018); // should be smarter
        }
    }

    public Timestamp toTimestamp(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Timestamp) {
            return (Timestamp)value;
        } else if(value instanceof Number) {
            return new Timestamp(((Number)value).longValue());
        } else {
            throw new AxionException("Can't convert " + value + " to Timestamp.", 22018); // should be smarter
        }
    }

    public Clob toClob(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Clob) {
            return (Clob)value;
        } else {
            return new StringClob(toString(value));
        }
    }

    public Blob toBlob(Object value) throws AxionException {
        if(null == value) {
            return null;
        } else if(value instanceof Blob) {
            return (Blob)value;
        } else {
            throw new AxionException("Can't convert " + value + " to Blob.", 22018);
        }
    }
    
    public static final AnyType INSTANCE = new AnyType();
}



