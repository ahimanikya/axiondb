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
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing a single char value.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class CharacterType extends BaseDataType implements DataType.NonFixedPrecision {

    public CharacterType() {
        this(1);
    }

    public CharacterType(int size) {
        _size = size;
    }

    /** @return {@link Types#CHAR} */
    public int getJdbcType() {
        return Types.CHAR;
    }

    public String getPreferredValueClassName() {
        return "java.lang.String";
    }
    
    public String getTypeName() {
        return "CHAR";
    }

    public final int getPrecision() {
        return _size;
    }

    public int getColumnDisplaySize() {
        return getPrecision();
    }

    public boolean isCaseSensitive() {
        return true;
    }

    /**
     * Returns <code>"character"</code>
     * 
     * @return <code>"character"</code>
     */
    public String toString() {
        return "character(" + _size + ")";
    }

    public byte[] toByteArray(Object value) throws AxionException {
        return toString(value).getBytes();
    }

    /**
     * Returns <code>true</code> iff <i>value </i> is <code>null</code>, a
     * <tt>Character</tt>, or a single character <tt>String</tt>.
     */
    public boolean accepts(Object value) {
        return true;
    }

    public boolean supportsSuccessor() {
        return true;
    }

    public Object successor(Object value) throws IllegalArgumentException {
        return ((String) value) + "\0";
    }

    /**
     * Returns an <tt>String</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        if (null == value) {
            return null;
        } if (value instanceof String) { 
            return process((String)value);
        } if (value instanceof char[]) { 
            return process(new String((char[]) value));
        } if (value instanceof byte[]) { 
            return process(new String((byte[]) value));
        } if(value instanceof Clob) {
            return process(new CLOBType().toString(value));
        } if(value instanceof Blob) {
            try {
                return process(new String(((Blob)value).getBytes(1, (int)((Blob)value).length())));
            } catch (Exception e) {
                throw new AxionException(e);
            }
        } else {
            return process(value.toString());
        }
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        String val = in.readUTF();
        if ("null".equals(val)) {
            if (!in.readBoolean()) {
                return null;
            }
        }
        return val;
    }

    /**
     * Writes the given <i>value </i> to the given {@link DataOutput}.<code>Null</code>
     * values are written as <code>Character.MIN_VALUE</code>,<code>false</code>.
     * <code>Character.MIN_VALUE</code> values are written as
     * <code>Character.MIN_VALUE</code>,<code>true</code>. All other values are
     * written directly.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}
     */
    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeUTF("null");
            out.writeBoolean(false);
        } else if ("null".equals(value)) {
            out.writeUTF("null");
            out.writeBoolean(true);
        } else {
            out.writeUTF(value.toString());
        }
    }

    public DataType makeNewInstance() {
        return new CharacterType();
    }

    public int compare(Object a, Object b) {
        return ((String) a).compareTo((String) b);
    }

    public void setPrecision(int newSize) {
        _size = newSize;
    }

    public String getLiteralPrefix() {
        return "'";
    }

    public String getLiteralSuffix() {
        return "'";
    }

    public short getSearchableCode() {
        return DatabaseMetaData.typeSearchable;
    }

    protected Comparator getComparator() {
        return this;
    }

    protected String process(String value) throws AxionException {
        return padOutput(truncateIfLegal(value));
    }

    protected final String truncateIfLegal(String source) throws AxionException {
        // A value longer than _size can be truncated if the excess characters are all
        // whitespace; otherwise, SQLSTATE 22001 must be raised.
        if (source.length() <= _size) {
            return source;
        } else {
            String trimmedSource = rightTrim(source);
            if (trimmedSource.length() <= _size) {
                return trimmedSource;
            }
            throw new AxionException(22001);
        }
    }
    
    protected final String rightTrim(String source) {
        // Per ANSI/ISO 2003 spec, do not trim whitespace from values whose lengths are less
        // than the max size for the datatype.
        if (source.length() <= _size) {
            return source;
        }
        
        int trimSourceLen = source.length();
        int i, len = trimSourceLen - 1;
        char trimChar = ' ';
        
        // ignore white space and all ASCII control characters as well
        for (i=len; i > 0 && source.charAt(i) <= trimChar; i--);
        return (i == len) ? source : source.substring(0, i + 1);
    }    

    private final String padOutput(String source) {
        if (source.length() == _size) {
            return source;
        }
        return source.concat(getOrCreatePadTemplate(_size - source.length()));
    }

    private final String getOrCreatePadTemplate(int size) {
        String padTemplate = (String) PAD_MAP.get(Integer.toString(size));
        if (padTemplate == null) {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < _size; i++) {
                buf.append(PAD_CHAR);
            }
            buf.setLength(size);

            padTemplate = buf.toString();
            PAD_MAP.put(Integer.toString(size), padTemplate);
        }
        return padTemplate;
    }

    private int _size = 1;

    private static final long serialVersionUID = 7980560473106205818L;
    private transient static final char PAD_CHAR = ' ';
    private transient static final Map PAD_MAP = new HashMap();
}
