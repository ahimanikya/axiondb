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

package org.axiondb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;

/**
 * The type of a field (column) that can be stored in a {@link Table}.
 * <p>
 * Responsible for {@link #accepts testing}that a value is assignable to fields of this
 * type, for {@link #convert converting}{@link Object Objects}to this type, and for
 * {@link #read reading}values from and {@link #write writing}values to a stream.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Rob Oxspring
 * @author Chuck Burdick
 */
public interface DataType extends Comparator, Serializable {
    /**
     * Return <code>true</code> if a field of my type can be assigned the given non-
     * <code>null</code> <i>value </i>, <code>false</code> otherwise.
     * 
     * @param value non- <code>null</code> value
     */
    boolean accepts(Object value);

    /**
     * Converts an {@link #accepts acceptable}value to one of the appropriate type.
     * 
     * @throws AxionException
     */
    Object convert(Object value) throws AxionException;

    /**
     * Value returned by {@link ResultSetMetaData#getColumnDisplaySize}for this data
     * type.
     * 
     * @see java.sql.ResultSetMetaData#getColumnDisplaySize
     */
    int getColumnDisplaySize();

    /**
     * Returns the JDBC {@link java.sql.Types type code}most closely matching this type.
     */
    int getJdbcType();

    /**
     * Prefix used to quote a literal to delimit value for this type when in SQL syntax or
     * result display
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     */
    String getLiteralPrefix();

    /**
     * Suffix used to quote a literal to delimit value for this type when in SQL syntax or
     * result display
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     */
    String getLiteralSuffix();

    /**
     * Code indicating that type does not accept, does accept, or does not disclose
     * acceptance of <code>null</code> values
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     */
    int getNullableCode();

    /**
     * Value returned by {@link ResultSetMetaData#getPrecision}for this data type.
     * 
     * @see java.sql.ResultSetMetaData#getPrecision
     */
    int getPrecision();

    /**
     * Indicates radix used to compute maximum number of significant digits for this
     * datatype, as returned by {@link #getPrecision}.
     * 
     * @return radix used to compute value of {@link #getPrecision}, typically 2 or 10.
     */
    int getPrecisionRadix();

    /**
     * Returns the "normal" type returned by {@link #convert}. Returns
     * <tt>java.lang.Object</tt> if unknown.
     * 
     * @see java.sql.ResultSetMetaData#getColumnClassName
     */
    String getPreferredValueClassName();

    /**
     * Value returned by {@link ResultSetMetaData#getScale}for this data type.
     * 
     * @see java.sql.ResultSetMetaData#getScale
     */
    int getScale();

    /**
     * Code indicating how much <code>WHERE ... LIKE</code> support is available across
     * a column of this type
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     */
    short getSearchableCode();

    /**
     * For character and string-related types, indicates whether type acknowledges case
     * when storing and retrieving values
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     * @see java.sql.ResultSetMetaData#isCaseSensitive
     */
    boolean isCaseSensitive();

    /**
     * @see java.sql.ResultSetMetaData#isCurrency
     */
    boolean isCurrency();

    /**
     * For numeric types, indicates whether type stores only non-negative (&gt;= 0) values
     * 
     * @see java.sql.DatabaseMetaData#getTypeInfo
     */
    boolean isUnsigned();

    /**
     * Instantiate an object of my type from the given {@link DataInput}. The next
     * sequence of bytes to be read from the <code>DataInput</code> will have been
     * written by {@link #write}.
     */
    Object read(DataInput in) throws IOException;

    /**
     * Returns the successor for the given value. For example, the successor of the
     * integer 1 is 2.
     */
    Object successor(Object value) throws UnsupportedOperationException;

    /**
     * Returns <code>true</code> if the {@link #successor}method is supported, false
     * otherwise.
     */
    boolean supportsSuccessor();

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a
     * <code>BigDecimal</code>, or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getBigDecimal
     */
    BigDecimal toBigDecimal(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a
     * <code>BigInteger</code>, or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getBigInteger
     */
    BigInteger toBigInteger(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link Blob}, or
     * throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getBlob
     */
    Blob toBlob(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>boolean</code>,
     * or throw a {@link SQLException}.
     * 
     * @see java.sql.ResultSet#getBoolean
     */
    boolean toBoolean(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>byte</code>,
     * or throw a {@link SQLException}.
     * 
     * @see java.sql.ResultSet#getByte
     */
    byte toByte(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>byte[]</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getBytes
     */
    byte[] toByteArray(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link Clob}, or
     * throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getClob
     */
    Clob toClob(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link java.sql.Date},
     * or throw a {@link SQLException}.
     * 
     * @see java.sql.ResultSet#getDate
     */
    Date toDate(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>double</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getDouble
     */
    double toDouble(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>float</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getFloat
     */
    float toFloat(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>int</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getInt
     */
    int toInt(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>long</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getLong
     */
    long toLong(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a <code>short</code>,
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getShort
     */
    short toShort(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link String}, or
     * throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getString
     */
    String toString(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link Time}, or
     * throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getTime
     */
    Time toTime(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link Timestamp},
     * or throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getTimestamp
     */
    Timestamp toTimestamp(Object value) throws AxionException;

    /**
     * Convert the given non- <code>null</code> <i>value </i> to a {@link URL}, or
     * throw a {@link AxionException}.
     * 
     * @see java.sql.ResultSet#getURL
     */
    URL toURL(Object value) throws AxionException;

    /**
     * Write an object of my type to the given {@link DataOutput}.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}to this
     *        <code>DataType</code>
     */
    void write(Object value, DataOutput out) throws IOException;

    /**
     * Creates a new instance of this DataType implementation.
     * 
     * @return new instance of this DataType implementation.
     */
    DataType makeNewInstance();

    /**
     * Extension of DataType to indicate that the precision of the implementing class is
     * not fixed by the implementation, but rather can be declared by the user.
     * 
     * @author Jonathan Giron
     */
    public static interface NonFixedPrecision extends DataType {
        /**
         * Overrides the default precision with the given value.
         * 
         * @param newSize new precision value. The appropriate value depends on the
         *        precision radix, which is fixed for each implementing type.
         */
        void setPrecision(int newSize);
    }

    /**
     * Extension of NonFixedPrecision to indicate that the scale of the implementing class
     * is not fixed by the implementation, but rather can be declared by the user.
     * 
     * @author Jonathan Giron
     */
    public static interface ExactNumeric extends NonFixedPrecision {
        /**
         * Overrides the default scale with the given value.
         * 
         * @param newScale new scale value. The appropriate value depends on the current
         *        precision and radix - precision can be modified by the user, but the
         *        radix is fixed for each implementing type.
         */
        void setScale(int newScale);

        /**
         * Creates a new instance of the implementing ExactNumeric type with the given
         * precision and scale.
         * 
         * @param newPrecision precision of the new instance
         * @param newScale scale of the new instance
         * @return new instance of ExactNumeric implementation with the given precision
         *         and scale
         */
        ExactNumeric makeNewInstance(int newPrecision, int newScale);
    }

    /**
     * Extension of DataType to indicate that the implementing class stores binary values,
     * and as such returns precision as the number of bits used to represent a given
     * value.
     * 
     * @author Jonathan Giron
     * @version 
     */
    public interface BinaryRepresentation extends DataType {
        /**
         * Returns the approximate number of decimal digits that would be used to
         * represent
         * 
         * @return
         */
        int getApproximateDecimalPrecision();
    }
}
