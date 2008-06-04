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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * Implements a date type which can generate instances of java.sql.Date and other JDBC
 * date-related types.
 * 
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 * @version 
 */
public class TimeType extends TimestampType { //implements DataType.NonFixedPrecision {
    /* Increment to use in computing a successor value. */
    static final long INCREMENT_MS = 1;
    static final long serialVersionUID = -1850471442550601883L;
    
    private static final Object LOCK_TIME_PARSING = new Object();   
    private static final Object LOCK_TIME_FORMATTING = new Object();
    
    // DateFormat objects are not thread safe. Do not share across threads w/o synch block.
    private static final DateFormat[] TIME_PARSING_FORMATS = new DateFormat[] { 
					    	    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", LOCALE),
					            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", LOCALE), 
					            new SimpleDateFormat("yyyy-MM-dd", LOCALE),
					            new SimpleDateFormat("MM-dd-yyyy", LOCALE), 
					            new SimpleDateFormat("HH:mm:ss", LOCALE),
					            DateFormat.getTimeInstance(DateFormat.SHORT, LOCALE)
					            };
    private static final DateFormat ISO_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private int _precision = 8;
    
    static {
        for (int i = 0; i < TIME_PARSING_FORMATS.length; i++) {
        	TIME_PARSING_FORMATS[i].setLenient(false);
        }
    }
    
    private static TimeZone TIME_ZONE = TimeZone.getDefault();

    public static TimeZone getTimeZone() {
        return TIME_ZONE;
    }

    public static long normalizeTime(long rawTimeMillis) {
        int dstOffset = (TIME_ZONE.inDaylightTime(new java.util.Date(rawTimeMillis))) ? TIME_ZONE.getDSTSavings() : 0;
        return (rawTimeMillis < DateType.INCREMENT_DAY) ? rawTimeMillis : (rawTimeMillis % DateType.INCREMENT_DAY) + dstOffset;
    }

    /**
     * @see org.axiondb.DataType#accepts(java.lang.Object)
     */
    public boolean accepts(Object value) {
        if (null == value) {
            return true;
        } else if (value instanceof Number) {
            return true;
        } else if (value instanceof String) {
            return true;
        } else if (value instanceof java.util.Date) {
            return true;
        } else if (value instanceof Date) {
            return true;
        } else if (value instanceof Time) {
            return true;
        } else {
            return false;
        }
    }

    private Time getNormalizedTime(long time){
    	Time ret = null;
    	ret = new Time (normalizeTime(time));
    	return ret;
    }
    
    private Time convertToTime(Object value) throws AxionException {
        if (null == value) {
            return null;
        } else if (value instanceof Number) {
            return getNormalizedTime( ((Number) value).longValue());
        } else if (value instanceof Timestamp) {
            return getNormalizedTime( ((Timestamp)value).getTime());
        } else if (value instanceof java.sql.Date) {
            return getNormalizedTime(((java.sql.Date) value).getTime());
        } else if (value instanceof java.sql.Time) {
            return (Time) value;
        } else if (value instanceof java.util.Date) {
            return getNormalizedTime(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            java.util.Date dVal = null;
            int i = 0;
            while (dVal == null && i < TIME_PARSING_FORMATS.length) {
            	synchronized(LOCK_TIME_PARSING){
            		dVal = TIME_PARSING_FORMATS[i].parse((String) value, new ParsePosition(0));
            	}
                i++;
            }

            if (dVal == null) {
                throw new AxionException(22007);
            }
            return getNormalizedTime(dVal.getTime());
        } else {
            throw new AxionException(22007);
        }
    }
    
    /**
     * Returns a <tt>java.sql.Date</tt> converted from the given <i>value </i>, or
     * throws {@link AxionException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        if (value instanceof Time) {
            return value;
        }
        return convertToTime(value);
    }

    /**
     * @see org.axiondb.DataType#getColumnDisplaySize()
     */
    public int getColumnDisplaySize() {
        return getPrecision();
    }

    public int getPrecision() {
        return _precision;
    }
    
    public void  setPrecision(int newPrec) {
        _precision = newPrec;
    }

    /**
     * @see org.axiondb.DataType#getJdbcType()
     */
    public int getJdbcType() {
        return java.sql.Types.TIME;
    }

    /**
     * @see org.axiondb.DataTypeFactory#makeNewInstance()
     */
    public DataType makeNewInstance() {
        return new TimeType();
    }

    /**
     * @see org.axiondb.DataType#successor(java.lang.Object)
     */
    public Object successor(Object value) throws IllegalArgumentException {
        try {
            return new Time(((Time) convert(value)).getTime() + INCREMENT_MS);
        } catch (AxionException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * @see org.axiondb.DataType#supportsSuccessor()
     */
    public boolean supportsSuccessor() {
        return true;
    }

    /**
     * @see org.axiondb.DataType#toBigDecimal(java.lang.Object)
     */
    public BigDecimal toBigDecimal(Object value) throws AxionException {
        Date dval = (Date) convert(value);
        Long time = new Long(dval.getTime());
        
        return super.toBigDecimal(time);
    }

    /**
     * @see org.axiondb.DataType#toDate(java.lang.Object)
     */
    public Date toDate(Object value) throws AxionException {
        throw new AxionException("Can't convert " + toString() + " to Date.");
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "time";
    }

    /**
     * @see org.axiondb.DataType#toString(java.lang.Object)
     */
    public String toString(Object value) throws AxionException {
        Time tval = toTime(value);
        if (null == tval) {
            return null;
        }

        synchronized (LOCK_TIME_FORMATTING) {
            return ISO_TIME_FORMAT.format(tval).toString();
        }
    }

    /**
     * @see org.axiondb.DataType#toTime(java.lang.Object)
     */
    public Time toTime(Object value) throws AxionException {
        return (value instanceof Time) ? (Time) value : convertToTime(value);
    }

    /**
     * @see org.axiondb.DataType#toTimestamp(java.lang.Object)
     */
    public Timestamp toTimestamp(Object value) throws AxionException {
        return new Timestamp(convertToTime(value).getTime());
    }

    /**
     * Overrides parent implementation to read only milliseconds (as a long) from the
     * input stream, ignoring any nanoseconds written by TimestampType.write(). We read
     * TimeType data assuming that they are in the same form as that of TimestampType data
     * in order to preserve backwards compatibility, as java.sql.Time was originally
     * mapped to {@link org.axiondb.types.TimestampType}.
     * 
     * @param value Time object (typically a <code>java.sql.Time</code> or other
     *        convertible form) to be unpersisted
     * @param out DataOutput to supply serialized data
     * @throws IOException if error occurs during read
     * @see org.axiondb.DataType#read
     */
    public Object read(DataInput in) throws IOException {
        Time result = null;
        int nanos = in.readInt();
        if (Integer.MIN_VALUE != nanos) {
            long millis = in.readLong();
            result = new Time(millis);
        }
        return result;
    }

    /**
     * Overrides parent implementation to always write time (in milliseconds) as a long,
     * writing a placeholder zero for the nanosecond field usually written by
     * TimestampType.write(). We write TimeType data in the same form as that of
     * TimestampType data to preserve backwards compatibility, as java.sql.Date was
     * originally mapped to {@link org.axiondb.types.TimestampType}.
     * 
     * @param value time object (typically a <code>java.sql.Time</code> or other
     *        convertible form) to be persisted
     * @param out DataOutput to receive serialized data
     * @throws IOException if error occurs during write
     * @see org.axiondb.DataType#write
     */
    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeInt(Integer.MIN_VALUE);
        } else {
            try {
                Time val = (Time) convert(value);
                out.writeInt(0);
                out.writeLong(val.getTime());
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    
    public static void setTimeZone(String id) {
        TimeZone tZone = TimeZone.getTimeZone(id);
        if (tZone != null) {
            synchronized (LOCK_TIME_PARSING) {
            	TIME_ZONE = tZone;
                for (int i = 0; i < TIME_PARSING_FORMATS.length; i++) {
                	TIME_PARSING_FORMATS[i].setTimeZone(tZone);
                }
            }
            
            synchronized (LOCK_TIME_FORMATTING) {           	
            	ISO_TIME_FORMAT.setTimeZone(tZone);
            }
        }
    }
    
}
