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
import java.util.Calendar;
import java.util.TimeZone;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * Implements a date type which can generate instances of java.sql.Date and other JDBC
 * date-related types.
 *
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 * @version 
 */
public class DateType extends TimestampType {
    private static final Object LOCK_DATE_PARSING = new Object();
    private static final Object LOCK_DATE_FORMATTING = new Object();

    // DateFormat objects are not thread safe. Do not share across threads w/o synch block.
    private static final DateFormat[] DATE_PARSING_FORMATS = new DateFormat[] {
                                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", LOCALE),
                                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", LOCALE),
                                    new SimpleDateFormat("yyyy-MM-dd", LOCALE),
                                    new SimpleDateFormat("MM-dd-yyyy", LOCALE),
                                    new SimpleDateFormat("dd-MM-yyyy", LOCALE),
                                    new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", LOCALE),
                                    new SimpleDateFormat("dd-MMM-yy HH:mm:ss", LOCALE),
                                    new SimpleDateFormat("dd-MMM-yy", LOCALE),
                                    new SimpleDateFormat("dd-MMM-yyyy", LOCALE),
                                    new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS", LOCALE),
                                    new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", LOCALE),
                                    new SimpleDateFormat("MM/dd/yyyy", LOCALE),
                                    new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS", LOCALE),
                                    new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", LOCALE),
                                    new SimpleDateFormat("dd/MM/yyyy", LOCALE),
                                    new SimpleDateFormat("HH:mm:ss", LOCALE),
                                    DateFormat.getTimeInstance(DateFormat.SHORT, LOCALE)
                                };

    private static final DateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private int _precision = 11;
    
    static {
        for (int i = 0; i < DATE_PARSING_FORMATS.length; i++) {
            DATE_PARSING_FORMATS[i].setLenient(false);
        }
    }

    /* Increment to use in computing a successor value. */
    // One day = 1 day x 24 hr/day x 60 min/hr x 60 sec/min x 1000 ms/sec
    static final long INCREMENT_DAY = 1 * 24 * 60 * 60 * 1000;

    static final long serialVersionUID = 7318179645848578342L;

    public static long normalizeToUTCZeroHour(long rawTimeMillis) {
        long remainder = rawTimeMillis % INCREMENT_DAY ;
        return (remainder == 0) ? rawTimeMillis : (rawTimeMillis - remainder);
    }

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

    private Date convertToDate(Object value) throws AxionException {
        Calendar cal = Calendar.getInstance();

        if (null == value) {
            return null;
        } else if (value instanceof Number) {
            cal.setTimeInMillis(((Number) value).longValue());
        } else if (value instanceof Timestamp) {
            cal.setTimeInMillis(((Timestamp)value).getTime());
        } else if (value instanceof java.sql.Date) {
            cal.setTimeInMillis(((java.sql.Date) value).getTime());
        } else if (value instanceof java.sql.Time) {
            cal.setTimeInMillis(((java.sql.Time) value).getTime());
        } else if (value instanceof java.util.Date) {
            cal.setTimeInMillis(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            java.util.Date dVal = null;
            int i = 0;
            while (dVal == null && i < DATE_PARSING_FORMATS.length) {
                synchronized(LOCK_DATE_PARSING){
                    dVal = DATE_PARSING_FORMATS[i].parse((String) value, new ParsePosition(0));
                }
                i++;
            }

            if (dVal == null) {
                throw new AxionException(22007);
            }
            cal.setTimeInMillis(dVal.getTime());
        } else {
            throw new AxionException(22007);
        }

        // Normalize to 0 hour in default time zone.
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    /**
     * Returns a <tt>java.sql.Date</tt> converted from the given <i>value </i>, or
     * throws {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        try {
            if (value instanceof Date) {
                return value;
            }
            return convertToDate(value);

        } catch (AxionException e) {
            throw new AxionException("Can't convert " + value.getClass().getName() + " " + value + ".");
        }
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
        return java.sql.Types.DATE;
    }

    /**
     * @see org.axiondb.DataTypeFactory#makeNewInstance()
     */
    public DataType makeNewInstance() {
        return new DateType();
    }

    /**
     * @see org.axiondb.DataType#successor(java.lang.Object)
     */
    public Object successor(Object value) throws IllegalArgumentException {
        try {
            return new Date(toDate(value).getTime() + INCREMENT_DAY);
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
        Date dval;
        try {
            dval = (Date) convert(value);
        } catch (IllegalArgumentException e) {
            throw new AxionException("Can't convert \"" + value + "\" to BigDecimal.", e);
        }

        return (dval != null) ? BigDecimal.valueOf(dval.getTime()) : null;
    }

    /**
     * @see org.axiondb.DataType#toDate(java.lang.Object)
     */
    public Date toDate(Object value) throws AxionException {
        return convertToDate(value);
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "date";
    }

    /**
     * @see org.axiondb.DataType#toString(java.lang.Object)
     */
    public String toString(Object value) throws AxionException {
        Date dval = convertToDate(value);
        if (null == dval) {
            return null;
        }

        String ret = null;

        synchronized (LOCK_DATE_FORMATTING){
            ret = ISO_DATE_FORMAT.format(dval);
        }
        return ret;
    }

    /**
     * @see org.axiondb.DataType#toTime(java.lang.Object)
     */
    public Time toTime(Object value) throws AxionException {
        return new Time(convertToDate(value).getTime());
    }

    /**
     * @see org.axiondb.DataType#toTimestamp(java.lang.Object)
     */
    public Timestamp toTimestamp(Object value) throws AxionException {
        return new Timestamp(convertToDate(value).getTime());
    }

    /**
     * Overrides parent implementation to read only milliseconds (as a long) from the
     * input stream, ignoring any nanoseconds written by TimestampType.write(). We read
     * TimeType data assuming that they are in the same form as that of TimestampType data
     * in order to preserve backwards compatibility, as java.sql.Time was originally
     * mapped to {@link org.axiondb.types.TimestampType}.
     *
     * @param value Date object (typically a <code>java.sql.Date</code> or other
     *        convertible form) to be unpersisted
     * @param out DataOutput to supply serialized data
     * @throws IOException if error occurs during read
     * @see org.axiondb.DataType#read
     */
    public Object read(DataInput in) throws IOException {
        Date result = null;
        int nanos = in.readInt();
        if (Integer.MIN_VALUE != nanos) {
            long millis = in.readLong();
            result = new Date(millis);
        }
        return result;
    }

    /**
     * Overrides parent implementation to always write time (in milliseconds) as a long,
     * writing a placeholder zero for the nanosecond field usually written by
     * TimestampType.write().
     *
     * @param value Date object (typically a <code>java.sql.Date</code> or other
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
                Date val = (Date) convert(value);
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
            synchronized (LOCK_DATE_PARSING) {
                for (int i = 0; i < DATE_PARSING_FORMATS.length; i++) {
                    DATE_PARSING_FORMATS[i].setTimeZone(tZone);
                }
            }

            synchronized (LOCK_DATE_FORMATTING) {
                ISO_DATE_FORMAT.setTimeZone(tZone);
            }
        }
    }
}
