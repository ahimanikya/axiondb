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
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.axiondb.AxionException;
import org.axiondb.DataType;

/**
 * A {@link DataType}representing a timestamp value.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Ritesh Adval
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public class TimestampType extends BaseDataType implements Comparator {
	// Irrespective of the JVM's Locale lets pick a Locale for use on any JVM
    public static final Locale LOCALE = Locale.UK;
    private static TimeZone TIMEZONE = TimeZone.getDefault();
    
    private static final Object LOCK_TIMESTAMP_PARSING = new Object();   
    private static final Object LOCK_TIMESTAMP_FORMATTING = new Object();    
    
    // DateFormat objects are not thread safe. Do not share across threads w/o synch block.
    private static final DateFormat[] TIMESTAMP_PARSING_FORMATS = new DateFormat[] { 
					    	    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", LOCALE),
					            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", LOCALE), 
					            new SimpleDateFormat("yyyy-MM-dd", LOCALE),
                                                    new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS", LOCALE), 
					            new SimpleDateFormat("MM-dd-yyyy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("MM-dd-yyyy", LOCALE), 
					            new SimpleDateFormat("HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("dd-MM-yyyy", LOCALE),
                                                    new SimpleDateFormat("dd-MMM-yy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS", LOCALE),
                                                    new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("MM/dd/yyyy", LOCALE),
                                                    new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS", LOCALE),
                                                    new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", LOCALE),
                                                    new SimpleDateFormat("dd/MM/yyyy", LOCALE),
					            DateFormat.getTimeInstance(DateFormat.SHORT, LOCALE)
					            };

    private int _precision = 10;
    private static final SimpleDateFormat ISO_TIMESTAMP_FORMATTING_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", LOCALE);
    
    static {
        for (int i = 0; i < TIMESTAMP_PARSING_FORMATS.length; i++) {
            TIMESTAMP_PARSING_FORMATS[i].setLenient(false);
        }
    }

    public TimestampType() {
    }

    public int getJdbcType() {
        return java.sql.Types.TIMESTAMP;
    }

    @Override
    public String getPreferredValueClassName() {
        return "java.lang.Long";
    }

    public String getTypeName() {
        return "TIMESTAMP";
    }
    
    @Override
    public String toString() {
        return "timestamp";
    }
    int columnDisplaySize = 29;

    @Override
    public int getColumnDisplaySize() {
        return columnDisplaySize;
    }
    
    public void setColumnDisplaySize(int colDispSize) {
      	columnDisplaySize = colDispSize;
     }

    @Override
    public int getPrecision() {
        return _precision;
    }
    
    public void  setPrecision(int newPrec) {
         _precision = newPrec;
    }

    /**
     * Returns <code>true</code> iff <i>value </i> is <code>null</code>, a
     * <tt>Number</tt>, or a <tt>String</tt> that can be converted to a <tt>Long</tt>.
     */
    public boolean accepts(Object value) {
        if (null == value) {
            return true;
        } else if (value instanceof Number) {
            return true;
        } else if (value instanceof java.util.Date) {
            return true;
        } else if (value instanceof String) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns an <tt>Byte</tt> converted from the given <i>value </i>, or throws
     * {@link IllegalArgumentException}if the given <i>value </i> isn't
     * {@link #accepts acceptable}.
     */
    public Object convert(Object value) throws AxionException {
        if (null == value) {
            return null;
        } else if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        } else if (value instanceof Timestamp) {
            return value;
        } else if (value instanceof java.sql.Date) {
            return new Timestamp(((java.sql.Date) value).getTime());
        } else if (value instanceof java.sql.Time) {
            return new Timestamp(((java.sql.Time) value).getTime());
        } else if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            java.util.Date dVal = null;
            int i = 0;
            while (dVal == null && i < TIMESTAMP_PARSING_FORMATS.length) {
            	synchronized(LOCK_TIMESTAMP_PARSING){
            		dVal = TIMESTAMP_PARSING_FORMATS[i].parse((String) value, new ParsePosition(0));
            	}
                i++;
            }

            if (dVal == null) {
                throw new AxionException(22007);
            }
            
            setColumnDisplaySize(((String)value).length());
            setPrecision(((String)value).length());
            return new Timestamp(dVal.getTime());
        } else {
            throw new AxionException(22007);
        }
    }

    @Override
    public BigDecimal toBigDecimal(Object value) throws AxionException {
        Timestamp tval;
        long time_ms, time_ns;
        Long time;
        try {
            tval = (Timestamp) convert(value);
        } catch (AxionException e) {
            throw new AxionException("Can't convert \"" + value + "\" to BigDecimal.", e);
        }
        // returns millisec since Jan 1 1970 00:00:00 GMT (integral sec only)
        time_ms = tval.getTime();
        // convert nanosec to millisec and add to time_ms (fractional sec)
        time_ns = tval.getNanos() / 1000000;
        time = new Long(time_ms + time_ns);
        return super.toBigDecimal(time);
    }

    private Timestamp convertToTimestamp(Object value) throws AxionException {
        try {
            Timestamp tval = (Timestamp) convert(value);
            return tval;
        } catch (IllegalArgumentException e) {
            throw new AxionException("Can't convert \"" + value + "\" to Timestamp.", e);
        }
    }

    @Override
    public java.sql.Date toDate(Object value) throws AxionException {
        return new java.sql.Date(convertToTimestamp(value).getTime());
    }

    @Override
    public String toString(Object value) throws AxionException {
        Timestamp val = convertToTimestamp(value);
        if (val == null) {
            return null;
        }
        String ret  = null;
        synchronized (LOCK_TIMESTAMP_FORMATTING){
        	ret = ISO_TIMESTAMP_FORMATTING_FORMAT.format(val);
        }
        return ret;
    }

    @Override
    public java.sql.Timestamp toTimestamp(Object value) throws AxionException {
        return convertToTimestamp(value);
    }

    @Override
    public java.sql.Time toTime(Object value) throws AxionException {
        return new java.sql.Time(convertToTimestamp(value).getTime());
    }

    @Override
    public boolean supportsSuccessor() {
        return true;
    }

    @Override
    public Object successor(Object value) throws IllegalArgumentException {
        Timestamp v = (Timestamp) value;
        Timestamp result = new Timestamp(v.getTime());
        if (v.getNanos() == 999999999) {
            result.setTime(result.getTime() + 1);
            result.setNanos(0);
        } else {
            result.setNanos(v.getNanos() + 1);
        }
        return result;
    }

    /**
     * @see #write
     */
    public Object read(DataInput in) throws IOException {
        Timestamp result = null;
        int nanos = in.readInt();
        if (Integer.MIN_VALUE != nanos) {
            long time = in.readLong();
            result = new Timestamp(time);
            result.setNanos(nanos);
        }
        return result;
    }

    /**
     * Writes the given <i>value </i> to the given <code>DataOutput</code>.
     * <code>Null</code> values are written as <code>Integer.MIN_VALUE</code>. All
     * other values are written directly with an <code>int</code> representing
     * nanoseconds first, and a <code>long</code> representing the time.
     * 
     * @param value the value to write, which must be {@link #accepts acceptable}
     */
    public void write(Object value, DataOutput out) throws IOException {
        if (null == value) {
            out.writeInt(Integer.MIN_VALUE);
        } else {
            try {
                Timestamp val = (Timestamp) convert(value);
                out.writeInt(val.getNanos());
                out.writeLong(val.getTime());
            } catch (AxionException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    public DataType makeNewInstance() {
        return new TimestampType();
    }

    @Override
    public int compare(Object left, Object right) {
        long leftmillis = getMillis(left);
        int leftnanos = getNanos(left);

        long rightmillis = getMillis(right);
        int rightnanos = getNanos(right);

        if (leftmillis == rightmillis) {
            if (leftnanos > rightnanos) {
                return 1;
            } else if (leftnanos == rightnanos) {
                return 0;
            } else {
                return -1;
            }
        } else if (leftmillis > rightmillis) {
            return 1;
        } else {
            return -1;
        }
    }

    private long getMillis(Object obj) {
        return ((Date) obj).getTime();
    }

    private int getNanos(Object obj) {
        if (obj instanceof Timestamp) {
            return ((Timestamp) obj).getNanos();
        }
        return 0;
    }

    @Override
    protected Comparator getComparator() {
        return this;
    }

    public static void setTimeZone(String id) {
        TimeZone tZone = TimeZone.getTimeZone(id);
        if (tZone != null) {
            synchronized (LOCK_TIMESTAMP_PARSING) {
                TIMEZONE = tZone;
                for (int i = 0; i < TIMESTAMP_PARSING_FORMATS.length; i++) {
                    TIMESTAMP_PARSING_FORMATS[i].setTimeZone(TIMEZONE);
                }
            }
            
            synchronized (LOCK_TIMESTAMP_FORMATTING) {
            	ISO_TIMESTAMP_FORMATTING_FORMAT.setTimeZone(TIMEZONE);
            }
        }
    }

    public static TimeZone getTimeZone() {
    	return TIMEZONE;
    }

    private static final long serialVersionUID = -4933113827044335868L;
}
