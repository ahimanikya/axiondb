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

package org.axiondb.functions;

import java.sql.Timestamp;
import java.util.Calendar;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.types.IntegerType;
import org.axiondb.types.StringType;
import org.axiondb.types.TimestampType;
import org.axiondb.util.DateTimeUtils;

/**
 * Syntax: DATEDIFF(interval_type, timestamp1, timestamp2)
 * 
 * @version  
 * @author Rupesh Ramachandran
 * @author Ritesh Adval
 */
public class DateDiffFunction extends BaseFunction implements ScalarFunction, FunctionFactory {

    /** Creates a new instance of Class */
    public DateDiffFunction() {
        super("DATEDIFF");
    }

    public ConcreteFunction makeNewInstance() {
        return new DateDiffFunction();
    }

    /** {@link DataType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    /**
     * Returns integer difference (timestamp2 - timestamp1) in units of interval of type
     * interval_type. Valid interval types are {day, month, year, second, millisecond,
     * minute, hour, week, quarter}
     */
    public Object evaluate(RowDecorator row) throws AxionException {
        // Get 'interval_type'
        int intervalType = DateTimeUtils.labelToCode((String) (STRING_TYPE.convert(getArgument(0).evaluate(row))));

        // Get 'timestamp1'
        Timestamp timestamp1 = null;

        Object val1 = getArgument(1).evaluate(row);
        timestamp1 = (Timestamp) TIMESTAMP_TYPE.convert(val1);

        // Get 'timestamp2'
        Timestamp timestamp2 = null;
        Object val2 = getArgument(2).evaluate(row);
        timestamp2 = (Timestamp) TIMESTAMP_TYPE.convert(val2);

        // Return null if either or both parameters are null.
        if (timestamp1 == null || timestamp2 == null) {
            return null;
        }

        // Calculate the difference between dates in unit specfied by interval type
        return new Long(calculateDateDiff(intervalType, timestamp1, timestamp2));
    }

    /*
     * Calculate date difference between two timestamp values timestamp t2 is subtract
     * from timestamp t1 @param intervalType interval type such as DAY, MONTH etc @param
     * t1 first time stamp @param t2 second time stamp @return the difference (t2-t1) of
     * in unit of intervalType specified
     */
    long calculateDateDiff(int intervalType, Timestamp t1, Timestamp t2) throws AxionException {
        Calendar c1 = null;
        Calendar c2 = null;

        if (intervalType == DateTimeUtils.DAY || intervalType == DateTimeUtils.WEEK || intervalType == DateTimeUtils.MONTH
            || intervalType == DateTimeUtils.QUARTER || intervalType == DateTimeUtils.YEAR) {

            c1 = Calendar.getInstance(TimestampType.getTimeZone());
            c2 = Calendar.getInstance(TimestampType.getTimeZone());

            //set start date
            c1.setTimeInMillis(t1.getTime());
            //set end date
            c2.setTimeInMillis(t2.getTime());
        }

        switch (intervalType) {
            default:
            case DateTimeUtils.MILLISECOND:
                return getMilisecondsBetween(t2, t1);
            case DateTimeUtils.SECOND:
                return getSecondsBetween(t2, t1);
            case DateTimeUtils.MINUTE:
                return getMinutesBetween(t2, t1);
            case DateTimeUtils.HOUR:
                return getHoursBetween(t2, t1);
            case DateTimeUtils.DAY:
                return getDaysBetween(t2, t1);
            case DateTimeUtils.WEEK:
                return getWeeksBetween(c2, c1);
            case DateTimeUtils.MONTH:
                return getMonthsBetween(c2, c1);
            case DateTimeUtils.QUARTER:
                return getQuartersBetween(c2, c1);
            case DateTimeUtils.YEAR:
                return getYearsBetween(c2, c1);
        }
    }

    /**
     * Calculates the number of miliseconds between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of miliseconds between the two dates. Zero is returned if the
     *         dates are the same, etc. The order of the dates does matter. If Calendar
     *         types of d1 and d2 are different, the result may not be accurate.
     */
    private long getYearsBetween(Calendar d1, Calendar d2) {
        boolean negativeResult = false;
        if (d1.after(d2)) { // swap dates so that d1 is start and d2 is end
            java.util.Calendar swap = d1;
            d1 = d2;
            d2 = swap;
        } else {
            negativeResult = true;
        }

        long years = d2.get(Calendar.YEAR) - d1.get(Calendar.YEAR);

        if (negativeResult) {
            years *= -1;
        }

        return years;
    }

    /**
     * Calculates the number of quarters between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of quarters between the two dates. Zero is returned if the dates
     *         are the same, etc. The order of the dates does matter. If Calendar types of
     *         d1 and d2 are different, the result may not be accurate.
     */
    private long getQuartersBetween(Calendar d1, Calendar d2) {
        long quarters = getMonthsBetween(d1, d2) / 3;
        return quarters;
    }

    /**
     * Calculates the number of months between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of months between the two dates. Zero is returned if the dates
     *         are the same, one if the months are adjacent, etc. The order of the dates
     *         does matter. If Calendar types of d1 and d2 are different, the result may
     *         not be accurate.
     */
    private long getMonthsBetween(Calendar d1, Calendar d2) {
        boolean negativeResult = false;
        if (d1.after(d2)) { // swap dates so that d1 is start and d2 is end
            java.util.Calendar swap = d1;
            d1 = d2;
            d2 = swap;
        } else {
            negativeResult = true;
        }

        long months = d2.get(Calendar.MONTH) - d1.get(Calendar.MONTH);
        int y2 = d2.get(Calendar.YEAR);
        if (d1.get(Calendar.YEAR) != y2) {
            d1 = (Calendar) d1.clone();
            do {
                months += 12;
                d1.add(Calendar.YEAR, 1);
            } while (d1.get(Calendar.YEAR) != y2);
        }

        if (negativeResult) {
            months *= -1;
        }
        return months;
    }

    /**
     * Calculates the number of weeks between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of weeks between the two dates. Zero is returned if the dates
     *         are the same, etc. The order of the dates does matter. If Calendar types of
     *         d1 and d2 are different, the result may not be accurate.
     */
    private long getWeeksBetween(java.util.Calendar d1, java.util.Calendar d2) {
        boolean negativeResult = false;

        if (d1.after(d2)) { // swap dates so that d1 is start and d2 is end
            java.util.Calendar swap = d1;
            d1 = d2;
            d2 = swap;
        } else {
            negativeResult = true;
        }

        long days = d2.get(Calendar.WEEK_OF_YEAR) - d1.get(Calendar.WEEK_OF_YEAR);
        int y2 = d2.get(Calendar.YEAR);
        if (d1.get(Calendar.YEAR) != y2) {
            d1 = (Calendar) d1.clone();
            do {
                days += d1.getActualMaximum(Calendar.WEEK_OF_YEAR);
                d1.add(Calendar.YEAR, 1);
            } while (d1.get(Calendar.YEAR) != y2);
        }
        if (negativeResult) {
            days *= -1;
        }

        return days;
    }

    /**
     * Calculates the number of days between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of days between the two dates. Zero is returned if the dates are
     *         the same, one if the dates are adjacent, etc. The order of the dates does
     *         matter. If Calendar types of d1 and d2 are different, the result may not be
     *         accurate.
     */
    private long getDaysBetween(Timestamp t1, Timestamp t2) {
        long days = (t1.getTime() - t2.getTime()) / DAYS_IN_MILLISECOND;
        return days;
    }

    /**
     * Calculates the number of hours between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of hours between the two dates. Zero is returned if the dates
     *         are the same, etc. The order of the dates does matter. If Calendar types of
     *         d1 and d2 are different, the result may not be accurate.
     */
    private long getHoursBetween(Timestamp t1, Timestamp t2) {
        long hours = (t1.getTime() - t2.getTime()) / HOURS_IN_MILLISECOND;
        return hours;
    }

    /**
     * Calculates the number of minutes between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of minutes between the two dates. Zero is returned if the dates
     *         are the same, etc. The order of the dates does matter. If Calendar types of
     *         d1 and d2 are different, the result may not be accurate.
     */
    private long getMinutesBetween(Timestamp t1, Timestamp t2) {
        long seconds = (t1.getTime() - t2.getTime()) / MINUTES_IN_MILLISECOND;
        return seconds;
    }

    /**
     * Calculates the number of seconds between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of seconds between the two dates. Zero is returned if the dates
     *         are the same, etc. The order of the dates does not matter. If Calendar
     *         types of d1 and d2 are different, the result may not be accurate.
     */
    private long getSecondsBetween(Timestamp t1, Timestamp t2) {
        long seconds = (t1.getTime() - t2.getTime()) / SECONDS_IN_MILLISECOND;
        return seconds;
    }

    /**
     * Calculates the number of miliseconds between two calendar days in a manner which is
     * independent of the Calendar type used. i.e. (d2-d1)
     * 
     * @param d1 The first date.
     * @param d2 The second date.
     * @return The number of miliseconds between the two dates. Zero is returned if the
     *         dates are the same, etc. The order of the dates does matter. If Calendar
     *         types of d1 and d2 are different, the result may not be accurate.
     */
    private long getMilisecondsBetween(Timestamp t1, Timestamp t2) {
        long milliseconds = t1.getTime() - t2.getTime();
        return milliseconds;
    }

    public boolean isValid() {
        return getArgumentCount() == 3;
    }

    private static final DataType RETURN_TYPE = new IntegerType();
    private static final DataType STRING_TYPE = new StringType();
    private static final DataType TIMESTAMP_TYPE = new TimestampType();

    private static final int SECONDS_IN_MILLISECOND = 1000;
    private static final int MINUTES_IN_MILLISECOND = 60 * SECONDS_IN_MILLISECOND;
    private static final int HOURS_IN_MILLISECOND = 60 * MINUTES_IN_MILLISECOND;
    private static final int DAYS_IN_MILLISECOND = 24 * HOURS_IN_MILLISECOND;

}
