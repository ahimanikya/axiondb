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
 * Syntax: DateAdd(interval_type, interval, timestamp)
 * 
 * @version  
 * @author Rupesh Ramachandran
 * @author Ritesh Adval
 */
public class DateAddFunction extends BaseFunction implements ScalarFunction, FunctionFactory {
    /** Creates a new instance of Class */
    public DateAddFunction() {
        super("DATEADD");
    }

    public ConcreteFunction makeNewInstance() {
        return new DateAddFunction();
    }

    /** {@link DataType} */
    public DataType getDataType() {
        return RETURN_TYPE;
    }

    /**
     * Returns new Timestamp which is (timestamp + interval) where interval is integer
     * units of interval_type. Valid interval types are {day, month, year, second,
     * millisecond, minute, hour, week, quarter}
     */
    public Object evaluate(RowDecorator row) throws AxionException {
        Timestamp timestamp = null;
        int interval = -1;

        // Get 'interval_type'
        int intervalType = DateTimeUtils.labelToCode((String) (STRING_TYPE.convert(getArgument(0).evaluate(row))));

        // Get 'interval'
        Object val = getArgument(1).evaluate(row);
        if (null == val) {
            return null;
        } else {
            interval = ((Integer) INT_TYPE.convert(val)).intValue();
        }

        // Get 'timestamp'
        Object val2 = getArgument(2).evaluate(row);
        if (val2 == null) {
            return null;
        } else {
            timestamp = (Timestamp) RETURN_TYPE.convert(val2);
        }

        return calculateDateAdd(intervalType, interval, timestamp);
    }

    /**
     * Calculates the addition of an interval of a given type to given date.
     * 
     * @param intervalType type of interval value ex- DAY, MONTH etc
     * @param interval value of the interval
     * @param t timestamp to which interval value needs to be added
     * @return the resultant timestamp after adding given interval of given type
     */
    private Timestamp calculateDateAdd(int intervalType, int interval, Timestamp t) {
        Calendar c = Calendar.getInstance(TimestampType.getTimeZone());
        c.setTimeInMillis(t.getTime());

        switch (intervalType) {
            default:
            case DateTimeUtils.MILLISECOND:
                return addMillisecondsTo(c, interval);
            case DateTimeUtils.SECOND:
                return addSecondsTo(c, interval);
            case DateTimeUtils.MINUTE:
                return addMinutesTo(c, interval);
            case DateTimeUtils.HOUR:
                return addHoursTo(c, interval);
            case DateTimeUtils.DAY:
                return addDaysTo(c, interval);
            case DateTimeUtils.WEEK:
                return addWeeksTo(c, interval);
            case DateTimeUtils.MONTH:
                return addMonthsTo(c, interval);
            case DateTimeUtils.QUARTER:
                return addQuartersTo(c, interval);
            case DateTimeUtils.YEAR:
                return addYearsTo(c, interval);
        }
    }

    /**
     * Adds millisecond to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param milliseconds milliseconds to add
     * @return date after adding milliseconds
     */
    private Timestamp addMillisecondsTo(Calendar c, int milliseconds) {
        c.add(Calendar.MILLISECOND, milliseconds);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds seconds to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param seconds seconds to add
     * @return date after adding seconds
     */
    private Timestamp addSecondsTo(Calendar c, int seconds) {
        c.add(Calendar.SECOND, seconds);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds minutes to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param minutes minutes to add
     * @return date after adding minutes
     */
    private Timestamp addMinutesTo(Calendar c, int minutes) {
        c.add(Calendar.MINUTE, minutes);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds hours to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param hours hours to add
     * @return date after adding hours
     */
    private Timestamp addHoursTo(Calendar c, int hours) {
        c.add(Calendar.HOUR, hours);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds days to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param days days to add
     * @return date after adding days
     */
    private Timestamp addDaysTo(Calendar c, int days) {
        c.add(Calendar.DAY_OF_YEAR, days);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds weeks to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param weeks weeks to add
     * @return date after adding weeks
     */
    private Timestamp addWeeksTo(Calendar c, int weeks) {
        c.add(Calendar.WEEK_OF_YEAR, weeks);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds months to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param months months to add
     * @return date after adding months
     */
    private Timestamp addMonthsTo(Calendar c, int months) {
        c.add(Calendar.MONTH, months);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * Adds quarters to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param quarters quarters to add
     * @return date after adding quarters
     */
    private Timestamp addQuartersTo(Calendar c, int quarters) {
        return addMonthsTo(c, quarters * 3);
    }

    /**
     * Adds years to a date represented by Calendar object.
     * 
     * @param c Calendar object representing a date
     * @param years years to add
     * @return date after adding years
     */
    private Timestamp addYearsTo(Calendar c, int years) {
        c.add(Calendar.YEAR, years);
        return new Timestamp(c.getTimeInMillis());
    }

    /**
     * @see org.axiondb.functions.BaseFunction#isValid
     */
    public boolean isValid() {
        return getArgumentCount() == 3;
    }

    private static final DataType RETURN_TYPE = new TimestampType();
    private static final DataType STRING_TYPE = new StringType();
    private static final DataType INT_TYPE = new IntegerType();
}
