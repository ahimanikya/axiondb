/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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
package org.axiondb.util;

import java.sql.Timestamp;
import java.text.DateFormatSymbols;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.axiondb.AxionException;
import org.axiondb.parser.AxionDateTimeFormatParser;
import org.axiondb.types.TimestampType;

/**
 * Methods to support manipulation and conversion of date, time and timestamp
 * values.
 * 
 * @version  
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */
public final class DateTimeUtils {
    
    /** Constant representing millisecond time interval */
    public static final int MILLISECOND = 1;
    
    /** Constant representing second time interval */
    public static final int SECOND = 2;
    
    /** Constant representing minute time interval */
    public static final int MINUTE = 4;
    
    /** Constant representing hour time interval */
    public static final int HOUR = 8;
    
    /** Constant representing day interval */
    public static final int DAY = 16;
    
    /** Constant representing week interval */
    public static final int WEEK = 32;
    
    /** Constant representing month interval */
    public static final int MONTH = 64;
    
    /** Constant representing quarter interval */
    public static final int QUARTER = 128;
    
    /** Constant representing year interval */
    public static final int YEAR = 256;

    /**
     * Creates a String representation of the given Timestamp object, using
     * the given format string as a template and the current Axion time zone.
     * 
     * @param timestamp Timestamp object to be converted to a String representation
     * @param formatStr template describing the desired format for the String
     * representation of <code>timestamp</code>
     * @return formatted String representation of <code>timestamp</code>
     * @throws AxionException if format is invalid
     * @see #convertToChar(Timestamp, String, java.util.TimeZone)
     */
    public static String convertToChar(Timestamp timestamp, String formatStr) 
            throws AxionException {
        return convertToChar(timestamp, formatStr, TimestampType.getTimeZone(),
                Locale.getDefault());
    }
    
    /**
     * Creates a String representation of the given Timestamp object, using
     * the given format string as a template, the given time zone, and the 
     * current default Locale.
     * 
     * @param timestamp Timestamp object to be converted to a String representation
     * @param formatStr template describing the desired format for the String
     * representation of <code>timestamp</code>
     * @param timezone TimeZone to use in interpreting the value of 
     * <code>timestamp</code> to the desired String representation
     * @return formatted String representation of <code>timestamp</code>
     * @throws AxionException if format is invalid
     * @see #convertToChar(Timestamp, String)
     * @see #convertToChar(Timestamp, String, TimeZone, Locale)
     */
    public static String convertToChar(Timestamp timestamp, String formatStr,
            TimeZone timezone) throws AxionException {
        return convertToChar(timestamp, formatStr, timezone, Locale.getDefault());
    }

    /**
     * Creates a String representation of the given Timestamp object, using
     * the given format string as a template, the given time zone, and the 
     * given Locale.
     * 
     * @param timestamp Timestamp object to be converted to a String representation
     * @param formatStr template describing the desired format for the String
     * representation of <code>timestamp</code>
     * @param timezone TimeZone to use in interpreting the value of
     * <code>timestamp</code> to the desired String representation
     * @param locale Locale to use in resolving date components
     * @return formatted String representation of <code>timestamp</code>
     * @throws AxionException if format is invalid
     * @see #convertToChar(Timestamp, String)
     * @see #convertToChar(Timestamp, String, TimeZone)
     */    
    public static String convertToChar(Timestamp timestamp, String formatStr, 
            TimeZone timezone, Locale locale) throws AxionException {
        try {
            String javaFormat = processFormatString(formatStr);
            
            SimpleDateFormat sdf = new SimpleDateFormat(javaFormat, locale);
            sdf.setCalendar(Calendar.getInstance(timezone));
            sdf.setLenient(false);
    
            return sdf.format(timestamp).toUpperCase();
        } catch (IllegalArgumentException e) {
            throw new AxionException(e.getLocalizedMessage());
        }        
    }

    /**
	 * Creates a String representation of the given Timestamp object, using
	 * the given format string as a template and the current Axion time zone.
	 * 
	 * @param dateStr String representation of Timestamp to be returned
	 * @param formatStr template describing the format of <code>dateStr</code>
	 * @return Timestamp containing date represented by <code>dateStr</code>
	 * @throws AxionException if format is invalid
     * @see #convertToTimestamp(String, String, java.util.TimeZone)
     * @see #convertToTimestamp(String, String, java.util.TimeZone, java.util.Locale)
	 */
	public static Timestamp convertToTimestamp(String dateStr, String formatStr) 
	        throws AxionException {
	    return convertToTimestamp(dateStr, formatStr, TimestampType.getTimeZone(), 
                Locale.getDefault());
	}
    
    /**
     * Creates a String representation of the given Timestamp object, using
     * the given format string as a template and the given time zone.
     * 
     * @param dateStr String representation of Timestamp to be returned
     * @param formatStr template describing the format of <code>dateStr</code>
     * @param timezone TimeZone to use in interpreting the value of <code>dateStr</code>
     * @return Timestamp containing date represented by <code>dateStr</code>
     * @throws AxionException if format is invalid
     * @see #convertToTimestamp(String, String) 
     * @see #convertToTimestamp(String, String, java.util.TimeZone, java.util.Locale)
     * 
     */
    public static Timestamp convertToTimestamp(String dateStr, String formatStr,
            TimeZone timezone) throws AxionException {
        return convertToTimestamp(dateStr, formatStr, timezone, Locale.getDefault());
    }

    /**
     * Creates a String representation of the given Timestamp object, using
     * the given format string as a template, the given time zone, and the
     * given Locale
     * 
     * @param dateStr String representation of Timestamp to be returned
     * @param formatStr template describing the format of <code>dateStr</code>
     * @param timezone TimeZone to use in interpreting the value of <code>dateStr</code>
     * @param locale Locale to use in resolving date components
     * @return Timestamp containing date represented by <code>dateStr</code>
     * @throws AxionException if format is invalid
     * @see #convertToTimestamp(String, String)
     * @see #convertToTimestamp(String, String, java.util.TimeZone)
     */
    public static Timestamp convertToTimestamp(String dateStr, String formatStr,
            TimeZone timezone, Locale locale) throws AxionException {
        try {
            String javaFormat = DateTimeUtils.processFormatString(formatStr);

            SimpleDateFormat sdf = new SimpleDateFormat(javaFormat, locale);
            sdf.setCalendar(Calendar.getInstance(timezone, locale));
            sdf.setLenient(false);

            ParsePosition pos = new ParsePosition(0);
            Date parsedDate = sdf.parse(dateStr, pos);
            if (pos.getIndex() == dateStr.length()) {
                String upperCaseFormat = formatStr.toUpperCase(); 
                if (upperCaseFormat.indexOf("YYYY") != -1) {
                    assertDateStrMatchesFourYearFormatStr(dateStr, javaFormat);
                } else if (upperCaseFormat.indexOf("YY") != -1) {
                    assertDateStrMatchesTwoYearFormatStr(dateStr, javaFormat);
                }
                return (Timestamp) TIMESTAMP_TYPE.convert(parsedDate);
            } else if (parsedDate == null) {
                throw new AxionException("Date string does not match format: " + formatStr);
            } else { 
                throw new AxionException("Extraneous characters detected in " + dateStr);
            }
        } catch (IllegalArgumentException e) {
            throw new AxionException(e.getLocalizedMessage(), 22007);
        }
    }

    /**
     * Tests for a two-digit year pattern in the given format string, and tests if the given
     * date string has the required two-digit year.  
     * 
     * @param dateStr
     * @param formatStr
     * @throws AxionException 
     */
    private static void assertDateStrMatchesTwoYearFormatStr(String dateStr, String formatStr) 
            throws AxionException {
        // This is a horrible workaround for the limitations of SimpleDateFormat with respect
        // to parsing two-digit years.  SimpleDateFormat is permissive with respect to reading
        // in four-digit years when a two-digit year pattern is specified, but we need to 
        // raise an exception in that situation.
        Matcher matcher = YEAR_PATTERN_1.matcher(formatStr);
        if (matcher.lookingAt()) {
            String nonYearMatch = matcher.group(1);
            if (nonYearMatch != null) {
                char charFollowingYear = nonYearMatch.charAt(0); 
                switch (charFollowingYear) {
                    case '-':
                    case ' ':
                    case '/':
                    case '.':
                        int charFollowingYearPos = matcher.start(1);
                        if (charFollowingYear != dateStr.charAt(charFollowingYearPos)) {
                            throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
                        }
                        return;
                        
                    default:
                        if (dateStr.length() != formatStr.length()) {
                            throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
                        }
                        return;
                }
            }
        }
        
        matcher = YEAR_PATTERN_2.matcher(formatStr);
        if (matcher.lookingAt()) {
            int startOfYearPos = matcher.start(1);
            if (dateStr.substring(startOfYearPos).length() != 2) {
                throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
            }
            return;
        }
    }
    

    /**
     * Tests for a two-digit year pattern in the given format string, and tests if the given
     * date string has the required two-digit year.  
     * 
     * @param dateStr
     * @param formatStr
     * @throws AxionException 
     */
    private static void assertDateStrMatchesFourYearFormatStr(String dateStr, String formatStr) 
            throws AxionException {
        // This is a horrible workaround for the limitations of SimpleDateFormat with respect
        // to parsing two-digit years.  SimpleDateFormat is permissive with respect to reading
        // in four-digit years when a two-digit year pattern is specified, but we need to 
        // raise an exception in that situation.
        Matcher matcher = YEAR_4_PATTERN_1.matcher(formatStr);
        if (matcher.lookingAt()) {
            String nonYearMatch = matcher.group(1);
            if (nonYearMatch != null) {
                char charFollowingYear = nonYearMatch.charAt(0); 
                switch (charFollowingYear) {
                    case '-':
                    case ' ':
                    case '/':
                    case '.':
                        int charFollowingYearPos = matcher.start(1);
                        if (charFollowingYear != dateStr.charAt(charFollowingYearPos)) {
                            throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
                        }
                        return;
                        
                    default:
                        int dateLength = dateStr.length();
                        // Adjust length of date string to account for fact that T in formatStr is escaped by single quotes.
                        if (formatStr.indexOf("T") != -1) {
                            dateLength += 2;
                        }
                        if (dateLength != formatStr.length()) {
                            throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
                        }
                        return;
                }
            }
        }
        
        matcher = YEAR_4_PATTERN_2.matcher(formatStr);
        if (matcher.lookingAt()) {
            int startOfYearPos = matcher.start(1);
            if (dateStr.substring(startOfYearPos).length() != 4) {
                throw new AxionException("Date string, " + dateStr + ", does not match format: " + formatStr);
            }
            return;
        }
    }    

    /**
     * Extracts the specified date/time element from the given Timestamp,
     * using the default Locale.
     * 
     * @param t timestamp from which date/time element will be extracted
     * @param partIdent date part to extract, e.g., 'yyyy', 'mm', etc.
     * 
     * @return String representation of extracted date/time element
     * @throws AxionException if error occurs during extraction
     * @see #getDatePart(Timestamp, String, Locale)
     */
    public static String getDatePart(Timestamp t, String partIdent) 
            throws AxionException {
        return getDatePart(t, partIdent, Locale.getDefault());
    }
    
    /**
     * Extracts the specified date/time element from the given Timestamp,
     * using the given Locale.
     * 
     * @param t timestamp from which date/time element will be extracted
     * @param partIdent date part to extract, e.g., 'yyyy', 'mm', etc.
     * @param locale Locale to use in resolving date components
     * 
     * @return String representation of extracted date/time element
     * @throws AxionException if error occurs during extraction
     * @see #getDatePart(Timestamp, String)
     */
    public static String getDatePart(Timestamp t, String partIdent, Locale locale) 
            throws AxionException {
        if ("q".equals(partIdent)) {
            int month = Integer.parseInt(DateTimeUtils.convertToChar(t, "mm"));
            return Integer.toString((month + 2) / 3);
        } else if ("dow".equals(partIdent)) {
            String dayOfWeek = DateTimeUtils.convertToChar(t, "dy").toUpperCase();
            String[] shortDays =  new DateFormatSymbols(locale).getShortWeekdays();    
            for (int i = Calendar.SUNDAY; i <= Calendar.SATURDAY; i++) {
                if (shortDays[i].equalsIgnoreCase(dayOfWeek)) {
                    return String.valueOf(i); 
                } 
            }
            
            throw new AxionException("Unrecognized day of week: " +
                    dayOfWeek);
        } else {
            return DateTimeUtils.convertToChar(t, partIdent);
        }
    }
    
    /**
	 * Converts the given Axion date format string into a format usable with
	 * java.util.SimpleDateFormat.
	 * 
	 * @param rawFormat format string using Axion date/time mnemonics
	 * @return format string using SimpleDateFormat date/time mnemonics
	 * @throws AxionException if <code>rawFormat</code> could not be successfully
	 * converted to the appropriate SimpleDateFormat version
	 */
	static String processFormatString(String rawFormat) throws AxionException {
        AxionDateTimeFormatParser parser = new AxionDateTimeFormatParser();
        String cookedFormat = parser.parseDateTimeFormatToJava(rawFormat);
        
	    _log.log(Level.FINE,"raw format: " + rawFormat + "; cooked format: " 
                + cookedFormat);
		return cookedFormat;
	}

	/**
     * Converts the given value, which represents a date or time interval,
     * to its corresponding constant value.
     * 
	 * @param value String representation of date or time interval
	 * @return constant value corresponding to <code>value</code>
	 * @throws AxionException if <code>value</code> does not have a 
     * corresponding constant.
	 */
	public static int labelToCode(String value) throws AxionException {
        value = value != null ? value.toUpperCase() : null;;
        
	    if ("DAY".equals(value)) {
	        return DAY;
	    } else if ("MONTH".equals(value)) {
	        return MONTH;
	    } else if ("YEAR".equals(value)) {
	        return YEAR;
	    } else if ("HOUR".equals(value)) {
	        return HOUR;
	    } else if ("MINUTE".equals(value)) {
	        return MINUTE;
	    } else if ("SECOND".equals(value)) {
	        return SECOND;
	    } else if ("WEEK".equals(value)) {
	        return WEEK;
	    } else if ("QUARTER".equals(value)) {
	        return QUARTER;
	    } else if ("MILLISECOND".equals(value)) {
	        return MILLISECOND;
	    } else {
	        throw new AxionException(22006);
	    }
	}
    
    private static final TimestampType TIMESTAMP_TYPE = new TimestampType();

    /**
     * @param partCode
     * @return
     */
    public static String getPartMnemonicFor(String partString) 
            throws AxionException {
        partString = partString!= null ? partString.toUpperCase() : null;
        if ("WEEKDAY".equals(partString)) {
            return "dow";
        } else if ("WEEKDAY3".equals(partString)) {
            return "dy";
        } else if ("WEEKDAYFULL".equals(partString)) {
            return "day";
        } else if ("DAY".equals(partString)) {
            return "dd";
        } else if ("MONTH".equals(partString)) {
            return "mm";
        } else if ("MONTH3".equals(partString)) {
            return "mon";
        } else if ("MONTHFULL".equals(partString)) {
            return "month";
        } else if ("YEAR".equals(partString)) {
            return "yyyy";
        } else if ("YEAR2".equals(partString)) {
            return "yy";
        } else if ("HOUR".equals(partString)) {
            return "h";
        } else if ("HOUR12".equals(partString)) {
            return "hh12";
        } else if ("HOUR24".equals(partString)) {
            return "hh24";
        } else if ("MINUTE".equals(partString)) {
            return "mi";
        } else if ("SECOND".equals(partString)) {
            return "ss";
        } else if ("WEEK".equals(partString)) {
            return "w";
        } else if ("QUARTER".equals(partString)) {
            return "q";
        } else if ("MILLISECOND".equals(partString)) {
            return "ff";
        } else if ("AMPM".equals(partString)) {
            return "am";
        } else {
            throw new AxionException(22006);
        }
    }
    
    private static Pattern YEAR_PATTERN_1 = Pattern.compile("^[^Y]*YY([^Y]).*$", Pattern.CASE_INSENSITIVE);
    private static Pattern YEAR_PATTERN_2 = Pattern.compile("^.*[^Y](YY)$", Pattern.CASE_INSENSITIVE);
    
    private static Pattern YEAR_4_PATTERN_1 = Pattern.compile("^[^Y]*YYYY([^Y]).*$", Pattern.CASE_INSENSITIVE);
    private static Pattern YEAR_4_PATTERN_2 = Pattern.compile("^.*[^Y](YYYY)$", Pattern.CASE_INSENSITIVE);
    
    private static Logger _log = Logger.getLogger(DateTimeUtils.class.getName());
}

