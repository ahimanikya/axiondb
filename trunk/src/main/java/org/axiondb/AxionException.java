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

import java.io.IOException;
import java.io.InputStream;
import java.util.PropertyResourceBundle;

/**
 * Root exception for Axion related or specific problems. This exception provides access
 * to Axion-specific SQL Vendor codes. Where possible they are mapped to SQL99 / XOPEN 99
 * SQL state codes.
 * <p>
 * SQLState codes consisti of 5 characters. The first 2 characters specify the error
 * class, the last three characters specify the subclass. For example, the SQLSTATE value
 * '22012' consists of class code 22 (data exception) and subclass code 012 (division by
 * zero). * Each of the five characters in a SQLSTATE value is a digit (0..9) or an
 * uppercase Latin letter (A..Z).
 * <p>
 * Class codes that begin with a digit in the range 0..4 or a letter in the range A..H are
 * reserved for predefined conditions. Within predefined classes, subclass codes that
 * begin with a digit in the range 0..4 or a letter in the range A..H are reserved for
 * predefined sub-conditions. All other subclass codes are reserved for
 * implementation-defined sub-conditions. (see ANSI-SQL99 specification).
 * 
 * @see {@link org.axiondb.util.ExceptionConverter}
 * @version  
 * @author Rodney Waldhoff
 */
public class AxionException extends Exception {
    public final static int DEFAULT_VENDOR_CODE = 99999; //catch-all
    protected static PropertyResourceBundle _bundle = null;

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable) AxionException(null,null,DEFAULT_VENDOR_CODE)}.
     */
    public AxionException() {
        this(null, null, DEFAULT_VENDOR_CODE);
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable,int) AxionException(null,null,vendorcode)}.
     */
    public AxionException(int vendorcode) {
        this(null, null, vendorcode);
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable,int) AxionException(message,null,DEFAULT_VENDOR_CODE)}.
     */
    public AxionException(String message) {
        this(message, null, DEFAULT_VENDOR_CODE);
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable,int) AxionException(message,null,vendorcode)}.
     */
    public AxionException(String message, int vendorcode) {
        this(message, null, vendorcode);
    }

    /**
     * Construct a new {@link AxionException}with the given <i>message </i>, wrapping the
     * given {@link Throwable}.
     * 
     * @param message my detailed message (possibly <code>null</code>)
     * @param nested a {@link Throwable}to wrap (possibly <code>null</code>)
     * @param vendorcode an error code
     */
    public AxionException(String message, Throwable nested, int vendorcode) {
        super(null == message ? (null == nested ? null : nested.toString()) : (null == nested ? message : message + " (" + nested.toString() + ")"));
        _nested = nested;
        _vendorCode = vendorcode;
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable) AxionException(message,nested,DEFAULT_VENDOR_CODE)}.
     */
    public AxionException(String message, Throwable nested) {
        this(message, nested, DEFAULT_VENDOR_CODE);
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable) AxionException(null,nested,DEFAULT_VENDOR_CODE)}.
     */
    public AxionException(Throwable nested) {
        this(null, nested, DEFAULT_VENDOR_CODE);
    }

    /**
     * Equivalent to
     * {@link #AxionException(java.lang.String,java.lang.Throwable) AxionException(null,nested,vendorcode)}.
     */
    public AxionException(Throwable nested, int vendorcode) {
        this(null, nested, vendorcode);
    }

    /** Returns the Axion-specific vendor code for this exception */
    public int getVendorCode() {
        return _vendorCode;
    }

    /** Returns the detail message string of this AxionException */
    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        }
        // Load other Error code translations from a properties file
        return getErrorMessage(_vendorCode);
    }

    /**
     * Returns the five-digit SQL State code (as defined in the ANSI-SQL 99 standard). The
     * translation works as follows:
     * <p>-- any vendor code lower than 100000 is just converted to a String.
     * <p>-- vendor codes 10000 and over use the following rules: a) first digit
     * indicates the position of a non-digit character.
     * <p>
     * <li>1: Second Class Digit is Alphanumeric (e.g. SQL StateCode "0Z001")
     * <li>2: Last Subclass Digit is Alphanumeric (e.g. "2200E")
     * <li>3: Second Class Digit and Last Subclass Digit are alphanumeric
     * <li>4: Other case (e.g. for class HZ) --> explicit conversion value (must be
     * assigned directly in code) b) Alpha Character is translated into its position in
     * the Alphabet (e.g. H=08; Z=26) etc. thus 1026001 is translated into 0Z001 32080001
     * would be translated into 2H00A
     */
    public String getSQLState() {
        String sqlstate;
        if (_vendorCode < 100000) {
            sqlstate = String.valueOf(_vendorCode);
            while (sqlstate.length() < 5) { //leftpad with zeros
                sqlstate = '0' + sqlstate;
            }
            return sqlstate;
        }

        final String CONVERSION_ERROR = "99099";
        sqlstate = String.valueOf(_vendorCode);
        if (sqlstate.charAt(0) > '3') { //special cases that can not be calculated
            if (_vendorCode == 499000) {
                return "HZ000";
            }
            return CONVERSION_ERROR;
        }

        // transform last two digits of class into a character
        if (sqlstate.charAt(0) == '1' || sqlstate.charAt(0) == '3') {
            int charnum = Integer.parseInt(sqlstate.substring(2, 4));
            sqlstate = sqlstate.substring(0, 2) + (char) (64 + charnum) + sqlstate.substring(4, sqlstate.length());
        }

        //transform last two digits of subclass into a character
        if (sqlstate.charAt(0) == '2' || sqlstate.charAt(1) == '3') {
            int charnum = Integer.parseInt(sqlstate.substring(sqlstate.length() - 2, sqlstate.length()));
            sqlstate = sqlstate.substring(0, 5) + (char) (64 + charnum);
        }
        sqlstate = sqlstate.substring(1, sqlstate.length());

        return sqlstate;
    }

    private static String getErrorMessage(int vendorcode) {
        try {
            if (_bundle == null) {
                getResourceBundle("org/axiondb/AxionStateCodes.properties");
            }
            return _bundle.getString(String.valueOf(vendorcode));
        } catch (Exception e) {
            return "-- Unknown Exception (" + vendorcode + ") --";
        }
    }

    private static void getResourceBundle(String bundleFile) throws IOException {
        ClassLoader cl = AxionException.class.getClassLoader();
        InputStream in = null;
        try {
            in = cl.getResourceAsStream(bundleFile);
            _bundle = new PropertyResourceBundle(in);
        } finally {
            in.close();
        }
    }

    /**
     * Return the {@link Throwable}I'm wrapping, if any.
     * 
     * @return the {@link Throwable}I'm wrapping, if any.
     */
    public Throwable getNestedThrowable() {
        return _nested;
    }

    private Throwable _nested = null;
    private int _vendorCode;
}
