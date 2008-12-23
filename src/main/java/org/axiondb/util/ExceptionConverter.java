/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionException;
import org.axiondb.AxionRuntimeException;

/**
 * Converts Axion-specific {@link Exception}s into {@link SQLException}s.
 * <p>
 * (This class should eventually handle converting various {@link AxionException}s in to
 * the proper SQLException with vendor message and code.)
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 */
public class ExceptionConverter {

    public static SQLException convert(AxionException e) {
        logConversion("AxionException", "SQLException", e);
        if (e.getNestedThrowable() instanceof SQLException) {
            return (SQLException) (e.getNestedThrowable());
        }
        SQLException  sqe = new SQLException(e.getMessage(), e.getSQLState(), e.getVendorCode());
        if(e.getNestedThrowable() instanceof AxionException) {
            sqe.setNextException(convert((AxionException)e.getNestedThrowable()));
        }
        return sqe;
    }

    public static SQLException convert(RuntimeException e) {
        return convert(null, e);
    }

    public static SQLException convert(String message, RuntimeException e) {
        logConversion("RuntimeException", "SQLException", e);
        message = (message == null ? "" :  (message + ": "));
        if(e instanceof AxionRuntimeException) {
            return new SQLException(message + e.toString(), ((AxionRuntimeException)e).getSQLState()); 
        }
        return new SQLException(message + e.toString());
    }

    public static SQLException convert(IOException e) {
        logConversion("IOException", "SQLException", e);
        return new SQLException(e.toString());
    }

    public static IOException convertToIOException(Exception e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        logConversion("Exception", "IOException", e);
        return new IOException(e.toString());
    }

    public static RuntimeException convertToRuntimeException(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else if (e instanceof AxionException) {
            return new AxionRuntimeException((AxionException)e);
        }
        logConversion("Exception", "RuntimeException", e);
        return new RuntimeException(e.toString());
    }

    private static final void logConversion(String from, String to, Throwable t) {
        _log.log(Level.FINE,"Converting " + from + " to " + to, t);
    }

    private static final Logger _log = Logger.getLogger(ExceptionConverter.class.getName());
}
