/*
 * $Id: TestAxionException.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.PropertyResourceBundle;

/**
 * @author Alexander
 */
public class TestAxionException extends TestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite(TestAxionException.class);
        return suite;
    }
    
    public void setUp() throws Exception {
        // This will make this test run my eclipse env
        // don't want to copy conf/AxionStateCodes.properties to org/axiondb
        ClassLoader cl = AxionException.class.getClassLoader();
        InputStream in = cl.getResourceAsStream("org/axiondb/AxionStateCodes.properties");
        if (in == null) {
            class MyAxionException extends AxionException{
                MyAxionException(PropertyResourceBundle bundle){
                    _bundle = bundle;
                }
            }
            in = cl.getResourceAsStream("AxionStateCodes.properties");
            new MyAxionException(new PropertyResourceBundle(in));
        }
    }

    public void testGetVendorCode() {
        AxionException e = new AxionException();
        assertEquals(99999, e.getVendorCode());
    }

    public void testGetMessage() {
        AxionException e = new AxionException(24000);
        assertEquals("invalid cursor state", e.getMessage());
        e = new AxionException("My custom message", 99000);
        assertEquals("My custom message", e.getMessage());
        
        e = new AxionException(898967598);
        assertEquals("-- Unknown Exception (898967598) --", e.getMessage());
    }
    
    public void testGetSQLState() {
        AxionException e = new AxionException(2010206);
        assertEquals("0102F", e.getSQLState());
        e = new AxionException(1022000);
        assertEquals("0V000", e.getSQLState());
        e = new AxionException(499000);
        assertEquals("HZ000", e.getSQLState());
        e = new AxionException(0);
        assertEquals("00000", e.getSQLState());
        
        e = new AxionException(499001);
        assertEquals("99099", e.getSQLState());
        
        e = new AxionException(8000);
        assertEquals("08000", e.getSQLState());
        
        e = new AxionException(222222222);
        assertEquals("2222V", e.getSQLState());
    }

    public void testGetNestedThrowable() {
        AxionException e = new AxionException(null, new SQLException("SQL Error", "1000", 1000),
            99999);
        assertNotNull(e.getNestedThrowable());
        assertTrue(e.getNestedThrowable() instanceof SQLException);
    }

}