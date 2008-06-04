/*
 * $Id: TestExceptionConverter.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Rodney Waldhoff
 */
public class TestExceptionConverter extends TestCase {

    //------------------------------------------------------------ Conventional
        
    public TestExceptionConverter(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestExceptionConverter.class);
    }

    //--------------------------------------------------------------- Lifecycle

    //------------------------------------------------------------------- Tests        

    public void testConvertAxionException() throws Exception {
        assertNotNull(ExceptionConverter.convert(new AxionException()));
    }

    public void testConvertRuntimeException() throws Exception {
        assertNotNull(ExceptionConverter.convert(new RuntimeException()));
    }

    public void testConvertIOException() throws Exception {
        assertNotNull(ExceptionConverter.convert(new IOException()));
    }

    public void testConvertToIOException() throws Exception {
        assertNotNull(ExceptionConverter.convertToIOException(new RuntimeException()));
        IOException e1 = new IOException();
        assertSame(e1,ExceptionConverter.convertToIOException(e1));
    }

    public void testConvertToRuntimeException() throws Exception {
        assertNotNull(ExceptionConverter.convertToRuntimeException(new Exception()));
        RuntimeException e1 = new RuntimeException();
        assertSame(e1,ExceptionConverter.convertToRuntimeException(e1));
    }
    
    public void testUnwrapNestedSQLException() throws Exception {
        SQLException sqle = new SQLException();
        AxionException ae = new AxionException(sqle);
        assertSame(sqle,ExceptionConverter.convert(ae));
    }
}
