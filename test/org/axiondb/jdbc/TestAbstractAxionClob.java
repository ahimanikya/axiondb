/*
 * $Id: TestAbstractAxionClob.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

package org.axiondb.jdbc;

import java.io.OutputStream;
import java.sql.Clob;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rod Waldhoff
 */
public class TestAbstractAxionClob extends TestCase {
    public TestAbstractAxionClob(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestAbstractAxionClob.class);
    }

    // currently AbstractAxionClob simply implements all methods
    // as throwing a SQLException("Unsupported"), so implementors
    // don't have to.
    public void testUnsupported() throws SQLException {
        AbstractAxionClob clob = new ConcreteClob();
        try {
            clob.getAsciiStream();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.getCharacterStream();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.getSubString(0L,10);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.length();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.position(CLOB,0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.position(STRING,0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.setAsciiStream(0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.setCharacterStream(0L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.setString(0L,STRING);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.setString(0L,STRING,0,3);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            clob.truncate(3L);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    private static class ConcreteClob extends AbstractAxionClob {
        public OutputStream setUtf8Stream(long pos) throws SQLException {
            throw new SQLException("Unsupported");
        }
    }

    private static final Clob CLOB = new ConcreteClob();
    private static final String STRING = "123";

}
