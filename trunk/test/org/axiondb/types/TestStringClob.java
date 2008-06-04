/*
 * $Id: TestStringClob.java,v 1.1 2007/11/28 10:01:40 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionClob;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:40 $
 * @author Rodney Waldhoff
 */
public class TestStringClob extends AbstractClobTest {

    public TestStringClob(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestStringClob.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestStringClob.class);
    }

    protected AxionClob getClob() throws Exception {
        return new StringClob(_text);
    }

    public void testConstructorOnNull() throws Exception  {
        try {
            new StringClob(null);
            fail("Expected NullPointerException");
        } catch(NullPointerException e) {
            // expected
        }
    }
    
    public void testLength() throws Exception  {
        AxionClob clob = getClob();
        assertEquals(_text.length(),clob.length());
    }

    public void testTruncate() throws Exception  {
        AxionClob clob = getClob();
        try {
            clob.truncate(("The quick".length()));
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }

    public void testSetString() throws Exception  {
        AxionClob clob = getClob();
        try {
            clob.setString(0,"xyzzy",0,3);
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testSetString2() throws Exception  {
        AxionClob clob = getClob();
        try {
            clob.setString(0,"xyzzy");
            fail("Expected SQLException"); // ok to implement, just isn't currently
        } catch(SQLException e) {
            // expected
        }
    }
    
}
