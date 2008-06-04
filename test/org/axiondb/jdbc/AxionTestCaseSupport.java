/*
 * $Id: AxionTestCaseSupport.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
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

package org.axiondb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import junit.framework.TestCase;

/**
 * A useful abstract base class for any JDBC test cases
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Chuck Burdick
 * @author James Strachan
 */
public class AxionTestCaseSupport extends TestCase {
    public AxionTestCaseSupport(String testName) {
        super(testName);
        new AxionDriver();
    }

    public void setUp() throws Exception {
        _conn = DriverManager.getConnection(CONNECT_STRING);
    }

    public void tearDown() throws Exception {
        try {
            _conn.close();
        } catch (Exception e) {
            // ignored
        }
        _conn = null;
        // shutdown the database
        {
            Connection conn = DriverManager.getConnection(CONNECT_STRING);
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
    }

    protected Connection getConnection() {
        return _conn;
    }

    protected AxionConnection getAxionConnection() {
        return (AxionConnection)_conn;
    }

    public void testCreateConnection() throws Exception {
        assertNotNull("Should be able to create a connection", getConnection());
    }

    protected static final String CONNECT_STRING = "jdbc:axiondb:memdb";
    private Connection _conn = null;

}
