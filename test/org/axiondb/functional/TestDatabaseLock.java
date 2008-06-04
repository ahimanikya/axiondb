/*
 * $Id: TestDatabaseLock.java,v 1.1 2007/11/28 10:01:30 jawed Exp $
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

package org.axiondb.functional;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.jdbc.AxionDataSource;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:30 $
 * @author Rodney Waldhoff
 */
public class TestDatabaseLock extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestDatabaseLock(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDatabaseLock.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private String _originalValue = null;

    public void setUp() throws Exception {
        super.setUp();
        _originalValue = System.getProperty("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE");
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(null == _originalValue) {
            System.getProperties().remove("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE");
        } else {
            System.setProperty("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE",_originalValue);
        }
        _originalValue = null;
    }

    //------------------------------------------------------------------- Tests

    protected File getDatabaseDirectory() {
        return new File(new File("."), "testdb");
    }  
    
    public void testCantOpenDiskDatabaseTwice() throws Exception {
        System.setProperty("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE","false");

        Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb:testdb");
        assertNotNull(conn);
        
        Thread.sleep(50L); // give the OS a chance to catch up if needed
        
        try {
            DriverManager.getConnection("jdbc:axiondb:testdb2:testdb");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }

        DataSource ds = new AxionDataSource("jdbc:axiondb:testdb2:testdb");
        try {
            ds.getConnection();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }

    }

    public void testCanOpenDiskDatabaseTwiceWhenOverridePropertyIsSet() throws Exception {
        System.setProperty("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE","true");
        
        try {
            Connection conn = DriverManager.getConnection("jdbc:axiondb:testdb:testdb");
            assertNotNull(conn);

            Connection conn2 = DriverManager.getConnection("jdbc:axiondb:testdb2:testdb");
            assertNotNull(conn2);

            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();

            Statement stmt2 = conn2.createStatement();
            stmt2.execute("shutdown");
            stmt2.close();
            conn2.close();
        } finally {
            System.setProperty("org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE","false");
        }
    }

}
