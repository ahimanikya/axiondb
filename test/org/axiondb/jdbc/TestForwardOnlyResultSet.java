/*
 * $Id: TestForwardOnlyResultSet.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * Tests forward-only ResultSet decorator for AxionResultSet.
 *
 * @author Jonathan Giron
 * @version $Revision: 1.1 $
 */
public class TestForwardOnlyResultSet extends AxionTestCaseSupport {
    private ResultSet _fwdOnlyRs;
    
    /**
     * @param testName
     */
    public TestForwardOnlyResultSet(String testName) {
        super(testName);
    }
    
    public static Test suite() {
        return new TestSuite(TestForwardOnlyResultSet.class);
    }    

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    public void setUp() throws Exception {
        super.setUp();
        
        Statement stmt = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.execute("create table foo (id int, name varchar(50))");
        stmt.execute("insert into foo values (1, 'my first line')");
        stmt.execute("insert into foo values (2, 'my second line')");
        
        _fwdOnlyRs = stmt.executeQuery("select * from foo");
    }
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#tearDown()
     */
    public void tearDown() throws Exception {
        super.tearDown();
    }
    
    public void testAfterLast() {
        try {
            _fwdOnlyRs.afterLast();
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testBeforeFirst() {
        try {
            _fwdOnlyRs.beforeFirst();
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testFirst() {
        try {
            _fwdOnlyRs.first();
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testLast() {
        try {
            _fwdOnlyRs.last();
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testPrevious() {
        try {
            _fwdOnlyRs.previous();
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testSetFetchDirection() {
        try {
            _fwdOnlyRs.setFetchDirection(ResultSet.FETCH_REVERSE);
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }

    }

    public void testAbsolute() {
        try {
            _fwdOnlyRs.absolute(15);
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testRelative() {
        try {
            _fwdOnlyRs.relative(-1);
            fail("Expected SQLException.");
        } catch (SQLException ignore) {
            // expected.
        }
    }
    
    public void testIteration() throws SQLException {
        assertTrue(_fwdOnlyRs.next());
        assertTrue(_fwdOnlyRs.isFirst());

        int rowId = 1;
        do {
            try {
                _fwdOnlyRs.previous();
                fail("Expected SQLException.");
            } catch (SQLException ignore) {
                // expected.
            }
            
            assertEquals(rowId++, _fwdOnlyRs.getInt(1));
        } while (_fwdOnlyRs.next());
    }
}
