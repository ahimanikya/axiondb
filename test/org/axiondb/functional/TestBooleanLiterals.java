/*
 * $Id: TestBooleanLiterals.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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

package org.axiondb.functional;

import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Rob Oxspring
 * @author Rodney Waldhoff
 */
public class TestBooleanLiterals extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestBooleanLiterals (String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestBooleanLiterals.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
        _stmt.execute("CREATE TABLE foobar (foo VARCHAR(3), bar BOOLEAN)");
        _stmt.execute("INSERT INTO foobar VALUES ('aaa', true)");
        _stmt.execute("INSERT INTO foobar VALUES ('bbb', false)");
        _stmt.execute("INSERT INTO foobar VALUES ('ccc', true)");
        _stmt.execute("INSERT INTO foobar VALUES ('ddd', false)");
    }

    public void tearDown() throws Exception {
        _stmt.execute("DROP TABLE foobar");
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testInsert() throws SQLException {
        _rset = _stmt.executeQuery("SELECT COUNT(*) FROM foobar");
        _rset.next();
        assertEquals("Should start with 4 elements",4,_rset.getInt(1));
        _rset.close();

        _stmt.execute("INSERT INTO foobar VALUES ('eee', TRUE )");
        _stmt.execute("INSERT INTO foobar VALUES ('fff', FALSE )");

        _rset = _stmt.executeQuery("SELECT COUNT(*) FROM foobar");
        _rset.next();
        assertEquals("Should now contain 6 elements",6,_rset.getInt(1));
        _rset.close();
    }

    public void testInsertOnStringLiteral() throws SQLException {
        _stmt.execute("INSERT INTO foobar VALUES ('eee', 'TRUE' )");
        _stmt.execute("INSERT INTO foobar VALUES ('fff', 'FALSE' )");
        
        _rset = _stmt.executeQuery("SELECT COUNT(*) FROM foobar");
        _rset.next();
        assertEquals("Should now contain 6 elements",6,_rset.getInt(1));
        _rset.close();
    }

    public void testWhere() throws SQLException {
        _rset = _stmt.executeQuery("SELECT foo FROM foobar WHERE bar=TRUE ORDER BY foo");
        assertTrue(_rset.next());
        assertEquals("aaa",_rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("ccc",_rset.getString(1));
        assertTrue(!_rset.next());
        _rset.close();

        _rset = _stmt.executeQuery("SELECT foo FROM foobar WHERE bar=FALSE ORDER BY foo");
        assertTrue(_rset.next());
        assertEquals("bbb",_rset.getString(1));
        assertTrue(_rset.next());
        assertEquals("ddd",_rset.getString(1));
        assertTrue(!_rset.next());
        _rset.close();
    }
    
    public void testOrderBy() throws SQLException {
        _rset = _stmt.executeQuery("SELECT bar, foo FROM foobar ORDER BY bar, foo");
        {
            assertTrue(_rset.next());
            assertEquals(false,_rset.getBoolean(1));
            assertEquals("bbb",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(false,_rset.getBoolean(1));
            assertEquals("ddd",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(true,_rset.getBoolean(1));
            assertEquals("aaa",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(true,_rset.getBoolean(1));
            assertEquals("ccc",_rset.getString(2));
        }
        assertTrue(!_rset.next());
        _rset.close();
    }

    
    public void testOrderByDesc() throws SQLException {
        _rset = _stmt.executeQuery("SELECT bar, foo FROM foobar ORDER BY bar DESC, foo");
        {
            assertTrue(_rset.next());
            assertEquals(true,_rset.getBoolean(1));
            assertEquals("aaa",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(true,_rset.getBoolean(1));
            assertEquals("ccc",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(false,_rset.getBoolean(1));
            assertEquals("bbb",_rset.getString(2));
        }
        {
            assertTrue(_rset.next());
            assertEquals(false,_rset.getBoolean(1));
            assertEquals("ddd",_rset.getString(2));
        }
        assertTrue(!_rset.next());
        _rset.close();
    }
} 