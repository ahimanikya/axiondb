/*
 * $Id: TestDDL.java,v 1.1 2007/11/28 10:01:29 jawed Exp $
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

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.Column;
import org.axiondb.Database;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.jdbc.AxionConnection;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:29 $
 * @author Rodney Waldhoff
 */
public class TestDDL extends TestCase {

    private Database _db = null;
    private Connection _conn = null;
    private ResultSet _rset = null;
    private Statement _stmt = null;

    //------------------------------------------------------------ Conventional

    public TestDDL(String testName) {
        super(testName);
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    public static Test suite() {
        return new TestSuite(TestDDL.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private String _connectString = "jdbc:axiondb:memdb";

    public void setUp() throws Exception {
        _conn = DriverManager.getConnection(_connectString);
        _db = ((AxionConnection) (_conn)).getDatabase();
        _stmt = _conn.createStatement();
    }

    public void tearDown() throws Exception {
        try {
            _rset.close();
        } catch (Exception t) {
        }
        try {
            _stmt.close();
        } catch (Exception t) {
        }
        try {
            _conn.close();
        } catch (Exception t) {
        }
        _rset = null;
        _stmt = null;
        _conn = null;
        {
            Connection conn = DriverManager.getConnection(_connectString);
            Statement stmt = conn.createStatement();
            stmt.execute("shutdown");
            stmt.close();
            conn.close();
        }
    }

    //------------------------------------------------------------------- Tests

    public void testCreateTable() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
        assertNull(_db.getTable("BAR"));
        _stmt.execute("create table BAR ( NUM integer, DESCR varchar(10) )");
        assertNotNull(_db.getTable("BAR"));
    }

    public void testCreateTableIfNotExists() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table if not exists FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
        _stmt.execute("create table if not exists FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
    }

    public void testCreateTableWithNoData() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
        assertNull(_db.getTable("BAR"));
        _stmt.execute("create table BAR ( NUM integer, DESCR varchar(10) )");
        assertNotNull(_db.getTable("BAR"));
        _stmt.execute("create table FOOBAR as select foo.NUM, bar.DESCR from foo , bar WITH NO DATA");
    }
    
    public void testCreateTableWithLiteralDefault() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM int default 1 )");
        assertNotNull(_db.getTable("FOO"));
    }

    public void testCreateTableWithFunctionDefault() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( TS timestamp default now() )");
        assertNotNull(_db.getTable("FOO"));
    }
    
    public void testCreateTableWithAlwaysGeneratedIdenity() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM int generated always as identity start with 1 increment by 1 maxvalue 1000 minvalue 1 cycle)");
        assertNotNull(_db.getTable("FOO"));
    }

    public void testCreateTableViaDataTypeClassName() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM org.axiondb.types.IntegerType )");
        assertNotNull(_db.getTable("FOO"));
        assertNull(_db.getTable("BAR"));
        _stmt.execute("create table BAR ( NUM org.axiondb.types.IntegerType, "
            + "DESCR org.axiondb.types.StringType )");
        assertNotNull(_db.getTable("BAR"));
    }

    public void testCantCreateDuplicateTable() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
        try {
            _stmt.execute("create table FOO ( NUM integer, DESCR varchar(10) )");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        assertNotNull(_db.getTable("FOO"));
    }

    public void testCreateTableWithObjectType() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( X java_object )");
        assertNotNull(_db.getTable("FOO"));
    }

    public void testCantDropNonExistentTable() throws Exception {
        assertNull(_db.getTable("FOO"));
        try {
            _stmt.execute("drop table FOO");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        assertNull(_db.getTable("FOO"));
    }

    public void testDropTable() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM integer )");
        assertNotNull(_db.getTable("FOO"));
        _stmt.execute("drop table FOO");
        assertNull(_db.getTable("FOO"));
    }

    public void testCreateIndex() throws Exception {
        assertNull(_db.getTable("FOO"));
        _stmt.execute("create table FOO ( NUM integer, DESCR varchar(10) )");
        assertNotNull(_db.getTable("FOO"));
        _stmt.execute("create index BAR on FOO ( NUM )");
        Table table = _db.getTable("FOO");
        Column column = table.getColumn("NUM");
        assertTrue(null != table.getIndexForColumn(column));
    }

    public void testCreateIndexIfNotExists() throws Exception {
        _stmt.execute("create table FOO ( NUM integer )");
        assertTrue(!_db.hasIndex("BAR"));
        _stmt.execute("create index if not exists BAR on FOO ( NUM )");
        assertTrue(_db.hasIndex("BAR"));
        _stmt.execute("create index if not exists BAR on FOO ( NUM )");
        assertTrue(_db.hasIndex("BAR"));
    }

    public void testCreateSequenceIfNotExists() throws Exception {
        assertTrue(!_db.hasSequence("FOO"));
        _stmt.execute("create sequence if not exists FOO");
        assertTrue(_db.hasSequence("FOO"));
        _stmt.execute("create sequence if not exists FOO");
        assertTrue(_db.hasSequence("FOO"));
    }

    public void testCreateAndDropSequence() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));

        _stmt.execute("create sequence FOO_SEQ");
        assertNotNull("Should find sequence", _db.getSequence("foo_seq"));
        assertEquals("Should have correct initial value", BigInteger.valueOf(0), _db.getSequence(
            "foo_seq").getValue());
        _stmt.execute("drop sequence FOO_SEQ");
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
    }

    public void testCreateAndDropSequenceAsBigInt() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));

        _stmt.execute("create sequence FOO_SEQ as bigint start with 0 increment by 1 "
            + "maxvalue 10000000000000 minvalue 0 cycle");
        Sequence seq = _db.getSequence("foo_seq");
        assertNotNull("Should find sequence", seq);
        assertEquals("Should have correct initial value", BigInteger.valueOf(0), seq.getValue());
        assertNull(seq.getCuurentValue());
        assertEquals("Should have correct next value", new Long(0), seq.evaluate());
        assertEquals("Should have correct next value", new Long(1), seq.evaluate());
        
        _stmt.execute("alter sequence FOO_SEQ restart with 0 increment by -1 "
            + "maxvalue 1000000000 minvalue 0 cycle");
        
        seq = _db.getSequence("foo_seq");
        assertEquals("Should have correct initial value", BigInteger.valueOf(0), seq.getValue());
        assertNull(seq.getCuurentValue());
        assertEquals("Should have correct next value", new Long(0), seq.evaluate());
        assertEquals("Should have correct next value", new Long(1000000000), seq.evaluate());
        
        _stmt.execute("alter sequence FOO_SEQ no cycle");
        
        seq = _db.getSequence("foo_seq");
        assertEquals("Should have correct initial value", BigInteger.valueOf(999999999), seq.getValue());
        assertNull(seq.getCuurentValue());
        assertEquals("Should have correct next value", new Long(999999999), seq.evaluate());
        assertEquals("Should have correct next value", new Long(999999998), seq.evaluate());

        _stmt.execute("alter sequence FOO_SEQ restart with 100");
        
        seq = _db.getSequence("foo_seq");
        assertEquals("Should have correct initial value", BigInteger.valueOf(100), seq.getValue());
        assertNull(seq.getCuurentValue());
        assertEquals("Should have correct next value", new Long(100), seq.evaluate());
        assertEquals("Should have correct next value", new Long(99), seq.evaluate());
        
        _stmt.execute("alter sequence FOO_SEQ restart with 1 increment by -1 "
            + "maxvalue 1000000000 minvalue 0 NO cycle");
        
        seq = _db.getSequence("foo_seq");
        assertEquals("Should have correct initial value", BigInteger.valueOf(1), seq.getValue());
        assertNull(seq.getCuurentValue());
        assertEquals("Should have correct next value", new Long(1), seq.evaluate());
        try {
            assertEquals("Should have correct next value", new Long(1000000000), seq.evaluate());
            fail("Expected exception");
        }catch(IllegalStateException ex) {
            // expected
        }

        _stmt.execute("drop sequence FOO_SEQ");
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        
        try {
            _stmt.execute("alter sequence FOO_SEQ restart with 1 increment by -1 "
                + "maxvalue 1000000000 minvalue 0 NO cycle");
            fail("Expected exception: sequence does not exist");
        }catch(Exception e) {
            // expected
        }
    }

    public void testCreateAndDropSequenceStartWith() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        _stmt.execute("create sequence FOO_SEQ start with 23");
        assertNotNull("Should find sequence", _db.getSequence("foo_seq"));
        assertEquals("Should have correct initial value", BigInteger.valueOf(23), _db.getSequence(
            "foo_seq").getValue());
        _stmt.execute("drop sequence FOO_SEQ");
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
    }

    public void testDropIndex() throws Exception {
        assertTrue(!_db.hasIndex("FOO_NDX"));
        _stmt.execute("create table FOO ( id int )");
        _stmt.execute("create index FOO_NDX on FOO ( id )");
        assertTrue(_db.hasIndex("FOO_NDX"));
        _stmt.execute("drop index FOO_NDX");
        assertTrue(!_db.hasIndex("FOO_NDX"));
    }

    public void testDropIndexIfExists() throws Exception {
        assertTrue(!_db.hasIndex("FOO_NDX"));
        _stmt.execute("drop index if exists FOO_NDX");
        assertTrue(!_db.hasIndex("FOO_NDX"));
        _stmt.execute("create table FOO ( id int )");
        _stmt.execute("create index FOO_NDX on FOO ( id )");
        assertTrue(_db.hasIndex("FOO_NDX"));
        _stmt.execute("drop index if exists FOO_NDX");
        assertTrue(!_db.hasIndex("FOO_NDX"));
        _stmt.execute("drop index if exists FOO_NDX");
        assertTrue(!_db.hasIndex("FOO_NDX"));
    }

    public void testDropTableIfExists() throws Exception {
        assertTrue(!_db.hasTable("FOO"));
        _stmt.execute("drop table if exists FOO");
        assertTrue(!_db.hasTable("FOO"));
        _stmt.execute("create table FOO ( id int )");
        assertTrue(_db.hasTable("FOO"));
        _stmt.execute("drop table if exists FOO");
        assertTrue(!_db.hasTable("FOO"));
    }

    public void testDropNonExistentSequence() throws Exception {
        assertTrue(!_db.hasSequence("FOO"));
        try {
            _stmt.execute("drop sequence foo");
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testDropSequenceIfExists() throws Exception {
        assertTrue(!_db.hasSequence("FOO"));
        _stmt.execute("drop sequence if exists FOO");
        assertTrue(!_db.hasSequence("FOO"));
        _stmt.execute("create sequence FOO");
        assertTrue(_db.hasSequence("FOO"));
        _stmt.execute("drop sequence if exists FOO");
        assertTrue(!_db.hasSequence("FOO"));
    }
    
}