/*
 * $Id: TestAxionResultSet.java,v 1.3 2008/02/21 13:00:26 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

import java.io.File;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.axiondb.Database;
import org.axiondb.Table;
import org.axiondb.engine.Databases;
import org.axiondb.engine.tables.MemoryTable;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:26 $
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Jonathan Giron
 */
public class TestAxionResultSet extends TestCase {
    // TODO: Revisit testUpdateXXX tests.
    protected Table _table = null;
    protected Connection _conn = null;
    protected AxionStatement _stmt = null;
    protected ResultSet _rset = null;
    
    protected static int IMAX = 26;
    protected static int JMAX = 10;

    public TestAxionResultSet(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(TestAxionResultSet.class);
    }

    public void setUp() throws Exception {
        Database db = createDatabase();
        _conn = new AxionConnection(db);
    }

    public void tearDown() throws Exception {
        doCleanup();
        dropDatabase();
    }

    public void testCreate() throws Exception {
        createBasicTable();
        
        assertNotNull("Should not be null", _rset);
        boolean foundRows = false;
        for (int i = 0; i < IMAX; i++) {
            for (int j = 0; j < JMAX; j++) {
                foundRows = true;
                assertTrue("Should have more rows", _rset.next());
                assertEquals("Should get letter back", String.valueOf((char) (65 + i)), _rset.getString(1));
                assertEquals("Should get number back", j, _rset.getInt(2));
            }
        }
        assertTrue("Should have found rows", foundRows);
    }
    
    public void testPrevious() throws Exception {
        createBasicTable();
        assertTrue("Should be able to reach last row in ResultSet", _rset.last());
        
        _rset.afterLast();
        assertTrue(_rset.isAfterLast());
        
        boolean foundRows = false;
        for (int i = IMAX -1; i >= 0; i--) {
            for (int j = JMAX -1; j >= 0; j--) {
                foundRows = true;
                assertTrue("Should have more rows going backwards", _rset.previous());
                assertEquals("Should get letter back", String.valueOf((char) (65 + i)), _rset.getString(1));
                assertEquals("Should get number back", j, _rset.getInt(2));
            }
        }
        assertTrue("Should have found rows", foundRows);
    }
    

    public void testGetType() throws Exception {
        createBasicTable();
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, _rset.getType());

        Statement fwdOnlyStmt = _conn.createStatement();
        ResultSet fwdOnlyRs = fwdOnlyStmt.executeQuery("select * from foo");
        try {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, fwdOnlyRs.getType());
        } finally {
            if (fwdOnlyRs != null) {
                fwdOnlyRs.close();
            }
            
            if (fwdOnlyStmt != null) {
                fwdOnlyStmt.close();
            }
        }
    }

    public void testClearWarnings() throws Exception {
        createBasicTable();

        // currently clearWarnings is a no-op, but we can check that it doesn't throw an
        // exception
        _rset.clearWarnings();
        assertNull(_rset.getWarnings());
    }

    public void testNegativeGets() throws Exception {
        createBasicTable();

        // rows overflow
        try {
            _rset.getDate(100);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        
        try {
            _rset.getInt(100);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        
        try {
            _rset.getLong(100);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
        
        // wrong data type
        _rset.beforeFirst();
        for (int i = 0; i < IMAX; i++) {
            for (int j = 0; j < JMAX; j++) {
                assertTrue("Should have more rows", _rset.next());
                try {
                    _rset.getDate(1);
                    fail("Expected SQLException");
                } catch (SQLException e) {
                    // expected
                }
            }
        }
    }

    public void testNextThrowsExceptionAfterClose() throws Exception {
        createBasicTable();

        assertTrue(_rset.next());
        _rset.close();
        try {
            _rset.next();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testNegativeCursor() throws Exception {
        AxionResultSet rset = new AxionResultSet(null, null, null);
        try {
            rset.beforeFirst();
            fail("Expected SQLException");
        } catch (SQLException e) {
            //expect
        }

        try {
            rset.next();
            fail("Expected SQLException");
        } catch (SQLException e) {
            //expect
        }
    }

    public void testCancelRowUpdates() throws Exception {
        createUpdateTable();

        _rset.next();
        final Object oldValue1 = _rset.getObject(1);
        final Object oldValue2 = _rset.getObject(2);
        final Object oldValue3 = _rset.getObject(3);

        _rset.updateObject(1, new Integer(Integer.MAX_VALUE));
        _rset.updateObject(3, new Date(0L));
        _rset.cancelRowUpdates();

        // Check that old values are still there in the current row.
        assertEquals(oldValue1, _rset.getObject(1));
        assertEquals(oldValue2, _rset.getObject(2));
        assertEquals(oldValue3, _rset.getObject(3));
    }
    
    public void testNegativeCancelRowUpdates() throws Exception {
        createUpdateTable();
        _rset.moveToInsertRow();
        
        try {
            _rset.cancelRowUpdates();
            fail("Expected SQLException - cannot call cancelRowUpdates() when on insert row.");
        } catch (SQLException expected) {
            // Expected
        }
    }

    public void testAbsolute() throws Exception {
        createUpdateTable();
        
        _rset.absolute(2);
        assertInMiddleInMultirowRS();
        assertEquals(2, _rset.getInt("id"));
        
        _rset.absolute(-2);
        assertInMiddleInMultirowRS();
        assertEquals(3, _rset.getInt("id"));
        
        _rset.absolute(0);
        assertBeforeFirst();
        
        _rset.absolute(1);
        assertFirstInMultirowRS();
        assertEquals(1, _rset.getInt("id"));
        
        _rset.absolute(4);
        assertLastInMultirowRS();
        assertEquals(4, _rset.getInt("id"));
        
        _rset.absolute(3);
        assertInMiddleInMultirowRS();
        assertEquals(3, _rset.getInt("id"));
        
        _rset.absolute(-1);
        assertLastInMultirowRS();
        assertEquals(4, _rset.getInt("id"));
        
        _rset.absolute(-3);
        assertInMiddleInMultirowRS();
        assertEquals(2, _rset.getInt("id"));
        
        _rset.absolute(-4);
        assertFirstInMultirowRS();
        assertEquals(1, _rset.getInt("id"));
    }
    
    public void testEmptyResultSet() throws Exception {
        createEmptyTable();
        assertEmptyResultSet();
        
        assertFalse(_rset.next());
        assertEmptyResultSet();        

        assertFalse(_rset.previous());
        assertEmptyResultSet();
        
        assertFalse(_rset.next());
        assertEmptyResultSet();
    }
    
    public void testOneRowResultSet() throws Exception {
        createOneRowTable();
        
        // At start, just prior to row 1.
        assertTrue(_rset.isBeforeFirst());
        
        // Now at row 1.
        assertTrue(_rset.next());
        assertFalse(_rset.isAfterLast());
        assertTrue(_rset.isFirst());
        assertTrue(_rset.isLast());
        assertEquals(1, _rset.getInt(1));
        
        // Now after row 1.
        assertFalse(_rset.next());
        assertTrue(_rset.isAfterLast());
        
        // Back to row 1.
        assertTrue(_rset.previous());
        assertTrue(_rset.isFirst());
        assertTrue(_rset.isLast());
        assertFalse(_rset.isAfterLast());
        assertEquals(1, _rset.getInt(1));

        // Now before row 1.
        assertFalse(_rset.previous());
        assertTrue(_rset.isBeforeFirst());
        
        // Now forward to row 1.
        assertTrue(_rset.next());
        assertTrue(_rset.isFirst());
        assertTrue(_rset.isLast());
        assertFalse(_rset.isAfterLast());
        assertEquals(1, _rset.getInt(1));
    }

    public void testPositionIndicators() throws Exception {
        createUpdateTable();

        assertBeforeFirst();

        assertTrue(_rset.next());
        assertEquals(1, _rset.getInt("id"));
        assertFirstInMultirowRS();
        
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        assertTrue(_rset.next());
        assertEquals(4, _rset.getInt("id"));
        assertLastInMultirowRS();
        
        assertFalse(_rset.next());
        assertAfterLast();
        
        assertTrue(_rset.previous());
        assertEquals(4, _rset.getInt("id"));
        assertLastInMultirowRS();
        
        assertTrue(_rset.previous());
        assertEquals(3, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        assertTrue(_rset.previous());
        assertEquals(2, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        assertTrue(_rset.previous());
        assertEquals(1, _rset.getInt("id"));
        assertFirstInMultirowRS();
        
        assertFalse(_rset.previous());
        assertBeforeFirst();

        _rset.afterLast();
        assertAfterLast();
        
        _rset.beforeFirst();
        assertBeforeFirst();
        
        _rset.relative(3);
        assertEquals(3, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        _rset.relative(-2);
        assertEquals(1, _rset.getInt("id"));
        assertFirstInMultirowRS();
        
        _rset.relative(3);
        assertEquals(4, _rset.getInt("id"));
        assertLastInMultirowRS();
        
        _rset.relative(-1);
        assertEquals(3, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        _rset.relative(2);
        assertAfterLast();
    }
    
    public void testPreviousFollowingAfterLast() throws Exception {
        createUpdateTable();
        assertBeforeFirst();

        _rset.afterLast();
        assertTrue(_rset.previous());
        assertTrue(_rset.previous());
        assertTrue(_rset.next());
        assertFalse(_rset.next());
        assertTrue(_rset.isAfterLast());
    }

    public void testDeleteFirstThenOtherRows() throws Exception {
        createUpdateTable();
        
        // Delete first row using first.
        _rset.first();
        _rset.deleteRow();
        
        // Per JDBC spec, cursor should now point to just before the next valid row, i.e., row 2.
        // Advance the cursor and test that row 2 is the current row.
        assertTrue(_rset.next());
        assertEquals(2, _rset.getInt("id"));
        
        // Now move forward to row 4, go back once to row 3, and delete it.
        assertTrue(_rset.next());
        assertTrue(_rset.next());
        assertEquals(4, _rset.getInt("id"));
        assertTrue(_rset.previous());
        assertEquals(3, _rset.getInt("id"));
        _rset.deleteRow();
        
        // Per JDBC spec, cursor should now point to just before the next valid row, i.e., row 4.
        // Advance the cursor and test that row 4 is the current row.
        assertTrue(_rset.next());
        assertEquals(4, _rset.getInt("id"));
        assertTrue(_rset.isLast());
        
        // Delete row 4.
        _rset.deleteRow();
        assertFalse(_rset.next());
        assertAfterLast();
        
        assertTrue(_rset.previous());
        assertEquals(2, _rset.getInt("id"));
        assertTrue(_rset.isFirst());
    }
    
    public void testDeleteRow2ThenOtherRows() throws Exception {
        createUpdateTable();
        
        // Delete row 2.
        assertTrue(_rset.absolute(2));
        _rset.deleteRow();
        
        // Per JDBC spec, cursor should now point to just before the next valid row, i.e., row 3.
        // Advance the cursor and test that row 3 is indeed the current row.
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt("id"));
        assertInMiddleInMultirowRS();
        
        // Move backward 1 row, then assert that this is the first row, then delete it.
        assertTrue(_rset.previous());
        assertEquals(1, _rset.getInt("id"));
        assertFirstInMultirowRS();
        _rset.deleteRow();
        
        // Per JDBC spec, cursor should now point to just before the next valid row, i.e., row 3.
        // Advance the cursor and test that row 3 is indeed the current row.
        assertTrue(_rset.next());
        assertEquals(3, _rset.getInt("id"));
        assertTrue(_rset.isFirst());
        _rset.deleteRow();
        
        // Per JDBC spec, cursor should now point to just before the next valid row, i.e., row 4.
        // Advance the cursor and test that row 4 is indeed the current row.
        assertTrue(_rset.next());
        assertEquals(4, _rset.getInt("id"));
        _rset.deleteRow();
        
        // There should be no more rows.
        assertFalse(_rset.next());
    }
    
    public void testDeleteEndThenOtherRows() throws Exception {
        createUpdateTable();
        
        // Delete last item (row 4).
        assertTrue(_rset.last());
        _rset.deleteRow();
        
        assertFalse(_rset.isLast());
        assertTrue(_rset.isAfterLast());
        
        // Move back 2 rows, then move ahead and assert that row 3 is the last row.
        assertTrue(_rset.previous());
        assertTrue(_rset.previous());
        assertTrue(_rset.next());
        assertTrue(_rset.isLast());
        
        // Move back to original row 2 and delete it.
        assertTrue(_rset.previous());
        assertEquals(2, _rset.getInt("id"));
        _rset.deleteRow();
        
        // Now verify that we are on the last row (#2 of 2 remaining rows)
        assertTrue(_rset.next());
        assertTrue(_rset.isLast());
        assertEquals(3, _rset.getInt("id"));
        assertFalse(_rset.next());
        
        // Now go back and delete row 1.
        assertTrue(_rset.first());
        assertTrue(_rset.isFirst());
        assertFalse(_rset.isBeforeFirst());
        _rset.deleteRow();
        
        assertTrue(_rset.next());
        assertTrue(_rset.isFirst());
    }
    
    public void testNegativeDeleteRow() throws Exception {
        createBasicTable();
        try {
            _rset.deleteRow();
            fail("Expected SQLException - not an updateable ResultSet.");
        } catch (SQLException expected) {
            // expected.
        }
        
        createUpdateTable();
        _rset.moveToInsertRow();
        try {
            _rset.deleteRow();
            fail("Expected SQLException - cannot call deleteRow() when on insert row.");
        } catch (SQLException expected) {
            // expected.
        }
    }

    public void testMoveToCurrentRow() throws Exception {
        createBasicTable();
        try {
            _rset.moveToCurrentRow();
            fail("Expected SQLException - not an updateable ResultSet.");
        } catch (SQLException expected) {
            // expected.
        }
        _rset.close();
        
        createUpdateTable();
        _rset.moveToCurrentRow();
    }

    public void testMoveToInsertRow() throws Exception {
        createBasicTable();
        try {
            _rset.moveToInsertRow();
            fail("Expected SQLException - not an updateable ResultSet");
        } catch (SQLException ignore) {
            // expected.
        }
        _rset.close();
        
        
        createUpdateTable();
        _rset.moveToInsertRow();
    }

    public void testInsertRowOnce() throws Exception {
        createUpdateTableWithNotNullColumn();
        _rset.next();
        
        // Remember values of current row for comparison later after invoking moveToCurrentRow().
        final Object currentValue1 = _rset.getObject(1); 
        final Object currentValue2 = _rset.getObject(2);
        final Object currentValue3 = _rset.getObject(3);
        final Object currentValue4 = _rset.getObject(4);
        final Object currentValue5 = _rset.getObject(5);
        final Object currentValue6 = _rset.getObject(6);
        
        try {
            _rset.insertRow();
            fail("Expected SQLException");
        } catch (SQLException expected) {
            // expected - _rset is not yet on insert row.
        }

        final Object newValue1 = new Integer(Integer.MAX_VALUE);
        final Object newValue2 = "MAX_VALUE";
        final Object newValue6 = Boolean.TRUE;
        
        _rset.moveToInsertRow();
        
        _rset.updateObject(1, newValue1);
        assertEquals(newValue1, _rset.getObject(1));
        _rset.updateObject(2, newValue2);
        assertEquals(newValue2, _rset.getObject(2));
        _rset.updateObject(6, newValue6);
        assertEquals(newValue6, _rset.getObject(6));
        _rset.insertRow();
        
        _rset.moveToCurrentRow();

        // Check that current row values reflect those we saved earlier.
        assertEquals(currentValue1, _rset.getObject(1));
        assertEquals(currentValue2, _rset.getObject(2));
        assertEquals(currentValue3, _rset.getObject(3));
        assertEquals(currentValue4, _rset.getObject(4));
        assertEquals(currentValue5, _rset.getObject(5));
        assertEquals(currentValue6, _rset.getObject(6));
        
        // Check that inserted row does indeed exist.
        ResultSet inserted = null;
        try {
            inserted = _stmt.executeQuery("select * from foo where id = " + Integer.MAX_VALUE);
            inserted.next();
            
            assertEquals(newValue1, inserted.getObject(1));
            assertEquals(newValue2, inserted.getObject(2));
            assertNull(inserted.getObject(3));
            assertNull(inserted.getObject(4));
            assertNull(inserted.getObject(5));
            assertEquals(newValue6, inserted.getObject(6));
        } finally {
            if (inserted != null) {
                inserted.close();
            }
        }
    }
    
    public void testInsertRowMultipleTimes() throws Exception {
        createUpdateTable();
        
        assertTrue(_rset.next());
        
        final Object newRow1Value1 = new Integer(Integer.MAX_VALUE - 1);
        final Object newRow1Value2 = "MAX - 1";
        final Object newRow1Value6 = Boolean.TRUE;
        
        _rset.moveToInsertRow();
        
        _rset.updateObject(1, newRow1Value1);
        _rset.updateObject(2, newRow1Value2);
        _rset.updateObject(6, newRow1Value6);
        
        _rset.insertRow();
        
        final Object newRow2Value1 = new Integer(Integer.MAX_VALUE);
        final Object newRow2Value2 = "MAX_VALUE";

        // Field 6 should still be the same as newRow1Value6. 
        _rset.updateObject(1, newRow2Value1);
        _rset.updateObject(2, newRow2Value2);
        
        _rset.insertRow();
        _rset.close();
        
        // Check that inserted row does indeed exist.
        ResultSet inserted = null;
        try {
            inserted = _stmt.executeQuery("select * from foo where id > 4 order by id");
            
            inserted.next();
            assertEquals(newRow1Value1, inserted.getObject(1));
            assertEquals(newRow1Value2, inserted.getObject(2));
            assertNull(inserted.getObject(3));
            assertNull(inserted.getObject(4));
            assertNull(inserted.getObject(5));
            assertEquals(newRow1Value6, inserted.getObject(6));
            
            inserted.next();
            assertEquals(newRow2Value1, inserted.getObject(1));
            assertEquals(newRow2Value2, inserted.getObject(2));
            assertNull(inserted.getObject(3));
            assertNull(inserted.getObject(4));
            assertNull(inserted.getObject(5));
            
            // Since we didn't modify the value of field 6 in the first insert, it should be
            // the same for the second insert.
            assertEquals(newRow1Value6, inserted.getObject(6));
        } finally {
            if (inserted != null) {
                inserted.close();
            }
        }        
    }
    
    public void testNegativeInsertRow() throws Exception {
        createBasicTable();
        try {
            _rset.insertRow();
            fail("Expected SQLException - not an updateable ResultSet.");
        } catch (SQLException expected) {
             // Expected.
        }
        
        createUpdateTableWithNotNullColumn();
        _rset.next();
        _rset.moveToInsertRow();
        
        final Object newValue1 = new Integer(Integer.MAX_VALUE);
        final Object newValue2 = "MAX_VALUE";
        
        _rset.updateObject(1, newValue1);
        _rset.updateObject(2, newValue2);
        try {
            _rset.insertRow();
            fail("Expected SQLException - non-null column not populated");
        } catch (SQLException expected) {
            if (!"22004".equals(expected.getSQLState())) {
                fail("Expected SQLState 22004:  null value not allowed");
            }
        }
        
        _rset.moveToCurrentRow();
        _rset.updateInt(1, 1000);
        try {
            _rset.insertRow();
            fail("Expected SQLException - not on insert row");
        } catch (SQLException expected) {
            // Expected.
        }
    }    

    public void testUpdateRow() throws Exception {
        createUpdateTable();

        final Object newValue1 = new Integer(Integer.MAX_VALUE);
        final Object newValue2 = "MAX_VALUE";
        
        while (_rset.next()) {
            Object oldValue3 = _rset.getObject(3);

            // TODO Test Date, Time and Timestamp when TimeZone complications are handled correctly.
            _rset.updateObject(1, newValue1);
            _rset.updateObject(2, newValue2);
            _rset.updateRow();

            // Check that updated values appear in the current row.
            assertEquals(newValue1, _rset.getObject(1));
            assertEquals(newValue2, _rset.getObject(2));
            assertEquals(oldValue3, _rset.getObject(3));
        }

        // Now open a new ResultSet and ensure that the changed columns are reflected
        // there as well.
        _rset.close();
        _rset = _stmt.executeQuery("select * from foo");
        while (_rset.next()) {
            assertEquals(newValue1, _rset.getObject(1));
            assertEquals(newValue2, _rset.getObject(2));
        }

        // Now close the ResultSet and Statement, create a read-only, forward-only
        // statement and ensure that the changed columns are reflected in its 
        // ResultSet as well.
        _rset.close();
        _stmt.close();
        _stmt = (AxionStatement) _conn.createStatement();
        _rset = _stmt.executeQuery("select * from foo");
        while (_rset.next()) {
            assertEquals(newValue1, _rset.getObject(1));
            assertEquals(newValue2, _rset.getObject(2));
        }
    }
    
    public void testUpdateNonNullColumn() throws Exception {
        createUpdateTableWithNotNullColumn();
        
        assertTrue(_rset.next());
        _rset.updateNull(6);
        
        try {
            _rset.updateRow();
            fail("Expected SQLException");
        } catch (SQLException expected) {
            if (!"22004".equals(expected.getSQLState())) {
                fail("Expected SQLState 22004:  null value not allowed");
            }
        }
        
        _rset.updateBoolean(6, true);
        _rset.updateRow();
        assertTrue(_rset.getBoolean(6));
    }
    
    public void testNegativeUpdateRow() throws Exception {
        createBasicTable();
        try {
            _rset.deleteRow();
            fail("Expected SQLException - not an updateable ResultSet.");
        } catch (SQLException expected) {
            // expected.
        }
        
        createUpdateTable();
        _rset.moveToInsertRow();
        
        try {
            _rset.updateRow();
        } catch (SQLException expected) {
            // expected.
        }
    }
    
    /*
     * Class under test for void updateNull(int)
     */
    public void testUpdateNullint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateNull(1);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Set int column to null and verify.
        _rset.next();
        _rset.updateNull(1);
        _rset.updateRow();
        assertNull(_rset.getObject(1));

        // Set varchar column to null and verify.
        _rset.updateNull(2);
        _rset.updateRow();
        assertNull(_rset.getObject(2));

        // Set date column to null and verify.
        _rset.updateNull(3);
        _rset.updateRow();
        assertNull(_rset.getObject(3));

        // Set time column to null and verify.
        _rset.updateNull(4);
        _rset.updateRow();
        assertNull(_rset.getObject(4));

        // Set time column to null and verify.
        _rset.updateNull(5);
        _rset.updateRow();
        assertNull(_rset.getObject(5));
    }

    /*
     * Class under test for void updateByte(int, byte)
     */
    public void testUpdateByteintbyte() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateByte(1, (byte) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        _rset.next();
        _rset.updateByte(1, (byte) 127);
        _rset.updateRow();
        assertEquals((byte) 127, _rset.getByte(1));
    }

    /*
     * Class under test for void updateDouble(int, double)
     */
    public void testUpdateDoubleintdouble() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateDouble(1, 0.0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        _rset.next();
        _rset.updateDouble(1, 1234567890.0);
        _rset.updateRow();
        assertEquals(1234567890.0, _rset.getDouble(1), 0.00001);
    }

    /*
     * Class under test for void updateFloat(int, float)
     */
    public void testUpdateFloatintfloat() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateFloat(1, 0.0f);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        _rset.next();
        _rset.updateFloat(1, 1234567.0f);
        _rset.updateRow();
        assertEquals(1234567.0f, _rset.getDouble(1), 0.00001);
    }

    /*
     * Class under test for void updateInt(int, int)
     */
    public void testUpdateIntintint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateInt(1, 1);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        _rset.next();
        _rset.updateInt(1, 1234567);
        _rset.updateRow();
        assertEquals(1234567, _rset.getInt(1));
    }

    /*
     * Class under test for void updateLong(int, long)
     */
    public void testUpdateLongintlong() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateLong(1, 0L);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final long newVal = 123457890L;
        _rset.updateLong(1, newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getInt(1));
    }

    /*
     * Class under test for void updateShort(int, short)
     */
    public void testUpdateShortintshort() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateShort(1, (short) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        _rset.updateShort(1, Short.MAX_VALUE);
        _rset.updateRow();
        assertEquals(Short.MAX_VALUE, _rset.getShort(1));
    }

    /*
     * Class under test for void updateBoolean(int, boolean)
     */
    public void testUpdateBooleanintboolean() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBoolean(1, false);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        _rset.next();

        // Expect a SQLException since boolean cannot be converted to a numeric in Axion.
        try {
            _rset.updateBoolean(1, true);
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        final boolean newVal = true;
        _rset.updateBoolean(6, newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getBoolean(6));
    }

    /*
     * Class under test for void updateBytes(int, byte[])
     */
    public void testUpdateBytesintbyteArray() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBytes(1, new byte[0]);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateAsciiStream(int, InputStream, int)
     */
    public void testUpdateAsciiStreamintInputStreamint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateAsciiStream(1, System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateBinaryStream(int, InputStream, int)
     */
    public void testUpdateBinaryStreamintInputStreamint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBinaryStream(1, System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateCharacterStream(int, Reader, int)
     */
    public void testUpdateCharacterStreamintReaderint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateCharacterStream(1, new InputStreamReader(System.in), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateObject(int, Object)
     */
    public void testUpdateObjectintObject_NoCurrentRow() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateObject(1, new Object());
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testUpdateObjectintObject_Integer() throws Exception {
        createUpdateTable();
        _rset.next();

        final Integer newInt = new Integer(123);

        _rset.updateObject(1, newInt);
        _rset.updateRow();
        assertEquals(newInt, _rset.getObject(1));

        _rset.updateObject(1, newInt.toString());
        _rset.updateRow();
        assertEquals(newInt, _rset.getObject(1));
    }

    public void testUpdateObjectintObject_Float() throws Exception {
        createUpdateTable();
        _rset.next();

        final Float newFloat = new Float(456.0f);

        _rset.updateObject(1, newFloat);
        _rset.updateRow();
        assertEquals(new Integer(newFloat.intValue()), _rset.getObject(1));

        _rset.updateObject(1, newFloat.toString());
        _rset.updateRow();
        assertEquals(new Integer(newFloat.intValue()), _rset.getObject(1));
    }

    public void testUpdateObjectintObject_Double() throws Exception {
        createUpdateTable();
        _rset.next();

        final Double newDouble = new Double(789.0);

        _rset.updateObject(1, newDouble);
        _rset.updateRow();
        assertEquals(new Integer(newDouble.intValue()), _rset.getObject(1));

        _rset.updateObject(1, newDouble.toString());
        _rset.updateRow();
        assertEquals(new Integer(newDouble.intValue()), _rset.getObject(1));
    }

    public void testUpdateObjectintObject_String() throws Exception {
        createUpdateTable();
        _rset.next();

        final String newStr = "new string";

        _rset.updateObject(2, newStr);
        _rset.updateRow();
        assertEquals(newStr, _rset.getObject(2));
    }

    public void testUpdateObjectintObject_DateTimeTimestamp() throws Exception {
        createUpdateTable();
        _rset.next();

        final long now = System.currentTimeMillis();
        final Date newDate = new Date(now);
        final Time newTime = new Time(now);
        final Timestamp newTimestamp = new Timestamp(now);

        _rset.updateObject(3, newDate);
        _rset.updateRow();
        assertEquals(newDate, _rset.getObject(3));

        _rset.updateObject(4, newTime);
        _rset.updateRow();
        assertEquals(newTime, _rset.getObject(4));

        _rset.updateObject(5, newTimestamp);
        _rset.updateRow();
        assertEquals(newTimestamp, _rset.getObject(5));
    }

    public void testUpdateObjectintObject_Boolean() throws Exception {
        createUpdateTable();
        _rset.next();

        final Boolean newVal = Boolean.TRUE;
        _rset.updateObject(6, newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getObject(6));
    }

    /*
     * Class under test for void updateObject(int, Object, int)
     */
    public void testUpdateObjectintObjectint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateObject(1, new Object(), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateString(int, String)
     */
    public void testUpdateStringintString() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateString(1, "");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final String newStr = "new string";
        _rset.updateString(2, newStr);
        _rset.updateRow();
        assertEquals(newStr, _rset.getString(2));
    }

    /*
     * Class under test for void updateNull(String)
     */
    public void testUpdateNullString() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateNull("id");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        _rset.updateNull("id");
        _rset.updateRow();
        _rset.getInt("id");
        assertEquals(0, _rset.getInt("id"));
        assertTrue(_rset.wasNull());
    }

    /*
     * Class under test for void updateByte(String, byte)
     */
    public void testUpdateByteStringbyte() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateByte("id", (byte) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final byte newVal = (byte) 127;
        _rset.updateByte("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getByte("id"));
    }

    /*
     * Class under test for void updateDouble(String, double)
     */
    public void testUpdateDoubleStringdouble() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateDouble("id", 0.0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final double newVal = 42.0;
        _rset.updateDouble("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getDouble("id"), 0.1);
    }

    /*
     * Class under test for void updateFloat(String, float)
     */
    public void testUpdateFloatStringfloat() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateFloat("id", 0.0f);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final float newVal = 42.0f;
        _rset.updateFloat("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getFloat("id"), 0.1f);
    }

    /*
     * Class under test for void updateInt(String, int)
     */
    public void testUpdateIntStringint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateInt("id", 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final int newVal = 150000;
        _rset.updateInt("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getInt("id"));
    }

    /*
     * Class under test for void updateLong(String, long)
     */
    public void testUpdateLongStringlong() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateLong("id", 0L);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final long newVal = 150000L;
        _rset.updateLong("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getLong("id"));
    }

    /*
     * Class under test for void updateShort(String, short)
     */
    public void testUpdateShortStringshort() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateShort("id", (short) 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final short newVal = (short) 32767;
        _rset.updateShort("id", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getShort("id"));
    }

    /*
     * Class under test for void updateBoolean(String, boolean)
     */
    public void testUpdateBooleanStringboolean() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBoolean("id", false);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final boolean newVal = true;
        _rset.updateBoolean("bool", newVal);
        _rset.updateRow();
        assertTrue(_rset.getBoolean("bool"));
    }

    /*
     * Class under test for void updateBytes(String, byte[])
     */
    public void testUpdateBytesStringbyteArray() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBytes("id", new byte[0]);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateBigDecimal(int, BigDecimal)
     */
    public void testUpdateBigDecimalintBigDecimal() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBigDecimal("id", BigDecimal.valueOf(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

    }

    /*
     * Class under test for void updateArray(int, Array)
     */
    public void testUpdateArrayintArray() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateArray(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateBlob(int, Blob)
     */
    public void testUpdateBlobintBlob() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBlob(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateClob(int, Clob)
     */
    public void testUpdateClobintClob() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateClob("id", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateDate(int, Date)
     */
    public void testUpdateDateintDate() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateDate(1, new Date(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        
        Calendar cal = Calendar.getInstance();
        cal.set(2005, 3, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        final Date newVal = new Date(cal.getTime().getTime());
        _rset.updateDate("dt", newVal);
        _rset.updateRow();
        
        assertEquals(newVal, _rset.getDate("dt"));
    }

    /*
     * Class under test for void updateRef(int, Ref)
     */
    public void testUpdateRefintRef() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateRef(1, null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateTime(int, Time)
     */
    public void testUpdateTimeintTime() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateTime(1, new Time(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final Time newVal = new Time(System.currentTimeMillis());
        _rset.updateTime(4, newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getTime(4));
    }

    /*
     * Class under test for void updateTimestamp(int, Timestamp)
     */
    public void testUpdateTimestampintTimestamp() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateTimestamp(1, new Timestamp(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final Timestamp newVal = new Timestamp(System.currentTimeMillis());
        _rset.updateTimestamp(5, newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getTimestamp(5));
    }

    /*
     * Class under test for void updateAsciiStream(String, InputStream, int)
     */
    public void testUpdateAsciiStreamStringInputStreamint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateAsciiStream("foo", System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

    }

    /*
     * Class under test for void updateBinaryStream(String, InputStream, int)
     */
    public void testUpdateBinaryStreamStringInputStreamint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBinaryStream("foo", System.in, 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateCharacterStream(String, Reader, int)
     */
    public void testUpdateCharacterStreamStringReaderint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateCharacterStream("foo", new InputStreamReader(System.in), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateObject(String, Object)
     */
    public void testUpdateObjectStringObject_NoCurrentRow() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateObject("id", new Object());
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }
    }

    public void testUpdateObjectStringObject_Integer() throws Exception {
        createUpdateTable();
        _rset.next();

        final Integer newInt = new Integer(123);

        _rset.updateObject("id", newInt);
        _rset.updateRow();
        assertEquals(newInt, _rset.getObject("id"));

        _rset.updateObject("id", newInt.toString());
        _rset.updateRow();
        assertEquals(newInt, _rset.getObject("id"));
    }

    public void testUpdateObjectStringObject_Float() throws Exception {
        createUpdateTable();
        _rset.next();

        final Float newFloat = new Float(456.0f);

        _rset.updateObject("id", newFloat);
        _rset.updateRow();
        assertEquals(new Integer(newFloat.intValue()), _rset.getObject("id"));

        _rset.updateObject("id", newFloat.toString());
        _rset.updateRow();
        assertEquals(new Integer(newFloat.intValue()), _rset.getObject("id"));
    }

    public void testUpdateObjectStringObject_Double() throws Exception {
        createUpdateTable();
        _rset.next();

        final Double newDouble = new Double(789.0);

        _rset.updateObject("id", newDouble);
        _rset.updateRow();
        assertEquals(new Integer(newDouble.intValue()), _rset.getObject("id"));

        _rset.updateObject("id", newDouble.toString());
        _rset.updateRow();
        assertEquals(new Integer(newDouble.intValue()), _rset.getObject("id"));
    }

    public void testUpdateObjectStringObject_String() throws Exception {
        createUpdateTable();
        _rset.next();

        final String newStr = "new string";

        _rset.updateObject("str_10", newStr);
        _rset.updateRow();
        assertEquals(newStr, _rset.getObject("str_10"));
    }

    public void testUpdateObjectStringObject_DateTimeTimestamp() throws Exception {
        createUpdateTable();
        _rset.next();

        final long now = System.currentTimeMillis();
        final Date newDate = new Date(now);
        final Time newTime = new Time(now);
        final Timestamp newTimestamp = new Timestamp(now);

        _rset.updateObject("dt", newDate);
        _rset.updateRow();
        assertEquals(newDate, _rset.getObject("dt"));

        _rset.updateObject("tm", newTime);
        _rset.updateRow();
        assertEquals(newTime, _rset.getObject("tm"));

        _rset.updateObject("ts", newTimestamp);
        _rset.updateRow();
        assertEquals(newTimestamp, _rset.getObject("ts"));
    }

    public void testUpdateObjectStringObject_DateTimeTimestamp_CaseInsensitivity() throws Exception {
        createUpdateTable();
        _rset.next();

        final long now = System.currentTimeMillis();
        final Date newDate = new Date(now);
        final Time newTime = new Time(now);
        final Timestamp newTimestamp = new Timestamp(now);

        // Ensure case-insensitivity is functional.
        _rset.updateObject("Dt", newDate);
        _rset.updateRow();
        assertEquals(newDate, _rset.getObject("Dt"));

        _rset.updateObject("tM", newTime);
        _rset.updateRow();
        assertEquals(newTime, _rset.getObject("Tm"));

        _rset.updateObject("Ts", newTimestamp);
        _rset.updateRow();
        assertEquals(newTimestamp, _rset.getObject("tS"));
    }

    public void testUpdateObjectStringObject_Boolean() throws Exception {
        createUpdateTable();
        _rset.next();

        final Boolean newVal = Boolean.TRUE;
        _rset.updateObject("bool", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getObject("bool"));
    }    

    /*
     * Class under test for void updateObject(String, Object, int)
     */
    public void testUpdateObjectStringObjectint() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateObject("foo", new Object(), 0);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateString(String, String)
     */
    public void testUpdateStringStringString() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateString("foo", "");
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final String newStr = "new string";
        _rset.updateString("str_10", newStr);
        _rset.updateRow();
        assertEquals(newStr, _rset.getString("str_10"));
    }

    /*
     * Class under test for void updateBigDecimal(String, BigDecimal)
     */
    public void testUpdateBigDecimalStringBigDecimal() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBigDecimal("foo", BigDecimal.valueOf(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateArray(String, Array)
     */
    public void testUpdateArrayStringArray() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateArray("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateBlob(String, Blob)
     */
    public void testUpdateBlobStringBlob() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateBlob("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateClob(String, Clob)
     */
    public void testUpdateClobStringClob() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateClob("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateDate(String, Date)
     */
    public void testUpdateDateStringDate() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateDate("foo", new Date(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
        
        Calendar cal = Calendar.getInstance();
        cal.set(2005, 3, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        
        final Date newVal = new Date(cal.getTime().getTime());
        _rset.updateDate("dt", newVal);
        _rset.updateRow();
        
        assertEquals(newVal, _rset.getDate("dt"));
    }

    /*
     * Class under test for void updateRef(String, Ref)
     */
    public void testUpdateRefStringRef() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateRef("foo", null);
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();
    }

    /*
     * Class under test for void updateTime(String, Time)
     */
    public void testUpdateTimeStringTime() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateTime("foo", new Time(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final Time newVal = new Time(System.currentTimeMillis());
        _rset.updateTime("tm", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getTime("tm"));
    }

    /*
     * Class under test for void updateTimestamp(String, Timestamp)
     */
    public void testUpdateTimestampStringTimestamp() throws Exception {
        createUpdateTable();

        // Test assertion that ResultSet has a current row
        try {
            _rset.updateTimestamp("foo", new Timestamp(0L));
            fail("Expected SQLException");
        } catch (SQLException ignore) {
            // expected.
        }

        // Now test normal usage.
        _rset.next();

        final Timestamp newVal = new Timestamp(System.currentTimeMillis());
        _rset.updateTimestamp("ts", newVal);
        _rset.updateRow();
        assertEquals(newVal, _rset.getTimestamp("ts"));
    }
    
    public void testWasNullWithInsertRow() throws Exception {
        createUpdateTable();
        
        _rset.moveToInsertRow();
        _rset.updateNull(1);
        assertEquals(0, _rset.getInt(1));
        assertTrue(_rset.wasNull());
    }
    
    public void testNegativeForwardOnly() throws Exception {
        // Section 14.2.2, JDBC 3.0 specification:
        // 
        // "For a ResultSet object that is of TYPE_FORWARD_ONLY, the only valid cursor movement is
        // next.  All other cursor movement methods throw an SQLException."
        createForwardOnlyTable();
        
        assertTrue(_rset.next());
        
        try {
            _rset.previous();
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.relative(-2);
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.last();
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.first();
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.afterLast();
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.beforeFirst();
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }
        
        try {
            _rset.absolute(2);
            fail("Expected SQLException - forward-only result set.");
        } catch (SQLException expected) {
            // Expected
        }        
    }
    
    /**
     * Creates Axion database for this test suite.
     * 
     * @return Database instance.
     * @throws Exception
     */
    protected Database createDatabase() throws Exception {
        return Databases.getOrCreateDatabase(getDatabaseName(), getDatabaseDirectory());        
    }
    
    /**
     * Executes cleanup methods that are local to this test suite.  A subclass should override
     * this method as necessary, but be sure to invoke it via super.doCleanup() if it does not
     * already close resources which are bound to this class instance. 
     */
    protected void doCleanup() throws Exception {
        if (_rset != null) {
            _rset.close();
        }

        try {
            if (_stmt != null) {
                _stmt.execute("drop table foo");
            }
        } catch (SQLException ignore) {
            // ignore
        }
        
        if (_stmt != null) {
            _stmt.close();
        }
        
        if (_conn != null) {
            _conn.close();
        }
    }    
    
    /**
     * Gets name of Axion database to use in this test suite.  Should be overridden by subclasses that
     * wish to use a distinct database.
     * 
     * @return
     */
    protected String getDatabaseName() {
        return "JDBC_MemoryDB";
    }

    /**
     * Gets File representing local filesystem directory in which Axion metadata files will be
     * stored - null for this instance since we're testing via memory database.  Should be 
     * overridden by subclasses that use the local file system for persistence.  
     * 
     * @return null for this instance
     */
    protected File getDatabaseDirectory() {
        return null;
    }

    /**
     * @return
     */
    protected Table createTableInstance() throws Exception {
        return new MemoryTable("foo");
    }
    
    /**
     * Creates basic table for use in general ResultSet testing.
     * 
     * @throws Exception if error occurs during table or ResultSet construction
     */
    protected void createBasicTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try {
            _stmt.executeUpdate("drop table foo");
        } catch (SQLException ignore) {
            // ignore - table doesn't exist.
        }
        
        _stmt.executeUpdate("create table foo (test varchar(10), num int)");
        _conn.setAutoCommit(false);
        for (int i = 0; i < IMAX; i++) {
            for (int j = 0; j < JMAX; j++) {
                _stmt.executeUpdate("insert into foo values ('" + String.valueOf((char) (65 + i)) 
                    + "', " + j + ")");
            }
        }
        _conn.commit();
        _conn.setAutoCommit(true);
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        _stmt.executeQuery("select * from foo");
        _rset = _stmt.getCurrentResultSet();
    }

    /**
     * Creates empty table for use in testing position logic against a degenerate AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction 
     */
    protected void createEmptyTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create table foo (id int)");
        
        _rset = _stmt.executeQuery("select * from foo");
    }

    protected void createForwardOnlyTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create table foo (id int)");
        _stmt.execute("insert into foo values (1)");
        _stmt.execute("insert into foo values (2)");
        _stmt.execute("insert into foo values (3)");
        _stmt.execute("insert into foo values (4)");
        
        _rset = _stmt.executeQuery("select * from foo");
    }
    
    /**
     * Creates empty table for use in testing position logic against a degenerate AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction 
     */
    protected void createOneRowTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create table foo (id int)");
        _stmt.execute("insert into foo values (1)");

        _rset = _stmt.executeQuery("select * from foo");
    }
    
    /**
     * Constructs a table for use in testing updating methods in AxionResultSet.
     * 
     * @throws Exception if error occurs during table or ResultSet construction
     */
    protected void createUpdateTable() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create table foo (id int, str_10 varchar(10), dt date, tm time, ts timestamp, bool boolean)");

        _conn.setAutoCommit(false);
        _stmt.execute("insert into foo values (1, 'This is 1', '2005-01-01', '12:34:56', '2005-03-31 23:56:00.0', false)");
        _stmt.execute("insert into foo values (2, 'This is 2', '2005-02-02', '23:45:00', '2005-04-01 00:00:00.0', false)");
        _stmt.execute("insert into foo values (3, 'This is 3', '2005-03-03', '06:30:30', '2005-04-02 01:23:45.6', false)");
        _stmt.execute("insert into foo values (4, 'This is 4', '2005-04-04', '07:45:45', '2005-04-03 02:34:32.1', false)");
        _conn.commit();
        _conn.setAutoCommit(true);
        
        _rset = _stmt.executeQuery("select * from foo");
    }
    
    protected void createUpdateTableWithNotNullColumn() throws Exception {
        if (_stmt != null) {
            _stmt.close();
        }
        
        _stmt = (AxionStatement) _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            _stmt.execute("drop table foo");
        } catch (SQLException ignore) {
            // ignore and continue            
        }
        _stmt.execute("create table foo (id int, str_10 varchar(10), dt date, tm time, ts timestamp, bool boolean not null)");

        _conn.setAutoCommit(false);
        _stmt.execute("insert into foo values (1, 'This is 1', '2005-01-01', '12:34:56', '2005-03-31 23:56:00.0', false)");
        _stmt.execute("insert into foo values (2, 'This is 2', '2005-02-02', '23:45:00', '2005-04-01 00:00:00.0', false)");
        _stmt.execute("insert into foo values (3, 'This is 3', '2005-03-03', '06:30:30', '2005-04-02 01:23:45.6', false)");
        _stmt.execute("insert into foo values (4, 'This is 4', '2005-04-04', '07:45:45', '2005-04-03 02:34:32.1', false)");
        _conn.commit();
        _conn.setAutoCommit(true);
        
        _rset = _stmt.executeQuery("select id, str_10, dt, tm, ts, bool from foo");
    }
    
    /**
     * Tests indicators that must be false when the result set is empty.
     *  
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertEmptyResultSet() throws Exception {
        assertFalse(_rset.isBeforeFirst());
        assertFalse(_rset.isFirst());
        assertFalse(_rset.isLast());
        assertFalse(_rset.isAfterLast());
    }

    /**
     * Tests indicators that show that the result set cursor is positioned before the first row in the 
     * ResultSet. 
     * 
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertBeforeFirst() throws Exception {
        assertTrue(_rset.isBeforeFirst());
        assertFalse(_rset.isFirst());
        assertFalse(_rset.isLast());
        assertFalse(_rset.isAfterLast());
    }

    /**
     * Tests indicators that show that the result set is in the middle of a ResultSet and not in either of
     * the extremes - use only for multi-row ResultSets. 
     * 
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertInMiddleInMultirowRS() throws Exception {
        assertFalse(_rset.isBeforeFirst());
        assertFalse(_rset.isFirst());
        assertFalse(_rset.isLast());
        assertFalse(_rset.isAfterLast());
    }
    
    /**
     * Tests indicators that show that the result set cursor is on the first row in the 
     * ResultSet - use only for multi-row ResultSets. 
     * 
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertFirstInMultirowRS() throws Exception {
        assertFalse(_rset.isBeforeFirst());
        assertTrue(_rset.isFirst());
        assertFalse(_rset.isLast());
        assertFalse(_rset.isAfterLast());
    }
    
    /**
     * Tests indicators that show that the result set cursor is on the last row in the 
     * ResultSet - use only for multi-row ResultSets.   
     * 
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertLastInMultirowRS() throws Exception {
        assertFalse(_rset.isBeforeFirst());
        assertFalse(_rset.isFirst());
        assertTrue(_rset.isLast());
        assertFalse(_rset.isAfterLast());
    }

    /**
     * Tests indicators that show that result set cursor is positioned after the last row in the 
     * ResultSet. 
     * 
     * @throws Exception if error occurs while executing methods under test.
     */
    protected void assertAfterLast() throws Exception {
        assertFalse(_rset.isBeforeFirst());
        assertFalse(_rset.isFirst());
        assertFalse(_rset.isLast());
        assertTrue(_rset.isAfterLast());
    }

    
    /**
     * Drops Axion database used for this test suite.
     */
    private void dropDatabase() {
        Databases.forgetDatabase(getDatabaseName());
    }
}
