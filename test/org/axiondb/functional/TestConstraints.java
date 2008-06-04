/*
 * $Id: TestConstraints.java,v 1.4 2008/02/21 13:00:26 jawed Exp $
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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.4 $ $Date: 2008/02/21 13:00:26 $
 * @author Rodney Waldhoff
 */
public class TestConstraints extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestConstraints(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestConstraints.class);
    }

    //--------------------------------------------------------------- Lifecycle

    //-------------------------------------------------------------------- Util
    
    private void createTableFoobar() throws Exception {
        _stmt.execute("create table FOOBAR ( NUM1 integer, NUM2 integer, STR varchar2, PRIMARY KEY (NUM1, NUM2) )");
    }

    private void populateTableFoobar() throws Exception {
        PreparedStatement pstmt = _conn.prepareStatement("insert into FOOBAR ( NUM1, NUM2, STR ) values ( ?, ?, ?)");
        for(int i=0;i<NUM_ROWS_IN_FOO;i++) {
            pstmt.setInt(1,i);
            pstmt.setInt(2,i);
            pstmt.setString(3,String.valueOf(i));
            pstmt.executeUpdate();
        }
        pstmt.close();
    }
    
    private void addNotNullConstraintToFooNum() throws Exception {
        _stmt.execute("alter table FOO add constraint FOO_NUM_NN not null ( NUM )");
    }

    private void addDeferredNotNullConstraintToFooNum() throws Exception {
        _stmt.execute("alter table FOO add constraint FOO_NUM_NN not null ( NUM ) deferrable initially deferred");
    }

    private void addUniqueConstraintToFooNum() throws Exception {
        _stmt.execute("alter table FOO add constraint FOO_NUM_UNIQUE unique ( NUM )");
    }

    private void addUniqueConstraintToFooStr() throws Exception {
        _stmt.execute("alter table FOO add constraint FOO_STR_UNIQUE unique ( STR )");
    }

    private void addUniqueConstraintToUpperFooStr() throws Exception {
        _stmt.execute("alter table FOO add constraint FOO_USTR_UNIQUE unique ( upper(STR) )");
    }
    
    private void addNotEmptyOrNullConstraintToFooStr() throws Exception {
        _stmt.execute("alter table FOO add constraint STR_NOT_EMPTY check ( STR is not null and str <> '' )");
    }
    
    //------------------------------------------------------------------- Tests


    public void testAddPrimaryKey() throws Exception {
        createTableFoo();   
        _stmt.execute("alter table FOO add constraint primary key (NUMTWO)");     
    }
    
    public void testAddForeignKey() throws Exception {
        createTableFoo(); 
        createTableBar();
        try {
            _stmt.execute("alter table FOO add constraint foreign key (NUM) REFERENCES BAR(ID)");
            fail("Expected SQLException: Key not found");
        } catch(SQLException e) {
            // expected
        }
        
        _stmt.execute("alter table BAR add constraint primary key (ID) ");
        _stmt.execute("alter table FOO add constraint foreign key (NUM) REFERENCES BAR(ID)");
        
        _stmt.execute("insert into BAR ( ID, DESCR, DESCR2 ) values ( 1, '1', '11')");
        _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 1, '1', 1)");
        
        try {
            _stmt.execute("insert into FOO ( NUM, STR, NUMTWO ) values ( 2, '1', 1)");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testCreateInvalidForeignKey() throws Exception {
        try {
            _stmt.execute("create table FOO ( NUM integer, STR varchar2(255), NUMTWO integer foreign key (NUM) REFERENCES BAR(ID))");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        
        // make sure if child command fails then table should not have been created.
        assertFalse(_conn.getDatabase().hasTable("FOO"));
    }

    public void testAddAtMostOnePrimaryKey() throws Exception {
        createTableFoo();   
        _stmt.execute("alter table FOO add constraint FOO_PK primary key (NUM)");     
        try {
            _stmt.execute("alter table FOO add constraint BAR_PK primary key (NUMTWO)");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        _stmt.execute("alter table FOO drop constraint FOO_PK");
        _stmt.execute("alter table FOO add constraint FOO_PK primary key (NUM)");     
    }

    public void testInsertNotNullInt() throws Exception {
        createTableFoo();        
        addNotNullConstraintToFooNum();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", '" + (NUM_ROWS_IN_FOO+1) + "', " + (NUM_ROWS_IN_FOO+1) + " )"));
    }

    public void testPrimaryKeyProhibitsNull() throws Exception {
        createTableFoobar();        
        try {
            _stmt.executeUpdate("insert into FOOBAR values ( null, 0, '0' )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsertNullInt() throws Exception {
        createTableFoo();        
        addNotNullConstraintToFooNum();
        populateTableFoo();
        try {
            _stmt.executeUpdate("insert into FOO values ( null, '0', 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testUpdateToNonNullInt() throws Exception {
        createTableFoo();        
        addNotNullConstraintToFooNum();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("update FOO set num = 2 where num = 2"));
    }

    public void testUpdateToNullInt() throws Exception {
        createTableFoo();        
        addNotNullConstraintToFooNum();
        populateTableFoo();
        try {
            _stmt.executeUpdate("update FOO set num = null where num = 2");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsertUniqueInt() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooNum();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", '" + (NUM_ROWS_IN_FOO+1) + "', " + (NUM_ROWS_IN_FOO+1) + " )"));
    }

    public void testInsertNonUniqueInt() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooNum();
        populateTableFoo();
        try {
            _stmt.executeUpdate("insert into FOO values ( 0, '0', 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsertUniqueStr() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", '" + (NUM_ROWS_IN_FOO+1) + "', " + (NUM_ROWS_IN_FOO+1) + " )"));
    }

    public void testInsertNonUniqueStr() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooStr();
        populateTableFoo();
        try {
            _stmt.executeUpdate("insert into FOO values ( 0, '0', 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testUpdateUniqueInt() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooNum();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("update FOO set num = 2 where str = '2'"));
    }

    public void testUpdateNonUniqueInt() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooNum();
        populateTableFoo();
        try {
            _stmt.executeUpdate("update FOO set num = 2 where num = 3");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testInsertUniquePrimaryKey() throws Exception {
        createTableFoobar();        
        populateTableFoobar();
        assertEquals(1,_stmt.executeUpdate("insert into FOOBAR values ( " + (NUM_ROWS_IN_FOO+1) + ", " + (NUM_ROWS_IN_FOO+1) + ", '" + (NUM_ROWS_IN_FOO+1) + "' )"));
        assertEquals(1,_stmt.executeUpdate("insert into FOOBAR values ( 0, 1, null )"));
        assertEquals(1,_stmt.executeUpdate("insert into FOOBAR values ( 1, 0, null )"));
    }

    public void testInsertNonUniquePrimaryKey() throws Exception {
        createTableFoo();        
        addUniqueConstraintToFooNum();
        populateTableFoo();
        try {
            _stmt.executeUpdate("insert into FOOBAR values ( 0, 0, '0')");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testDeferredSuccess() throws Exception {
        _conn.setAutoCommit(false);
        createTableFoo();        
        addDeferredNotNullConstraintToFooNum();
        populateTableFoo();
        _conn.commit();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", '" + (NUM_ROWS_IN_FOO+1) + "', " + (NUM_ROWS_IN_FOO+1) + " )"));
        _conn.commit();
    }

    public void testDeferredSuccess2() throws Exception {
        _conn.setAutoCommit(false);
        createTableFoo();        
        addDeferredNotNullConstraintToFooNum();
        populateTableFoo();
        _conn.commit();
        _stmt.executeUpdate("insert into FOO values ( null, '0', 0 )");
        _stmt.executeUpdate("delete from FOO where NUM is null");
        _conn.commit();
    }

    public void testDeferredFailure() throws Exception {
        _conn.setAutoCommit(false);
        createTableFoo();        
        addDeferredNotNullConstraintToFooNum();
        populateTableFoo();
        _conn.commit();
        _stmt.executeUpdate("insert into FOO values ( null, '0', 0 )");
        try {
            _conn.commit();
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testInsertUniqueUpperStr() throws Exception {
        createTableFoo();        
        addUniqueConstraintToUpperFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", 'test', " + (NUM_ROWS_IN_FOO+1) + " )"));
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+2) + ", 'test2', " + (NUM_ROWS_IN_FOO+2) + " )"));
    }

    public void testInsertNonUniqueUpperStr() throws Exception {
        createTableFoo();        
        addUniqueConstraintToUpperFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", 'test', " + (NUM_ROWS_IN_FOO+1) + " )"));
        try {
            _stmt.executeUpdate("insert into FOO values ( 0, 'Test', 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    public void testInsertCheckSuccess() throws Exception {
        createTableFoo();        
        addNotEmptyOrNullConstraintToFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", 'test', " + (NUM_ROWS_IN_FOO+1) + " )"));
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+2) + ", 'test2', " + (NUM_ROWS_IN_FOO+2) + " )"));
    }

    public void testInsertCheckFailure1() throws Exception {
        createTableFoo();        
        addNotEmptyOrNullConstraintToFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", 'test', " + (NUM_ROWS_IN_FOO+1) + " )"));
        try {
            _stmt.executeUpdate("insert into FOO values ( 0, '', 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testInsertCheckFailure2() throws Exception {
        createTableFoo();        
        addNotEmptyOrNullConstraintToFooStr();
        populateTableFoo();
        assertEquals(1,_stmt.executeUpdate("insert into FOO values ( " + (NUM_ROWS_IN_FOO+1) + ", 'test', " + (NUM_ROWS_IN_FOO+1) + " )"));
        try {
            _stmt.executeUpdate("insert into FOO values ( 0, null, 0 )");
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }
    
    // Self-Referring Table
    public void testSelfReferringTable() throws Exception {
        _stmt.executeUpdate("create table a(x number(5), y number(5))");
        _stmt.executeUpdate("alter table a add constraint primary key (x)");
        _stmt.executeUpdate("alter table a add constraint foreign key (y) references a(x)");
    }
    
    
    // Dropping child table should drop related FK-constraints
    public void testDropChildTable() throws Exception { 
        _stmt.executeUpdate("create table x(y number(5), constraint x_pk primary key(y))"); 
        _stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x)"); 
        _stmt.executeUpdate("drop table y");
        _stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x)");
    }
    
    public void testNullValueForRefColumns() throws Exception {
        _stmt.executeUpdate("create table a(x number(9) not null, y number(9))");
        _stmt.executeUpdate("create table b(x number(9), y number(9))");
        _stmt.executeUpdate("alter table a add constraint a_pk primary key (x)");
        _stmt.executeUpdate("alter table b add constraint b_fk foreign key (x) references a(x)");
        _stmt.executeUpdate("insert into b values (null, 1)");
        _stmt.executeUpdate("insert into a values (1, 1)");
        _stmt.executeUpdate("insert into b values (1, 1)");
    }
    
    public void testNullValueForRefColumns2() throws Exception {
        _stmt.executeUpdate("create table a(x number(9) not null, y number(9))");
        _stmt.executeUpdate("create table c(x number(9), y number(9))");

        _stmt.executeUpdate("alter table a add constraint a_pk primary key (x,y)");
        _stmt.executeUpdate("alter table c add constraint c_fk foreign key (x,y) references a(x,y)");

        _stmt.executeUpdate("insert into c values (null, null)");
        _stmt.executeUpdate("insert into c values (1, null)");
        _stmt.executeUpdate("insert into a values (1, 1)");
        _stmt.executeUpdate("insert into c values (1, 1)");
    }
    
    //multi-column-PK/FK-feature issues
    public void testMultiColumnPKFK() throws Exception {
        _stmt.executeUpdate("create table A(X number(9,0), Y number(9,0))");
        _stmt.executeUpdate("create table B (X number(9,0),  Y number(9,0))");
        _stmt.executeUpdate("ALTER TABLE A ADD CONSTRAINT A_PK PRIMARY KEY (X,Y)");
        _stmt.executeUpdate("ALTER TABLE B ADD CONSTRAINT B2A FOREIGN KEY (X,Y) REFERENCES A");

        _stmt.executeUpdate("insert into a values (1,1)");
        _stmt.executeUpdate("insert into a values (1,2)");
        _stmt.executeUpdate("insert into b values (1,1)");
        _stmt.executeUpdate("insert into b values (1,2)");
    }
    
    public void testMultiColumnPKFK2() throws Exception {
        _stmt.executeUpdate("create table A  (X number(9,0),  Y number(9,0))");
        _stmt.executeUpdate("create table B (X number(9,0),  Y number(9,0))");
        _stmt.executeUpdate("ALTER TABLE A ADD CONSTRAINT A_PK PRIMARY KEY (X,Y)");

        _stmt.executeUpdate("insert into a values (1,1)");
        _stmt.executeUpdate("insert into a values (1,2)");
        _stmt.executeUpdate("insert into a values (2,1)");
        _stmt.executeUpdate("insert into a values (2,2)");

        _stmt.executeUpdate("ALTER TABLE B ADD CONSTRAINT B2A FOREIGN KEY (X,Y) REFERENCES A(X,Y)");
    }
    
    public void testMultiTableFKForOnePK() throws Exception {
        _stmt.executeUpdate("create table A (X number(9,0))");
        _stmt.executeUpdate("create table B (X number(9,0))"); 
        _stmt.executeUpdate("create table C (Y number(9,0))");

        _stmt.executeUpdate("ALTER TABLE A ADD CONSTRAINT A_PK PRIMARY KEY (X)");
        _stmt.executeUpdate("ALTER TABLE B ADD CONSTRAINT B2A FOREIGN KEY (X)  REFERENCES A");
        _stmt.executeUpdate("ALTER TABLE C ADD CONSTRAINT C2A FOREIGN KEY (Y)  REFERENCES A");
        // TODO: make this test richer        
    }
    
    public void testAutoReferenceFK() throws Exception {
        _stmt.executeUpdate("create table x(y number(5), constraint x_pk primary key(y))"); 
        _stmt.executeUpdate("create table y(y number(5) references x)");
        // TODO: make this test richer
    }
    
    //<referential action> ::= CASCADE | SET NULL | SET DEFAULT | RESTRICT | NO ACTION
    public void testReferentialActions() throws Exception {
        _stmt.executeUpdate("create table x(y number(5), constraint x_pk primary key(y))");
        _stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON DELETE CASCADE)");
        // TODO: make this test richer
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON DELETE SET NULL)");
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON DELETE SET DEFAULT)");
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON DELETE RESTRICT)");
    }
    
    //<referential action> ::= CASCADE | SET NULL | SET DEFAULT | RESTRICT | NO ACTION
    public void testReferentialActions2() throws Exception {
        _stmt.executeUpdate("create table x(y number(5), constraint x_pk primary key(y))");
        _stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON UPDATE CASCADE)");
        // TODO: make this test richer
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON UPDATE SET NULL)");
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON UPDATE SET DEFAULT)");
        //_stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x ON UPDATE RESTRICT)");
    }
    
    // Deferred evaluation of constraints
    public void testDeferredEvaluationOfConstraints() throws Exception {
        _stmt.executeUpdate("create table x(y number(5))");
        _stmt.executeUpdate("alter table x add  constraint x_pk primary key(y) deferrable initially deferred"); 

        _stmt.executeUpdate("create table y(y number(5))");
        _stmt.executeUpdate("alter table y add  constraint y_fk foreign key(y) references x deferrable initially deferred");

        _conn.setAutoCommit(false);
        _stmt.executeUpdate("insert into x values(1)");
        _stmt.executeUpdate("insert into x values(2)");

        _stmt.executeUpdate("insert into y values(2)");
        _conn.commit();
    }
    
    // Dropping of PK while FK exists
    // When PK is deleted the FK depend on it should be deleted too, 
    // may be we can force CASCADE to be used.
    public void testDroppingPKWhileFKExists() throws Exception { 
        _stmt.executeUpdate("create table x(y number(5), constraint x_pk primary key(y))"); 
        _stmt.executeUpdate("create table y(y number(5), constraint y_fk foreign key (y) references x)"); 
        _stmt.executeUpdate("alter table x drop constraint x_pk cascade");
        _stmt.executeUpdate("insert into y values(1)");
    }
    
    // TODO : test "drop table table_name cascade constraints"
    // should delete all foreign keys that reference the table to be dropped, 
    // then drops the table. dropping table should check if there is any view 
    // refering the table.
    
    // TODO : test "truncate table table_name cascade constraints"
    // Allow table to be truncated if all the child tables has zero rows; 
    // fail when child table exist with non-zero row count.
    
    // TODO : test "drop view table_name" 
    // view could be refered by another view; in such case throw exception
    
    // TODO: Test check for reference while:    
    // a) renaming table b) dropping/renaming column. (may be used by index/constraint)
    
    // TODO : Test, should not be able to delete dblink if one or more external table is 
    // still referring it, unless user choose to use cascade.

    // Note: CASCADE can be used to force the operation by removing the refence or object.
    
    // TODO : test "drop table table_name cascade constraints"    
}
