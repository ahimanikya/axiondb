/*
 * $Id: TestInsertCommand.java,v 1.1 2007/11/28 10:01:24 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.commands;

import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:24 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TestInsertCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestInsertCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestInsertCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestInsertCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    org.axiondb.Database _db = null;

    public void setUp() throws Exception {
        _db = new MemoryDatabase("testdb");
        CreateTableCommand cmd = new CreateTableCommand("FOO");
        cmd.addColumn("A", "varchar", "10");
        cmd.execute(_db);

        //create emp source table
        cmd = new CreateTableCommand("EMP");
        cmd.addColumn("ID", "integer");
        cmd.addColumn("FNAME", "varchar", "10");
        cmd.addColumn("LNAME", "varchar", "10");
        cmd.execute(_db);

        //create salary source table
        cmd = new CreateTableCommand("salary");
        cmd.addColumn("eid", "integer");
        cmd.addColumn("base_salary", "integer");
        cmd.execute(_db);

        //create emp target table 1
        cmd = new CreateTableCommand("EMP_TGT1");
        cmd.addColumn("ID", "integer");
        cmd.addColumn("NAME", "varchar", "20");
        cmd.execute(_db);

        //create emp target table 2
        cmd = new CreateTableCommand("EMP_TGT2");
        cmd.addColumn("ID", "integer");
        cmd.addColumn("NAME", "varchar", "20");
        cmd.execute(_db);
        
        //create emp target table 3
        cmd = new CreateTableCommand("EMP_TGT3");
        cmd.addColumn("ID", "integer");
        cmd.addColumn("NAME", "varchar", "20");
        cmd.execute(_db);
    }

    public void tearDown() throws Exception {
        _db.shutdown();
        _db = null;
    }

    //------------------------------------------------------------------- Tests

    public void testInsert() throws Exception {
        assertEquals("Should have no rows", 0, _db.getTable("FOO").getRowCount());
        AxionCommand cmd = new InsertCommand(new TableIdentifier("FOO"), Arrays
            .asList(new ColumnIdentifier[] { new ColumnIdentifier("A")}), Arrays
            .asList(new Literal[] { new Literal("BAR", new CharacterVaryingType(10))}));
        cmd.execute(_db);
        assertEquals("Should have 1 row", 1, _db.getTable("FOO").getRowCount());
    }

    public void testMultiTableInsert() throws Exception {
        InsertCommand cmd = null;

        //**START source table emp
        //first insert some rows into emp table
        TableIdentifier emp_src_tid = new TableIdentifier("EMP");
        ArrayList emp_src_columns = new ArrayList();
        ArrayList emp_src_values = new ArrayList();

        //table columns
        ColumnIdentifier emp_src_id = new ColumnIdentifier("ID");
        emp_src_id.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_id);

        ColumnIdentifier emp_src_fname = new ColumnIdentifier("FNAME");
        emp_src_fname.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_fname);

        ColumnIdentifier emp_src_lname = new ColumnIdentifier("LNAME");
        emp_src_lname.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_lname);

        //first row of table values
        Literal l1 = new Literal(new Integer(1), new IntegerType());
        emp_src_values.add(l1);

        Literal l2 = new Literal("Amy", new CharacterVaryingType(3));
        emp_src_values.add(l2);

        Literal l3 = new Literal("Mathews", new CharacterVaryingType(7));
        emp_src_values.add(l3);

        //insert one row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);

        //second row of table values
        emp_src_values = new ArrayList();
        Literal l4 = new Literal(new Integer(2), new IntegerType());
        emp_src_values.add(l4);

        Literal l5 = new Literal("Linda", new CharacterVaryingType(10));
        emp_src_values.add(l5);

        Literal l6 = new Literal("Tracy", new CharacterVaryingType(10));
        emp_src_values.add(l6);

        //insert second row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);
        
        //3rd row of table values
        emp_src_values = new ArrayList();
        Literal l7 = new Literal(new Integer(3), new IntegerType());
        emp_src_values.add(l7);

        Literal l8 = new Literal("Linda22", new CharacterVaryingType(10));
        emp_src_values.add(l8);

        Literal l9 = new Literal("Tracy22", new CharacterVaryingType(10));
        emp_src_values.add(l9);

        //insert second row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);        

        assertEquals("Should have 3 row", 3, _db.getTable("EMP").getRowCount());

        //**END source table emp

        //emp_tgt1 table
        TableIdentifier emp_tgt1_tid = new TableIdentifier("EMP_TGT1");
        ArrayList emp_tgt1_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt1_id = new ColumnIdentifier("ID");
        emp_tgt1_id.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_id);

        ColumnIdentifier emp_tgt1_name = new ColumnIdentifier("NAME");
        emp_tgt1_name.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_name);

        //emp_tgt2 table
        TableIdentifier emp_tgt2_tid = new TableIdentifier("EMP_TGT2");
        ArrayList emp_tgt2_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt2_id = new ColumnIdentifier("ID");
        emp_tgt2_id.setTableIdentifier(emp_tgt2_tid);
        emp_tgt2_tableColumns.add(emp_tgt2_id);

        ColumnIdentifier emp_tgt2_name = new ColumnIdentifier("NAME");
        emp_tgt2_name.setTableIdentifier(emp_tgt2_tid);
        emp_tgt2_tableColumns.add(emp_tgt2_name);
        
        //emp_tgt3 table
        TableIdentifier emp_tgt3_tid = new TableIdentifier("EMP_TGT3");
        ArrayList emp_tgt3_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt3_id = new ColumnIdentifier("ID");
        emp_tgt3_id.setTableIdentifier(emp_tgt3_tid);
        emp_tgt3_tableColumns.add(emp_tgt3_id);

        ColumnIdentifier emp_tgt3_name = new ColumnIdentifier("NAME");
        emp_tgt3_name.setTableIdentifier(emp_tgt3_tid);
        emp_tgt3_tableColumns.add(emp_tgt3_name);

        //now invoke multi table insert
        cmd = new InsertCommand();

        //first create sub select command which represents what we are trying to insert.
        AxionQueryContext ctx = new AxionQueryContext();

        ctx.addFrom(emp_src_tid);
        ctx.addSelect(emp_src_id);
        ctx.addSelect(emp_src_fname);
        ctx.addSelect(emp_src_lname);
        SubSelectCommand subselect = new SubSelectCommand(ctx);
        subselect.setAlias("S");
        cmd.setSubSelect(subselect);

        ColumnIdentifier emp_id = new ColumnIdentifier("ID");
        emp_id.setTableIdentifier(new TableIdentifier("S"));

        ColumnIdentifier emp_fname = new ColumnIdentifier("FNAME");
        emp_fname.setTableIdentifier(new TableIdentifier("S"));

        ColumnIdentifier emp_lname = new ColumnIdentifier("LNAME");
        emp_lname.setTableIdentifier(new TableIdentifier("S"));

        //create first when clause
        //condition is emp.id = 1
        FunctionIdentifier eq1 = new FunctionIdentifier("=");
        eq1.addArgument(emp_id);
        eq1.addArgument(new Literal(new Integer(1), new IntegerType()));

        DMLWhenClause w1 = new DMLWhenClause(eq1);

        //create second when clause
        //condition is emp.id = 2
        FunctionIdentifier eq2 = new FunctionIdentifier("=");
        eq2.addArgument(emp_id);
        eq2.addArgument(new Literal(new Integer(2), new IntegerType()));

        DMLWhenClause w2 = new DMLWhenClause(eq2);

        //insert values
        ArrayList emp_tgt1_insertValues = new ArrayList();
        ArrayList emp_tgt2_insertValues = new ArrayList();
        ArrayList emp_tgt3_insertValues = new ArrayList();

        emp_tgt1_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf1 = new FunctionIdentifier("||");
        cf1.addArgument(emp_fname);
        cf1.addArgument(emp_lname);
        emp_tgt1_insertValues.add(cf1);

        emp_tgt2_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf2 = new FunctionIdentifier("||");
        cf2.addArgument(emp_fname);
        cf2.addArgument(emp_lname);
        emp_tgt2_insertValues.add(cf2);
        
        emp_tgt3_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf3 = new FunctionIdentifier("||");
        cf3.addArgument(emp_fname);
        cf3.addArgument(emp_lname);
        emp_tgt3_insertValues.add(cf3);

        //add multi table info
        cmd.addInsertIntoClause(w1, emp_tgt1_tid, emp_tgt1_tableColumns, emp_tgt1_insertValues);
        cmd.addInsertIntoClause(w2, emp_tgt2_tid, emp_tgt2_tableColumns, emp_tgt2_insertValues);
        
        assertFalse(cmd.isInsertIntoListEmpty());
        if(!cmd.isInsertIntoListEmpty()) {
            cmd.setElseClause(emp_tgt3_tid, emp_tgt3_tableColumns, emp_tgt3_insertValues);
        }
        
        cmd.setMultiTableEvaluationMode(InsertCommand.WHEN_ALL);

        //execute multitable insert
        cmd.execute(_db);

        // emp_tgt1 should have one row
        assertEquals("Should have 1 row", 1, _db.getTable("EMP_TGT1").getRowCount());
        
        //emp_tgt2 should have one row
        assertEquals("Should have 1 row", 1, _db.getTable("EMP_TGT2").getRowCount());

        //emp_tgt2 should have one row
        assertEquals("Should have 1 row", 1, _db.getTable("EMP_TGT3").getRowCount());

    }
    
    
    public void testMultiTableInsertWhenfirst() throws Exception {
        InsertCommand cmd = null;

        //**START source table emp
        //first insert some rows into emp table
        TableIdentifier emp_src_tid = new TableIdentifier("EMP");
        ArrayList emp_src_columns = new ArrayList();
        ArrayList emp_src_values = new ArrayList();

        //table columns
        ColumnIdentifier emp_src_id = new ColumnIdentifier("ID");
        emp_src_id.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_id);

        ColumnIdentifier emp_src_fname = new ColumnIdentifier("FNAME");
        emp_src_fname.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_fname);

        ColumnIdentifier emp_src_lname = new ColumnIdentifier("LNAME");
        emp_src_lname.setTableIdentifier(emp_src_tid);
        emp_src_columns.add(emp_src_lname);

        //first row of table values
        Literal l1 = new Literal(new Integer(1), new IntegerType());
        emp_src_values.add(l1);

        Literal l2 = new Literal("Amy", new CharacterVaryingType(3));
        emp_src_values.add(l2);

        Literal l3 = new Literal("Mathews", new CharacterVaryingType(7));
        emp_src_values.add(l3);

        //insert one row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);

        //second row of table values
        emp_src_values = new ArrayList();
        Literal l4 = new Literal(new Integer(2), new IntegerType());
        emp_src_values.add(l4);

        Literal l5 = new Literal("Linda", new CharacterVaryingType(5));
        emp_src_values.add(l5);

        Literal l6 = new Literal("Tracy", new CharacterVaryingType(5));
        emp_src_values.add(l6);

        //insert second row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);
        
        //3rd row of table values
        emp_src_values = new ArrayList();
        Literal l7 = new Literal(new Integer(3), new IntegerType());
        emp_src_values.add(l7);

        Literal l8 = new Literal("Linda22", new CharacterVaryingType(7));
        emp_src_values.add(l8);

        Literal l9 = new Literal("Tracy22", new CharacterVaryingType(7));
        emp_src_values.add(l9);

        //insert second row of value
        cmd = new InsertCommand(emp_src_tid, emp_src_columns, emp_src_values);
        cmd.execute(_db);        

        assertEquals("Should have 3 row", 3, _db.getTable("EMP").getRowCount());

        //**END source table emp

        //emp_tgt1 table
        TableIdentifier emp_tgt1_tid = new TableIdentifier("EMP_TGT1");
        ArrayList emp_tgt1_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt1_id = new ColumnIdentifier("ID");
        emp_tgt1_id.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_id);

        ColumnIdentifier emp_tgt1_name = new ColumnIdentifier("NAME");
        emp_tgt1_name.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_name);

        //emp_tgt2 table
        TableIdentifier emp_tgt2_tid = new TableIdentifier("EMP_TGT2");
        ArrayList emp_tgt2_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt2_id = new ColumnIdentifier("ID");
        emp_tgt2_id.setTableIdentifier(emp_tgt2_tid);
        emp_tgt2_tableColumns.add(emp_tgt2_id);

        ColumnIdentifier emp_tgt2_name = new ColumnIdentifier("NAME");
        emp_tgt2_name.setTableIdentifier(emp_tgt2_tid);
        emp_tgt2_tableColumns.add(emp_tgt2_name);
        
        //emp_tgt3 table
        TableIdentifier emp_tgt3_tid = new TableIdentifier("EMP_TGT3");
        ArrayList emp_tgt3_tableColumns = new ArrayList();

        //table columns
        ColumnIdentifier emp_tgt3_id = new ColumnIdentifier("ID");
        emp_tgt3_id.setTableIdentifier(emp_tgt3_tid);
        emp_tgt3_tableColumns.add(emp_tgt3_id);

        ColumnIdentifier emp_tgt3_name = new ColumnIdentifier("NAME");
        emp_tgt3_name.setTableIdentifier(emp_tgt3_tid);
        emp_tgt3_tableColumns.add(emp_tgt3_name);

        //now invoke multi table insert
        cmd = new InsertCommand();

        //first create sub select command which represents what we are trying to insert.
        AxionQueryContext ctx = new AxionQueryContext();

        ctx.addFrom(emp_src_tid);
        ctx.addSelect(emp_src_id);
        ctx.addSelect(emp_src_fname);
        ctx.addSelect(emp_src_lname);
        SubSelectCommand subselect = new SubSelectCommand(ctx);
        subselect.setAlias("S");
        cmd.setSubSelect(subselect);

        ColumnIdentifier emp_id = new ColumnIdentifier("ID");
        emp_id.setTableIdentifier(new TableIdentifier("S"));

        ColumnIdentifier emp_fname = new ColumnIdentifier("FNAME");
        emp_fname.setTableIdentifier(new TableIdentifier("S"));

        ColumnIdentifier emp_lname = new ColumnIdentifier("LNAME");
        emp_lname.setTableIdentifier(new TableIdentifier("S"));

        //create first when clause
        //condition is emp.id = 1
        FunctionIdentifier eq1 = new FunctionIdentifier("=");
        eq1.addArgument(emp_id);
        eq1.addArgument(new Literal(new Integer(1), new IntegerType()));

        DMLWhenClause w1 = new DMLWhenClause(eq1);

        //create second when clause
        //condition is emp.id = 2
        FunctionIdentifier eq2 = new FunctionIdentifier("=");
        eq2.addArgument(emp_id);
        eq2.addArgument(new Literal(new Integer(2), new IntegerType()));

        DMLWhenClause w2 = new DMLWhenClause(eq2);

        //insert values
        ArrayList emp_tgt1_insertValues = new ArrayList();
        ArrayList emp_tgt2_insertValues = new ArrayList();
        ArrayList emp_tgt3_insertValues = new ArrayList();

        emp_tgt1_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf1 = new FunctionIdentifier("||");
        cf1.addArgument(emp_fname);
        cf1.addArgument(emp_lname);
        emp_tgt1_insertValues.add(cf1);

        emp_tgt2_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf2 = new FunctionIdentifier("||");
        cf2.addArgument(emp_fname);
        cf2.addArgument(emp_lname);
        emp_tgt2_insertValues.add(cf2);
        
        emp_tgt3_insertValues.add(emp_id);
        //concat first name, last name
        FunctionIdentifier cf3 = new FunctionIdentifier("||");
        cf3.addArgument(emp_fname);
        cf3.addArgument(emp_lname);
        emp_tgt3_insertValues.add(cf3);

        //add multi table info
        cmd.addInsertIntoClause(w1, emp_tgt1_tid, emp_tgt1_tableColumns, emp_tgt1_insertValues);
        cmd.addInsertIntoClause(w2, emp_tgt2_tid, emp_tgt2_tableColumns, emp_tgt2_insertValues);
        
        assertFalse(cmd.isInsertIntoListEmpty());
        if(!cmd.isInsertIntoListEmpty()) {
            cmd.setElseClause(emp_tgt3_tid, emp_tgt3_tableColumns, emp_tgt3_insertValues);
        }
        
        cmd.setMultiTableEvaluationMode(InsertCommand.WHEN_FIRST);

        //execute multitable insert
        cmd.execute(_db);

        // emp_tgt1 should have one row
        assertEquals("Should have 1 row", 1, _db.getTable("EMP_TGT1").getRowCount());
    }
    
    public void testMultiTableInsertBad() throws Exception {
        // source table
        TableIdentifier emp_src_tid = new TableIdentifier("EMP");
        
        //table columns of src table
        ColumnIdentifier emp_src_id = new ColumnIdentifier("ID");
        emp_src_id.setTableIdentifier(emp_src_tid);

        ColumnIdentifier emp_src_fname = new ColumnIdentifier("FNAME");
        emp_src_fname.setTableIdentifier(emp_src_tid);

        ColumnIdentifier emp_src_lname = new ColumnIdentifier("LNAME");
        emp_src_lname.setTableIdentifier(emp_src_tid);

        // first create sub select command which represents 
        // what we are trying to insert.
        AxionQueryContext ctx = new AxionQueryContext();

        ctx.addFrom(emp_src_tid);
        ctx.addSelect(emp_src_id);
        ctx.addSelect(emp_src_fname);
        ctx.addSelect(emp_src_lname);
        SubSelectCommand subselect = new SubSelectCommand(ctx);
        subselect.setAlias("S");

        //emp_tgt1 table 
        TableIdentifier emp_tgt1_tid = new TableIdentifier("EMP_TGT1");
        ArrayList emp_tgt1_tableColumns = new ArrayList();

        //table columns of target table
        ColumnIdentifier emp_tgt1_id = new ColumnIdentifier("ID");
        emp_tgt1_id.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_id);

        ColumnIdentifier emp_tgt1_name = new ColumnIdentifier("NAME");
        emp_tgt1_name.setTableIdentifier(emp_tgt1_tid);
        emp_tgt1_tableColumns.add(emp_tgt1_name);


        //insert values : to raise exception we have only one value here.
        ArrayList emp_tgt1_insertValues = new ArrayList();
        ColumnIdentifier emp_id = new ColumnIdentifier("ID");
        emp_id.setTableIdentifier(new TableIdentifier("S"));
        emp_tgt1_insertValues.add(emp_id);

        //create when clause for condition is emp.id = 1
        FunctionIdentifier eq1 = new FunctionIdentifier("=");
        eq1.addArgument(emp_id);
        eq1.addArgument(new Literal(new Integer(1), new IntegerType()));
        DMLWhenClause w1 = new DMLWhenClause(eq1);

        // now invoke multi table insert
        InsertCommand  cmd = new InsertCommand();
        cmd.setSubSelect(subselect);
        cmd.addInsertIntoClause(w1, emp_tgt1_tid, emp_tgt1_tableColumns, emp_tgt1_insertValues);
        cmd.setMultiTableEvaluationMode(InsertCommand.WHEN_ALL);

        //execute multitable insert
        try {
            cmd.execute(_db);
            fail("Expected Exception for less number of values");
        } catch(Exception e) {
            // expected
        }
    }
    
    public void testExecuteQueryIsNotSupported() throws Exception {
        try {
            InsertCommand cmd = new InsertCommand();
            cmd.executeQuery(_db);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
    
    public void testInsertIntoClauseUnsupported() throws Exception {
        class InsertIntoClauseImpl extends InsertIntoClause{
            InsertIntoClauseImpl(){
               super(null, null, null, null); 
            }
        };
        
        InsertIntoClauseImpl cmd = new InsertIntoClauseImpl();
        assertFalse(cmd.isTargetTablePartOfSubQuery());
        assertNotNull(cmd.getBindVariableIterator());
        assertNotNull(cmd.getBindVariableIterator());
        try {
            cmd.executeQuery(_db);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        
        try {
            cmd.execute(_db);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        
        try {
            cmd.executeUpdate(_db);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        
    }
}