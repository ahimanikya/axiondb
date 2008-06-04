/*
 * $Id: TestSelectCommand.java,v 1.1 2007/11/28 10:01:24 jawed Exp $
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

package org.axiondb.engine.commands;

import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.BaseDatabase;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:24 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TestSelectCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestSelectCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestSelectCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestSelectCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    Database _db = null;

    public void setUp() throws Exception {
        super.setUp();
        _db = new MemoryDatabase();
        {
            CreateTableCommand cmd = new CreateTableCommand("FOO");
            cmd.addColumn("A","varchar", "10");
            cmd.addColumn("B","integer");
            cmd.execute(_db);
        }

        AxionCommand cmd = new InsertCommand(
            new TableIdentifier("FOO"), 
            Arrays.asList(new ColumnIdentifier[] {new ColumnIdentifier("A") , new ColumnIdentifier("B")}),
            Arrays.asList(new Literal[] {new Literal("alabama",new CharacterVaryingType(10)),new Literal("100",new IntegerType())}));
        cmd.executeUpdate(_db);
        
        cmd = new InsertCommand(
            new TableIdentifier("FOO"), 
            Arrays.asList(new ColumnIdentifier[] {new ColumnIdentifier("A") , new ColumnIdentifier("B")}),
            Arrays.asList(new Literal[] {new Literal("alaska",new CharacterVaryingType(10)),new Literal("200",new IntegerType())}));
        cmd.executeUpdate(_db);

        cmd = new InsertCommand(
            new TableIdentifier("FOO"),
            Arrays.asList(new ColumnIdentifier[] {new ColumnIdentifier("A") , new ColumnIdentifier("B")}),
            Arrays.asList(new Literal[] {new Literal("alabama",new CharacterVaryingType(10)),new Literal("100",new IntegerType())}));
        cmd.executeUpdate(_db);
    }


    //------------------------------------------------------------------- Tests

    public void testSelect() throws Exception {
        assertEquals("Should have 3 rows",
                     3, _db.getTable("FOO").getRowCount());
        
        AxionQueryContext ctx = new AxionQueryContext();
        TableIdentifier tid = new TableIdentifier("FOO");
        ColumnIdentifier aid = new ColumnIdentifier(tid, "A");
        ColumnIdentifier bid = new ColumnIdentifier(tid, "B");
        ctx.addSelect(aid);
        ctx.addSelect(bid);
        assertNotNull(ctx.toString());
        
        ctx.addFrom(tid);
        ctx.setDistinct(true);
        ctx.setLimit(new Literal("1", new IntegerType()));
        assertNotNull(ctx.toString());

        FunctionIdentifier fn = new FunctionIdentifier("=");
        fn.addArgument(bid);
        fn.addArgument(new Literal("100", new CharacterVaryingType(10)));
        ctx.setWhere(fn);
        
        assertNotNull(ctx.toString());
        List glist = new ArrayList();
        glist.add(aid);
        glist.add(bid);
        ctx.setGroupBy(glist);
        assertNotNull(ctx.toString());
        
        List olist = new ArrayList();
        olist.add(new OrderNode(aid, true));
        olist.add(new OrderNode(bid, false));
        ctx.setOrderBy(olist);
        assertNotNull(ctx.toString());
        
        SelectCommand cmd = new SelectCommand(ctx);
        
        assertNotNull(cmd.toString()); 
        
        ResultSet rset = cmd.executeQuery(_db);
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alabama", rset.getString(1));
        assertTrue("Should not have row", !rset.next());

        try {
            ctx.setGroupBy(glist);
            fail("Exception Expected, already resolved");
        }catch (IllegalStateException e) {
            // expected
        }
        
        try {
            ctx.addFrom(tid);
            fail("Exception Expected, already resolved");
        }catch (IllegalStateException e) {
            // expected
        }

    }
    
    public void testSelect2() throws Exception {
        assertEquals("Should have 3 rows",
                     3, _db.getTable("FOO").getRowCount());
        
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        ctx.addFrom(new TableIdentifier("FOO"));
        SelectCommand cmd = new SelectCommand(ctx);
        
        assertNotNull(cmd.toString()); // prove toString doesn't do anything too weird
        
        ResultSet rset = cmd.executeQuery(_db);
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alabama", rset.getString(1));
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alaska", rset.getString(1));
        
        try {
            ctx.setSelect(0, new ColumnIdentifier(new TableIdentifier("FOO"), "B"));
            fail("Exception Expected, already resolved");
        }catch (IllegalStateException e) {
            // expected
        }
        
        RowIterator rows = cmd.makeRowIterator(_db, true);
        assertTrue("Should have a row", rows.hasNext());
        Row row = rows.next();
        assertEquals("Should get value", "alabama", row.get(0));
        assertTrue("Should have a row", rows.hasNext());
        row = rows.next();
        assertEquals("Should get value", "alaska", row.get(0));

    }
    
    public void testSubSelect() throws Exception {
        assertEquals("Should have 3 rows",
                     3, _db.getTable("FOO").getRowCount());
        
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.setAliasName("MYFOO");
        SubSelectCommand cmd = new SubSelectCommand(ctx);
        cmd.setDB(_db);
        
        assertNotNull(cmd.toString()); // prove toString doesn't do anything too weird
        
        try {
            cmd.executeQuery(_db);
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
        
        try {
            cmd.execute(_db);
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
        
        try {
            cmd.executeUpdate(_db);
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
        
        assertEquals("MYFOO", cmd.getAlias().toUpperCase());
        assertEquals("MYFOO", cmd.getLabel().toUpperCase());
        
        RowIterator iter = (RowIterator)cmd.evaluate(null);
        
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
    }

    public void testScalarSubSelect() throws Exception {
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.setAliasName("MYFOO");

        SubSelectCommand cmd = new SubSelectCommand(ctx);
        cmd.setDB(_db);
        cmd.setEvaluteAsScalarValue();
        
        EqualFunction eq = new EqualFunction();
        eq.addArgument(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        eq.addArgument(new Literal("alaska",new CharacterVaryingType(10)));
        ctx.setWhere(eq);
        assertEquals("alaska", cmd.evaluate(null));
    }  
    
    public void testReadonlyDBForSubSelect() throws Exception {
        InputStream in = BaseDatabase.class.getClassLoader().getResourceAsStream("org/axiondb/axiondb.properties");
        Properties prop = new Properties();
        prop.load(in);
        prop.setProperty("readonly", "yes");
        MemoryDatabase db = new MemoryDatabase("readonlydb", prop);
        
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("AXION_TABLES"), "TABLE_NAME"));
        ctx.addFrom(new TableIdentifier("AXION_TABLES"));
        
        SubSelectCommand cmd = new SubSelectCommand(ctx);
        cmd.setDB(db);
        cmd.setEvaluteAsScalarValue();
        
        EqualFunction eq = new EqualFunction();
        eq.addArgument(new ColumnIdentifier(new TableIdentifier("AXION_TABLES"), "TABLE_NAME"));
        eq.addArgument(new Literal("AXION_COLUMNS",new CharacterVaryingType(20)));
        ctx.setWhere(eq);
        assertEquals("AXION_COLUMNS", cmd.evaluate(null));
    }
    
    public void testBadScalarSubSelect() throws Exception {
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.setAliasName("MYFOO");

        SubSelectCommand cmd = new SubSelectCommand(ctx);
        cmd.setDB(_db);
        cmd.setEvaluteAsScalarValue();
        
        try {
            cmd.evaluate(null);
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
    }    

    public void testGroupBy() throws Exception {
        AxionQueryContext ctx = new AxionQueryContext();
        
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        Selectable countfn = new FunctionIdentifier("COUNT",
            Arrays.asList(new ColumnIdentifier[] {new ColumnIdentifier("B")}));
        ctx.addSelect(countfn);
        ctx.addFrom(new TableIdentifier("FOO"));
        ctx.setGroupBy(Arrays.asList(new ColumnIdentifier[] {new ColumnIdentifier(new TableIdentifier("FOO"), "A")}));
        ctx.setOrderBy(Arrays.asList(new OrderNode[] {new OrderNode(new ColumnIdentifier(new TableIdentifier("FOO"), "A"),true)}));
        ctx.setHaving(new FunctionIdentifier(">", Arrays.asList(new Selectable[] { countfn, new Literal(new Integer(0))})));
        assertNotNull(ctx.toString());
        SelectCommand cmd = new SelectCommand(ctx);
        
        ResultSet rset = cmd.executeQuery(_db);
        assertTrue("Should have a row", rset.next());
        // since resultset is ordered by dfault in descending order..
        assertEquals("Should get value", "alaska", rset.getString(1));
        assertEquals("Should get count 1 for alaska ", "1", rset.getString(2));
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alabama", rset.getString(1));
        assertEquals("Should get count 2 for alabama ", "2", rset.getString(2));
        
        // should fail
        try{
            ctx.setHaving(null);    
            fail("Expected Already resolved exception");
        }catch(IllegalStateException e){
            // expected
        }
        
        rset.close();


    }

    public void testCantChangeAfterResolved() throws Exception {
        AxionQueryContext ctx = new AxionQueryContext();
        
        ctx.addSelect(new ColumnIdentifier(new TableIdentifier("FOO"), "A"));
        ctx.addFrom(new TableIdentifier("FOO"));
        SelectCommand cmd = new SelectCommand(ctx);
        ResultSet rset = cmd.executeQuery(_db);
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alabama", rset.getString(1));
        assertTrue("Should have a row", rset.next());
        assertEquals("Should get value", "alaska", rset.getString(1));
        rset.close();
        
        try {
            ctx.addSelect(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
        
        try {
            ctx.setSelect(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
        
        try {
            ctx.setFrom(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
        
        try {
            ctx.setWhere(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
        
        try {
            ctx.setOrderBy(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
                
        try {
            ctx.addOrderBy(null);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }

        try {
            ctx.setDistinct(true);
            fail("Expected IllegalStateException");
        } catch(IllegalStateException e) {
            // expected
        }
    }
}
