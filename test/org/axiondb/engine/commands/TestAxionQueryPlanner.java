/*
 * $Id: TestAxionQueryPlanner.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AbstractDbdirTest;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.FromNode;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.engine.tables.DiskTable;
import org.axiondb.functions.EqualFunction;
import org.axiondb.types.IntegerType;

/**
 * <p>
 * TODO: Add Test for the following
 * <li>If we use a composite condition in join (i.e. join condition which involves "and"
 * or "or" function rather than single comparison function) then index information is not
 * used to carry out join. We should find out comparison functions which involves column
 * from both left and right join table and still should find the index iterator and use it
 * if available.
 * <li>If ansi join uses a composite condition then the comparison function which
 * involves only one table column is applied at table level.
 * <li>If we do not use new ansi join syntax and join condition is composite join
 * condition, then Comparison function involving only one table column in that join
 * condition are being applied twice. Once at each table level and once after creation of
 * join iterator.
 * <li>column literal functions are applied at indexed level when possible.
 * <li>Test in two table right outer join, if only left table of join has an index then
 * IndexedNestedLoopJoinedRowIterator should be used.
 * <li>Test three table join (two table have index)results in two indexed joined iterator
 * nesting.
 * <li>Test in two table inner join, use of composite condition (condition using and
 * function) in ansi join on clause still results in use of
 * IndexedNestedLoopJoinedRowIterator, if only one table is indexed.
 * <li>Test in non ansi join syntax (where join is specified in where clause), if implied
 * column literal conditions are applied at table level.
 * <li>Add Test for dynamic index.
 * <li>Test dynamic Index : if created then, should use
 * IndexedNestedLoopJoinedRowIterator if there is no Index available on the join columns
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Ahimanikya Satapathy
 */
public class TestAxionQueryPlanner extends AbstractDbdirTest {

    //------------------------------------------------------------ Conventional

    public TestAxionQueryPlanner(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestAxionQueryPlanner.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestAxionQueryPlanner.class);
    }

    //--------------------------------------------------------------- Lifecycle
    Database _db = null;

    public void setUp() throws Exception {
        getDbdir().mkdirs();
        _db = new DiskDatabase(getDbdir());
        super.setUp();
    }

    public void tearDown() throws Exception {
        _db.shutdown();
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests
    protected void addRows(Table table, boolean addpk) throws Exception {
        Row row = new SimpleRow(2);
        for (int i = 0; i < 5; i++) {
            row.set(0, new Integer(1 + i));
            row.set(1, new Integer(51 + i));
            table.addRow(row);
        }
        assertEquals("Should have 5 rows", 5, table.getRowCount());

        if (addpk) {
            addPK(table);
        }
    }

    protected void addColumns(Table table) throws Exception {
        table.addColumn(new Column("ID", new IntegerType()));
        table.addColumn(new Column("SID", new IntegerType()));
    }

    protected void addColumns(CreateTableCommand create) throws Exception {
        create.addColumn("ID", "int");
        create.addColumn("SID", "integer");
    }

    protected void addPK(Table table) throws Exception {
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("pk_" + table.getName());
        ColumnIdentifier colId = new ColumnIdentifier(new TableIdentifier(table.getName()), "ID");
        pk.addSelectable(colId);
        table.addConstraint(pk);
    }

    protected Table createTable(String name) throws Exception {
        return new DiskTable(name, _db);
    }

    private void createTables1() throws Exception {

        Table table1 = createTable("A");
        addColumns(table1);
        addRows(table1, true);
        _db.addTable(table1);

        Table table2 = createTable("B");
        addColumns(table2);
        addRows(table2, true);
        _db.addTable(table2);

        Table table3 = createTable("C");
        addColumns(table3);
        _db.addTable(table3);

        Table table4 = createTable("A2");
        addColumns(table4);
        addRows(table4, true);
        _db.addTable(table4);
    }

    public void testNestedLoopJoinWithIndexedJoin1() throws Exception {
        createTables1();
        doTestNestedLoopJoinWithIndexedJoin();
    }

    private void createTables2() throws Exception {
        CreateTableCommand create = new CreateTableCommand("A");
        addColumns(create);
        create.execute(_db);
        Table table1 = _db.getTable("A");
        addRows(table1, true);

        create = new CreateTableCommand("B");
        addColumns(create);
        create.execute(_db);
        Table table2 = _db.getTable("B");
        addRows(table2, true);

        create = new CreateTableCommand("C");
        addColumns(create);
        create.execute(_db);

        create = new CreateTableCommand("A2");
        addColumns(create);
        create.execute(_db);
        Table table4 = _db.getTable("A2");
        addRows(table4, true);
    }

    public void testNestedLoopJoinWithIndexedJoin2() throws Exception {
        createTables2();
        doTestNestedLoopJoinWithIndexedJoin();
    }

    private void doTestNestedLoopJoinWithIndexedJoin() throws Exception {
        CreateViewCommand cmd = new CreateViewCommand();
        cmd.setObjectName("threetablejoinview");
        cmd.setIfNotExists(true);
        cmd.setSubQuery("select a.* from a inner join b on " + " a.id = b.id left outer join c on c.id = a.id");
        cmd.execute(_db);
        Table view = _db.getTable("threetablejoinview");
        assertNotNull(view);
        assertNotNull(view.getRowIterator(false));
        assertEquals(5, view.getRowCount());

        AxionQueryContext ctx = new AxionQueryContext();
        
        TableIdentifier taid = new TableIdentifier("A");
        TableIdentifier tbid = new TableIdentifier("B");
        TableIdentifier tcid = new TableIdentifier("C");
        TableIdentifier ta2id = new TableIdentifier("A2");

        ColumnIdentifier aid = new ColumnIdentifier(taid, "ID", "AID");
        ColumnIdentifier bid = new ColumnIdentifier(tbid, "ID", "BID");
        ColumnIdentifier bsid = new ColumnIdentifier(tbid, "SID", "BSID");
        ColumnIdentifier cid = new ColumnIdentifier(tcid, "ID", "CID");
        ColumnIdentifier a2id = new ColumnIdentifier(ta2id, "ID");

        ctx.addSelect(aid);
        ctx.addSelect(bsid);
        ctx.addSelect(cid);

        FromNode from1 = new FromNode();
        from1.setLeft(taid);
        assertTrue(from1.hasLeft());
        from1.setRight(tbid);
        EqualFunction eq1 = new EqualFunction();
        eq1.addArgument(aid);
        eq1.addArgument(bid);
        from1.setCondition(eq1);
        assertNotNull(from1.toString());
        from1.setType(FromNode.TYPE_INNER);
        assertNotNull(from1.toString());
        assertEquals("unknown?", FromNode.typeToString(999));

        FromNode from2 = new FromNode();
        from2.setLeft(from1);
        assertTrue(from2.hasLeft());
        from2.setRight(tcid);
        EqualFunction eq2 = new EqualFunction();
        eq2.addArgument(aid);
        eq2.addArgument(cid);
        assertFalse(from2.hasCondition());
        from2.setCondition(eq2);
        assertTrue(from2.hasCondition());
        from2.setType(FromNode.TYPE_LEFT);
        assertNotNull(from2.toString());

        // Nested Right join
        FromNode from3 = new FromNode();
        from3.setLeft(ta2id);
        assertTrue(from3.hasLeft());
        from3.setRight(from2);
        EqualFunction eq3 = new EqualFunction();
        eq3.addArgument(a2id);
        eq3.addArgument(aid);
        assertFalse(from3.hasCondition());
        from3.setCondition(eq3);
        assertTrue(from3.hasCondition());
        from3.setType(FromNode.TYPE_RIGHT);
        assertNotNull(from3.toString());

        ctx.setFrom(from3);
        SubSelectCommand selcmd = new SubSelectCommand(ctx);
        RowIterator iter = selcmd.getRowIterator(_db);

        assertNotNull(iter);
        for (int i = 0; i < 5; i++) {
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            assertEquals(new Integer(1 + i), row.get(0));
            assertEquals(new Integer(51 + i), row.get(1));
            assertEquals(null, row.get(2));
        }
        assertTrue(!iter.hasNext());

        iter.reset();
        for (int i = 0; i < 5; i++) {
            assertTrue(iter.hasNext());
            Row row = iter.next();
            assertNotNull(row);
            assertEquals(new Integer(1 + i), row.get(0));
            assertEquals(new Integer(51 + i), row.get(1));
            assertEquals(null, row.get(2));
        }
        assertTrue(!iter.hasNext());
    }
    
    public void testPlannerToString() {
        AxionQueryPlanner planner = new AxionQueryPlanner(new AxionQueryContext());
        assertNotNull(planner.toString());
        AxionQueryPlanner.JoinCondition jcondition = planner.new JoinCondition(new HashSet());
        assertNotNull(jcondition.toString());
        
        AxionQueryPlanner.QueryPlannerJoinContext jcontext = planner.new QueryPlannerJoinContext();
        assertNotNull(jcontext.toString());
    }

    public void testDecomposeWhereConditionNodes() {
        Set set = new HashSet();
        String str = "BOGUS";
        set.add(str);
        Set newSet[] = AxionQueryOptimizer.decomposeWhereConditionNodes(set, false);
        assertTrue(newSet[0].isEmpty());
        assertTrue(newSet[1].isEmpty());
        assertTrue(!newSet[2].isEmpty());

    }
    
    public void testcreateOneRootFunctionForNullSet() {
        assertNull(AxionQueryOptimizer.createOneRootFunction(new HashSet()));
        assertNull(AxionQueryOptimizer.createOneRootFunction(null));
    }

}