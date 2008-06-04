/*
 *  $Id: AbstractParserTest.java,v 1.1 2007/11/28 10:01:38 jawed Exp $
 *  =======================================================================
 *  Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in
 *  the documentation and/or other materials provided with the
 *  distribution.
 *
 *  3. The names "Tigris", "Axion", nor the names of its contributors may
 *  not be used to endorse or promote products derived from this
 *  software without specific prior written permission.
 *
 *  4. Products derived from this software may not be called "Axion", nor
 *  may "Tigris" or "Axion" appear in their names without specific prior
 *  written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 *  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *  =======================================================================
 */
package org.axiondb.parser;

import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.BindVariable;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.OrderNode;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.CheckConstraint;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.NullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.constraints.UniqueConstraint;
import org.axiondb.engine.commands.AddConstraintCommand;
import org.axiondb.engine.commands.CreateIndexCommand;
import org.axiondb.engine.commands.CreateTableCommand;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.commands.DropConstraintCommand;
import org.axiondb.engine.commands.DropTableCommand;
import org.axiondb.engine.commands.InsertCommand;
import org.axiondb.engine.commands.RemountCommand;
import org.axiondb.engine.commands.SelectCommand;
import org.axiondb.engine.commands.ShutdownCommand;
import org.axiondb.engine.commands.UpdateCommand;
import org.axiondb.functions.FunctionIdentifier;
import org.axiondb.types.BooleanType;
import org.axiondb.types.CharacterType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:38 $
 * @author  Rodney Waldhoff
 * @author  Chuck Burdick
 * @author  Dave Pekarek Krohn
 */
public abstract class AbstractParserTest extends TestCase {

    //------------------------------------------------------------ Conventional

    public AbstractParserTest(String testName) {
        super(testName);
    }

    //---------------------------------------------------------------- Abstract

    public abstract Parser getParser();

    //--------------------------------------------------------------- Lifecycle

    //------------------------------------------------------------------- Tests

    public void testBadSQL() throws Exception {
        Parser parser = getParser();
        try {
            parser.parse("xyzzy");
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testVarious() throws Exception {
        Parser parser = getParser();
        assertNotNull(parser.parse("select * from foobar where foo or bar"));
        assertNotNull(parser.parse("select * from foobar where foo or true"));
        assertNotNull(parser.parse("select * from foobar where true or foo"));
        assertNotNull(parser.parse("select 1 + foo from foobar"));
        assertNotNull(parser.parse("select foo + 1 from foobar"));
        assertNotNull(parser.parse("select * from foobar where foo + 1 > 5"));
    }

    // create unique index BAR on FOO ( NUM )
    public void test_create_unique_index_bar_on_foo_num() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create unique index BAR on FOO ( NUM )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateIndexCommand", cmd instanceof CreateIndexCommand);
        CreateIndexCommand create = (CreateIndexCommand)cmd;
        assertTrue("Should be unique",create.isUnique());
        assertTrue("Type should be null",null == create.getType());
        assertEquals("Name of index should be BAR","BAR",create.getObjectName());
        assertEquals("Name of table should be FOO","FOO",create.getTable().getTableName());
        assertEquals("Should have one column",1,create.getColumnCount());
        assertEquals("Name of column should be NUM","NUM",create.getColumn(0).getName());
    }

    // create index BAR on FOO ( NUM )
    public void test_create_index_bar_on_foo_num() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create index BAR on FOO ( NUM )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateIndexCommand", cmd instanceof CreateIndexCommand);
        CreateIndexCommand create = (CreateIndexCommand)cmd;
        assertTrue("Should not be unique",!create.isUnique());
        assertTrue("Type should be null",null == create.getType());
        assertEquals("Name of index should be BAR","BAR",create.getObjectName());
        assertEquals("Name of table should be FOO","FOO",create.getTable().getTableName());
        assertEquals("Should have one column",1,create.getColumnCount());
        assertEquals("Name of column should be NUM","NUM",create.getColumn(0).getName());
    }

    // create index BAR on FOO ( NUM, DESCR )
    public void test_create_index_bar_on_foo_num_descr() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create index BAR on FOO ( NUM, DESCR )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateIndexCommand", cmd instanceof CreateIndexCommand);
        CreateIndexCommand create = (CreateIndexCommand)cmd;
        assertTrue("Should not be unique",!create.isUnique());
        assertTrue("Type should be null",null == create.getType());
        assertEquals("Name of index should be BAR","BAR",create.getObjectName());
        assertEquals("Name of table should be FOO","FOO",create.getTable().getTableName());
        assertEquals("Should have two columns",2,create.getColumnCount());
        assertEquals("Name of column should be NUM","NUM",create.getColumn(0).getName());
        assertEquals("Name of column should be DESCR","DESCR",create.getColumn(1).getName());
    }

    // create TRANSIENT index BAR on FOO ( NUM )
    public void test_create_transient_index_bar_on_foo_num() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create TRANSIENT index BAR on FOO ( NUM )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateIndexCommand", cmd instanceof CreateIndexCommand);
        CreateIndexCommand create = (CreateIndexCommand)cmd;
        assertTrue("Should not be unique",!create.isUnique());
        assertEquals("Type should be TRANSIENT","TRANSIENT",create.getType());
        assertEquals("Name of index should be BAR","BAR",create.getObjectName());
        assertEquals("Name of table should be FOO","FOO",create.getTable().getTableName());
        assertEquals("Should have one column",1,create.getColumnCount());
        assertEquals("Name of column should be NUM","NUM",create.getColumn(0).getName());
    }

    // create unique TRANSIENT index BAR on FOO ( NUM )
    public void test_create_unique_transient_index_bar_on_foo_num() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create unique TRANSIENT index BAR on FOO ( NUM )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateIndexCommand", cmd instanceof CreateIndexCommand);
        CreateIndexCommand create = (CreateIndexCommand)cmd;
        assertTrue("Should be unique",create.isUnique());
        assertEquals("Type should be TRANSIENT","TRANSIENT",create.getType());
        assertEquals("Name of index should be BAR","BAR",create.getObjectName());
        assertEquals("Name of table should be FOO","FOO",create.getTable().getTableName());
        assertEquals("Should have one column",1,create.getColumnCount());
        assertEquals("Name of column should be NUM","NUM",create.getColumn(0).getName());
    }

    // create table FOO ( NUM integer, DESCR string )
    public void testCreateTable() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertNull(create.getType());
        assertTrue(!create.isIfNotExists());
    }

    // create table if not exists FOO ( NUM integer, DESCR string )
    public void testCreateTableIfNotExists() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table if not exists FOO ( NUM integer, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertTrue(create.isIfNotExists());
    }

    // create table FOO ( NUM integer, blah VARCHAR (30) )
    public void testCreateTableWithVarChar() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, blah VARCHAR(30) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertNull(create.getType());
    }

    // create table FOO ( NUM integer, blah VARCHAR (30), foo INTEGER )
    public void testCreateTableWithVarCharAndInteger() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, blah VARCHAR(30), foo INTEGER )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertNull(create.getType());
    }

    // create table FOO ( NUM integer, DESCR string );
    public void testCreateTableWithSemicolon() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, DESCR varchar(10) );");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertNull(create.getType());
    }

    // create MYTYPE table FOO ( NUM integer, DESCR string )
    public void testTypedCreateTable() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create MYTPE table FOO ( NUM integer, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        assertEquals("MYTPE",create.getType());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, PRIMARY KEY(NUM, NUM2) )
    public void testCreateTableWithPrimaryKey() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), PRIMARY KEY(NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a PrimaryKeyConstraint",ccmd.getConstraint() instanceof PrimaryKeyConstraint);
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(ccmd.getConstraint());
        assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
        assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
        assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", pk.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, constraint PRIMARY KEY(NUM, NUM2) )
    public void testCreateTableWithConstraintPrimaryKey() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), constraint PRIMARY KEY(NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a PrimaryKeyConstraint",ccmd.getConstraint() instanceof PrimaryKeyConstraint);
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(ccmd.getConstraint());
        assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
        assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
        assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", pk.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, constraint FOO_PK primary key(NUM, NUM2) )
    public void testCreateTableWithNamedPrimaryKey() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), constraint FOO_PK primary key (NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a PrimaryKeyConstraint",ccmd.getConstraint() instanceof PrimaryKeyConstraint);
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(ccmd.getConstraint());
        assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
        assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
        assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", pk.getName());
        assertEquals("Constraint name should be FOO_PK", "FOO_PK",pk.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, UNIQUE (NUM, NUM2) )
    public void testCreateTableWithUniqueTableConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), UNIQUE (NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
        assertEquals("Number of unique columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, UNIQUE (NUM, NUM2) DEFERRABLE INITIALLY DEFERRED)
    public void testCreateTableWithDeferredUniqueTableConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), UNIQUE (NUM, NUM2) DEFERRABLE INITIALLY DEFERRED)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
        assertEquals("Number of unique columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertTrue("Constraint should be deferrable", constraint.isDeferrable());
        assertTrue("Constraint should be deferred", constraint.isDeferred());
    }

    // create table FOO ( NUM integer, NUM2 integer, DESCR string, UNIQUE (NUM, DESCR), PRIMARY KEY (NUM, NUM2) )
    public void testCreateTableWithUniqueAndPrimaryKeyTableConstraints() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer, DESCR varchar(10), UNIQUE (NUM, DESCR), PRIMARY KEY (NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child commands",2,create.getChildCommandCount());
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
            assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
            UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
            assertEquals("Number of unique columns", 2, constraint.getSelectableCount());
            assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
            assertEquals("column name", "DESCR", constraint.getSelectable(1).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(1) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(1));
            assertTrue("Constraint should be a PrimaryKeyConstraint",ccmd.getConstraint() instanceof PrimaryKeyConstraint);
            PrimaryKeyConstraint constraint = (PrimaryKeyConstraint)(ccmd.getConstraint());
            assertEquals("Number of unique columns", 2, constraint.getSelectableCount());
            assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
            assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
    }
    
    // create table FOO ( NUM integer NOT NULL, NUM2 integer NULL, DESCR string )
    public void testCreateTableWithNotNullAndNullConstraints() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer NOT NULL, NUM2 integer NULL, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have two child commands",2,create.getChildCommandCount());
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
            assertTrue("Constraint should be a NotNullConstraint",ccmd.getConstraint() instanceof NotNullConstraint);
            NotNullConstraint constraint = (NotNullConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "NUM", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(1) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(1));
            assertTrue("Constraint should be a NullConstraint",ccmd.getConstraint() instanceof NullConstraint);
            NullConstraint constraint = (NullConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "NUM2", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
    }

    // create table FOO ( NUM integer, NUM2 integer UNIQUE, DESCR string )
    public void testCreateTableWithUniqueConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer UNIQUE, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
        assertEquals("Number of unique columns", 1, constraint.getSelectableCount());
        assertEquals("Unique column name", "NUM2", constraint.getSelectable(0).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer constraint FOO_UNIQUE unique, DESCR string )
    public void testCreateTableWithNamedUniqueConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer constraint FOO_UNIQUE unique, DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
        assertEquals("Number of unique columns", 1, constraint.getSelectableCount());
        assertEquals("Unique column name", "NUM2", constraint.getSelectable(0).getLabel());
        assertEquals("Constraint name", "FOO_UNIQUE", constraint.getName());
    }

    // create table FOO ( NUM integer, NUM2 integer CHECK ( NUM2 > NUM), DESCR string )
    public void testCreateTableWithCheckConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer, NUM2 integer CHECK ( NUM2 > NUM), DESCR varchar(10) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        assertEquals("Should have one child command",1,create.getChildCommandCount());
        assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
        AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
        assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof CheckConstraint);
        CheckConstraint constraint = (CheckConstraint)(ccmd.getConstraint());
        assertNotNull(constraint.getCondition());

        assertTrue(constraint.getCondition() instanceof FunctionIdentifier);
        assertEquals(">",constraint.getCondition().getName());
    }

    // create table FOO ( NUM integer NOT NULL, NUM2 integer UNIQUE NOT NULL, DESCR string NOT NULL, PRIMARY KEY(NUM, NUM2) )
    public void testCreateTableWithMultipleConstraints() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("create table FOO ( NUM integer NOT NULL, NUM2 integer UNIQUE NOT NULL, DESCR varchar(10) NOT NULL, PRIMARY KEY(NUM, NUM2) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a CreateTableCommand", cmd instanceof CreateTableCommand);
        CreateTableCommand create = (CreateTableCommand)cmd;
        
        List columns = create.getColumnNames();
        assertEquals("NUM", columns.get(0));
        assertEquals("NUM2", columns.get(1));
        assertEquals("DESCR", columns.get(2));
        
        //assertEquals("Should have five child commands",5,create.getChildCommandCount());
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(0) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(0));
            assertTrue("Constraint should be a NotNullConstraint",ccmd.getConstraint() instanceof NotNullConstraint);
            NotNullConstraint constraint = (NotNullConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "NUM", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(1) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(1));
            assertTrue("Constraint should be a UniqueConstraint",ccmd.getConstraint() instanceof UniqueConstraint);
            UniqueConstraint constraint = (UniqueConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "NUM2", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(2) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(2));
            assertTrue("Constraint should be a NotNullConstraint",ccmd.getConstraint() instanceof NotNullConstraint);
            NotNullConstraint constraint = (NotNullConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "NUM2", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(3) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(3));
            assertTrue("Constraint should be a NotNullConstraint",ccmd.getConstraint() instanceof NotNullConstraint);
            NotNullConstraint constraint = (NotNullConstraint)(ccmd.getConstraint());
            assertNotNull(constraint.getSelectable(0));
            assertEquals("nn column name", "DESCR", constraint.getSelectable(0).getLabel());
            assertNotNull("Constraint name should not be null", constraint.getName());
        }
        {
            assertTrue("Child command should be an AddConstraintCommand",create.getChildCommand(4) instanceof AddConstraintCommand);
            AddConstraintCommand ccmd = (AddConstraintCommand)(create.getChildCommand(4));
            assertTrue("Constraint should be a PrimaryKeyConstraint",ccmd.getConstraint() instanceof PrimaryKeyConstraint);
            PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(ccmd.getConstraint());
            assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
            assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
            assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
            assertNotNull("Constraint name should not be null", pk.getName());
        }
    }

    // alter table FOO add constraint primary key (NUM, NUM2)
    public void testAlterTableAddConstraintPrimaryKey() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint primary key (NUM, NUM2)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a PrimaryKeyConstraint",add.getConstraint() instanceof PrimaryKeyConstraint);
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(add.getConstraint());
        assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
        assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
        assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", pk.getName());
    }

    // alter table FOO add constraint BAR primary key (NUM, NUM2)
    public void testAlterTableAddConstraintNamePrimaryKey() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint BAR primary key(NUM, NUM2)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a PrimaryKeyConstraint",add.getConstraint() instanceof PrimaryKeyConstraint);
        PrimaryKeyConstraint pk = (PrimaryKeyConstraint)(add.getConstraint());
        assertEquals("Number of primary key columns", 2, pk.getSelectableCount());
        assertEquals("PK column name", "NUM", pk.getSelectable(0).getLabel());
        assertEquals("PK column name", "NUM2", pk.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", pk.getName());
        assertEquals("Constraint name should be BAR", "BAR", pk.getName());
    }

    // alter table FOO add constraint unique (NUM, NUM2)
    public void testAlterTableAddConstraintUnique() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint unique (NUM, NUM2)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
    }

    // alter table FOO add constraint BAR unique (NUM, NUM2)
    public void testAlterTableAddConstraintNameUnique() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint BAR unique (NUM, NUM2)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertEquals("Constraint name should be BAR", "BAR", constraint.getName());
        assertTrue("Constraint should not be deferrable", !constraint.isDeferrable());
        assertTrue("Constraint should not be deferred", !constraint.isDeferred());
    }

    // alter table FOO add constraint unique (NUM, NUM2) deferrable
    public void testAlterTableAddConstraintUniqueDeferrable() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint unique (NUM, NUM2) deferrable");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertTrue("Constraint should be deferrable", constraint.isDeferrable());
        assertTrue("Constraint should not be deferred", !constraint.isDeferred());
    }

    // alter table FOO add constraint unique (NUM, NUM2) deferrable initially deferred
    public void testAlterTableAddConstraintUniqueDeferrableInitiallyDeferred() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint unique (NUM, NUM2) deferrable initially deferred");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertTrue("Constraint should be deferrable", constraint.isDeferrable());
        assertTrue("Constraint should be deferred", constraint.isDeferred());
    }

    // alter table FOO add constraint unique (NUM, NUM2) deferrable initially immediate
    public void testAlterTableAddConstraintUniqueDeferrableInitiallyImmediate() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint unique (NUM, NUM2) deferrable initially immediate");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertTrue("Constraint should be deferrable", constraint.isDeferrable());
        assertTrue("Constraint should not be deferred", !constraint.isDeferred());
    }

    // alter table FOO add constraint unique (NUM, NUM2) not deferrable
    public void testAlterTableAddConstraintUniqueNotDeferrableInitiallyImmediate() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint unique (NUM, NUM2) not deferrable");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a UniqueConstraint",add.getConstraint() instanceof UniqueConstraint);
        UniqueConstraint constraint = (UniqueConstraint)(add.getConstraint());
        assertEquals("Number of columns", 2, constraint.getSelectableCount());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertEquals("column name", "NUM2", constraint.getSelectable(1).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertTrue("Constraint should not be deferrable", !constraint.isDeferrable());
        assertTrue("Constraint should not be deferred", !constraint.isDeferred());
    }

    // alter table FOO add constraint not null (NUM)
    public void testAlterTableAddConstraintNotNull() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint not null (NUM)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a NotNullConstraint",add.getConstraint() instanceof NotNullConstraint);
        NotNullConstraint constraint = (NotNullConstraint)(add.getConstraint());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
    }

    // alter table FOO add constraint BAR not null (NUM)
    public void testAlterTableAddConstraintNameNotNull() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO add constraint BAR not null (NUM)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an AddConstraintCommand", cmd instanceof AddConstraintCommand);
        AddConstraintCommand add = (AddConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertTrue("Constraint should be a NotNullConstraint",add.getConstraint() instanceof NotNullConstraint);
        NotNullConstraint constraint = (NotNullConstraint)(add.getConstraint());
        assertEquals("column name", "NUM", constraint.getSelectable(0).getLabel());
        assertNotNull("Constraint name should not be null", constraint.getName());
        assertEquals("Constraint name should be BAR", "BAR", constraint.getName());
    }

    // alter table FOO drop constraint BAR
    public void testAlterTableDropConstraint() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("alter table FOO drop constraint BAR");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an DropConstraintCommand", cmd instanceof DropConstraintCommand);
        DropConstraintCommand add = (DropConstraintCommand)cmd;
        assertEquals("FOO",add.getTableName());        
        assertEquals("BAR",add.getConstraintName());        
    }

    // drop table FOO
    public void testDropTable() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("drop table FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DropTableCommand", cmd instanceof DropTableCommand);
    }

    // delete from FOO
    public void test_delete_from_foo() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("delete from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DeleteCommand", cmd instanceof DeleteCommand);
        DeleteCommand delete = (DeleteCommand)cmd;
        assertEquals("Table name should be FOO", "FOO", delete.getTable().getTableName());
        assertTrue("Where clause should be null.", null == delete.getWhere());
    }

    // delete from FOO where NUM = 7 and BAR > 9
    public void test_delete_from_foo_with_where() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("delete from FOO where NUM = 7 and BAR > 9");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DeleteCommand", cmd instanceof DeleteCommand);
        DeleteCommand delete = (DeleteCommand)cmd;
        assertEquals("Table name should be FOO", "FOO", delete.getTable().getTableName());
        assertNotNull("Where clause should not be null.", delete.getWhere());
    }

    // delete FOO
    public void test_delete_foo() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("delete FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DeleteCommand", cmd instanceof DeleteCommand);
        DeleteCommand delete = (DeleteCommand)cmd;
        assertEquals("Table name should be FOO", "FOO", delete.getTable().getTableName());
        assertTrue("Where clause should be null.", null == delete.getWhere());
    }

    // delete FOO where NUM = 7 and BAR > 9
    public void test_delete_foo_with_where() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("delete FOO where NUM = 7 and BAR > 9");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DeleteCommand", cmd instanceof DeleteCommand);
        DeleteCommand delete = (DeleteCommand)cmd;
        assertEquals("Table name should be FOO", "FOO", delete.getTable().getTableName());
        assertNotNull("Where clause should not be null.", delete.getWhere());
    }

    // delete FOO where NUM = 7 and BAR > 9;
    public void test_delete_foo_with_where_semicolon() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("delete FOO where NUM = 7 and BAR > 9;");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a DeleteCommand", cmd instanceof DeleteCommand);
        DeleteCommand delete = (DeleteCommand)cmd;
        assertEquals("Table name should be FOO", "FOO", delete.getTable().getTableName());
        assertNotNull("Where clause should not be null.", delete.getWhere());
    }

    // select VAL from FOO
    public void test_select_val_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));

        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
    }

    // select VAL from FOO where FALSE
    public void test_select_val_from_foo_where_true() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where FALSE");
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue(select.getQueryContext().getWhere() instanceof Literal);
        Literal literal = (Literal)(select.getQueryContext().getWhere());
        assertTrue(literal.evaluate(null) instanceof Boolean);
        assertTrue(!((Boolean)literal.evaluate(null)).booleanValue());
        
    }

    // select 'Literal' from FOO
    public void test_select_string_literal_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select 'Literal' from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one selectable in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be a literal", select.getQueryContext().getSelect(0) instanceof Literal);
        Literal literal = (Literal)(select.getQueryContext().getSelect(0));
        assertEquals("Literal should have the value \"Literal\".", "Literal", literal.evaluate(null));
    }

    // select 'Literal'
    public void test_select_string_literal_without_from() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select 'Literal'");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one selectable in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be a literal", select.getQueryContext().getSelect(0) instanceof Literal);
        Literal literal = (Literal)(select.getQueryContext().getSelect(0));
        assertEquals("Literal should have the value \"Literal\".", "Literal", literal.evaluate(null));
    }

    // select max(VAL) from FOO
    public void test_select_max_of_val_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select max(VAL) from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one Selectable in the SELECT part", 1, select.getQueryContext().getSelectCount());
        Selectable selected = select.getQueryContext().getSelect(0);
        assertTrue("Selectable should be a FunctionIdentifier", selected instanceof FunctionIdentifier);
        FunctionIdentifier funid = (FunctionIdentifier)(selected);
        assertEquals("Function's name should be \"max\"", "MAX", funid.getName());
        assertEquals("Function should have one argument.", 1, funid.getArgumentCount());
        assertTrue("Function's one argument should be a ColumnIdentifier.", funid.getArgument(0) instanceof ColumnIdentifier);
        assertTrue("Argument's column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(funid.getArgument(0))).getName()));
    }

    // select count(*) from FOO
    public void test_select_countstar_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select count(*) from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one Selectable in the SELECT part", 1, select.getQueryContext().getSelectCount());
        Selectable selected = select.getQueryContext().getSelect(0);
        assertTrue("Selectable should be a FunctionIdentifier", selected instanceof FunctionIdentifier);
        FunctionIdentifier funid = (FunctionIdentifier)(selected);
        assertEquals("Function's name should be \"count\"", "COUNT", funid.getName());
        assertEquals("Function should have one argument.", 1, funid.getArgumentCount());
        assertTrue("Function's one argument should be a ColumnIdentifier.", funid.getArgument(0) instanceof ColumnIdentifier);
        assertTrue("Argument's column should be named *", "*".equalsIgnoreCase(((ColumnIdentifier)(funid.getArgument(0))).getName()));
    }

    // select xyzzy() from FOO
    public void test_select_xyzzy_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select xyzzy() from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one Selectable in the SELECT part", 1, select.getQueryContext().getSelectCount());
        Selectable selected = select.getQueryContext().getSelect(0);
        assertTrue("Selectable should be a FunctionIdentifier", selected instanceof FunctionIdentifier);
        FunctionIdentifier funid = (FunctionIdentifier)(selected);
        assertEquals("Function's name should be \"xyzzy\"", "XYZZY", funid.getName());
        assertEquals("Function should have no arguments.", 0, funid.getArgumentCount());
    }

    // select VAL, VALTWO, VALTHREE from FOO
    public void test_select_val_valtwo_valthree_from_foo() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL, VALTWO, VALTHREE from FOO");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly three columns in the SELECT part", 3, select.getQueryContext().getSelectCount());
        assertTrue("First column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));
        assertTrue("First column should be named VALTWO", "VALTWO".equalsIgnoreCase(((ColumnIdentifier)select.getQueryContext().getSelect(1)).getName()));
        assertTrue("First column should be named VALTHREE", "VALTHREE".equalsIgnoreCase(((ColumnIdentifier)select.getQueryContext().getSelect(2)).getName()));

        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
    }

    // select VAL, VALTWO from FOO, BAR
    public void test_select_val_valtwo_from_foo_bar() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL, VALTWO from FOO, BAR");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly two columns in the SELECT part", 2, select.getQueryContext().getSelectCount());
        assertTrue("First column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));
        assertTrue("First column should be named VALTWO", "VALTWO".equalsIgnoreCase(((ColumnIdentifier)select.getQueryContext().getSelect(1)).getName()));

        assertEquals("SelectCommand should have exactly two tables in the FROM part", 2, select.getQueryContext().getFromCount());
        assertTrue("First selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
        assertTrue("Second selected table should be named BAR", "BAR".equalsIgnoreCase(select.getQueryContext().getFrom(1).getTableName()));
    }

    // select VAL, VALTWO from FOO, BAR limit 10
    public void test_select_val_from_foo_limit() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL, VALTWO from FOO, BAR limit 10");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("Should have a limit", select.getQueryContext().getLimit());
        assertEquals("Limit should be 10", 10, ((Number)(select.getQueryContext().getLimit().evaluate(null))).intValue());
    }

    // select VAL, VALTWO from FOO, BAR where FOO.ID = BAR.ID limit 10
    public void test_select_with_literal_limit() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL, VALTWO from FOO, BAR where FOO.ID = BAR.ID limit 10");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("Should have a limit", select.getQueryContext().getLimit());
        assertEquals("Limit should be 10", 10, ((Number)(select.getQueryContext().getLimit().evaluate(null))).intValue());
    }

    // select VAL, VALTWO from FOO, BAR where FOO.ID = BAR.ID limit ?
    public void test_select_with_bindvar_limit() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL, VALTWO from FOO, BAR where FOO.ID = BAR.ID limit ?");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("Should have a limit", select.getQueryContext().getLimit());
        assertTrue("Limit should be a bind variable", select.getQueryContext().getLimit() instanceof BindVariable);
    }

    // select VAL from FOO where VALTWO = 3
    public void test_select_val_from_foo_where_valtwo_eq_3() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 3");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));

        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
    }

    // where VALTWO = 3
    public void testSelectWhereValEqNumericLiteral() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 3");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        /*
        assertTrue("WHERE should be a LeafWhereNode", select.getQueryContext().getWhere() instanceof LeafWhereNode);
        LeafWhereNode leaf = (LeafWhereNode)(select.getQueryContext().getWhere());
        assertNotNull("Left side should not be null", leaf.getLeft());
        assertTrue("Left side should be a ColumnIdentifier", leaf.getLeft() instanceof ColumnIdentifier);
        assertEquals("Column name should be VALTWO", "VALTWO", ((ColumnIdentifier)(leaf.getLeft())).getName());
        assertEquals(ComparisonOperator.EQUAL, leaf.getOperator());
        assertNotNull("Right side should not be null", leaf.getOperator());
        assertTrue("Right side should be a Literal", leaf.getRight() instanceof Literal);
        assertEquals("Right value should be 3", new Long(3), ((Literal)(leaf.getRight())).evaluate(null));
        */
    }

    // where VALTWO > 3
    public void testSelectWhereValGtNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO > 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals(">",select.getQueryContext().getWhere().getName());
    }

    // where VALTWO >= 3
    public void testSelectWhereValGtEqNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO >= 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals(">=",select.getQueryContext().getWhere().getName());
    }

    // where VALTWO < 3
    public void testSelectWhereValLtNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO < 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("<",select.getQueryContext().getWhere().getName());
    }

    // where VALTWO <= 3
    public void testSelectWhereValLtEqNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO <= 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("<=",select.getQueryContext().getWhere().getName());
    }

    // where VALTWO != 3
    public void testSelectWhereValBangEqNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO != 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("!=",select.getQueryContext().getWhere().getName());
    }

    // where NOT VALTWO = 3
    public void testSelectWhereNotValEqNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where NOT VALTWO = 3");
        SelectCommand select = (SelectCommand)cmd;
        FunctionIdentifier not = (FunctionIdentifier)(select.getQueryContext().getWhere());
        assertEquals("NOT",not.getName());
        assertEquals(1,not.getArgumentCount());
        assertEquals("=",not.getArgument(0).getName());
    }

    // where NOT (VALTWO = 3)
    public void testSelectWhereNotParenValEqNumericLiteralParen() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where NOT (VALTWO = 3)");
        SelectCommand select = (SelectCommand)cmd;
        FunctionIdentifier not = (FunctionIdentifier)(select.getQueryContext().getWhere());
        assertEquals("NOT",not.getName());
        assertEquals(1,not.getArgumentCount());
        assertEquals("=",not.getArgument(0).getName());
    }

    // where (NOT (VALTWO = 3))
    public void testSelectWhereParenNotParenValEqNumericLiteralParenParen() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where (NOT (VALTWO = 3))");
        SelectCommand select = (SelectCommand)cmd;
        FunctionIdentifier not = (FunctionIdentifier)(select.getQueryContext().getWhere());
        assertEquals("NOT",not.getName());
        assertEquals(1,not.getArgumentCount());
        assertEquals("=",not.getArgument(0).getName());
    }

    // where VALTWO <> 3
    public void testSelectWhereValLtGtNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO <> 3");
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("!=",select.getQueryContext().getWhere().getName());
    }

    // where VALTWO between 3 and 5
    public void testSelectWhereValBetweenNumericLiteral() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO between 3 and 5");
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull(select.getQueryContext().getWhere());
    }

    // where VALTWO between 3 and vALTHREE
    public void testSelectWhereValBetweenNumericLiteralAndColumn() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO between 3 and vALTHREE");
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull(select.getQueryContext().getWhere());
    }

    // where VALTWO = 'Literal'
    public void testSelectWhereValEqStringLiteral() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 'Literal'");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        /*
        assertTrue("WHERE should be a LeafWhereNode", select.getQueryContext().getWhere() instanceof LeafWhereNode);
        LeafWhereNode leaf = (LeafWhereNode)(select.getQueryContext().getWhere());
        assertNotNull("Left side should not be null", leaf.getLeft());
        assertTrue("Left side should be a ColumnIdentifier", leaf.getLeft() instanceof ColumnIdentifier);
        assertEquals("Column name should be VALTWO", "VALTWO", ((ColumnIdentifier)(leaf.getLeft())).getName());
        assertEquals(ComparisonOperator.EQUAL, leaf.getOperator());
        assertNotNull("Right side should not be null", leaf.getOperator());
        assertTrue("Right side should be a Literal", leaf.getRight() instanceof Literal);
        assertEquals("Right value should be \"Literal\"", "Literal", ((Literal) (leaf.getRight())).evaluate(null));
        */
    }

    // where VALTWO = 'Liter''l'
    public void testSelectWhereValEqStringLiteral2() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 'Liter''l'");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        /*
        assertTrue("WHERE should be a LeafWhereNode", select.getQueryContext().getWhere() instanceof LeafWhereNode);
        LeafWhereNode leaf = (LeafWhereNode)(select.getQueryContext().getWhere());
        assertNotNull("Left side should not be null", leaf.getLeft());
        assertTrue("Left side should be a ColumnIdentifier", leaf.getLeft() instanceof ColumnIdentifier);
        assertEquals("Column name should be VALTWO", "VALTWO", ((ColumnIdentifier)(leaf.getLeft())).getName());
        assertEquals(ComparisonOperator.EQUAL, leaf.getOperator());
        assertNotNull("Right side should not be null", leaf.getOperator());
        assertTrue("Right side should be a Literal", leaf.getRight() instanceof Literal);
        assertEquals("Right value should be \"Liter'l\"", "Liter'l", ((Literal)(leaf.getRight())).evaluate(null));
        */
    }

    // select VAL from FOO where VALTWO = 3 and A = B;
    public void test_select_val_from_foo_where_valtwo_eq_3_and_a_eq_b_semicolon() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 3 and A = B;");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));
        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
    }

    // select VAL from FOO where VALTWO = 3 and A = B
    public void test_select_val_from_foo_where_valtwo_eq_3_and_a_eq_b() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 3 and A = B");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);

        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));

        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));

        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
    }

    //  where (VALTWO = 3 and A = B)
    public void test_select_val_from_foo_where_paren_valtwo_eq_3_and_a_eq_b_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where (VALTWO = 3 and A = B)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be AND",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be AND","AND",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where VALTWO = 3 and (A = B)
    public void test_select_val_from_foo_where_valtwo_eq_3_and_paren_a_eq_b_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO = 3 and (A = B)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be AND",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be AND","AND",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where A = 1 and B = 2 or C = 3
    public void test_where_A_and_B_or_C() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where A = 1 and B = 2 or C = 3");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where A = 1 and B = 2 or (C = 3)
    public void test_where_A_and_B_or_paren_C_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where A = 1 and B = 2 or ( C = 3 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where A = 1 and (B = 2 or C = 3)
    public void test_where_A_and_paren_B_or_C_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where A = 1 and ( B = 2 or C = 3 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be AND",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be AND","AND",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where (A = 1 and B = 2 or C = 3)
    public void test_where_paren_A_and_B_or_C_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where ( A = 1 and B = 2 or C = 3 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where (A = 1 and B = 2 or (C = 3))
    public void test_where_paren_A_and_B_or_paren_C_paren_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where ( A = 1 and B = 2 or ( C = 3 ) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where (A = 1 and (B = 2 or C = 3))
    public void test_where_paren_A_and_paren_B_or_C_paren_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where ( A = 1 and ( B = 2 or C = 3 ) )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be AND",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be AND","AND",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where ((A = 1 and B = 2) or C = 3)
    public void test_where_paren_paren_A_and_B_paren_or_C_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where ( ( A = 1 and B = 2 ) or C = 3 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    //  where ((A = 1) and B = 2 or C = 3)
    public void test_where_paren_paren_A_paren_and_B_or_C_paren() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where ( ( A = 1 ) and B = 2 or C = 3 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
        assertTrue("Root WHERE node should be OR",select.getQueryContext().getWhere() instanceof FunctionIdentifier);
        assertEquals("Root WHERE node should be OR","OR",((FunctionIdentifier)(select.getQueryContext().getWhere())).getName());
    }

    // insert into FOO ( VAL ) values ( 1 )
    public void test_insert_into_foo_val_values_1() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("insert into FOO ( VAL ) values ( 1 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator colIter = insert.getColumnIterator();
        assertNotNull(colIter);
        assertTrue(colIter.hasNext());
        ColumnIdentifier colid = (ColumnIdentifier)(colIter.next());
        assertTrue("VAL".equalsIgnoreCase(colid.getName()));
        assertTrue(!colIter.hasNext());

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);
        assertTrue(valIter.hasNext());
        Literal value = (Literal)valIter.next();
        assertEquals(1, ((Number)(value.evaluate(null))).intValue());
        assertTrue(!valIter.hasNext());

        assertNotNull("Insert should have a table", insert.getTable());
        assertTrue("Table should be named FOO", "FOO".equalsIgnoreCase(insert.getTable().getTableName()));
    }

    // insert into FOO ( VAL ) values ( [LOTS_OF_DIGITS] )
    public void test_insert_into_foo_val_values_manydigits() throws Exception {
        Parser parser = getParser();

        long time = System.currentTimeMillis();
        AxionCommand cmd = parser.parse("insert into FOO ( VAL ) values ( " + time + " )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);
        assertTrue(valIter.hasNext());
        Literal value = (Literal)valIter.next();
        assertEquals(time, ((Number)(value.evaluate(null))).longValue());
        assertTrue(!valIter.hasNext());
    }

    // insert into FOO ( VAL, VALTWO ) values ( 1, 2 )
    public void test_insert_into_foo_val_valtwo_values_1_2() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("insert into FOO ( VAL, VALTWO ) values ( 1, 2 )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator colIter = insert.getColumnIterator();
        assertNotNull(colIter);
        assertTrue(colIter.hasNext());
        ColumnIdentifier colid = (ColumnIdentifier)(colIter.next());
        assertTrue("VAL".equalsIgnoreCase(colid.getName()));
        colid = (ColumnIdentifier)(colIter.next());
        assertTrue("VALTWO".equalsIgnoreCase(colid.getName()));
        assertTrue(!colIter.hasNext());

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);
        assertTrue(valIter.hasNext());
        Literal value = (Literal)valIter.next();
        assertEquals(1, ((Number)(value.evaluate(null))).intValue());
        value = (Literal)valIter.next();
        assertEquals(2, ((Number)(value.evaluate(null))).intValue());
        assertTrue(!valIter.hasNext());

        assertNotNull("Insert should have a table", insert.getTable());
        assertTrue("Table should be named FOO", "FOO".equalsIgnoreCase(insert.getTable().getTableName()));
    }

    // insert into FOO columns (val, valtwo) values (1, 'a')
    public void test_insert_into_foo_val_valtwo_values_1_a() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("insert into FOO columns(VAL,TYPE) values(1,'a')");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator colIter = insert.getColumnIterator();
        assertNotNull(colIter);
        assertTrue(colIter.hasNext());
        ColumnIdentifier colid = (ColumnIdentifier)(colIter.next());
        assertTrue("VAL".equalsIgnoreCase(colid.getName()));
        colid = (ColumnIdentifier)(colIter.next());
        assertTrue("TYPE".equalsIgnoreCase(colid.getName()));
        assertTrue(!colIter.hasNext());

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);
        assertTrue(valIter.hasNext());
        Literal value = (Literal)valIter.next();
        assertEquals(1, ((Number)(value.evaluate(null))).intValue());
        value = (Literal)valIter.next();
        assertEquals("a", value.evaluate(null));
        assertTrue(!valIter.hasNext());

        assertNotNull("Insert should have a table", insert.getTable());
        assertTrue("Table should be named FOO", "FOO".equalsIgnoreCase(insert.getTable().getTableName()));
    }
    
    // insert into FOO columns (val, valtwo) values (1, '\u0145')
    public void testInsertUnicode() throws Exception {
        /*
         * Insert an L stroke character and read it back
         */
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("insert into FOO columns(VAL,TYPE) values(1,'\u0145')");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator colIter = insert.getColumnIterator();
        assertNotNull(colIter);
        assertTrue(colIter.hasNext());
        ColumnIdentifier colid = (ColumnIdentifier)(colIter.next());
        assertTrue("VAL".equalsIgnoreCase(colid.getName()));
        colid = (ColumnIdentifier)(colIter.next());
        assertTrue("TYPE".equalsIgnoreCase(colid.getName()));
        assertTrue(!colIter.hasNext());

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);
        assertTrue(valIter.hasNext());
        Literal value = (Literal)valIter.next();
        assertEquals(1, ((Number)(value.evaluate(null))).intValue());
        value = (Literal)valIter.next();
        assertEquals("\u0145", value.evaluate(null));
        assertTrue(!valIter.hasNext());

        assertNotNull("Insert should have a table", insert.getTable());
        assertTrue("Table should be named FOO", "FOO".equalsIgnoreCase(insert.getTable().getTableName()));
    }

    // select VAL from FOO order by VAL
    public void test_select_val_from_foo_order_by_val() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by VAL");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertEquals("Should indicate VAL column","VAL", order.getSelectable().getName());
        assertTrue("Should not be descending", !order.isDescending());
    }

    // select VAL from FOO order by VAL desc
    public void test_select_val_from_foo_order_by_val_desc() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by VAL desc");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertEquals("Should indicate VAL column","VAL", order.getSelectable().getName());
        assertTrue("Should be descending", order.isDescending());
    }

    // select VAL from FOO order by VAL asc
    public void test_select_val_from_foo_order_by_val_asc() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by VAL asc");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertEquals("Should indicate VAL column","VAL", order.getSelectable().getName());
        assertTrue("Should not be descending", !order.isDescending());
    }

    // select VAL from FOO order by UPPER(VAL) asc
    public void test_select_val_from_foo_order_by_upper_val_asc() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by UPPER(VAL) asc");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertTrue(order.getSelectable() instanceof FunctionIdentifier);
        assertTrue("Should not be descending", !order.isDescending());
    }

    // select VAL from FOO order by UPPER(VAL)
    public void test_select_val_from_foo_order_by_upper_val() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by UPPER(VAL)");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertTrue(order.getSelectable() instanceof FunctionIdentifier);
        assertTrue("Should not be descending", !order.isDescending());
    }

    // select VAL from FOO order by UPPER(TRIM(VAL)) asc
    public void test_select_val_from_foo_order_by_upper_trim_val_asc() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("select VAL from FOO order by UPPER(TRIM(VAL)) asc");
        assertNotNull("Command should not be null", cmd);
        SelectCommand select = (SelectCommand)cmd;

        assertEquals("Should have 1 orderby node",
                1, select.getQueryContext().getOrderByCount());
        OrderNode order = select.getQueryContext().getOrderBy(0);
        assertTrue(order.getSelectable() instanceof FunctionIdentifier);
        assertTrue("Should not be descending", !order.isDescending());
    }

    public void test_update_foo_set_bar_eq_3() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("update FOO set BAR = 3");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an UpdateCommand", cmd instanceof UpdateCommand);
        UpdateCommand update = (UpdateCommand)cmd;
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Table name should be FOO", "FOO", update.getTable().getTableName());
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Should have one column", 1, update.getColumnCount());
        assertEquals("First column should be named BAR", "BAR",
                ((ColumnIdentifier)(update.getColumnIterator().next())).getName());
        assertEquals("Should have one value", 1, update.getValueCount());
        assertTrue("First value should be a literal",
                (update.getValueIterator().next()) instanceof Literal);
        assertEquals(3, ((Number)(((Literal)(update.getValueIterator().next())).evaluate(null))).intValue());
        assertTrue("Should not have a where clause", null == update.getWhere());
    }

    // update FOO set BAR = 3 where BARTWO > 7
    public void test_update_foo_set_bar_eq_3_where_bar_gt_7() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("update FOO set BAR = 3 where BARTWO > 7");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an UpdateCommand", cmd instanceof UpdateCommand);
        UpdateCommand update = (UpdateCommand)cmd;
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Table name should be FOO", "FOO", update.getTable().getTableName());
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Should have one column", 1, update.getColumnCount());
        assertEquals("First column should be named BAR", "BAR",
                ((ColumnIdentifier)(update.getColumnIterator().next())).getName());
        assertEquals("Should have one value", 1, update.getValueCount());
        assertTrue("First value should be a literal",
                (update.getValueIterator().next()) instanceof Literal);
        assertEquals(3, ((Number)(((Literal)(update.getValueIterator().next())).evaluate(null))).intValue());

        assertNotNull("Should have WHERE part", update.getWhere());
        /*
        assertTrue("WHERE should be a LeafWhereNode", update.getWhere() instanceof LeafWhereNode);
        LeafWhereNode leaf = (LeafWhereNode)(update.getWhere());
        assertNotNull("Left side should not be null", leaf.getLeft());
        assertTrue("Left side should be a ColumnIdentifier", leaf.getLeft() instanceof ColumnIdentifier);
        assertEquals("Column name should be BARTWO", "BARTWO", ((ColumnIdentifier)(leaf.getLeft())).getName());
        assertEquals(ComparisonOperator.GREATER_THAN, leaf.getOperator());
        assertNotNull("Right side should not be null", leaf.getOperator());
        assertTrue("Right side should be a Literal", leaf.getRight() instanceof Literal);
        assertEquals("Right value should be 7", new Long(7), ((Literal)(leaf.getRight())).evaluate(null));
        */
    }

    // update FOO set BAR = 3, BARSTR = 'Literal' where BARTWO > 7
    public void test_update_foo_set_bar_eq_3_barstr_eq_literalstr_where_bar_gt_7() throws Exception {
        Parser parser = getParser();

        AxionCommand cmd = parser.parse("update FOO set BAR = 3, BARSTR = 'Literal' where BARTWO > 7");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an UpdateCommand", cmd instanceof UpdateCommand);
        UpdateCommand update = (UpdateCommand)cmd;
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Table name should be FOO", "FOO", update.getTable().getTableName());
        assertNotNull("Should have a table", update.getTable());
        assertEquals("Should have two columns", 2, update.getColumnCount());
         {
            Iterator iter = update.getColumnIterator();
            assertEquals("First column should be named BAR", "BAR",
                    ((ColumnIdentifier)(iter.next())).getName());
            assertEquals("First column should be named BARSTR", "BARSTR",
                    ((ColumnIdentifier)(iter.next())).getName());
        }
        assertEquals("Should have two values", 2, update.getValueCount());
         {
            Iterator iter = update.getValueIterator();
            assertEquals(3, ((Number)(((Literal)(iter.next())).evaluate(null))).intValue());
            assertEquals("Second value should be 'Literal'", "Literal",
                    ((Literal)(iter.next())).evaluate(null));
        }
        assertNotNull("Should have WHERE part", update.getWhere());
        /*
        assertTrue("WHERE should be a LeafWhereNode", update.getWhere() instanceof LeafWhereNode);
        LeafWhereNode leaf = (LeafWhereNode)(update.getWhere());
        assertNotNull("Left side should not be null", leaf.getLeft());
        assertTrue("Left side should be a ColumnIdentifier", leaf.getLeft() instanceof ColumnIdentifier);
        assertEquals("Column name should be BARTWO", "BARTWO", ((ColumnIdentifier)(leaf.getLeft())).getName());
        assertEquals(ComparisonOperator.GREATER_THAN, leaf.getOperator());
        assertNotNull("Right side should not be null", leaf.getOperator());
        assertTrue("Right side should be a Literal", leaf.getRight() instanceof Literal);
        assertEquals("Right value should be 7", new Long(7), ((Literal)(leaf.getRight())).evaluate(null));
        */
    }

    // these tests moved here from TestAxionSqlParser,
    // but are as generic as anything else

    public void testCreateTableOneColumn() throws Exception {
        AxionCommand command = getParser().parse("create table myTable (myCol varchar)");
        assertNotNull("Command should not be null", command);
    }


    public void testCreateTableMultipleColumns() throws Exception {
        AxionCommand command = getParser().parse("create table myTable (myCol varchar, myCol2 integer)");
        assertNotNull("Command should not be null", command);
    }


    public void testInsert() throws Exception {
        AxionCommand command = getParser().parse("insert into myTable ( a, b, c) values ( 'aa', 'bb', 'cc' )");
        assertNotNull("Command should not be null", command);
    }


    public void testSelectColumnFromOneTable() throws Exception {
        AxionCommand command = getParser().parse("select fins from tunafish");

        assertNotNull("Command should not be null", command);
        assertTrue("Command should not be of type SelectCommand", (command instanceof SelectCommand));

        SelectCommand select = (SelectCommand)command;
        assertEquals("There should be one ColumnIdentifier",1,select.getQueryContext().getSelectCount());
        assertTrue("Should have column \"fins\"", ((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName().equalsIgnoreCase("fins"));
    }


    public void testSelectColumnsFromOneTable() throws Exception {
        AxionCommand command = getParser().parse("select fins, fishy from tunafish");

        assertNotNull("Command should not be null", command);
        assertTrue("Command should not be of type SelectCommand", (command instanceof SelectCommand));

        SelectCommand select = (SelectCommand)command;
        assertEquals("There should be two ColumnIdentifiers",2,select.getQueryContext().getSelectCount());


        boolean hasFins = false;
        boolean hasFishy = false;

        for(int i = 0; i < select.getQueryContext().getSelectCount(); i++) {
            hasFins |= ((ColumnIdentifier)(select.getQueryContext().getSelect(i))).getName().equalsIgnoreCase("fins");
            hasFishy |= ((ColumnIdentifier)(select.getQueryContext().getSelect(i))).getName().equalsIgnoreCase("fishy");
        }

        assertTrue("Should have column \"fins\"", hasFins);
        assertTrue("Should have column \"fishy\"", hasFishy);
    }


    public void testSelectAllFromOneTable() throws Exception {
        AxionCommand command = getParser().parse("select * from tunafish");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should not be of type SelectCommand", (command instanceof SelectCommand));
        SelectCommand select = (SelectCommand)command;
        assertEquals("There should be one ColumnIdentifier",1,select.getQueryContext().getSelectCount());
        assertTrue("Should have column \"*\"", ((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName().equals("*"));
    }

    public void test_shutdown() throws Exception {
        AxionCommand command = getParser().parse("shutdown");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should be of type ShutdownCommand", (command instanceof ShutdownCommand));
    }
    
    public void test_remount() throws Exception {
        AxionCommand command = getParser().parse("remount 'D:\\\\f''oo//bar'");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should be of type RemountCommand", (command instanceof RemountCommand));
        RemountCommand remount = (RemountCommand)(command);
        assertEquals("D:\\\\f'oo//bar",remount.getDirectory());
        assertNull(remount.getTable());
        assertTrue(!remount.getDataFilesOnly());
    }
    
    public void test_remount_with_bindvar() throws Exception {
        AxionCommand command = getParser().parse("remount ?");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should be of type RemountCommand", (command instanceof RemountCommand));
        RemountCommand remount = (RemountCommand)(command);
        assertTrue(remount.getDirectory() instanceof BindVariable);
    }
    
    public void test_remount_table() throws Exception {
        AxionCommand command = getParser().parse("remount BAR '/dbs/foo/bar'");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should be of type RemountCommand", (command instanceof RemountCommand));
        RemountCommand remount = (RemountCommand)(command);
        assertEquals("/dbs/foo/bar",remount.getDirectory());
        assertNotNull(remount.getTable());
        assertEquals(new TableIdentifier("BAR"),remount.getTable());
        assertTrue(!remount.getDataFilesOnly());
    }
    
    public void test_remount_table_data() throws Exception {
        AxionCommand command = getParser().parse("remount BAR data '/dbs/foo/bar'");
        assertNotNull("Command should not be null", command);
        assertTrue("Command should be of type RemountCommand", (command instanceof RemountCommand));
        RemountCommand remount = (RemountCommand)(command);
        assertEquals("/dbs/foo/bar",remount.getDirectory());
        assertNotNull(remount.getTable());
        assertEquals(new TableIdentifier("BAR"),remount.getTable());
        assertTrue(remount.getDataFilesOnly());
    }
    
    public void test_bad_remount() throws Exception {
        try {
            getParser().parse("remount");
            fail("Expected exception to be thrown.");
        } catch(Exception e) {
            // expected
        }
    }
    
    public void testWithComment() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL /* this is a comment */ from /* this is a comment */ FOO where /* this is a comment */ VALTWO = 3");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));
        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
    }

    public void testAxionSpecificKeywordsAreSafe() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("create table ARRAY ( CHECKPOINT integer, REMOUNT integer, SHUTDOWN integer, \"DATA\" integer, TYPE integer, \"INTEGER\" integer )");
        assertNotNull(cmd);
    }

    public void testBooleanLiterals() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("insert into FOO values ( TRUE, FALSE, true, false, 'true', 'false' )");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be an Insert", cmd instanceof InsertCommand);

        InsertCommand insert = (InsertCommand)cmd;

        Iterator valIter = insert.getValueIterator();
        assertNotNull(valIter);

        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals(Boolean.TRUE, value.evaluate(null));
            assertTrue(value.getDataType() instanceof BooleanType);
        }
        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals(Boolean.FALSE, value.evaluate(null));
            assertTrue(value.getDataType() instanceof BooleanType);
        }
        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals(Boolean.TRUE, value.evaluate(null));
            assertTrue(value.getDataType() instanceof BooleanType);
        }
        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals(Boolean.FALSE, value.evaluate(null));
            assertTrue(value.getDataType() instanceof BooleanType);
        }
        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals("true", value.evaluate(null));
            assertTrue(value.getDataType() instanceof CharacterType);
        }
        {
            assertTrue(valIter.hasNext());
            Literal value = (Literal)valIter.next();
            assertEquals("false", value.evaluate(null));
            assertTrue(value.getDataType() instanceof CharacterType);
        }

        assertTrue(!valIter.hasNext());

    }

    public void testWhereIn() throws Exception {
        Parser parser = getParser();
        AxionCommand cmd = parser.parse("select VAL from FOO where VALTWO in (1,2, 3)");
        assertNotNull("Command should not be null", cmd);
        assertTrue("Command should be a SelectCommand", cmd instanceof SelectCommand);
        SelectCommand select = (SelectCommand)cmd;
        assertEquals("SelectCommand should have exactly one column in the SELECT part", 1, select.getQueryContext().getSelectCount());
        assertTrue("Selected column should be named VAL", "VAL".equalsIgnoreCase(((ColumnIdentifier)(select.getQueryContext().getSelect(0))).getName()));
        assertEquals("SelectCommand should have exactly one table in the FROM part", 1, select.getQueryContext().getFromCount());
        assertTrue("Selected table should be named FOO", "FOO".equalsIgnoreCase(select.getQueryContext().getFrom(0).getTableName()));
        assertNotNull("SelectCommand should have WHERE part", select.getQueryContext().getWhere());
//        assertTrue("Where clause should be IN type", select.getQueryContext().getWhere() instanceof InWhereNode);
    }
    
//    public void testRejectionOfNegativePrecision() throws Exception {
//        Parser parser = getParser();
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, BNF definition of precision
//            parser.parse("create table foo (mybadnum numeric(-1))");
//            fail("Expected parser to reject negative precision value.");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }        
//    }
//    
//    public void testRejectionOfZeroPrecisionNoScaleSupplied() throws Exception {
//        Parser parser = getParser();
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, Syntax Rule 4
//            parser.parse("create table foo (mybadnum numeric(0))");
//            fail("Expected parser to reject precision value of zero (no scale supplied).");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }
//    }
//    
//    public void testRejectionOfZeroPrecisionWithScaleSupplied() throws Exception {
//        Parser parser = getParser();        
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, Syntax Rule 4
//            parser.parse("create table foo (mybadnum numeric(0, 4))");
//            fail("Expected parser to reject precision value of zero (with scale supplied).");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }        
//    }
//    
//    public void testRejectionOfNegativeScale() throws Exception {
//        Parser parser = getParser();
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, BNF definition of scale
//            parser.parse("create table foo (mybadnum numeric(10, -1))");
//            fail("Expected parser to reject negative scale value.");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }        
//    }
//    
//    public void testRejectionOfScaleLargerThanPrecision() throws Exception {
//        Parser parser = getParser();
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, Syntax Rule 19
//            parser.parse("create table foo (mybadnum numeric(3,10))");
//            fail("Expected parser to reject scale value that is larger than precision value.");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }
//    }
//    
//    public void testRejectionOfZeroCharLength() throws Exception {
//        Parser parser = getParser();
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, Syntax Rule 4
//            parser.parse("create table foo (mybadchar char(0))");
//            fail("Expected parser to reject zero length value for char datatype.");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }
//    }
//    
//    public void testRejectionOfZeroVarcharLength() throws Exception {
//        Parser parser = getParser();        
//        try {
//            // ISO/IEC 9075-2:2003, Section 6.1, Syntax Rule 4
//            parser.parse("create table foo (mybadvarchar varchar(0))");
//            fail("Expected parser to reject zero length value for varchar datatype.");
//        } catch (AxionException ignore) {
//            // Expected this exception - move on.
//        }
//    }
}

