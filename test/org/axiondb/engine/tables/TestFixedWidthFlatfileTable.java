/*
 * $Id: TestFixedWidthFlatfileTable.java,v 1.1 2007/11/28 10:01:28 jawed Exp $
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

package org.axiondb.engine.tables;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.ExternalTableLoader;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.commands.AxionQueryContext;
import org.axiondb.engine.commands.CreateTableCommand;
import org.axiondb.engine.commands.SubSelectCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BooleanType;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:28 $
 * @author Ahimanikya Satapathy
 */
public class TestFixedWidthFlatfileTable extends AbstractTableTest {

    //------------------------------------------------------------ Conventional

    public TestFixedWidthFlatfileTable(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestFixedWidthFlatfileTable.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Lifecycle

    protected DiskDatabase _db = null;
    protected String tableName = null;
    protected String dataFileName = null;

    protected Table createTable(String name) throws Exception {
        tableName = name;
        ExternalTableLoader loader = new FixedWidthFlatfileTableLoader();
        ExternalTable t = (ExternalTable) loader.createTable(_db, name);
        t.loadExternalTable(setProperties(name));
        return t;
    }
    
    protected Database getDatabase() throws Exception {
        return _db;
    }
    
    protected File getDataFile() throws Exception {
        return new File(getDbdir(), dataFileName);
    }

    protected Properties setProperties(String name) {
        Properties props = new Properties();

        props.setProperty(ExternalTable.PROP_LOADTYPE, "fixedwidth");
        String eol = System.getProperty("line.separator");
        props.setProperty(BaseFlatfileTable.PROP_RECORDDELIMITER, eol);
        props.setProperty(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "false");

        dataFileName = name + ".txt";
        props.setProperty(BaseFlatfileTable.PROP_FILENAME, dataFileName);

        return props;
    }

    protected String getTableName() {
        return tableName != null ? tableName : "FWTXT";
    }

    public void setUp() throws Exception {
        getDbdir().mkdirs();
        _db = new DiskDatabase(getDbdir());
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _db.shutdown();
        File data = new File(getDbdir(), tableName + ".txt");
        data.delete();
    }

    //------------------------------------------------------------------- Tests

    public void testObjectTable() throws Exception {
        // TODO: Make this test pass, define a interface MarshallableObject for method
        // toString and toObject, or or MarshallableXMLObject toXMLString , toObject , If
        // the Object implement this then we can use them in flat file.
    }

    public void testInvalidPropertyKey() throws Exception {
        try {
            Properties badProps = new Properties();
            badProps.put(ExternalTable.PROP_LOADTYPE, "fixedwidth");
            badProps.put("UNKNOWN_PROPERTY", Boolean.TRUE);
            ExternalTableFactory factory = new ExternalTableFactory();
            factory.createTable(_db, "BADTABLE", badProps, buildColumns());
            fail("Expected AxionException due to unrecognized property name 'UNKNOWN_PROPERTY'");
        } catch (AxionException expected) {
            // Expected AxionException due to unrecognized property name.
        }
    }
    
    public void testDataTypes() throws Exception {
        Table typeTable = createTable("TYPETABLE");

        ((ExternalTable)typeTable).loadExternalTable(setProperties("TYPETABLE"));
        
        typeTable.addColumn(new Column("STRCOL",new CharacterVaryingType(255)));
        typeTable.addColumn(new Column("INTCOL",new IntegerType()));
        Column bolCol = new Column("BOOLCOL",new BooleanType());
        typeTable.addColumn(bolCol);
        
        // This will allow us to test truncating value 
        // note: unless user mentioned a size for string its default to 255
        String name = "";
        for(int i =0 ; i < 10 ; i++) {
            name += "cccddvvffvv";
        }

        Object[][] values = new Object[][] {
            new Object[] { "", "A String", name, null },
            new Object[] { new Integer(17), new Integer(0), new Integer(5575), null },
            new Object[] { Boolean.TRUE, Boolean.TRUE, Boolean.FALSE, null }
        };

        Random random = new Random();
        int numRows = 7;
        
        for(int i=0;i<numRows;i++) {
            Row row = new SimpleRow(typeTable.getColumnCount());
            for(int j=0;j<typeTable.getColumnCount();j++) {
                row.set(j,values[j][random.nextInt(values[j].length)]);
            }
            typeTable.addRow(row);
        }

        RowIterator iter = typeTable.getRowIterator(true);
        assertNotNull(iter);
        for(int i=0;i<numRows;i++) {
            assertTrue(iter.hasNext());
            assertNotNull(iter.next());
        }
        assertTrue(!iter.hasNext());
        typeTable.shutdown();
    }

    public void testDiskInsert() throws Exception {
        testAddRow();
        table.shutdown();
        File data = new File(getDbdir(), "FWTXT.txt");
        assertTrue("Should have data file", data.exists());
        assertTrue("Should have some data in data file", data.length() > 43);
    }

    public void testDiskDrop() throws Exception {
        testAddRow();
        File tabledir = new File(getDbdir(), "FWTXT");
        File meta = new File(tabledir, "FWTXT.META");
        assertTrue("Table directory should exist", tabledir.exists());
        assertTrue("Meta file should exist", meta.exists());
        table.drop();
        assertTrue("Meta file should not exist", !meta.exists());
        assertTrue("Table directory should not exist", !tabledir.exists());
    }
    
    public void testRemount() throws Exception {
        testAddRow();
        ((ExternalTable)table).remount();

        RowIterator iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
    }
    
    public void testFileRead() throws Exception {
        File data = new File(getDbdir(), "FFTest.txt");
        FileWriter out = new FileWriter(data);

        String eol = System.getProperty("line.separator");

        out.write("ID         NAME " + eol); // Header
        out.write("1          aa   " + eol); // 1
        out.write("2.00       bbb  " + eol); // 2
        out.write("3.00       ccc  " + eol); // 3
        out.write("4.00       ddd  " + eol); // 4
        out.write("" + eol);                 // skip
        out.write("we       " + eol);        // bad 1
        out.write("7          dfdf " + eol); // 5
        out.write("7.0f       ccc  " + eol); // 6
        out.write("xx         xx   " + eol); // bad 2
        out.write("5          test " + eol); // 7
        out.write("10-1       hhhh " + eol); // bad 3
        out.write("                " + eol); // bad 4
        out.close();

        ExternalTableFactory factory = new ExternalTableFactory();
        Properties prop = setProperties("FFTest");
        prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "1");

        try {
            Table table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
            RowIterator itr = table2.getRowIterator(false);
    
            int rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
    
            assertEquals("Valid row count should have correct value", 7, rowCount);
            table2.drop();
            
            
            prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "2");
            
            try {
                table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
                itr = table2.getRowIterator(false);
                while (itr.hasNext()) {
                    itr.next();
                }
                fail("Expected Exception");
            } catch(Exception e) {
                // expected
            }
            table2.drop();
            
            // bad property value, should use default value
            prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "-10"); 
            prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "-10");
            
            table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
            itr = table2.getRowIterator(false);
    
            rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
    
            assertEquals("Valid row count should have correct value", 7, rowCount);
            table2.drop();
            
            // bad property value, should use default value        
            prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "ABC");
            prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "ABC");
    
            table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
            itr = table2.getRowIterator(false);
    
            rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
    
            assertEquals("Valid row count should have correct value", 7, rowCount);
            table2.drop();
    
            prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "5");
            prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "0");
            table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
            itr = table2.getRowIterator(false);
    
            rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
    
            assertEquals("Valid row count should have correct value", 7, rowCount);
            
            _db.addTable(table2);
            CreateTableCommand cmd = new CreateTableCommand();
            cmd.setObjectName("FFTEST2");
            cmd.setType("external");
            prop.put(BaseFlatfileTable.PROP_FILENAME, "FFTest2.txt");
            prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");
            cmd.setProperties(prop);
            AxionQueryContext ctx = new AxionQueryContext();
            ctx.addSelect(new ColumnIdentifier("*"));
            ctx.addFrom(new TableIdentifier("FFTEST"));
            SubSelectCommand subSelect = new SubSelectCommand(ctx);
            cmd.setSubQuery(subSelect);
            cmd.execute(_db);
            
            Table table3 = _db.getTable("FFTEST2");
            itr = table3.getRowIterator(false);
            
            assertEquals("Valid row count should have correct value", 7, table3.getRowCount());
    
            rowCount = 0;
            while (itr.hasNext()) {
                itr.next();
                rowCount++;
            }
    
            assertEquals("Valid row count should have correct value", 7, rowCount);
        } finally {
            try {
                _db.dropTable("FFTEST");
            } catch (AxionException ignore) {
                // ignore
            }
            
            try {
                _db.dropTable("FFTEST2");
            } catch (AxionException ignore) {
                // ignore
            }
    
            data.delete();
        }

    }
    
    public void testFileReadForPackedRecord() throws Exception {
        File data = new File(getDbdir(), "FFTEST.txt");
        FileWriter out = new FileWriter(data);

        out.write("ID         NAME "); // Header
        out.write("1          aa   "); // 1
        out.write("2          bbb  "); // 2
        out.write("3.00       ccc  "); // 3
        out.write("4.00       ddd  "); // 4
        out.write("");                 // skip
        out.write("we              "); // bad 1
        out.write("7          dfdf "); // 5
        out.write("7.0f       ccc  "); // 6
        out.write("xx         xx   "); // bad 2
        out.write("10-1       hhhh "); // bad 3
        out.write("                "); // bad 4
        out.write("5          test");  // 7 EOF before end of record      
        out.close();

        ExternalTableFactory factory = new ExternalTableFactory();
        Properties prop = setProperties("FFTEST");
        prop.put(BaseFlatfileTable.PROP_RECORDDELIMITER, "");
        prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "1");
        
        Table table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
        RowIterator itr = table2.getRowIterator(false);

        int rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        table2.drop();
        
        
        prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "2");
        
        try {
            table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
            itr = table2.getRowIterator(false);
            while (itr.hasNext()) {
                itr.next();
            }
            fail("Expected Exception");
        } catch(Exception e) {
            // expected
        }
        table2.drop();
        
        // bad property value, should use default value
        prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "-10"); 
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "-10");
        
        table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
        itr = table2.getRowIterator(false);

        rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        table2.drop();
        
        // test headerBytesOffset 
        prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "false");
        prop.put(FixedWidthFlatfileTable.PROP_HEADERBYTESOFFSET, "16"); 
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "0");
        
        table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
        itr = table2.getRowIterator(false);
        
        rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        table2.drop();        
        
        // bad property value, should use default value    
        prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");
        prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "ABC");
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "ABC");

        table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
        itr = table2.getRowIterator(false);

        rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        table2.drop();

        prop.put(BaseFlatfileTable.PROP_MAXFAULTS, "5");
        prop.put(BaseFlatfileTable.PROP_ROWSTOSKIP, "0");
        table2 = factory.createTable(_db, "FFTEST", prop, buildColumns());
        itr = table2.getRowIterator(false);

        rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        
        _db.addTable(table2);
        CreateTableCommand cmd = new CreateTableCommand();
        cmd.setObjectName("FFTEST2");
        cmd.setType("external");
        prop.put(BaseFlatfileTable.PROP_FILENAME, "FFTest2.txt");
        prop.put(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");
        cmd.setProperties(prop);
        AxionQueryContext ctx = new AxionQueryContext();
        ctx.addSelect(new ColumnIdentifier("*"));
        ctx.addFrom(new TableIdentifier("FFTEST"));
        SubSelectCommand subSelect = new SubSelectCommand(ctx);
        cmd.setSubQuery(subSelect);
        cmd.execute(_db);
        
        Table table3 = _db.getTable("FFTEST2");
        itr = table3.getRowIterator(false);
        
        assertEquals("Valid row count should have correct value", 7, table3.getRowCount());

        rowCount = 0;
        while (itr.hasNext()) {
            itr.next();
            rowCount++;
        }

        assertEquals("Valid row count should have correct value", 7, rowCount);
        _db.dropTable("FFTEST");
        _db.dropTable("FFTEST2");

        data.delete();

    }
    
    public void testRestartDB() throws Exception {
        testAddRow();
        table.shutdown();
        table = null;
        _db.shutdown();

        _db = new DiskDatabase(getDbdir());
        assertTrue(_db.hasTable(getTableName()));
        table = _db.getTable(getTableName());
        testGetName();
        RowIterator iter = table.getRowIterator(true);
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
        assertTrue(!iter.hasNext());
    }

    private List buildColumns() {
        List list = new ArrayList(2);
        Column id = new Column("ID", new IntegerType());
        list.add(id);
        
        Column name = new Column("NAME", new CharacterVaryingType(5));
        list.add(name);
        return list;
    }
}