/*
 * $Id: TestTaggedEBCDICFlatfileTable.java,v 1.3 2008/02/21 13:00:27 jawed Exp $
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
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.constraints.NotNullConstraint;
import org.axiondb.constraints.NullConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.BigDecimalType;
import org.axiondb.types.BooleanType;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.3 $ $Date: 2008/02/21 13:00:27 $
 * @author Ahimanikya Satapathy
 */
public class TestTaggedEBCDICFlatfileTable extends AbstractTableTest {

    //------------------------------------------------------------ Conventional

    public TestTaggedEBCDICFlatfileTable(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestTaggedEBCDICFlatfileTable.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    //--------------------------------------------------------------- Lifecycle

    protected DiskDatabase _db = null;
    protected String tableName = null;
    protected String dataFileName = null;

    protected Table createTable(String name) throws Exception {
        name = name.toUpperCase();
        tableName = name;
        ExternalTableLoader loader = new TaggedEBCDICTableLoader();
        ExternalTable t = (ExternalTable) loader.createTable(_db, name);
        t.loadExternalTable(setProperties(name));
        return t;
    }
    
    protected Database getDatabase() throws Exception {
        return _db;
    }
    
    public void testAddThenDropColumn() throws Exception {
        // TODO: make this test case pass
    }
    
    protected File getDataFile() throws Exception {
        return new File(getDbdir(), dataFileName);
    }

    protected Properties setProperties(String name) {
        Properties props = new Properties();
        
        props.setProperty(ExternalTable.PROP_LOADTYPE, "taggedebcdic");
        props.setProperty(TaggedEBCDICTable.PROP_RECORDLENGTH, "213");
        props.setProperty(TaggedEBCDICTable.PROP_HEADERBYTESOFFSET, "24");
        props.setProperty(TaggedEBCDICTable.PROP_TAGLENGTH, "4");
        props.setProperty(TaggedEBCDICTable.PROP_MINTAGCOUNT, "1");
        props.setProperty(TaggedEBCDICTable.PROP_MAXTAGCOUNT, "48");
        props.setProperty(TaggedEBCDICTable.PROP_TAGBYTECOUNT, "2");
        props.setProperty(TaggedEBCDICTable.PROP_ENCODING, "cp037");

        dataFileName = name.toUpperCase() + ".txt";
        props.setProperty(BaseFlatfileTable.PROP_FILENAME, dataFileName);

        return props;
    }

    protected String getTableName() {
        return tableName != null ? tableName : "INPUT";
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
            badProps.put(ExternalTable.PROP_LOADTYPE, "taggedebcdic");
            badProps.put("UNKNOWN_PROPERTY", Boolean.TRUE);
            ExternalTableFactory factory = new ExternalTableFactory();
            factory.createTable(_db, "BadTable", badProps, buildColumns());
            fail("Expected AxionException due to unrecognized property name 'UNKNOWN_PROPERTY'");
        } catch (AxionException expected) {
            // Expected AxionException due to unrecognized property name.
        }
    }
    
    public void testDataTypes() throws Exception {
        Table typeTable = createTable("TYPETABLE");

        typeTable.addColumn(new Column("STRCOL",new CharacterVaryingType(30)));
        typeTable.addColumn(new Column("INTCOL",new IntegerType()));
        typeTable.addColumn(new Column("BOOLCOL",new BooleanType()));

        // TODO: Make null value test pass
        Object[][] values = new Object[][] {
            new Object[] { "", "A String", "Another String", "Yet Another String" },
            new Object[] { new Integer(17), new Integer(0), new Integer(5575), new Integer(22) },
            new Object[] { Boolean.TRUE, Boolean.TRUE, Boolean.FALSE, Boolean.TRUE }
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
        File data = new File(getDbdir(), "INPUT.txt");
        assertTrue("Should have data file", data.exists());
        assertTrue("Should have some data in data file", data.length() > 41);
    }
    
    public void testAddPrimaryKeyConstraintOnPopulatedTable() throws Exception {
        // TODO: Make this test pass
    }
    
    public void testAddThenDropConstraint() throws Exception {
        addColumns();
        Database db = getDatabase();
        db.addTable(table);
        PrimaryKeyConstraint pk = new PrimaryKeyConstraint("PK_FOO");
        pk.addSelectable(new ColumnIdentifier(new TableIdentifier(table.getName()), "ID"));
        table.addConstraint(pk);
        addRows();
        Column column = table.getColumn("ID");
        if (!table.isColumnIndexed(column)) {
            assertFalse(table.hasIndex("BOGUS"));
            Index index1 = db.getIndexFactory("btree").makeNewSystemInstance(table, column, db.getDBDirectory() == null);
            Index index2 = db.getIndexFactory("array").makeNewInstance("INDEX_FOO", column, true, db.getDBDirectory() == null);
            db.addIndex(index1, table, true);
            assertTrue(table.hasIndex(index1.getName()));
            db.addIndex(index2, table, true);
            assertTrue(table.hasIndex(index2.getName()));
        }

        try {
            table.addConstraint(new PrimaryKeyConstraint("PK_BAR"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }
        try {
            table.addConstraint(new NotNullConstraint("PK_FOO"));
            fail("Expected AxionException");
        } catch (AxionException e) {
            // expected
        }

        table.removeConstraint("this constraint does not exist");
        table.removeConstraint("PRIMARYKEY");
        //table.addConstraint(pk);
        table.removeConstraint("pk_foo");
        table.removeConstraint("PRIMARYKEY"); // shd be silent

        //table.addConstraint(pk);
        table.addConstraint(new NotNullConstraint("NN_FOO"));
        table.removeConstraint("NN_FOO");
        table.addConstraint(new NullConstraint("N_FOO"));
        table.removeConstraint("N_FOO");
        table.removeConstraint(null);
        db.dropTable(table.getName());
        table = null;
    }    

    public void testDiskDrop() throws Exception {
        testAddRow();
        File tabledir = new File(getDbdir(), "INPUT");
        File meta = new File(tabledir, "INPUT.META");
        assertTrue("Table directory should exist", tabledir.exists());
        assertTrue("Meta file should exist", meta.exists());
        table.drop();
        assertTrue("Meta file should not exist", !meta.exists());
        assertTrue("Table directory should not exist", !tabledir.exists());
    }
    
//    private void writeBytes(RandomAccessFile out, String str) throws Exception{
//        byte[] ascii = str.getBytes();
//        AsciiEbcdicEncoder.convertAsciiToEbcdic(ascii);
//        out.write(ascii);
//    }
    
//    public void testFileRead() throws Exception {
//        File data = new File(getDbdir(), "FFTest.txt");
//        RandomAccessFile out = new BufferedRandomAccessFile(data, "rw");
//
//        writeBytes(out,"ID  NAME " ); // Header
//        writeBytes(out,"1   aa   " ); // 1
//        writeBytes(out,"2.00bbb  " ); // 2
//        writeBytes(out,"3.00ccc  " ); // 3
//        writeBytes(out,"4.00ddd  " ); // 4
//        writeBytes(out,"" );          // skip
//        writeBytes(out,"we       " ); // bad 1
//        writeBytes(out,"7   dfdf " ); // 5
//        writeBytes(out,"7.0fccc  " ); // 6
//        writeBytes(out,"xx  xx   " ); // bad 2
//        writeBytes(out,"5   test " ); // 7
//        writeBytes(out,"10-1hhhh " ); // bad 3
//        writeBytes(out,"         " ); // bad 4
//        out.close();
//        
//        Properties props = new Properties();
//        
//        props.setProperty(ExternalTable.PROP_LOADTYPE, "taggedebcdic");
//        props.setProperty(TaggedEBCDICTable.PROP_RECORDLENGTH, "9");
//        props.setProperty(TaggedEBCDICTable.PROP_HEADERBYTESOFFSET, "0");
//        props.setProperty(TaggedEBCDICTable.PROP_TAGLENGTH, "4");
//        props.setProperty(TaggedEBCDICTable.PROP_MINTAGCOUNT, "1");
//        props.setProperty(TaggedEBCDICTable.PROP_MAXTAGCOUNT, "48");
//        props.setProperty(TaggedEBCDICTable.PROP_TAGBYTECOUNT, "2");
//        props.setProperty(TaggedEBCDICTable.PROP_ENCODING, "cp037");
//        props.setProperty(BaseFlatfileTable.PROP_FILENAME, "FFTest.txt");
//        
//        ExternalTableFactory factory = new ExternalTableFactory();
//        
//        Table table2 = factory.createTable(_db, "FFTest", props, buildColumns());
//        RowIterator itr = table2.getRowIterator(false);
//
//        // the intial load should ignore empty line
//        assertEquals("Total line Should have correct value", 11, table2.getRowCount());
//
//        int rowCount = 0;
//        while (itr.hasNext()) {
//            itr.next();
//            rowCount++;
//        }
//
//        assertEquals("Valid row Count Should have correct value", 7, rowCount);
//        table2.drop();
//
//    }
    
    public void testDefrag() throws Exception {
    
    }
    
    private List buildColumns() {
        List list = new ArrayList(2);
        Column id = new Column("ID", new BigDecimalType(4));
        list.add(id);
        
        Column name = new Column("NAME", new CharacterVaryingType(5));
        list.add(name);
        return list;
    }
}