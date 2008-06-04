/*
 * $Id: TestDiskDatabase.java,v 1.2 2007/11/29 16:35:36 jawed Exp $
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

package org.axiondb.engine;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.Literal;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.AbstractDatabaseTest;
import org.axiondb.engine.commands.CreateTableCommand;
import org.axiondb.engine.commands.InsertCommand;
import org.axiondb.types.IntegerType;
import org.axiondb.types.LOBType;

/**
 * @version $Revision: 1.2 $ $Date: 2007/11/29 16:35:36 $
 * @author Chuck Burdick
 */
public class TestDiskDatabase extends AbstractDatabaseTest {

    //------------------------------------------------------------ Conventional

    public TestDiskDatabase(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestDiskDatabase.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestDiskDatabase.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        _dbs = new ArrayList();
        super.setUp();       
    }

    public void tearDown() throws Exception {
        for(Iterator iter = _dbs.iterator(); iter.hasNext();) {
            DiskDatabase db = (DiskDatabase)(iter.next());
            db.shutdown();
        }
        super.tearDown();
        deleteFile(_tempdir);
    }

    //-------------------------------------------------------------------- Util

    private List _dbs = null;
    
    private File _tempdir = new File(new File("."), "testmove");

    protected Database createDatabase(String name) throws Exception {
        DiskDatabase db = new DiskDatabase(name, getDbdir());
        _dbs.add(db);
        return db;
    }

    //------------------------------------------------------------------- Tests
//Commented on 29-Nov-2007
//    public void testCreateTableGetTable() throws Exception {
//        super.testCreateTableGetTable();
//        assertTrue("Should have a db dir", getDbdir().exists());
//        File tabledir = new File(getDbdir(),"FOO");
//        File meta = new File(tabledir, "FOO.DATA");
//        assertTrue("Should have table directory", tabledir.exists());
//        assertTrue("Should have a meta file", meta.exists());
//        getDb().shutdown();
//        
//        // Recreate from disk
//        Database another = createDatabase(getDb().getName());
//        Table sysCol = another.getTable("AXION_COLUMNS");
//        assertNotNull("Should have system table of columns", sysCol);
//        boolean found = findStringInTable("ID", sysCol, "COLUMN_NAME");
//        assertTrue("Should find entry for column in system table", found);
//    }

    public void testDropTable() throws Exception {
        super.testDropTable();
        File tabledir = new File(getDbdir(),"FOO1");
        File meta = new File(tabledir, "FOO1.DATA");
        assertTrue("Should not have table directory", !tabledir.exists());
        assertTrue("Should not have a meta file", !meta.exists());
    }
    
    public void testCreateDB() throws Exception {
        DiskDatabase bogus;
        try {
            bogus = new DiskDatabase("junk8998", null);
            fail("Should fail for null dir");
        }catch(AxionException e) {
            // expected
        }

        bogus = null;
        File file = new File(new File("."), "junk8998");
        file.createNewFile();
        try {
            bogus = new DiskDatabase("junk8998", file);
            fail("Should fail for invalid dir");
        }catch(AxionException e) {
            // expected
        } finally {
            file.delete();
        }
        
        if(bogus != null) {
            bogus.shutdown();
        }
    }

    public void testSequence() throws Exception {
        File seq = new File(getDbdir(), getDb().getName().toUpperCase() + ".SEQ");
        File ver = new File(getDbdir(), getDb().getName().toUpperCase() + ".VER");
        assertTrue("Should not have sequence file", !seq.exists());
        getDb().createSequence(new Sequence("SEQ1", 0));
        assertTrue("Should have sequence file", seq.exists());
        assertTrue("Should have version file", ver.exists());
        assertNotNull("Should get sequence", getDb().getSequence("SEQ1"));
        getDb().getSequence("seq1").evaluate();
        getDb().getSequence("seq1").evaluate();
        assertEquals("Should have correct value",
            BigInteger.valueOf(2), getDb().getSequence("SEQ1").getValue());
        getDb().shutdown();

        Database another = createDatabase(getDb().getName());
        assertNotNull("Should get sequence back", another.getSequence("SEQ1"));
        assertEquals("Should have correct value",
                     BigInteger.valueOf(2), another.getSequence("SEQ1").getValue());
    }
    
    public void testMetaFile() throws Exception {
        CreateTableCommand cmd = new CreateTableCommand("LOB");
        cmd.addColumn("ID","integer");
        cmd.addColumn("TEXT2","clob");
        cmd.addColumn("TEXT","clob");
        cmd.execute(getDb());
        
        File tabledir = new File(getDbdir(),"LOB");
        File meta = new File(tabledir, "LOB.DATA");
        assertTrue("Should have table directory", tabledir.exists());
        assertTrue("Should have a meta file", meta.exists());
        
        Reader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(meta)));
        StringWriter writer = new StringWriter();
        int next = -1;
        while ((next = reader.read()) != -1) {
            writer.write(next);
        }
        
        String metaText = writer.toString();
        
        reader.close();
        writer.close();

        List columns = new ArrayList(3);
        columns.add(new ColumnIdentifier("ID"));
        columns.add(new ColumnIdentifier("TEXT"));
        columns.add(new ColumnIdentifier("TEXT2"));
        for(int i=0;i<10;i++) {
            List values = new ArrayList(3);
            values.add(new Literal(new Integer(i),new IntegerType()));
            values.add(new Literal(String.valueOf(i),new LOBType()));
            values.add(new Literal("***" + String.valueOf(i) + "***",new LOBType()));
            InsertCommand insertCmd = new InsertCommand(new TableIdentifier("LOB"),columns,values);
            insertCmd.executeUpdate(getDb());
        }
        
        File textLobDir = new File(new File(tabledir, "LOBS"), "TEXT");
        assertTrue("Should have lobs directory", textLobDir.exists());
        assertTrue("getPath() should be relative", 
                   !textLobDir.getCanonicalPath().equals(textLobDir.getPath()));
        assertTrue("getPath() should be relative to '.'", textLobDir.getPath().startsWith("."));
        assertTrue("Should not find relative lob path: " + metaText, 
                   metaText.indexOf(textLobDir.getPath()) == -1);
        assertTrue("Should not find absolute lob path: " + metaText, 
                   metaText.indexOf(textLobDir.getCanonicalPath()) == -1);

        //The following code demonstrates issue 9 (http://axion.tigris.org/issues/show_bug.cgi?id=9), which should now be fixed
        textLobDir = new File(new File(tabledir, "LOBS"), "TEXT2");
        assertTrue("Should have lobs directory", textLobDir.exists());
        assertTrue("getPath() should be relative", 
                   !textLobDir.getCanonicalPath().equals(textLobDir.getPath()));
        assertTrue("getPath() should be relative to '.'", textLobDir.getPath().startsWith("."));
        assertTrue("Should not find relative lob path: " + metaText, 
                   metaText.indexOf(textLobDir.getPath()) == -1);
        assertTrue("Should not find absolute lob path: " + metaText, 
                   metaText.indexOf(textLobDir.getCanonicalPath()) == -1);
    }

    public void testMoveDb() throws Exception {
        testMetaFile();
        getDb().shutdown();

        assertTrue("renaming should succeed", getDbdir().renameTo(_tempdir ));
        assertTrue("Old dir shouldn't exist anymore", !getDbdir().exists());

        Database db = new DiskDatabase("temp", _tempdir );

        List columns = new ArrayList(3);
        columns.add(new ColumnIdentifier("ID"));
        columns.add(new ColumnIdentifier("TEXT"));
        columns.add(new ColumnIdentifier("TEXT2"));
        for(int i=30;i<40;i++) {
            List values = new ArrayList(3);
            values.add(new Literal(new Integer(i),new IntegerType()));
            values.add(new Literal(String.valueOf(i),new LOBType()));
            values.add(new Literal("***" + String.valueOf(i) + "***",new LOBType()));
            InsertCommand insertCmd = new InsertCommand(new TableIdentifier("LOB"),columns,values);
            insertCmd.executeUpdate(db);
        }
        
        db.shutdown();

        if (getDbdir().exists()) {
            deleteFile(_tempdir );
            fail("Old dir still shouldn't exist");
        }

        deleteFile(_tempdir );
    }
}
