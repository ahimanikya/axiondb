/*
 * $Id: TestDelimitedFlatfileTableRowIterator.java,v 1.2 2008/01/24 13:13:45 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.RowIterator;
import org.axiondb.Table;
import org.axiondb.engine.Databases;
import org.axiondb.engine.rowiterators.AbstractRowIteratorTest;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.FileUtil;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * Tests internal RowIterator implementation of 
 * {@link org.axiondb.engine.tables.DelimitedFlatfileTable}.
 * 
 * @version $Revision: 1.2 $ $Date: 2008/01/24 13:13:45 $
 * @author Jonathan Giron
 */
public class TestDelimitedFlatfileTableRowIterator extends AbstractRowIteratorTest {

    protected static final String TABLE_NAME = "X_DELIMITED";
    
    private static final String DATABASE_NAME = "TESTDB";
    private static final String FILE_EXTENSION = ".csv";
    
    //------------------------------------------------------------ Conventional

    public TestDelimitedFlatfileTableRowIterator(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestDelimitedFlatfileTableRowIterator.class);
        return suite;
    }

    private Table _table;
    private File _dbDir = new File(new File("."), DATABASE_NAME);
    private Database _db;
    private String _dataFileName;
    
    protected void setUp() throws Exception {
        _dbDir.mkdirs();
        _db = Databases.getOrCreateDatabase(DATABASE_NAME, _dbDir);
        _table = createTable(TABLE_NAME);
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
        _table.truncate();
        _table.drop();
        _db.shutdown();
        
        FileUtil.delete(_dbDir);
    }    

    protected Table createTable(String name) throws Exception {
        File data = new File(_dbDir, name + FILE_EXTENSION);
        FileWriter out = new FileWriter(data);

        String eol = System.getProperty("line.separator");

        out.write("ID,NAME" + eol); // Header
        out.write("1,a" + eol); // 1
        out.write("2,bb" + eol); // 2
        out.write("3,ccc" + eol); // 3
        out.close();        
        
        ExternalTableFactory factory = new ExternalTableFactory();
        ExternalTable t = factory.createTable(_db, name, setProperties(name), buildColumns());
        return t;
    }    

    protected Properties setProperties(String name) {
        Properties props = new Properties();

        props.setProperty(ExternalTable.PROP_LOADTYPE, "delimited");
        props.setProperty(DelimitedFlatfileTable.PROP_FIELDDELIMITER, ",");
        
        String eol = System.getProperty("line.separator");
        props.setProperty(BaseFlatfileTable.PROP_RECORDDELIMITER, eol);
        props.setProperty(BaseFlatfileTable.PROP_ISFIRSTLINEHEADER, "true");

        _dataFileName = name + FILE_EXTENSION;
        props.setProperty(BaseFlatfileTable.PROP_FILENAME, _dataFileName);

        return props;
    }    

    protected RowIterator makeRowIterator() {
        try {
            return _table.getRowIterator(false);
        } catch (AxionException e) {
            throw new UnsupportedOperationException("Could not create RowIterator");
        }
    }
    
    protected int getSize() {
        return 3;
    }

    protected List makeRowList() {
        List list = new ArrayList();
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new BigDecimal(1));
            row.set(1, "a");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new BigDecimal(2));
            row.set(1, "bb");
            list.add(row);
        }
        {
            SimpleRow row = new SimpleRow(2);
            row.set(0, new BigDecimal(3));
            row.set(1, "ccc");
            list.add(row);
        }
        return list;
    }

    private List buildColumns() {
        List list = new ArrayList(2);
        
        Column id = new Column("id", new IntegerType()); 
        id.setSqlType("integer");
        list.add(id);
        
        Column name = new Column("name", new CharacterVaryingType(10));
        name.setSqlType("varchar");
        list.add(name);
        return list;
    }    
    
}