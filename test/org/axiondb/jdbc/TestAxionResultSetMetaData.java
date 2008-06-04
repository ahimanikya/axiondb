/*
 * $Id: TestAxionResultSetMetaData.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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

import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.Literal;
import org.axiondb.Selectable;
import org.axiondb.TableIdentifier;
import org.axiondb.types.CharacterVaryingType;
import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rod Waldhoff
 */
public class TestAxionResultSetMetaData extends TestCase {

    private AxionResultSetMetaData _meta = null;
    private Selectable[] _sel = new Selectable[] {
        new ColumnIdentifier(new TableIdentifier("TABLE"),"FOO","F",new CharacterVaryingType(10)),
        new Literal(new Integer(17),new IntegerType())
    };
    
    public TestAxionResultSetMetaData(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestAxionResultSetMetaData.class);
    }

    public void setUp() throws Exception {
        super.setUp();
        _meta = new AxionResultSetMetaData(_sel);
    }

    public void tearDown() throws Exception {        
        super.tearDown();
        _meta = null;
    }
    
    // tests
    
    public void testBadIndex() throws Exception {
        try {
            _meta.getCatalogName(0);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
        try {
            _meta.getCatalogName(3);
            fail("Expected SQLException");
        } catch(SQLException e) {
            // expected
        }
    }

    public void testGetCatalogName() throws Exception {
        assertEquals("",_meta.getCatalogName(1));
    }

    public void testGetSchemaName() throws Exception {
        assertEquals("",_meta.getSchemaName(1));
    }
    
    public void testGetTableName() throws Exception {
        assertEquals("TABLE",_meta.getTableName(1));
        assertEquals("",_meta.getTableName(2));
    }

    public void testGetColumnCount() throws Exception {
        assertEquals(2,_meta.getColumnCount());
    }

    public void testGetColumDisplaySize() throws Exception {
        assertTrue(_meta.getColumnDisplaySize(1) > 0);
    }

    public void testGetColumnLabel() throws Exception {
        assertNotNull(_meta.getColumnLabel(1));
    }

    public void testGetColumnName() throws Exception {
        assertNotNull(_meta.getColumnName(1));
    }

    public void testGetColumnClassName() throws Exception {
        assertNotNull(_meta.getColumnClassName(1));
    }

    public void testIsAutoIncrement() throws Exception {
        assertTrue(! _meta.isAutoIncrement(1));
    }

    public void testIsCaseSensitive() throws Exception {
        assertTrue(_meta.isCaseSensitive(1));
    }

    public void testIsCurrency() throws Exception {
        assertTrue(! _meta.isCurrency(1));
    }

    public void testIsNullable() throws Exception {
        assertEquals(1,_meta.isNullable(1));
    }

    public void testIsSearchable() throws Exception {
        assertTrue(_meta.isSearchable(1));
    }

    public void testIsSigned() throws Exception {
        assertTrue(_meta.isSigned(1)); // ???
    }

    public void testIsReadOnly() throws Exception {
        assertTrue(! _meta.isReadOnly(1));
    }

    public void testIsWritable() throws Exception {
        assertTrue(_meta.isWritable(1));
    }

    public void testIsDefinitelyWritable() throws Exception {
        assertTrue(!_meta.isDefinitelyWritable(1));
    }
}
