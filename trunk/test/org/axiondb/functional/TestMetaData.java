/*
 * $Id: TestMetaData.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

package org.axiondb.functional;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Chuck Burdick
 */
public class TestMetaData extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestMetaData(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMetaData.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testSimpleColumnMetaData() throws Exception {
        createTableFoo();
        populateTableFoo();
        _rset = _stmt.executeQuery("SELECT * FROM foo");
        ResultSetMetaData meta = _rset.getMetaData();

        assertEquals("Should indicate 3 columns", 3, meta.getColumnCount());

        helpTestColumnMetaData(meta, 1, "NUM", Types.INTEGER, 10, 0);
        helpTestColumnMetaData(meta, 2, "STR", Types.VARCHAR, 255, 0);
    }
    
    public void testNumericColumnMetaData() throws Exception {
        _stmt.execute("CREATE TABLE nums (\"INT\" INTEGER, \"BIGINT\" BIGINT, \"FLOAT\" FLOAT, BIG NUMBER)");

        _rset = _stmt.executeQuery("SELECT * FROM nums");
        ResultSetMetaData meta = _rset.getMetaData();

        assertEquals("Should indicate 4 columns", 4, meta.getColumnCount());
        helpTestColumnMetaData(meta, 1, "INT", Types.INTEGER, 10, 0);
        helpTestColumnMetaData(meta, 2, "BIGINT", Types.BIGINT, 19, 0);
        helpTestColumnMetaData(meta, 3, "FLOAT", Types.FLOAT, 12, 0);
        helpTestColumnMetaData(meta, 4, "BIG", Types.NUMERIC, 22, 0);
        
        _rset = _stmt.executeQuery("SELECT * FROM axion_columns WHERE table_name = 'NUMS' ORDER BY column_name");
        
        helpTestSystemTableColumns(_rset, "BIG", Types.NUMERIC, 22, 0, 10);
        helpTestSystemTableColumns(_rset, "BIGINT", Types.BIGINT, 19, 0, 10);
        helpTestSystemTableColumns(_rset, "FLOAT", Types.FLOAT, 12, 0, 10);
        helpTestSystemTableColumns(_rset, "INT", Types.INTEGER, 10, 0, 10);
        assertTrue("Should have no more rows", !_rset.next());
    }

    private void helpTestColumnMetaData(
        ResultSetMetaData meta,
        int colId,
        String name,
        int type,
        int precision,
        int scale)
        throws Exception {
        assertEquals("Should get expected column name", name, meta.getColumnName(colId));
        assertEquals("Should be expected type", type, meta.getColumnType(colId));
        assertEquals("Should be expected precision", precision, meta.getPrecision(colId));
        assertEquals("Should be expected scale", scale, meta.getScale(colId));
    }
    
    private void helpTestSystemTableColumns(
            ResultSet rset,
            String name,
            int type,
            int precision,
            int scale,
            int radix) throws Exception {
        assertTrue("Should have a row", _rset.next());
        assertEquals("Should get expected column name", name, rset.getString(4));
        assertEquals("Should be expected type", type, rset.getInt(5));
        assertEquals("Should be expected precision", precision, rset.getInt(7));
        assertEquals("Should be expected scale", scale, rset.getInt(9));
        assertEquals("Should be expected radix", radix, rset.getInt(10));
    }
}