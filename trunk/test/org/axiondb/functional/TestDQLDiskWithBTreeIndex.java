/*
 * $Id: TestDQLDiskWithBTreeIndex.java,v 1.1 2007/11/28 10:01:30 jawed Exp $
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
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:30 $
 * @author Dave Pekarek Krohn
 * @author Rodney Waldhoff
 */
public class TestDQLDiskWithBTreeIndex extends TestDQLDisk {

    public TestDQLDiskWithBTreeIndex(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDQLDiskWithBTreeIndex.class);
    }

    protected void createIndexOnFoo() throws Exception {
        _stmt.execute("create unique btree index FOO_NUM_NDX on FOO ( NUM )");
        _stmt.execute("create btree index FOO_STR_NDX on FOO ( STR )");
    }

    public void testDeleteFromBTreeIndexWithDuplicateKeys() throws Exception {
        Statement stmt = _conn.createStatement();
        stmt.execute("create table mytable ( intcol integer, strcol varchar(10), extra integer )");
        stmt.execute("create btree index intindex on mytable (intcol)");
        stmt.execute("create btree index strindex on mytable (strcol)");
        for(int i=0;i<10;i++) {
            stmt.execute("insert into mytable values ( 1, 'key', " + i + " )");
        }

        assertNRows(10,"select intcol, strcol, extra from mytable");
        assertNRows(10,"select intcol, strcol, extra from mytable where strcol = 'key'");
        assertNRows(10,"select intcol, strcol, extra from mytable where intcol = 1");
        
        assertEquals(1,stmt.executeUpdate("delete from mytable where extra = 0"));

        assertNRows(9,"select intcol, strcol, extra from mytable");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable");
            while(rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }

        assertNRows(9,"select intcol, strcol, extra from mytable where strcol = 'key'");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where strcol = 'key'");
            while(rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        assertNRows(9,"select intcol, strcol, extra from mytable where intcol = 1");
        {
            ResultSet rset = stmt.executeQuery("select extra from mytable where intcol = 1");
            while(rset.next()) {
                assertTrue(rset.getInt(1) != 0);
            }
            rset.close();
        }
        stmt.close();
    }
    

}
