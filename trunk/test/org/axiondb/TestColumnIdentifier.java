/*
 * $Id: TestColumnIdentifier.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

package org.axiondb;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.types.IntegerType;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 */
public class TestColumnIdentifier extends BaseSelectableTest {

    //------------------------------------------------------------ Conventional

    public TestColumnIdentifier(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestColumnIdentifier.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    //--------------------------------------------------------------- Framework
    
    public Selectable makeSelectable() {
        return new ColumnIdentifier(new TableIdentifier("TNAME","TALIAS"),"CNAME","CALIAS",new IntegerType());
    }

    //------------------------------------------------------------------- Tests

    public void testDottedAsterisk() {
        ColumnIdentifier id = new ColumnIdentifier("FOO.*");
        assertEquals("Should get just column name back", "*", id.getName());
        assertEquals("Should get table-or-alias name back", "FOO", id.getTableName());
    }

    public void testAsterisk() {
        ColumnIdentifier id = new ColumnIdentifier("*");
        assertEquals("Should get just column name back", "*", id.getName());
        assertEquals("Should get table-or-alias name back", null, id.getTableName());
    }

    public void testDottedIdentifier() {
        ColumnIdentifier id = new ColumnIdentifier("FOO.BAR");
        assertEquals("Should get just column name back", "BAR", id.getName());
        assertEquals("Should get table-or-alias name back", "FOO", id.getTableName());
    }

    public void testDottedIdentifier2() {
        ColumnIdentifier id = new ColumnIdentifier(null, "FOO.BAR", null, null);
        assertEquals("Should get just column name back", "BAR", id.getName());
        assertEquals("Should get table-or-alias name back", "FOO", id.getTableName());
    }

    public void testNullTableConstructor() {
        ColumnIdentifier id = new ColumnIdentifier(null, "BAR");
        assertEquals("Should get just column name back", "BAR", id.getName());
    }

    public void testFullConstructorWithNulls() {
        ColumnIdentifier id = new ColumnIdentifier(null, "BAR", null, null);
        assertEquals("Should get just column name back", "BAR", id.getName());
    }

    public void testNullConstructor() {
        ColumnIdentifier id = new ColumnIdentifier(null);
        assertNull(id.getName());
        assertNull(id.getAlias());
        assertEquals(id,new ColumnIdentifier(null));
        assertEquals(id.hashCode(),(new ColumnIdentifier(null)).hashCode());
    }

    public void testEquals() throws Exception {
        ColumnIdentifier column1 = new ColumnIdentifier(null,"NAME","ALIAS");
        ColumnIdentifier column2 = new ColumnIdentifier(null,"NAME","ALIAS2");
        ColumnIdentifier column3 = new ColumnIdentifier(null,"NAME");
        assertTrue(column1.equals(column1));
        assertTrue(column2.equals(column2));
        assertTrue(column3.equals(column3));
        assertTrue(column1.getCanonicalIdentifier().equals(column2.getCanonicalIdentifier()));
        assertTrue(column2.getCanonicalIdentifier().equals(column1.getCanonicalIdentifier()));
        assertTrue(column1.getCanonicalIdentifier().equals(column3.getCanonicalIdentifier()));
        assertTrue(column3.getCanonicalIdentifier().equals(column1.getCanonicalIdentifier()));
        assertTrue(! column1.equals(column2));
        assertTrue(! column2.equals(column1));
        assertTrue(! column1.equals(column3));
        assertTrue(! column3.equals(column1));
        assertTrue(column1.hashCode() != column2.hashCode());
        assertTrue(column1.hashCode() != column3.hashCode());
        assertTrue(column2.hashCode() != column3.hashCode());
    }

    public void testCanonicalIdentifier() {
        ColumnIdentifier column = new ColumnIdentifier(null,"NAME","ALIAS2");
        assertNull(column.getCanonicalIdentifier().getAlias());
    }
    
    public void testTableAlias() {
        ColumnIdentifier column = new ColumnIdentifier(null,"NAME","ALIAS2");
        assertNull(column.getTableAlias());
        ColumnIdentifier id = new ColumnIdentifier(new TableIdentifier("TABLENAME","TABLEALIAS"),"COLMNNAME","COLUMNALIAS");
        assertNotNull(id.getTableAlias());
    }
    
    public void testGetLabel() {
        {
            ColumnIdentifier id = new ColumnIdentifier(new TableIdentifier("TABLENAME","TABLEALIAS"),"COLUMNNAME","COLUMNALIAS");
            assertEquals("COLUMNALIAS",id.getLabel());
        }
        {
            ColumnIdentifier id = new ColumnIdentifier(new TableIdentifier("TABLENAME","TABLEALIAS"),"COLUMNNAME",null);
            assertEquals("COLUMNNAME",id.getLabel());
        }
    }

    public void testToString() {
        ColumnIdentifier id = new ColumnIdentifier(null,"XYZZY");
        assertNotNull(id.toString());
        id.setTableIdentifier(new TableIdentifier("THETABLE"));
        assertNotNull(id.toString());
    }
    
    public void testToStringWithAlias() {
        ColumnIdentifier id = new ColumnIdentifier(null,"XYZZY");
        assertNotNull(id.toString());
        id.setTableIdentifier(new TableIdentifier("THETABLE"));
        assertNotNull(id.toString());
        id.setAlias("COLALIAS");
        assertTrue(id.toString().toUpperCase().indexOf("AS") != -1 );
    }

}
