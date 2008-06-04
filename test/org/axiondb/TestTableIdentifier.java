/*
 * $Id: TestTableIdentifier.java,v 1.1 2007/11/28 10:01:22 jawed Exp $
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

import java.io.Serializable;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:22 $
 * @author Rodney Waldhoff
 */
public class TestTableIdentifier extends BaseSerializableTest {

    //------------------------------------------------------------ Conventional

    public TestTableIdentifier(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestTableIdentifier.class);
        return suite;
    }

    //--------------------------------------------------------------- Lifecycle
    
    protected Serializable makeSerializable() {
        return new TableIdentifier("NAME","ALIAS");    
    }

    //------------------------------------------------------------------- Tests

    public void testEqualsColumnIdentifier1() throws Exception {
        TableIdentifier table = new TableIdentifier("NAME","ALIAS");
        ColumnIdentifier column = new ColumnIdentifier(table,"COLNAME","COLALIAS");
        assertEquals(table,column.getTableIdentifier());
    }

    public void testEquals() throws Exception {
        TableIdentifier table1 = new TableIdentifier("NAME","ALIAS");
        TableIdentifier table2 = new TableIdentifier("NAME","ALIAS2");
        TableIdentifier table3 = new TableIdentifier("NAME");
        assertTrue(table1.equals(table1));
        assertTrue(table2.equals(table2));
        assertTrue(table3.equals(table3));
        assertTrue(! table1.equals(table2));
        assertTrue(! table2.equals(table1));
        assertTrue(! table1.equals(table3));
        assertTrue(! table3.equals(table1));
        assertTrue(table1.hashCode() != table2.hashCode());
        assertTrue(table1.hashCode() != table3.hashCode());
        assertTrue(table2.hashCode() != table3.hashCode());
    }

    public void testCanonicalIdentifier() {
        TableIdentifier table = new TableIdentifier("NAME","ALIAS");
        assertNull(table.getCanonicalIdentifier().getTableAlias());
    }
    
    public void testToString() throws Exception {
        {
            TableIdentifier table = new TableIdentifier("NAME","ALIAS");
            assertEquals("NAME AS ALIAS",table.toString());
        }
        {
            TableIdentifier table = new TableIdentifier("NAME",null);
            assertEquals("NAME",table.toString());
        }
    }

    public void testGetTableName() throws Exception {
        {
            TableIdentifier table = new TableIdentifier("NAME","ALIAS");
            assertEquals("NAME",table.getTableName());
        }
        {
            TableIdentifier table = new TableIdentifier("NAME",null);
            assertEquals("NAME",table.getTableName());
        }
        {
            TableIdentifier table = new TableIdentifier(null,"ALIAS");
            assertNull(table.getTableName());
        }
        {
            TableIdentifier table = new TableIdentifier(null,null);
            assertNull(table.getTableName());
        }
    }

    public void testGetTableAlias() throws Exception {
        {
            TableIdentifier table = new TableIdentifier("NAME","ALIAS");
            assertEquals("ALIAS",table.getTableAlias());
        }
        {
            TableIdentifier table = new TableIdentifier("NAME",null);
            assertNull(table.getTableAlias());
        }
        {
            TableIdentifier table = new TableIdentifier(null,"ALIAS");
            assertEquals("ALIAS",table.getTableAlias());
        }
        {
            TableIdentifier table = new TableIdentifier(null,null);
            assertNull(table.getTableAlias());
        }
    }

}

