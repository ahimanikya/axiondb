/*
 * $Id: BaseRowTest.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
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

package org.axiondb.engine.rows;

import junit.framework.TestCase;

import org.axiondb.Row;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Rod Waldhoff
 */
public abstract class BaseRowTest extends TestCase {

    //------------------------------------------------------------ Conventional

    public BaseRowTest(String testName) {
        super(testName);
    }

    public abstract Row makeRow(Object[] values);

    //------------------------------------------------------------------- Tests

    public void testToString() throws Exception {
        assertNotNull(makeRow(STRINGS_ONE).toString());
    }
    
    public void testEqualsAndHashCode() throws Exception {
        Row row = makeRow(STRINGS_ONE);
        assertEquals(row,row);
        assertEquals(row.hashCode(),row.hashCode());
        
        assertEquals(row,makeRow(STRINGS_ONE));
        assertEquals(row.hashCode(),makeRow(STRINGS_ONE).hashCode());

        assertTrue(! row.equals(null) );

        assertTrue(! row.equals(makeRow(STRINGS_TWO)) );
        assertTrue(! makeRow(STRINGS_TWO).equals(row) );        
        assertTrue(row.hashCode() != makeRow(STRINGS_TWO).hashCode());
        
        assertTrue(! row.equals(makeRow(STRINGS_THREE)));
        assertTrue(! makeRow(STRINGS_THREE).equals(row) );        
        assertTrue(row.hashCode() != makeRow(STRINGS_THREE).hashCode());

        assertTrue(! row.equals(makeRow(INTS_ONE)) );
        assertTrue(! makeRow(INTS_ONE).equals(row) );        
        assertTrue(row.hashCode() != makeRow(INTS_ONE).hashCode());

        assertTrue(! row.equals(makeRow(MIXED_ONE)) );
        assertTrue(! makeRow(MIXED_ONE).equals(row) );        
        assertTrue(row.hashCode() != makeRow(MIXED_ONE).hashCode());

        assertEquals(makeRow(STRINGS_TWO),makeRow(STRINGS_TWO));
        assertEquals(makeRow(STRINGS_THREE),makeRow(STRINGS_THREE));
        assertEquals(makeRow(MIXED_ONE),makeRow(MIXED_ONE));
        assertEquals(makeRow(INTS_ONE),makeRow(INTS_ONE));

        assertTrue(! row.equals(makeRow(NULLS)) );
        assertTrue(! makeRow(NULLS).equals(row) );        
        assertTrue(row.hashCode() != makeRow(NULLS).hashCode());

    }


    private static final Object[] STRINGS_ONE = { "one", "two", "three", "four" };
    private static final Object[] STRINGS_TWO = { "1", "2", "3", "4" };
    private static final Object[] STRINGS_THREE = { "one", "two", "three" };
    private static final Object[] INTS_ONE = { new Integer(1), new Integer(2), new Integer(3), new Integer(4) };
    private static final Object[] MIXED_ONE = { new Integer(1), "2", null, "4" };
    private static final Object[] NULLS = { null,null,null,null };
}
