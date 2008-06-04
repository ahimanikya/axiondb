/*
 * $Id: TestReferencesOtherTablesWhereNodeVisitor.java,v 1.1 2007/11/28 10:01:28 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.visitors;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.ColumnIdentifier;
import org.axiondb.TableIdentifier;
import org.axiondb.engine.visitors.ReferencesOtherTablesWhereNodeVisitor;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:28 $
 * @author Rod Waldhoff
 */
public class TestReferencesOtherTablesWhereNodeVisitor extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestReferencesOtherTablesWhereNodeVisitor(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestReferencesOtherTablesWhereNodeVisitor.class);
        return suite;
    }

    //------------------------------------------------------------------- Tests
    
    public void testVisitOtherTable() {
        TableIdentifier A = new TableIdentifier("ALPHA");
        ReferencesOtherTablesWhereNodeVisitor visitor = new ReferencesOtherTablesWhereNodeVisitor(A);
        assertTrue(! visitor.hasResult());
        ColumnIdentifier ACOL = new ColumnIdentifier(A,"COL");
        visitor.visit(ACOL);
        assertTrue(! visitor.hasResult());
        TableIdentifier B = new TableIdentifier("BETA");
        ColumnIdentifier BCOL = new ColumnIdentifier(B,"COL");
        visitor.visit(BCOL);
        assertTrue(visitor.hasResult());
        assertTrue(!visitor.getResult());
    }

    public void testVisitAliasTable() {
        TableIdentifier A = new TableIdentifier("ALPHA","A1");
        ReferencesOtherTablesWhereNodeVisitor visitor = new ReferencesOtherTablesWhereNodeVisitor(A);
        assertTrue(! visitor.hasResult());
        ColumnIdentifier ACOL = new ColumnIdentifier(A,"COL");
        visitor.visit(ACOL);
        assertTrue(! visitor.hasResult());
        TableIdentifier B = new TableIdentifier("ALPHA","A2");
        ColumnIdentifier BCOL = new ColumnIdentifier(B,"COL");
        visitor.visit(BCOL);
        assertTrue(visitor.hasResult());
        assertTrue(!visitor.getResult());
    }

    public void testVisitTwice() {
        TableIdentifier A = new TableIdentifier("ALPHA");
        ReferencesOtherTablesWhereNodeVisitor visitor = new ReferencesOtherTablesWhereNodeVisitor(A);
        assertTrue(! visitor.hasResult());
        ColumnIdentifier ACOL = new ColumnIdentifier(A,"COL");
        visitor.visit(ACOL);
        assertTrue(! visitor.hasResult());
        TableIdentifier B = new TableIdentifier("BETA");
        ColumnIdentifier BCOL = new ColumnIdentifier(B,"COL");
        visitor.visit(BCOL);
        assertTrue(visitor.hasResult());
        assertTrue(!visitor.getResult());
        visitor.visit(BCOL);
        assertTrue(visitor.hasResult());
        assertTrue(!visitor.getResult());
    }

}
