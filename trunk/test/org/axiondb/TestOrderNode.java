/*
 * $Id: TestOrderNode.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Rodney Waldhoff
 */
public class TestOrderNode extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestOrderNode(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestOrderNode.class);
    }

    //--------------------------------------------------------------- Lifecycle

    private Selectable _sel1 = new ColumnIdentifier(new TableIdentifier("table1"),"column1");
    private Selectable _sel2 = new ColumnIdentifier(new TableIdentifier("table2"),"column2");

    //------------------------------------------------------------------- Tests

    public void testWithNullSelectable() {
        OrderNode node = new OrderNode(null,false);
        assertNull(node.getSelectable());
        assertTrue(! node.isDescending() );
        assertNotNull(node.toString());
    }

    public void testSettersAndGetters() {
        OrderNode node = new OrderNode(_sel1,false);
        assertSame(_sel1,node.getSelectable());
        assertTrue(! node.isDescending() );
        String str1 = node.toString();
        assertNotNull(str1);

        node.setSelectable(_sel2);
        assertSame(_sel2,node.getSelectable());
        assertTrue(! node.isDescending() );

        String str2 = node.toString();
        assertNotNull(str2);
        assertTrue(!str1.equals(str2));
        
        node.setDescending(true);
        assertSame(_sel2,node.getSelectable());
        assertTrue(node.isDescending());

        String str3 = node.toString();
        assertNotNull(str3);
        assertTrue(!str2.equals(str3));
    }
}
