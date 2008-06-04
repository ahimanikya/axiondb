/*
 * $Id: TestCreateSequenceCommand.java,v 1.1 2007/11/28 10:01:23 jawed Exp $
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

package org.axiondb.engine.commands;

import java.math.BigInteger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionCommand;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.SequenceEvaluator;
import org.axiondb.engine.MemoryDatabase;
import org.axiondb.engine.visitors.ResolveSelectableVisitor;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:23 $
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TestCreateSequenceCommand extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestCreateSequenceCommand(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestCreateSequenceCommand.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestCreateSequenceCommand.class);
    }

    //--------------------------------------------------------------- Lifecycle
    org.axiondb.Database _db = null;

    public void setUp() throws Exception {
        _db = new MemoryDatabase();
    }

    public void tearDown() throws Exception {
    }

    //------------------------------------------------------------------- Tests

    public void testCreateSequence() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        AxionCommand create = new CreateSequenceCommand("foo_seq", 0);
        create.execute(_db);
        assertNotNull("Should find sequence", _db.getSequence("foo_seq"));
        assertEquals("Should have correct initial value", BigInteger.valueOf(0), _db
            .getSequence("foo_seq").getValue());
    }

    public void testCreateSequenceStartVal() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        AxionCommand create = new CreateSequenceCommand("foo_seq", 23);
        create.execute(_db);
        assertNotNull("Should find sequence", _db.getSequence("foo_seq"));
        assertEquals("Should have correct initial value", BigInteger.valueOf(23), _db.getSequence(
            "foo_seq").getValue());
    }

    public void testCreateSequenceAll() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        CreateSequenceCommand create = new CreateSequenceCommand("foo_seq", 23);
        create.setIncrementBy("10");
        create.setMinValue("0");
        create.setMaxValue("40");
        create.setCycle(true);
        create.execute(_db);
        Sequence seq = _db.getSequence("foo_seq");
        assertNotNull("Should find sequence", seq);
        assertEquals("Should have correct initial value", BigInteger.valueOf(23), seq.getValue());
        assertEquals("Should have correct next value", new Integer(23), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(33), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(0), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(10), seq.evaluate());
    }
    
    public void testDefaultStartValue() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        CreateSequenceCommand create = new CreateSequenceCommand();
        create.setObjectName("foo_seq");
        create.setIncrementBy("10");
        create.setMinValue("0");
        create.setMaxValue("40");
        create.setCycle(true);
        create.execute(_db);
        Sequence seq = _db.getSequence("foo_seq");
        assertNotNull("Should find sequence", seq);
        assertEquals("Should have correct initial value", BigInteger.valueOf(0), seq.getValue());
        assertEquals("Should have correct next value", new Integer(0), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(10), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(20), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(30), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(40), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(0), seq.evaluate());
    }

    public void testDefaultStartValue2() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        CreateSequenceCommand create = new CreateSequenceCommand();
        create.setObjectName("foo_seq");
        create.setIncrementBy("-10");
        create.setMinValue("0");
        create.setMaxValue("40");
        create.setCycle(true);
        create.execute(_db);
        Sequence seq = _db.getSequence("foo_seq");
        assertNotNull("Should find sequence", seq);
        assertEquals("Should have correct initial value", BigInteger.valueOf(40), seq.getValue());
        assertEquals("Should have correct next value", new Integer(40), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(30), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(20), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(10), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(0), seq.evaluate());
        assertEquals("Should have correct next value", new Integer(40), seq.evaluate());
    }
    
    public void testCreateSequenceBad() throws Exception {
        assertNull("Should not find sequence", _db.getSequence("foo_seq"));
        CreateSequenceCommand create = new CreateSequenceCommand("foo_seq", 23);
        create.setIncrementBy("10");
        create.setMinValue("0");
        create.setMaxValue("40");
        create.setCycle(false); // NO CYCLE
        create.execute(_db);
        Sequence seq = _db.getSequence("foo_seq");
        assertNotNull("Should find sequence", seq);
        assertEquals("Should have correct initial value", BigInteger.valueOf(23), seq.getValue());
        assertEquals("Should have correct next value", new Integer(23), seq.evaluate());
        
        try {
            seq.evaluate();
            fail("No more next value expected...");
        } catch(IllegalStateException ex) {
            // expected
        }
        
        try {
            create.execute(_db);
            fail("already exists");
        } catch(Exception ex) {
            // expected
        }
        
        try {
            _db.dropSequence("foo_seq");
            create.setMaxValue("10000000000000000");
            create.execute(_db);
            fail("too big number exception expected");
        } catch(Exception ex) {
            // expected
        }

    }
    
    public void testResolveAsSelectable() throws Exception{
        ResolveSelectableVisitor visitor = new ResolveSelectableVisitor(_db);
        Sequence seq = new Sequence("foo", 0);
        Selectable sel = new SequenceEvaluator( seq, "NEXTVAL");

        try {
            visitor.visit(sel, null, null);
            fail("Expected Exception: Couldn't resolve Selectable");
        }catch(Exception e) {
            // expected
        }
    }
}