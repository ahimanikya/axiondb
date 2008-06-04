/*
 * $Id: TestObjectBTree.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
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

package org.axiondb.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.axiondb.AbstractDbdirTest;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Dave Pekarek Krohn
 */
public class TestObjectBTree extends AbstractDbdirTest {
    private static Log _log = LogFactory.getLog(TestObjectBTree.class);
    private ObjectBTree _tree = null;
    private int _deg = 3;
    private int _max = (int) Math.pow(2 * _deg, 2);
    private Random _random = new Random();
    private static String[] _stringArray = { "abc", "bcd", "cde", "def", "efg", "fgh", "ghi",
            "hij", "ijk", "jkl", "lmn", "mno", "nop", "pqr", "qrs", "rst", "stu", "tuv", "uvw",
            "vwx", "wxy", "xyz", "yza", "zab"};

    //------------------------------------------------------------ Conventional

    public TestObjectBTree(String testName) {
        super(testName);
    }

    public static void main(String args[]) {
        String[] testCaseName = { TestIntBTree.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

    public static Test suite() {
        return new TestSuite(TestObjectBTree.class);
    }

    //--------------------------------------------------------------- Lifecycle

    public void setUp() throws Exception {
        super.setUp();
        long newSeed = _random.nextLong();
        _log.debug("New Seed: " + newSeed);
        _random.setSeed(newSeed);
        _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
    }

    public void tearDown() throws Exception {
        _tree = null;
        super.tearDown();
    }

    //------------------------------------------------------------------- Tests

    public void testDeleteOneOfMany() throws Exception {
        IntList expected = new ArrayIntList();
        for (int i = 0; i < 10; i++) {
            _tree.insert("key", i);
            expected.add(i);
        }
        {
            IntList found = new ArrayIntList();
            for (IntIterator iter = _tree.getAll("key"); iter.hasNext();) {
                found.add(iter.next());
            }
            assertEquals(expected.size(), found.size());
            assertTrue(found.containsAll(expected));
        }
        _tree.delete("key", 5);
        expected.removeElement(5);
        {
            IntList found = new ArrayIntList();
            for (IntIterator iter = _tree.getAll("key"); iter.hasNext();) {
                found.add(iter.next());
            }
            assertEquals(expected.size(), found.size());
            assertTrue(found.containsAll(expected));
        }
    }
    

    public void testCreate() throws Exception {
        assertTrue("Should have dbdir avail", getDbdir().exists());
        assertTrue("Dbdir should be directory", getDbdir().isDirectory());
        assertNotNull("Should not be null", _tree);
        assertTrue("Should be leaf", _tree.isLeaf());
        assertEquals("Should have no entries", 0, _tree.size());
        File idx = new File(getDbdir(), "IDX" + ".0");
        assertTrue("Should not have an index file before save", !idx.exists());
        _tree.insert("key", 1);
        _tree.save(getDbdir());
        assertTrue("Should have an index file after save", idx.exists());
    }

    public void testInsertAndRetrieve() throws Exception {
        _tree.insert("abc", 456);
        assertTrue("Tree should be valid after insert", _tree.isValid());
        assertEquals("Should have one value", 1, _tree.size());
        assertEquals("Should get value back", new Integer(456), _tree.get("abc"));
    }

    public void testInsertAndRetrieveMany() throws Exception {
        for (int i = 0; i < _stringArray.length; i++) {
            _log.debug("Inserting key " + i);
            _tree.insert(_stringArray[i], _stringArray.length - i);
            _log.debug(_tree.toString());
            assertTrue("Tree should be valid after insert", _tree.isValid());
            assertEquals("Should get the value back as inserting for " + i, new Integer(
                _stringArray.length - i), _tree.get(_stringArray[i]));
        }
        for (int i = 0; i < _stringArray.length; i++) {
            assertEquals("Should get the value back for " + i,
                new Integer(_stringArray.length - i), _tree.get(_stringArray[i]));
        }
    }

    public void testReloadTree() throws Exception {
        //Insert a bunch of items
        {
            for (int i = 0; i < _stringArray.length; i++) {
                _log.debug("Inserting key " + i);
                _tree.insert(_stringArray[i], _stringArray.length - i);
                _log.debug(_tree.toString());
                assertTrue("Tree should be valid after insert", _tree.isValid());
                assertEquals("Should get the value back as inserting for " + i, new Integer(
                    _stringArray.length - i), _tree.get(_stringArray[i]));
            }
            _tree.save(getDbdir());
        }

        //Reload tree and make sure items are still there
        //then delete some of them
        {
            _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
            for (int i = 0; i < _stringArray.length; i++) {
                assertEquals("Should get the value back for " + i, new Integer(_stringArray.length
                    - i), _tree.get(_stringArray[i]));
            }

            for (int i = 0; i < _stringArray.length / 2; i++) {
                _tree.delete(_stringArray[i], _stringArray.length - i);
                assertEquals("Should not get the value back for " + i, null, _tree
                    .get(_stringArray[i]));
            }
            _tree.save(getDbdir());
        }

        //Reload the tree and make sure that items that should be there are there
        //and those that should not aren't. Then add the deleted items back
        {
            _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
            for (int i = _stringArray.length / 2; i < _stringArray.length; i++) {
                assertEquals("Should get the value back for " + i, new Integer(_stringArray.length
                    - i), _tree.get(_stringArray[i]));
            }

            for (int i = 0; i < _stringArray.length / 2; i++) {
                assertEquals("Should not get the value back for " + i, null, _tree
                    .get(_stringArray[i]));
                _tree.insert(_stringArray[i], _stringArray.length - i);
            }
            _tree.save(getDbdir());
        }

        //Reload the tree and make sure that all items are there
        {
            _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
            for (int i = 0; i < _stringArray.length; i++) {
                assertEquals("Should get the value back for " + i, new Integer(_stringArray.length
                    - i), _tree.get(_stringArray[i]));
            }
            _tree.save(getDbdir());
        }
    }

    public void testInsertIdenticalAndGetAll() throws Exception {
        _tree = new ObjectBTree(getDbdir(), "idx", 8, new ComparableComparator());
        int max = 100;
        for (int i = 0; i < max; i++) {
            _log.debug("Inserting key " + i);
            _tree.insert(new Integer(i), i);
            _tree.insert(new Integer(i), max - i);
            _tree.insert(new Integer(i), -1 * i);
            _log.debug(_tree.toString());
            assertTrue("Tree should be valid after insert", _tree.isValid());
            IntIterator iter = _tree.getAll(new Integer(i));
            List valList = new ArrayList();
            valList.add(new Integer(i));
            valList.add(new Integer(max - i));
            valList.add(new Integer(-1 * i));
            for (int j = 0; j < 3; j++) {
                assertTrue("Should have another value for: " + i, iter.hasNext());
                Integer val = new Integer(iter.next());
                assertTrue("Should contain value: " + val + " for: " + i, valList.contains(val));
                valList.remove(val);
            }
        }
        for (int i = 0; i < max; i++) {
            IntIterator iter = _tree.getAll(new Integer(i));
            List valList = new ArrayList();
            valList.add(new Integer(i));
            valList.add(new Integer(max - i));
            valList.add(new Integer(-1 * i));
            for (int j = 0; j < 3; j++) {
                assertTrue("Should have another value for: " + i, iter.hasNext());
                Integer val = new Integer(iter.next());
                assertTrue("Should contain value: " + val + " for: " + i, valList.contains(val));
                valList.remove(val);
            }
        }
    }

    public void testInsertAndRetrieveManyRandom() throws Exception {
        Map tests = new HashMap();
        for (int i = 0; i < _stringArray.length; i++) {
            Integer key;
            while (tests.containsKey(key = new Integer(_random.nextInt(_stringArray.length)))) {
            };
            Integer value = new Integer(_random.nextInt());
            if (!tests.containsKey(key)) {
                tests.put(key, value);
                _log.debug("Inserting key " + key);
                _tree.insert(_stringArray[key.intValue()], value.intValue());
                _log.debug(_tree.toString());
                assertTrue("Tree should be valid after insert", _tree.isValid());
                assertEquals("Should get value back as inserting", value, _tree
                    .get(_stringArray[key.intValue()]));
            }
        }
        assertEquals("Should have " + _stringArray.length + " insertions", _stringArray.length,
            tests.keySet().size());
        Iterator i = tests.keySet().iterator();
        while (i.hasNext()) {
            Integer cur = (Integer) i.next();
            assertEquals("Should get value back", tests.get(cur), _tree.get(_stringArray[cur
                .intValue()]));
        }
    }

    public void testInsertAndRetrieveManyIdentical() throws Exception {
        Set tests = new HashSet();
        for (int i = 0; i < _max; i++) {
            _log.debug("Insertion number " + (i + 1));
            _tree.insert("abc", i);
            assertTrue("Tree should be valid after insert", _tree.isValid());
            tests.add(new Integer(i));
            _log.debug(_tree.toString());
        }
        assertEquals("Should have full number of tests", _max, tests.size());
        IntIterator i = _tree.getAll("abc");
        int count = 0;
        while (i.hasNext()) {
            tests.remove(new Integer(i.next()));
            count++;
        }
        assertEquals("Should have found all matching inserts", _max, count);
        assertTrue("All test vals should have been retrieved", tests.isEmpty());
        i = _tree.getAll("def");
        assertTrue("Should not have any matches", !i.hasNext());
    }

    public void testInsertAndDeleteLots() throws Exception {
        //Do inserts
        for (int i = 0; i < _stringArray.length; i++) {
            _log.debug("Inserting key " + i);
            _tree.insert(_stringArray[i], i);
            _log.debug(_tree.toString());
            assertTrue("Tree should be valid after insert", _tree.isValid());
            assertEquals("Should get value back as inserting", i, _tree.get(_stringArray[i])
                .intValue());
        }

        for (int i = 0; i < _stringArray.length; i++) {
            int val = (i + 20) % (_stringArray.length);
            assertEquals("Should get value back", val, _tree.get(_stringArray[val]).intValue());
            _log.debug("Deleting key " + val);
            _log.debug("Tree before delete: " + _tree.toString());
            _tree.delete(_stringArray[val], val);
            _log.debug("Tree after delete: " + _tree.toString());
            assertTrue("Tree should be valid after delete", _tree.isValid());
            assertNull("Value should be deleted for key: " + val, _tree.get(_stringArray[val]));
        }

        assertTrue("Tree should be empty", _tree.size() == 0);
    }

    public void testInsertAndDeleteLotsWithReload() throws Exception {
        //Do inserts
        for (int i = 0; i < _stringArray.length; i++) {
            _log.debug("Inserting key " + i);
            _tree.insert(_stringArray[i], i);
            _log.debug(_tree.toString());
            assertTrue("Tree should be valid after insert", _tree.isValid());
            assertEquals("Should get value back as inserting", i, _tree.get(_stringArray[i])
                .intValue());
        }
        _tree.save(getDbdir());

        _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
        for (int i = 0; i < _stringArray.length; i++) {
            int val = (i + 20) % (_stringArray.length);
            assertEquals("Should get value back", val, _tree.get(_stringArray[val]).intValue());
            _log.debug("Deleting key " + val);
            _log.debug("Tree before delete: " + _tree.toString());
            _tree.delete(_stringArray[val], val);
            _log.debug("Tree after delete: " + _tree.toString());
            assertTrue("Tree should be valid after delete", _tree.isValid());
            assertNull("Value should be deleted for key: " + val, _tree.get(_stringArray[val]));
        }
        _tree.save(getDbdir());

        _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
        assertTrue("Tree should be empty", _tree.size() == 0);
    }

    public void testInsertAndDeleteRandom() throws Exception {
        Map tests = new HashMap();
        //Do inserts
        int max = _max * 10;
        for (int i = 0; i < max; i++) {
            Integer value = new Integer(_random.nextInt());
            String key = value.toString();
            if (!tests.containsKey(key)) {
                tests.put(key, value);
                _log.debug("Inserting key " + key);
                _tree.insert(key, value.intValue());
                _log.debug(_tree.toString());
                assertTrue("Tree should be valid after insert", _tree.isValid());
                assertEquals("Should get value back as inserting", value, _tree.get(key));
            }
        }
        assertEquals("Should have " + max + " insertions", max, tests.keySet().size());
        //Do deletes
        Iterator i = tests.keySet().iterator();
        while (i.hasNext()) {
            String cur = (String) i.next();
            Integer val = (Integer)tests.get(cur);
            assertEquals("Should get value back", val, _tree.get(cur));
            _log.debug("Deleting key " + cur);
            _log.debug("Tree before delete: " + _tree.toString());
            _tree.delete(cur, val.intValue());
            _log.debug("Tree after delete: " + _tree.toString());
            assertTrue("Tree should be valid after delete", _tree.isValid());
            assertNull("Value should be deleted for key: " + cur, _tree.get(cur));
        }
        assertEquals("Tree should be empty", 0, _tree.size());
    }

    public void testInsertAndDeleteRandomWithReload() throws Exception {
        Map tests = new HashMap();
        //Do inserts
        int max = _max * 10;
        for (int i = 0; i < max; i++) {
            Integer value = new Integer(_random.nextInt());
            String key = value.toString();
            if (!tests.containsKey(key)) {
                tests.put(key, value);
                _log.debug("Inserting key " + key);
                _tree.insert(key, value.intValue());
                _log.debug(_tree.toString());
                assertTrue("Tree should be valid after insert", _tree.isValid());
                assertEquals("Should get value back as inserting", value, _tree.get(key));
            }
        }
        assertEquals("Should have " + max + " insertions", max, tests.keySet().size());

        _tree.save(getDbdir());

        _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
        //Do deletes
        Iterator i = tests.keySet().iterator();
        while (i.hasNext()) {
            String cur = (String) i.next();
            Integer val = (Integer)tests.get(cur);
            assertEquals("Should get value back", val, _tree.get(cur));
            _log.debug("Deleting key " + cur);
            _log.debug("Tree before delete: " + _tree.toString());
            _tree.delete(cur, val.intValue());
            _log.debug("Tree after delete: " + _tree.toString());
            assertTrue("Tree should be valid after delete", _tree.isValid());
            assertNull("Value should be deleted for key: " + cur, _tree.get(cur));
        }

        _tree.save(getDbdir());

        _tree = new ObjectBTree(getDbdir(), "idx", _deg, new ComparableComparator());
        assertEquals("Tree should be empty", 0, _tree.size());
    }

    public void testGetAllTo() throws Exception {
        testInsertAndRetrieveMany();

        int topNum = 19;
        String topKey = _stringArray[topNum];
        IntIterator iter = _tree.getAllTo(topKey);
        for (int i = 0; i < topNum; i++) {
            assertTrue("Iterator should have another value", iter.hasNext());
            Integer val = new Integer(iter.next());
            _log.debug("Iterator Value: " + val);
            assertEquals("Should get the value back for " + i,
                new Integer(_stringArray.length - i), val);
        }
    }

    public void testGetAllFrom() throws Exception {
        testInsertAndRetrieveMany();

        int bottomNum = 19;
        String bottomKey = _stringArray[bottomNum];
        IntIterator iter = _tree.getAllFrom(bottomKey);
        for (int i = bottomNum; i < _stringArray.length; i++) {
            assertTrue("Iterator should have another value", iter.hasNext());
            Integer val = new Integer(iter.next());
            _log.debug("Iterator Value: " + val);
            assertEquals("Should get the value back for " + i,
                new Integer(_stringArray.length - i), val);
        }
    }

    public void testChangeId() throws Exception {
        testInsertAndRetrieveMany();

        String key = "def";
        int oldId = (_tree.get(key)).intValue();
        _tree.replaceId(key, oldId, oldId + 1000);
        assertEquals("Should get new id back for " + key, oldId + 1000, ( _tree.get(key))
            .intValue());
    }

    public void testChangeIdIdentical() throws Exception {
        testInsertAndRetrieveManyIdentical();

        String key = "abc";
        IntList oldIdList = new ArrayIntList();
        IntIterator iter = null;
        {
            ArrayIntList ids = new ArrayIntList();
            for (IntIterator tocopy = _tree.getAll(key); tocopy.hasNext();) {
                ids.add(tocopy.next());
            }
            iter = ids.iterator();
        }
        for (int i = 0; iter.hasNext(); i++) {
            int oldId = iter.next();
            oldIdList.add(oldId);
            if ((i % 2) == 0) {
                _tree.replaceId(key, oldId, oldId + 1000);
            }
        }
        {
            ArrayIntList ids = new ArrayIntList();
            for (IntIterator tocopy = _tree.getAll(key); tocopy.hasNext();) {
                ids.add(tocopy.next());
            }
            iter = ids.iterator();
        }
        for (int i = 0; iter.hasNext(); i++) {
            int newId = iter.next();
            int oldId = oldIdList.get(i);
            if ((i % 2) == 0) {
                assertEquals("Should get new id back for " + key, oldId + 1000, newId);
            } else {
                assertEquals("Should get old id back for " + key, oldId, newId);
            }
        }
    }
}