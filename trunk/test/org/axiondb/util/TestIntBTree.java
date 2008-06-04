/*
 * $Id: TestIntBTree.java,v 1.1 2007/11/28 10:01:52 jawed Exp $
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

package org.axiondb.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.IntListIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.axiondb.AbstractDbdirTest;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:52 $
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 * @author Rod Waldhoff
 */
public class TestIntBTree extends AbstractDbdirTest {
   private static Log _log = LogFactory.getLog(TestIntBTree.class);
   private IntBTree _tree = null;
   private int _deg = 3;
   private int _max = (int)Math.pow(2*_deg, 2);
   private Random _random = new Random();

   //------------------------------------------------------------ Conventional
        

   public TestIntBTree(String testName) {
      super(testName);
   }

   public static void main(String args[]) {
      String[] testCaseName = { TestIntBTree.class.getName()};
      junit.textui.TestRunner.main(testCaseName);
   }

   public static Test suite() {
      return new TestSuite(TestIntBTree.class);
   }

   //--------------------------------------------------------------- Lifecycle
        
   public void setUp() throws Exception {
      super.setUp();
      long newSeed = _random.nextLong();
      _log.debug("New Seed: " + newSeed);
      _random.setSeed(newSeed);
      _tree = new IntBTree(getDbdir(), "idx", _deg);
   }

   public void tearDown() throws Exception {
      _tree = null;
      super.tearDown();
   }

   //------------------------------------------------------------------- Tests

   public void testCreate() throws Exception {
      assertTrue("Should have dbdir avail", getDbdir().exists());
      assertTrue("Dbdir should be directory", getDbdir().isDirectory());
      assertNotNull("Should not be null", _tree);
      assertTrue("Should be leaf", _tree.isLeaf());
      assertEquals("Should have no entries", 0, _tree.size());
      File idx = new File(getDbdir(), "IDX" + ".0");
      assertTrue("Should not have an index file before save", !idx.exists());
      _tree.insert(1,1);
      _tree.save(getDbdir());
      assertTrue("Should have an index file after save", idx.exists());
   }

   public void testValueIteratorGreaterThan() throws Exception {
       for(int i=0;i<_max;i++) {
           _tree.insert(i,i);      
       }
       for(int j=0;j<_max;j++) {
           IntListIterator iter = _tree.valueIteratorGreaterThan(j-1);
           assertTrue(!iter.hasPrevious());
           for(int i=j;i<_max;i++) {
               assertTrue(iter.hasNext());
               assertEquals(j+","+i,i,iter.next());
           }
           assertTrue(!iter.hasNext());
           for(int i=_max-1;i>=j;i--) {
               assertTrue(iter.hasPrevious());
               assertEquals(i,iter.previous());
           }
           assertTrue(!iter.hasPrevious());
       }
   }

   public void testGetAllFrom() throws Exception {
       for(int i=0;i<_max;i++) {
           _tree.insert(i,i);      
       }
       for(int j=0;j<_max;j++) {
           IntListIterator iter = _tree.getAllFrom(j);
           assertTrue(!iter.hasPrevious());
           for(int i=j;i<_max;i++) {
               assertTrue(iter.hasNext());
               assertEquals(j+","+i,i,iter.next());
           }
           assertTrue(!iter.hasNext());
           for(int i=_max-1;i>=j;i--) {
               assertTrue(iter.hasPrevious());
               assertEquals(j+","+i,i,iter.previous());
           }
           assertTrue(!iter.hasPrevious());
       }
   }

   public void testEmptyValueIteratorGreaterThan() throws Exception {
       for(int i=0;i<10;i++) {
           _tree.insert(i,i);      
       }
       IntListIterator iter = _tree.valueIteratorGreaterThan(9);
       assertTrue(!iter.hasNext());
       assertTrue(!iter.hasPrevious());
   }
   
   public void testCompleteValueIteratorGreaterThan() throws Exception {
       for(int i=0;i<10;i++) {
           _tree.insert(i,i);      
       }
       IntListIterator iter = _tree.valueIteratorGreaterThan(-1);
       for(int i=0;i<10;i++) {
           assertTrue(iter.hasNext());
           assertEquals(i,iter.next());
       }
       assertTrue(!iter.hasNext());
       for(int i=9;i>=0;i--) {
           assertTrue(iter.hasPrevious());
           assertEquals(i,iter.previous());
       }
       assertTrue(!iter.hasPrevious());
   }
   
   public void testValueIterator() throws Exception {
       for(int i=0;i<10;i++) {
           _tree.insert(i,i);      
       }
       IntListIterator iter = _tree.valueIterator();
       for(int i=0;i<10;i++) {
           assertTrue(iter.hasNext());
           assertEquals(i,iter.next());
       }
       assertTrue(!iter.hasNext());
       for(int i=9;i>=0;i--) {
           assertTrue(iter.hasPrevious());
           assertEquals(i,iter.previous());
       }
       assertTrue(!iter.hasPrevious());
   }

   public void testValueIteratorLong() throws Exception {
       for(int i=0;i<_max;i++) {
           _tree.insert(i,i);      
       }
       IntListIterator iter = _tree.valueIterator();
       for(int i=0;i<_max;i++) {
           assertTrue(iter.hasNext());
           assertEquals(i,iter.next());
       }
       assertTrue(!iter.hasNext());
       for(int i=_max-1;i>=0;i--) {
           assertTrue(iter.hasPrevious());
           assertEquals(i,iter.previous());
       }
       assertTrue(!iter.hasPrevious());
   }

   public void testEmptyValueIterator() throws Exception {
       IntIterator iter = _tree.valueIterator();
       assertTrue(!iter.hasNext());
       try {
           iter.next();
           fail("Expected NoSuchElementException");           
       } catch(NoSuchElementException e) {
           // expected
       }
   }

   public void testSingleValueIterator() throws Exception {
       _tree.insert(0,1);
       IntIterator iter = _tree.valueIterator();
       assertTrue(iter.hasNext());
       assertEquals(1,iter.next());
       assertTrue(!iter.hasNext());
   }

   public void testInsertAndRetrieve() throws Exception {
      _tree.insert(123, 456);
      assertTrue("Tree should be valid after insert", _tree.isValid());
      assertEquals("Should have one value", 1, _tree.size());
      assertEquals("Should get value back", new Integer(456), _tree.get(123));
   }

   public void testInsertAndRetrieveMany() throws Exception {
      for (int i = 0; i < _max; i++) {
         _log.debug("Inserting key " + i);
         _tree.insert(i, _max - i);
         _log.debug(_tree.toString());
         assertTrue("Tree should be valid after insert", _tree.isValid());
         assertEquals("Should get the value back as inserting for " + i,
                      new Integer(_max - i), _tree.get(i));
      }
      for (int i = 0; i < _max; i++) {
         assertEquals("Should get the value back for " + i,
                      new Integer(_max - i), _tree.get(i));
      }
   }

   public void testInsertIdenticalAndGetAll() throws Exception {
      _tree = new IntBTree(getDbdir(), "idx", 8);
      int max = 100;
      for (int i = 0; i < max; i++) {
         _log.debug("Inserting key " + i);
         _tree.insert(i, i);
         _tree.insert(i, max - i);
         _tree.insert(i, -1 * i);
         _log.debug(_tree.toString());
         assertTrue("Tree should be valid after insert", _tree.isValid());
         IntIterator iter = _tree.getAll(i);
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
         IntIterator iter = _tree.getAll(i);
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

   public void testReloadTree() throws Exception {
      //Insert a bunch of items
      {
          for (int i = 0; i < _max; i++) {
             _log.debug("Inserting key " + i);
             _tree.insert(i, _max - i);
             _log.debug(_tree.toString());
             assertTrue("Tree should be valid after insert", _tree.isValid());
             assertEquals("Should get the value back as inserting for " + i,
                          new Integer(_max - i), _tree.get(i));
          }
          _tree.save(getDbdir());
      }

      //Reload tree and make sure items are still there
      //then delete some of them
      {
          _tree = new IntBTree(getDbdir(), "idx", _deg);
          for (int i = 0; i < _max; i++) {
             assertEquals("Should get the value back for " + i + " from tree: " + _tree,
                          new Integer(_max - i), _tree.get(i));
          }
          
          for (int i = 0; i < _max / 2; i++) {
              _tree.delete(i, _max - i);
             assertEquals("Should not get the value back for " + i,
                          null, _tree.get(i));
          }
          _tree.save(getDbdir());
      }   

      //Reload the tree and make sure that items that should be there are there
      //and those that should not aren't.  Then add the deleted items back
      {
          _tree = new IntBTree(getDbdir(), "idx", _deg);
          for (int i = _max / 2; i < _max; i++) {
             assertEquals("Should get the value back for " + i,
                          new Integer(_max - i), _tree.get(i));
          }
          
          for (int i = 0; i < _max / 2; i++) {
             assertEquals("Should not get the value back for " + i,
                          null, _tree.get(i));
             _tree.insert(i, _max - i);
          }
          _tree.save(getDbdir());
      }

      //Reload the tree and make sure that all items are there
      {
          _tree = new IntBTree(getDbdir(), "idx", _deg);
          for (int i = 0; i < _max; i++) {
             assertEquals("Should get the value back for " + i,
                          new Integer(_max - i), _tree.get(i));
          }
          _tree.save(getDbdir());
      }
   }

   public void testInsertAndRetrieveManyRandom() throws Exception {
      Map tests = new HashMap();
      for (int i = 0; i < _max; i++) {
         Integer key = new Integer(_random.nextInt());
         Integer value = new Integer(_random.nextInt());
         if (!tests.containsKey(key)) {
            tests.put(key, value);
            _log.debug("Inserting key " + key);
            _tree.insert(key.intValue(), value.intValue());
            _log.debug(_tree.toString());
            assertTrue("Tree should be valid after insert", _tree.isValid());
            assertEquals("Should get value back as inserting",
                         value, _tree.get(key.intValue()));
         }
      }
      assertEquals("Should have " + _max + " insertions",
                   _max, tests.keySet().size());
      Iterator i = tests.keySet().iterator();
      while (i.hasNext()) {
         Integer cur = (Integer)i.next();
         assertEquals("Should get value back",
                      tests.get(cur), _tree.get(cur.intValue()));
      }
   }

   public void testInsertAndRetrieveManyIdentical() throws Exception {
       Set tests = new HashSet();
       for (int i = 0; i < _max; i++) {
         _log.debug("Insertion number " + (i+1));
         _tree.insert(3, i);
         assertTrue("Tree should be valid after insert", _tree.isValid());
         tests.add(new Integer(i));
         _log.debug(_tree.toString());
       }
       assertEquals("Should have full number of tests", _max, tests.size());
       IntIterator i = _tree.getAll(3);
       int count = 0;
       while (i.hasNext()) {
           tests.remove(new Integer(i.next()));
           count++;
       }
       assertEquals("Should have found all matching inserts", _max, count);
       assertTrue("All test vals should have been retrieved", tests.isEmpty());
       i = _tree.getAll(4);
       assertTrue("Should not have any matches", !i.hasNext());
   }

   public void testInsertAndDeleteCaseTests() throws Exception {
       //Do inserts
       for (int i = 1; i <= 26; i++) {
             _log.debug("Inserting key " + i);
             _tree.insert(i, i);
             _log.debug(_tree.toString());
             assertEquals("Should get value back as inserting", i, _tree.get(i).intValue());
             assertTrue("Tree should be valid after insert", _tree.isValid());
       }
       
       int[] deleteArray = {6, 13, 7, 2, 5, 21, 14, 11, 1, 4, 25, 23};
       
       for (int i = 0; i < deleteArray.length; i++) {
           int val = deleteArray[i];
           assertEquals("Should get value back", val, _tree.get(val).intValue());
           _log.debug("Deleting key " + val);
           _log.debug("Tree before delete: " + _tree.toString());
           _tree.delete(val, val);
           _log.debug("Tree after delete: " + _tree.toString());
           assertTrue("Tree should be valid after delete", _tree.isValid());
           assertNull("Value should be deleted for key: " + val, _tree.get(val));
       }       
   }

   public void testInsertAndDeleteLots() throws Exception {
       //Do inserts
       int max = _max * 10;
       for (int i = 0; i <= max; i++) {
           _log.debug("Inserting key " + i);
           _tree.insert(i, i);
           _log.debug(_tree.toString());
           assertTrue("Tree should be valid after insert", _tree.isValid());
           assertEquals("Should get value back as inserting", i, _tree.get(i).intValue());
       }
       
       for (int i = 0; i <= max; i++) {
           int val = (i + 20) % (max + 1);
           assertEquals("Should get value back", val, _tree.get(val).intValue());
           _log.debug("Deleting key " + val);
           _log.debug("Tree before delete: " + _tree.toString());
           _tree.delete(val, val);
           _log.debug("Tree after delete: " + _tree.toString());
           assertTrue("Tree should be valid after delete", _tree.isValid());
           assertNull("Value should be deleted for key: " + val, _tree.get(val));
       }       

       assertTrue("Tree should be empty", _tree.size() == 0);
   }

   public void testInsertAndDeleteLotsWithReload() throws Exception {
       //Do inserts
       int max = _max * 10;
       for (int i = 0; i <= max; i++) {
           _log.debug("Inserting key " + i);
           _tree.insert(i, i);
           _log.debug(_tree.toString());
           assertTrue("Tree should be valid after insert", _tree.isValid());
           assertEquals("Should get value back as inserting", i, _tree.get(i).intValue());
       }
       _tree.save(getDbdir());
       
       _tree = new IntBTree(getDbdir(), "idx", _deg);
       for (int i = 0; i <= max; i++) {
           int val = (i + 20) % (max + 1);
           assertEquals("Should get value back", val, _tree.get(val).intValue());
           _log.debug("Deleting key " + val);
           _log.debug("Tree before delete: " + _tree.toString());
           _tree.delete(val, val);
           _log.debug("Tree after delete: " + _tree.toString());
           assertNull("Value should be deleted for key: " + val, _tree.get(val));
       }       
       _tree.save(getDbdir());
       
       _tree = new IntBTree(getDbdir(), "idx", _deg);
       assertTrue("Tree should be empty", _tree.size() == 0);
   }

   public void testInsertAndDeleteRandom() throws Exception {
       Map tests = new HashMap();
       //Do inserts
       int max = _max * 10;
       for (int i = 0; i < max; i++) {
          Integer key = new Integer(_random.nextInt());
          Integer value = key;
          if (!tests.containsKey(key)) {
             tests.put(key, value);
             _log.debug("Inserting key " + key);
             _tree.insert(key.intValue(), value.intValue());
             _log.debug(_tree.toString());
             assertTrue("Tree should be valid after insert", _tree.isValid());
             assertEquals("Should get value back as inserting",
                          value, _tree.get(key.intValue()));
          }
       }
       assertEquals("Should have " + max + " insertions",
                    max, tests.keySet().size());
       //Do deletes
       Iterator i = tests.keySet().iterator();
       while (i.hasNext()) {
          Integer cur = (Integer)i.next();
          Integer value = (Integer) tests.get(cur);
          assertEquals("Should get value back",
                       value, _tree.get(cur.intValue()));
          _log.debug("Deleting key " + cur);
          _log.debug("Tree before delete: " + _tree.toString());
          _tree.delete(cur.intValue(), value.intValue());
          _log.debug("Tree after delete: " + _tree.toString());
          assertTrue("Tree should be valid after delete", _tree.isValid());
          assertNull("Value should be deleted for key: " + cur.intValue(),
                       _tree.get(cur.intValue()));
       }
       assertEquals("Tree should be empty", 0, _tree.size());
   }

   public void testInsertAndDeleteRandomWithReload() throws Exception {
       Map tests = new HashMap();
       //Do inserts
       int max = _max * 10;
       for (int i = 0; i < max; i++) {
          Integer key = new Integer(_random.nextInt());
          Integer value = key;
          if (!tests.containsKey(key)) {
             tests.put(key, value);
             _log.debug("Inserting key " + key);
             _tree.insert(key.intValue(), value.intValue());
             _log.debug(_tree.toString());
             assertTrue("Tree should be valid after insert", _tree.isValid());
             assertEquals("Should get value back as inserting",
                          value, _tree.get(key.intValue()));
          }
       }
       assertEquals("Should have " + max + " insertions",
                    max, tests.keySet().size());

       _tree.save(getDbdir());

       _tree = new IntBTree(getDbdir(), "idx", _deg);
       //Do deletes
       Iterator i = tests.keySet().iterator();
       while (i.hasNext()) {
          Integer cur = (Integer)i.next();
          Integer val = (Integer)tests.get(cur);
          assertEquals("Should get value back",
                       val, _tree.get(cur.intValue()));
          _log.debug("Deleting key " + cur);
          _log.debug("Tree before delete: " + _tree.toString());
          _tree.delete(cur.intValue(), val.intValue());
          _log.debug("Tree after delete: " + _tree.toString());
          assertNull("Value should be deleted for key: " + cur.intValue(),
                       _tree.get(cur.intValue()));
       }

       _tree.save(getDbdir());

       _tree = new IntBTree(getDbdir(), "idx", _deg);
       assertEquals("Tree should be empty", 0, _tree.size());
   }

   public void testGetAllTo() throws Exception {
       testInsertAndRetrieveMany();

       int topNum = 23;
       IntIterator iter = _tree.getAllTo(topNum);
       for (int i = 0; i < topNum; i++) {
          assertTrue("Iterator should have another value", iter.hasNext());
          Integer val = new Integer(iter.next());
          _log.debug("Iterator Value: " + val);
          assertEquals("Should get the value back for " + i,
                       new Integer(_max - i), val);
       }
   }

   public void testChangeId() throws Exception {
       testInsertAndRetrieveMany();

       int key = 18;
       int oldId = (_tree.get(key)).intValue();
       _tree.replaceId(key, oldId, oldId + 1000);
       assertEquals("Should get new id back for " + key, oldId + 1000, (_tree.get(key)).intValue());
   }

   public void testChangeIdIdentical() throws Exception {
       testInsertAndRetrieveManyIdentical();

       int key = 3;
       IntList oldIdList = new ArrayIntList();
       IntIterator iter = null;
       {
           // copy the old ids to a new list since _tree.replaceId will change the list
           IntList ids = new ArrayIntList();       
           for(IntIterator tocopy = _tree.getAll(key);tocopy.hasNext();) {
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
           // copy the old ids to a new list since _tree.replaceId will change the list
           IntList ids = new ArrayIntList();       
           for(IntIterator tocopy = _tree.getAll(key);tocopy.hasNext();) {
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
