/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.rowcollection;

import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntListIterator;

/**
 * An int set that uses IntHashMap to keep ids.
 * 
 * @author Ahimanikya Satapathy
 */
public class IntSet implements IntCollection {

    // Dummy value to associate with an Object in the backing Map
    private static final Object PRESENT = new Object();

    private transient IntHashMap map;

    /**
     * Constructs a new, empty set; the backing <tt>HashMap</tt> instance has default
     * initial capacity (16) and load factor (0.75).
     */
    public IntSet() {
        map = new IntHashMap(16);
    }

    /**
     * Constructs a new, empty set; the backing <tt>HashMap</tt> instance has the
     * specified initial capacity and default load factor, which is <tt>0.75</tt>.
     * 
     * @param initialCapacity the initial capacity of the hash table.
     * @throws IllegalArgumentException if the initial capacity is less than zero.
     */
    public IntSet(int initialCapacity) {
        map = new IntHashMap(initialCapacity);
    }

    /**
     * Adds the specified element to this set if it is not already present.
     * 
     * @param id element to be added to this set.
     * @return <tt>true</tt> if the set did not already contain the specified element.
     */
    public boolean add(int id) {
        return map.put(id, PRESENT) == null;
    }

    /**
     * Removes all of the elements from this set.
     */
    public void clear() {
        map.clear();
    }

    /**
     * Returns <tt>true</tt> if this set contains the specified element.
     * 
     * @param id element whose presence in this set is to be tested.
     * @return <tt>true</tt> if this set contains the specified element.
     */
    public boolean contains(int id) {
        return map.containsKey(id);
    }

    /**
     * Returns <tt>true</tt> if this set contains no elements.
     * 
     * @return <tt>true</tt> if this set contains no elements.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    public IntIterator iterator() {
        return map.keyIterator();
    }
    
    public IntListIterator listIterator() {
        return map.keyIterator();
    }

    /**
     * Removes the specified element from this set if it is present.
     * 
     * @param id object to be removed from this set, if present.
     * @return <tt>true</tt> if the set contained the specified element.
     */
    public boolean remove(int id) {
        return map.remove(id) == PRESENT;
    }

    /**
     * Returns the number of elements in this set (its cardinality).
     * 
     * @return the number of elements in this set (its cardinality).
     */
    public int size() {
        return map.size();
    }

    public boolean addAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeElement(int element) {
        return map.remove(element) == PRESENT;
    }

    public boolean retainAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    public int[] toArray() {
        throw new UnsupportedOperationException();
    }

    public int[] toArray(int[] a) {
        throw new UnsupportedOperationException();
    }
}

