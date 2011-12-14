/* 
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

import java.util.NoSuchElementException;

import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.RowCollection;

/**
 * Int key and Object value Map, this does not implement java.util.Map interface and has
 * limited Map like API. Does not implement EntrySet and and KeySet, tather it just
 * retunds their iterator.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */

public class IntHashMap {

    /** Creates an IntHashMap of small initial capacity. */
    public IntHashMap() {
        this(16);
    }

    /**
     * Creates an IntHashMap of specified initial capacity. Unless the map size exceeds the
     * specified capacity no memory allocation is ever performed.
     * 
     * @param capacity the initial capacity.
     */
    public IntHashMap(int capacity) {
        int tableLength = 16;
        while (tableLength < capacity) {
            tableLength <<= 1;
        }
        _entries = new IntHashMap.Entry[tableLength];
        _head._next = _tail;
        _tail._previous = _head;
        Entry previous = _tail;
        for (int i = 0; i++ < capacity;) {
            Entry newEntry = newEntry();
            newEntry._previous = previous;
            previous._next = newEntry;
            previous = newEntry;
        }
    }

    /**
     * Creates a IntHashMap containing the specified entries, in the order they are
     * returned by the map's iterator.
     * 
     * @param map the map whose entries are to be placed into this map.
     */
    public IntHashMap(IntHashMap map) {
        this(map.size());
        putAll(map);
    }

    /**
     * Removes all mappings from this {@link IntHashMap}.
     */
    public void clear() {
        // Clears all keys, values and buckets linked lists.
        for (Entry e = _head, end = _tail; (e = e._next) != end;) {
            e._key = -1;
            e._value = null;
            final Entry[] table = e._table;
            table[e._keyHash & (table.length - 1)] = null;
        }
        _tail = _head._next;
        _size = 0;

        // Discards old entries.
        _oldEntries = null;
    }

    /**
     * Indicates if this {@link IntHashMap}contains a mapping for the specified key.
     * 
     * @param key the key whose presence in this map is to be tested.
     * @return <code>true</code> if this map contains a mapping for the specified key;
     *         <code>false</code> otherwise.
     */
    public final boolean containsKey(int key) {
        final int keyHash = key;
        Entry entry = _entries[keyHash & (_entries.length - 1)];
        while (entry != null) {
            if ((key == entry._key) || ((entry._keyHash == keyHash) && (key == entry._key))) {
                return true;
            }
            entry = entry._beside;
        }
        return (_oldEntries != null) ? _oldEntries.containsKey(key) : false;
    }

    /**
     * Indicates if this {@link IntHashMap}maps one or more keys to the specified value.
     * 
     * @param value the value whose presence in this map is to be tested.
     * @return <code>true</code> if this map maps one or more keys to the specified
     *         value.
     */
    public final boolean containsValue(Object value) {
        for (Entry e = headEntry(), end = tailEntry(); (e = e._next) != end;) {
            if (value.equals(e._value)) {
                return true;
            }
        }
        return false;
    }

    public EntryIterator entryIterator() {
        return new EntryIterator();
    }

    /**
     * Compares the specified object with this {@link IntHashMap}for equality. Returns
     * <code>true</code> if the given object is also a map and the two maps represent
     * the same mappings (regardless of collection iteration order).
     * 
     * @param obj the object to be compared for equality with this map.
     * @return <code>true</code> if the specified object is equal to this map;
     *         <code>false</code> otherwise.
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof IntHashMap) {
            IntHashMap that = (IntHashMap) obj;
            if (this._size == that._size) {
                for (Entry e = _head, end = _tail; (e = e._next) != end;) {
                    if (!that.containsKey(e._key)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Returns the value to which this {@link IntHashMap}maps the specified key.
     * 
     * @param key the key whose associated value is to be returned.
     * @return the value to which this map maps the specified key, or <code>null</code>
     *         if there is no mapping for the key.
     */
    public final Object get(int key) {
        final int keyHash = key;
        Entry entry = _entries[keyHash & (_entries.length - 1)];
        while (entry != null) {
            if ((key == entry._key) || ((entry._keyHash == keyHash) && (key == entry._key))) {
                return entry._value;
            }
            entry = entry._beside;
        }
        return (_oldEntries != null) ? _oldEntries.get(key) : null;
    }

    /**
     * Returns the entry with the specified key.
     * 
     * @param key the key whose associated entry is to be returned.
     * @return the entry for the specified key or <code>null</code> if none.
     */
    public final Entry getEntry(int key) {
        final int keyHash = key;
        Entry entry = _entries[keyHash & (_entries.length - 1)];
        while (entry != null) {
            if ((key == entry._key) || ((entry._keyHash == keyHash) && (key == entry._key))) {
                return entry;
            }
            entry = entry._beside;
        }
        return (_oldEntries != null) ? _oldEntries.getEntry(key) : null;
    }

    /**
     * Returns the hash code value for this {@link IntHashMap}.
     * 
     * @return the hash code value for this map.
     */
    public int hashCode() {
        int code = 0;
        for (Entry e = _head, end = _tail; (e = e._next) != end;) {
            code += e.hashCode();
        }
        return code;
    }

    /**
     * Returns the head entry of this map.
     * 
     * @return the entry such as <code>headEntry().getNextEntry()</code> holds the first
     *         map entry.
     */
    public final Entry headEntry() {
        return _head;
    }

    /**
     * Indicates if this {@link IntHashMap}contains no key-value mappings.
     * 
     * @return <code>true</code> if this map contains no key-value mappings;
     *         <code>false</code> otherwise.
     */
    public final boolean isEmpty() {
        return _head._next == _tail;
    }

    public IntListIterator keyIterator() {
        return new IntKeyIterator();
    }

    /**
     * Associates the specified value with the specified key in this {@link IntHashMap}.
     * If the {@link IntHashMap}previously contained a mapping for this key, the old value
     * is replaced.
     * 
     * @param key the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @return the previous value associated with specified key, or <code>null</code> if
     *         there was no mapping for key. A <code>null</code> return can also
     *         indicate that the map previously associated <code>null</code> with the
     *         specified key.
     */
    public final Object put(int key, Object value) {
        final int keyHash = key;
        Entry entry = _entries[keyHash & (_entries.length - 1)];
        while (entry != null) {
            if ((key == entry._key) || ((entry._keyHash == keyHash) && (key == entry._key))) {
                Object prevValue = entry._value;
                entry._value = value;
                return prevValue;
            }
            entry = entry._beside;
        }
        // No mapping in current map, checks old one.
        if (_oldEntries != null) {
            // For safe unsynchronized access we don't remove old key.
            if (_oldEntries.containsKey(key)) {
                return _oldEntries.put(key, value);
            }
        }

        // The key is not mapped.
        addEntry(keyHash, key, value);
        return null;
    }

    /**
     * Copies all of the mappings from the specified map to this {@link IntHashMap}.
     * 
     * @param map the mappings to be stored in this map.
     */
    public final void putAll(IntHashMap that) {
        for (Entry e = that._head, end = that._tail; (e = e._next) != end;) {
            put(e._key, e._value);
        }
    }

    /**
     * Removes the mapping for this key from this {@link IntHashMap}if present.
     * 
     * @param key the key whose mapping is to be removed from the map.
     * @return previous value associated with specified key, or <code>null</code> if
     *         there was no mapping for key. A <code>null</code> return can also
     *         indicate that the map previously associated <code>null</code> with the
     *         specified key.
     */
    public final Object remove(int key) {
        final int keyHash = key;
        Entry entry = _entries[keyHash & (_entries.length - 1)];
        while (entry != null) {
            if ((key == entry._key) || ((entry._keyHash == keyHash) && (key == entry._key))) {
                Object prevValue = entry._value;
                removeEntry(entry);
                return prevValue;
            }
            entry = entry._beside;
        }
        // No mapping in current map.
        if ((_oldEntries != null) && _oldEntries.containsKey(key)) {
            _size--;
            _oldEntries._tail = _tail; // Specifies the tail for entry storage.
            return _oldEntries.remove(key);
        }
        return null;
    }

    /**
     * Removes the specified entry from the map.
     * 
     * @param entry the entry to be removed.
     */
    public void removeEntry(Entry entry) {
        // Updates size.
        _size--;

        // Clears value and key.
        entry._key = -1;
        entry._value = null;

        // Detaches from map.
        entry.detach();

        // Re-inserts next tail.
        final Entry next = _tail._next;
        entry._previous = _tail;
        entry._next = next;
        _tail._next = entry;
        if (next != null) {
            next._previous = entry;
        }
    }

    /**
     * Returns a list iterator over the values in this list in proper sequence, (this map
     * maintains the insertion order).
     * 
     * @return a list iterator of the values in this list (in proper sequence).
     */
    public final ValueIterator valueIterator() {
        return new ValueIterator();
    }

    /**
     * Returns the number of key-value mappings in this {@link IntHashMap}.
     * 
     * @return this map's size.
     */
    public final int size() {
        return _size;
    }

    /**
     * Returns the tail entry of this map.
     * 
     * @return the entry such as <code>tailEntry().getPreviousEntry()</code> holds the
     *         last map entry.
     */
    public final Entry tailEntry() {
        return _tail;
    }

    /**
     * Returns the textual representation of this {@link IntHashMap}.
     * 
     * @return the textual representation of the entry set.
     */
    public String toString() {
        return _entries.toString();
    }

    /**
     * Returns a {@link RowCollection}view of the values contained in this
     * {@link IntHashMap}. The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa. The collection supports element
     * removal, which removes the corresponding mapping from this map, via the
     * <code>RowIterator.remove</code>,<code>RowCollection.remove</code> and
     * <code>clear</code> operations.
     * 
     * @return a row collection view of the values contained in this map.
     */
    public final Values values() {
        return _values;
    }
    
    public IntCollection keys() {
        return _keys;
    }

    /**
     * Returns a new entry for this map; sub-classes may override this method to use
     * custom entries.
     * 
     * @return a new entry potentially preallocated.
     */
    protected Entry newEntry() {
        return new Entry();
    }

    /**
     * Adds a new entry for the specified key and value.
     * 
     * @param hash the hash of the key, generated with {@link #keyHash}.
     * @param key the entry's key.
     * @param value the entry's value.
     */
    protected void addEntry(int hash, int key, Object value) {
        // Updates size.
        if (_size++ > _entries.length) { // Check if entry table too small.
            increaseEntryTable();
        }

        Entry newTail = _tail._next;
        if (newTail == null) {
            newTail = _tail._next = newEntry();
            newTail._previous = _tail;
        }
        // Setups entry parameters.
        _tail._key = key;
        _tail._value = value;
        _tail._keyHash = hash;
        _tail._table = _entries;

        // Connects to bucket.
        final int index = hash & (_entries.length - 1);
        Entry beside = _entries[index];
        _tail._beside = beside;
        _entries[index] = _tail; // Volatile.

        // Moves tail forward.
        _tail = newTail;
    }

    private void increaseEntryTable() {
        int minLength = _entries.length << 1;
        IntHashMap oldEntries;
        oldEntries = new IntHashMap(1 << 6);
        if (minLength <= (1 << 6)) { // 64
            oldEntries = new IntHashMap(1 << 6);
        } else if (minLength <= (1 << 8)) { // 256
            oldEntries = new IntHashMap(1 << 8);
        } else if (minLength <= (1 << 10)) { // 1,024
            oldEntries = new IntHashMap(1 << 10);
        } else if (minLength <= (1 << 14)) { // 16,384
            oldEntries = new IntHashMap(1 << 14);
        } else if (minLength <= (1 << 18)) { // 262,144
            oldEntries = new IntHashMap(1 << 18);
        } else if (minLength <= (1 << 22)) { // 4,194,304
            oldEntries = new IntHashMap(1 << 22);
        } else if (minLength <= (1 << 26)) { // 67,108,864
            oldEntries = new IntHashMap(1 << 26);
        } else { // 1,073,741,824
            oldEntries = new IntHashMap(1 << 30);
        }
        // Swaps entries.
        Entry[] newEntries = oldEntries._entries;
        oldEntries._entries = _entries;
        _entries = newEntries;

        // Setup the oldEntries map (used only for hash access).
        oldEntries._oldEntries = _oldEntries;
        oldEntries._head = null;
        oldEntries._tail = null;
        oldEntries._size = -1;

        // Done. We have now a much larger entry table.
        // Still, we keep reference to the old entries through oldEntries
        // until the map is cleared.
        _oldEntries = oldEntries;
    }

    /**
     * This class represents a {@link IntHashMap}entry.
     */
    public static class Entry {

        /** Holds the next entry in the same bucket. */
        private Entry _beside;

        /** Holds the entry key. */
        private int _key;

        /** Holds the key hash code. */
        private int _keyHash;

        /** Holds the next node. */
        private Entry _next;

        /** Holds the previous node. */
        private Entry _previous;

        /** Holds the hash table this entry belongs to. */
        private Entry[] _table;

        /** Holds the entry value. */
        private Object _value;

        /** Default constructor (allows sub-classing). */
        protected Entry() {
        }

        /**
         * Indicates if this entry is considered equals to the specified entry (default
         * object equality to ensure symetry)
         * 
         * @param that the object to test for equality.
         * @return <code>true<code> if both entry have equal keys and values.
         *         <code>false<code> otherwise.
         */
        public boolean equals(Object that) {
            if (that instanceof Entry) {
                Entry entry = (Entry) that;
                return (_key == getKey()) && ((_value != null) ? _value.equals(entry.getValue()) : (entry.getValue() == null));
            } else {
                return false;
            }
        }

        /**
         * Returns the key for this entry.
         * 
         * @return the entry key.
         */
        public final int getKey() {
            return _key;
        }

        /**
         * Returns the entry after this one.
         * 
         * @return the next entry.
         */
        public Entry getNextEntry() {
            return _next;
        }

        /**
         * Returns the entry before this one.
         * 
         * @return the previous entry.
         */
        public Entry getPreviousEntry() {
            return _previous;
        }

        /**
         * Returns the value for this entry.
         * 
         * @return the entry value.
         */
        public final Object getValue() {
            return _value;
        }

        /**
         * Returns the hash code for this entry.
         * 
         * @return this entry hash code.
         */
        public int hashCode() {
            return _key ^ ((_value != null) ? _value.hashCode() : 0);
        }

        /**
         * Sets the value for this entry.
         * 
         * @param value the new value.
         * @return the previous value.
         */
        public final Object setValue(Object value) {
            Object old = _value;
            _value = value;
            return old;
        }

        /**
         * Detaches this entry from the entry table and list.
         */
        private void detach() {
            // Removes from list.
            _previous._next = _next;
            _next._previous = _previous;

            // Removes from bucket.
            final int index = _keyHash & (_table.length - 1);
            final Entry beside = _beside;
            Entry previous = _table[index];
            if (previous == this) { // First in bucket.
                _table[index] = beside;
            } else {
                while (previous._beside != this) {
                    previous = previous._beside;
                }
                previous._beside = beside;
            }
        }
    }

    public class EntryIterator {

        private int _currentIndex;
        private Entry _currentNode;
        private int _nextIndex;
        private Entry _nextNode;

        public EntryIterator() {
            _nextNode = _head._next;
            _currentNode = null;
            _nextIndex = 0;
            _currentIndex = -1;
        }

        public void addEntry(int hash, int key, Object value) {
            IntHashMap.this.addEntry(hash, key, value);
            _currentNode = null;
            _nextIndex++;
            _currentIndex = -1;
        }

        public Entry currentEntry() {
            if (!hasCurrent())
                throw new NoSuchElementException();
            return _currentNode;
        }

        public int currentIndex() {
            return _currentIndex;
        }

        public Entry firstEntry()  {
            reset();
            return peekNextEntry();
        }

        public boolean hasCurrent() {
            return (_currentNode != null);
        }

        public boolean hasNext() {
            return (_nextIndex < IntHashMap.this._size);
        }

        public boolean hasPrevious() {
            return _nextIndex > 0;
        }

        public boolean isEmpty() {
            return IntHashMap.this.isEmpty();
        }

        public Entry lastEntry()  {
            if (!hasNext()) {
                previousEntry();
            }
            Entry entry = null;
            while (hasNext()) {
                entry = nextEntry();
            }
            return entry;
        }

        public Entry nextEntry() {
            if (_nextIndex == IntHashMap.this._size)
                throw new NoSuchElementException();
            _currentIndex = _nextIndex;
            _nextIndex++;
            _currentNode = _nextNode;
            _nextNode = _nextNode._next;
            return _currentNode;
        }

        public int nextIndex() {
            return _nextIndex;
        }

        public Entry peekNextEntry()  {
            nextEntry();
            return previousEntry();
        }

        public Entry peekPreviousEntry()  {
            previousEntry();
            return nextEntry();
        }

        public Entry previousEntry() {
            if (_nextIndex == 0)
                throw new NoSuchElementException();
            _nextIndex--;
            _currentIndex = _nextIndex;
            _currentNode = _nextNode = _nextNode._previous;
            return _currentNode;
        }

        public int previousIndex() {
            return _nextIndex - 1;
        }

        public void remove() {
            if (_currentNode != null) {
                if (_nextNode == _currentNode) { // previous() has been called.
                    _nextNode = _nextNode._next;
                } else {
                    _nextIndex--;
                }
                IntHashMap.this.removeEntry(_currentNode);
                _currentNode = null;
                _currentIndex = -1;
            } else {
                throw new IllegalStateException();
            }
        }

        public void reset() {
            _nextNode = _head._next;
            _currentNode = null;
            _nextIndex = 0;
            _currentIndex = -1;
        }

        public void setEntry(Entry o) {
            if (_currentNode != null) {
                _currentNode = o;
            } else {
                throw new IllegalStateException();
            }
        }

        public int size()  {
            return IntHashMap.this._size;
        }
    }

    private class IntKeyIterator extends EntryIterator implements IntListIterator {
        public int next() {
            return nextEntry().getKey();
        }

        public void add(int element) {
            throw new UnsupportedOperationException();
        }

        public int previous() {
            return previousEntry().getKey();
        }

        public void set(int element) {
            throw new UnsupportedOperationException();
        }
    }

    public class ValueIterator extends EntryIterator {

        public void add(Object row) throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

        public Object current() throws NoSuchElementException {
            return currentEntry().getValue();
        }

        public Object first() throws NoSuchElementException {
            return firstEntry().getValue();
        }

        public Object last() throws NoSuchElementException {
            return lastEntry().getValue();
        }

        public Object next() throws NoSuchElementException {
            return nextEntry().getValue();
        }

        public Object peekNext() throws NoSuchElementException {
            return peekNextEntry().getValue();
        }

        public Object peekPrevious() throws NoSuchElementException {
            return peekPreviousEntry().getValue();
        }

        public Object previous() throws NoSuchElementException {
            return previousEntry().getValue();
        }

        public void set(Object row) throws UnsupportedOperationException {
            if (!hasCurrent()) {
                throw new IllegalStateException();
            }
            currentEntry().setValue(row);
        }

        public int next(int count)  {
            for (int i = 0; i < count; i++) {
                next();
            }
            return currentEntry().getKey();
        }

        public int previous(int count)  {
            for (int i = 0; i < count; i++) {
                previous();
            }
            return currentEntry().getKey();
        }

    }
    
    private class Keys implements IntCollection {

        public boolean add(int id) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            IntHashMap.this.clear();
        }

        public boolean contains(int id) {
            return IntHashMap.this.containsKey(id);
        }

        public boolean isEmpty() {
            return IntHashMap.this.isEmpty();
        }

        public IntIterator iterator() {
            return IntHashMap.this.keyIterator();
        }

        public boolean remove(int id) {
            return IntHashMap.this.remove(id) != null;
        }

        public int size() {
            return IntHashMap.this.size();
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
            return IntHashMap.this.remove(element) != null;
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
    
    protected class Values {

        public boolean add(Object row) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            IntHashMap.this.clear();
        }

        public ValueIterator iterator() {
            return valueIterator();
        }

        public int size() {
            return _size;
        }

        public boolean contains(Object row) {
            return IntHashMap.this.containsValue(row);
        }

        public boolean isEmpty() {
            return IntHashMap.this.isEmpty();
        }

        public boolean remove(Object row) {
            for (Entry r = headEntry(), end = tailEntry(); (r = r._next) != end;) {
                if (row.equals(r._value)) {
                    IntHashMap.this.removeEntry(r);
                    return true;
                }
            }
            return false;
        }
    }

    /** Holds the map's hash table (volatile for unsynchronized access). */
    private volatile transient Entry[] _entries;

    /**
     * Holds the head entry to which the first entry attaches. The head entry never
     * changes (entries always added last).
     */
    private transient Entry _head = newEntry();

    /** Holds a reference to a map having the old entries when resizing. */
    private transient IntHashMap _oldEntries;

    /** Holds the current size. */
    private transient int _size;

    /**
     * Holds the tail entry to which the last entry attaches. The tail entry changes as
     * entries are added/removed.
     */
    private transient Entry _tail = newEntry();

    /** Holds the values view. */
    private transient Values _values = new Values();
    
    /** Holds the keys view. */
    private transient Keys _keys = new Keys();
}
