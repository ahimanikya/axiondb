/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.io.AxionFileSystem;

/**
 * A B-Tree for <code>Object</code>s, based on the implementation described in
 * "Introduction to Algorithms" by Cormen, Leiserson and Rivest (CLR).
 * 
 * @version  
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 * @author Rodney Waldhoff
 * @author Charles Ye
 * @author Ahimanikya Satapathy
 */
public class ObjectBTree extends BaseBTree {

    // NB: We are aware that declaring methods as final doesn't improve performance in
    // general. The stuff declared final is declared final so that we know it's not
    // overriden in any subclass. This makes refactoring easier.

    /**
     * Create or load a new root node.
     */
    public ObjectBTree(File idxDir, String idxName, int minimizationFactor, Comparator comp) throws IOException, ClassNotFoundException {
        super(idxDir, idxName, minimizationFactor);
        _keys = new ArrayList(getMinimizationFactor());
        _comparator = comp;
        if ((null != getBTreeMetaData().getDataDirectory() && getBTreeMetaData().getCounterFile().exists())) {
            setFileId(0);
            getBTreeMetaData().loadCounter();
            read();
        } else {
            getBTreeMetaData().assignFileId(this);
        }
    }

    /**
     * Create a new, non-root node.
     */
    protected ObjectBTree(BTreeMetaData meta, Comparator comp) throws IOException, ClassNotFoundException {
        super(meta);
        _keys = new ArrayList(getMinimizationFactor());
        _comparator = comp;
        meta.assignFileId(this);
    }

    /**
     * Create a non-root node by reading it from disk.
     */
    protected ObjectBTree(BTreeMetaData meta, Comparator comp, int fileId) throws IOException, ClassNotFoundException {
        super(meta);
        _keys = new ArrayList(getMinimizationFactor());
        _comparator = comp;
        setFileId(fileId);
        read();
    }

    /**
     * Clear my keys, values, and file ids. Flags me as dirty.
     */
    public final void clearData() {
        getKeys().clear();
        super.clearData();
    }

    /**
     * Optimized: Delete an arbitrary instance of the specified key and the specifiied
     * rowid
     */
    public final void delete(Object key, int rowid) throws IOException, ClassNotFoundException {
        // Comments refer to the cases described in CLR (19.3)
        if (size() <= 0) {
            return;
        }
        int i = findNearestKeyBelow(key, rowid);
        if (i >= 0 && (isEqual(getKey(i), key) && (getValue(i) != rowid))) {
            //Case 0
            int pivotLoc = i;
            if (!isLeaf()) {
                if (pivotLoc > 0 && getChild(pivotLoc - 1).size() >= getMinimizationFactor()) {
                    // Case 0a, borrow-left
                    borrowLeft(pivotLoc);
                    deleteFromChild(pivotLoc, key, rowid);
                } else if (pivotLoc + 1 < getChildIds().size() && getChild(pivotLoc + 1).size() >= getMinimizationFactor()) {
                    // Case 0a, borrow-right
                    borrowRight(pivotLoc);
                    deleteFromChild(pivotLoc, key, rowid);
                } else {
                    // Case 0b: if the key is on the far right,
                    // then we need to merge the last two nodes
                    int mergeLoc;
                    if (pivotLoc < size()) {
                        mergeLoc = pivotLoc;
                    } else {
                        mergeLoc = pivotLoc - 1;
                    }
                    mergeChildren(mergeLoc, getKey(mergeLoc));
                    maybeCollapseTree();
                    delete(key, rowid);
                }
            }
            // else no row found, ignored
        } else if ((i < 0 && isNotEqual(getKey(i + 1), key)) || isNotEqual(getKey(i), key)) {
            // Case 3
            int pivotLoc = i + 1;
            if (!isLeaf() && getChild(pivotLoc).size() < getMinimizationFactor()) {
                if (pivotLoc > 0 && getChild(pivotLoc - 1).size() >= getMinimizationFactor()) {
                    // Case 3a, borrow-left
                    borrowLeft(pivotLoc);
                    deleteFromChild(pivotLoc, key, rowid);
                } else if (pivotLoc + 1 < getChildIds().size() && getChild(pivotLoc + 1).size() >= getMinimizationFactor()) {
                    // Case 3a, borrow-right
                    borrowRight(pivotLoc);
                    deleteFromChild(pivotLoc, key, rowid);
                } else {
                    // Case 3b: if the key is on the far right,
                    // then we need to merge the last two nodes
                    int mergeLoc;
                    if (pivotLoc < size()) {
                        mergeLoc = pivotLoc;
                    } else {
                        mergeLoc = pivotLoc - 1;
                    }

                    mergeChildren(mergeLoc, getKey(mergeLoc));
                    maybeCollapseTree();
                    delete(key, rowid);
                }
            } else {
                deleteFromChild(i + 1, key, rowid);
            }
        } else {
            if (isLeaf()) {
                // Case 1
                removeKeyValuePairAt(i);
            } else {
                // Case 2
                if (getChild(i).size() >= getMinimizationFactor()) {
                    // Case 2a, move predecessor up
                    Object[] keyParam = new Object[1];
                    int[] valueParam = new int[1];
                    getChild(i).getRightMost(keyParam, valueParam);
                    setKeyValuePairAt(i, keyParam[0], valueParam[0]);
                    deleteFromChild(i, keyParam[0], valueParam[0]);
                } else if (getChild(i + 1).size() >= getMinimizationFactor()) {
                    // Case 2b, move successor up
                    Object[] keyParam = new Object[1];
                    int[] valueParam = new int[1];
                    getChild(i + 1).getLeftMost(keyParam, valueParam);
                    setKeyValuePairAt(i, keyParam[0], valueParam[0]);
                    deleteFromChild(i + 1, keyParam[0], valueParam[0]);
                } else {
                    // Case 2c, merge nodes
                    mergeChildren(i, key);

                    // Now delete from the newly merged node
                    deleteFromChild(i, key, rowid);

                    maybeCollapseTree();
                }
            }
        }
    }

    /**
     * Find some occurance of the given key.
     */
    public final Integer get(Object key) throws IOException, ClassNotFoundException {
        Integer result = null;
        int i = findNearestKeyAbove(key);
        if ((i < size()) && (isEqual(key, getKey(i)))) {
            result = new Integer(getValue(i));
        } else if (!isLeaf()) {
            // recurse to children
            result = getChild(i).get(key);
        }
        return result;
    }

    /**
     * Obtain an iterator over all values associated with the given key.
     */
    public final IntListIterator getAll(Object key) throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        getAll(key, chain);
        return chain;
    }

    /**
     * Obtain an iterator over all values excluding null key values
     */
    public final IntListIterator getAllExcludingNull() throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        getAllExcludingNull(chain);
        return chain;
    }

    /**
     * Obtain an iterator over all values greater than or equal to the given key.
     */
    public final IntListIterator getAllFrom(Object key) throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        getAllFrom(key, chain);
        return chain;
    }

    /**
     * Obtain an iterator over all values strictly less than the given key.
     */
    public final IntListIterator getAllTo(Object key) throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        getAllTo(key, chain);
        return chain;
    }

    public IntListIteratorChain inorderIterator() throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        inorderIterator(chain);
        return chain;
    }

    /**
     * Insert the given key/value pair.
     */
    public final void insert(Object key, int value) throws IOException, ClassNotFoundException {
        // The general strategy here is to:
        //  (a) find the appropriate node
        //  (b) if it is not full, simply insert the key/value pair
        //  (c) else, split the node into two, pulling up the median key to the parent
        // Since "pulling up the median key" may require splitting the parent,
        // to avoid multiple passes we'll split each full node as we move down the tree.

        if (isFull()) {
            // if I'm full,
            //  create a new node
            ObjectBTree child = allocateNewNode();
            //  copy all my data to that node
            child.addTuples(getKeys(), getValues(), getChildIds());
            // clear me
            clearData();
            // add that new child
            addFileId(0, child.getFileId());
            // split that child (since it is also full)
            subdivideChild(0, child);
        }
        insertNotfull(key, value);
    }

    /**
     * Replace any occurance of oldRowId associated with the given key with newRowId. It
     * is assumed oldId occurs at most once in the tree.
     */
    public final void replaceId(Object key, int oldRowId, int newRowId) throws ClassNotFoundException, IOException {
        int i = findNearestKeyAbove(key);
        boolean valSet = false;
        while ((i < size()) && isEqual(key, getKey(i))) {
            if (!isLeaf()) {
                replaceIdInChild(i, key, oldRowId, newRowId);
            }
            if (getValue(i) == oldRowId) {
                setValue(i, newRowId);
                valSet = true;
                getBTreeMetaData().setDirty(this);
                break;
            }
            i++;
        }
        if (!valSet && !isLeaf()) {
            replaceIdInChild(i, key, oldRowId, newRowId);
        }
    }

    /**
     * Save this tree and all of its children.
     * 
     * @deprecated See {@link #save(File)}
     */
    public final void save() throws IOException, ClassNotFoundException {
        saveCounterIfRoot();
        write();
        for (int i = 0, I = getChildIds().size(); i < I; i++) {
            getChild(i).save();
        }
    }

    /**
     * Returns the number of keys I currently contain. Note that by construction, this
     * will be at least {@link #getMinimizationFactor minimizationFactor}-1 and at most
     * 2*{@link #getMinimizationFactor minimizationFactor}-1 for all nodes except the
     * root (which may have fewer than {@link #getMinimizationFactor minimizationFactor}
     * -1 keys).
     */
    public final int size() {
        return getKeys().size();
    }

    /**
     * Obtain a String representation of this node, suitable for debugging.
     */
    public final String toString() {
        return toString(0);
    }

    public void truncate() {
        getKeys().clear();
        super.truncate();
    }

    /**
     * Return the child node at the given index, or throw an exception if no such row
     * exists.
     */
    final ObjectBTree getChild(int index) throws IOException, ClassNotFoundException {
        if (index >= getChildIds().size()) {
            throw new IOException("Node " + getFileId() + " has no child at index " + index);
        }
        return getChildByFileId(getFileIdForIndex(index));
    }

    final boolean isValid() throws IOException, ClassNotFoundException {
        return isValid(true);
    }

    protected final void addKeyValuePair(Object key, int value, boolean setDirty) {
        getKeys().add(key);
        getValues().add(value);
        if (setDirty) {
            getBTreeMetaData().setDirty(this);
        }
    }

    /**
     * Create a new node.
     */
    protected ObjectBTree createNode(BTreeMetaData meta, Comparator comp) throws IOException, ClassNotFoundException {
        return new ObjectBTree(meta, comp);
    }

    protected void getAllExcludingNull(IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = getKeys().lastIndexOf(getNullKey());

        if (isLeaf() && start != -1 && (start + 1) < size()) {
            chain.addIterator(getValues().subList(start + 1, size()).listIterator());
        } else {
            if (start == -1) {
                start = 0;
            }
            for (int i = start, size = size(); i < size + 1; i++) {
                if(getChild(i).size() > 0) {
                    getChild(i).getAllExcludingNull(chain);
                }
                if (i < size) {
                    chain.addIterator(getValue(i));
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Obtain the key stored at the specified index.
     */
    protected final Object getKey(int index) {
        return getKeys().get(index);
    }

    protected Object getNullKey() {
        return NullObject.INSTANCE;
    }

    /**
     * Read the node with the specified fileId from disk.
     */
    protected ObjectBTree loadNode(BTreeMetaData meta, Comparator comp, int fileId) throws IOException, ClassNotFoundException {
        return new ObjectBTree(meta, comp, fileId);
    }

    /**
     * Reads in the node. This doesn't read in the entire subtree, which happens
     * incrementally as files are needed.
     */
    protected void read() throws IOException, ClassNotFoundException {
        ObjectInputStream in = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            in = fs.openObjectInputSteam(getBTreeMetaData().getFileById(getFileId()));
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                addKeyValuePair(in.readObject(), in.readInt(), false);
            }
            size = in.readInt();
            for (int i = 0; i < size; i++) {
                getChildIds().add(in.readInt());
            }
        } finally {
            fs.closeInputStream(in);
        }
    }

    /**
     * Writes the node file out. This is differentiated from save in that it doesn't save
     * the entire tree or the counter file.
     */
    protected void write() throws IOException {
        ObjectOutputStream out = null;
        AxionFileSystem fs = new AxionFileSystem();    
        try {
            out = fs.createObjectOutputSteam(getBTreeMetaData().getFileById(getFileId()));
            int size = size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeObject(getKey(i));
                out.writeInt(getValue(i));
            }
            
            size = getChildIds().size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeInt(getChildIds().get(i));
            }
        } finally {
            fs.closeOutputStream(out);
        }
    }

    /** Adds the given key/value pair at the given index. Assumes I am not full. */
    private final void addKeyValuePair(int index, Object key, int value) {
        getKeys().add(index, key);
        getValues().add(index, value);
        getBTreeMetaData().setDirty(this);
    }

    /** Adds the given key/value pair. Assumes I am not full. */
    private final void addKeyValuePair(Object key, int value) {
        addKeyValuePair(key, value, true);
    }

    /** Adds the given key/value pair. Assumes I am not full. */
    private final void addKeyValuePairs(List keys, IntList values) {
        getKeys().addAll(keys);
        getValues().addAll(values);
        getBTreeMetaData().setDirty(this);
    }

    /** Adds the given key/value/fileId tuples. Assumes I am not full. */
    private final void addTuples(List keys, IntList values, IntList fileIds) {
        getKeys().addAll(keys);
        getValues().addAll(values);
        getChildIds().addAll(fileIds);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Create a new node and flag it as dirty.
     */
    private final ObjectBTree allocateNewNode() throws IOException, ClassNotFoundException {
        ObjectBTree tree = createNode(getBTreeMetaData(), _comparator);
        getBTreeMetaData().cacheNode(tree.getFileId(), tree);
        getBTreeMetaData().setDirty(tree);
        return tree;
    }

    private final void borrowLeft(int borrowLoc) throws IOException, ClassNotFoundException {
        ObjectBTree leftSibling = getChild(borrowLoc - 1);
        ObjectBTree rightSibling = getChild(borrowLoc);

        //Add the upper key to as the first entry of the right sibling
        rightSibling.addKeyValuePair(0, getKey(borrowLoc - 1), getValue(borrowLoc - 1));

        //Make the upper node's key the last key from the left sibling
        setKeyValuePairAt(borrowLoc - 1, leftSibling.getKey(leftSibling.size() - 1), leftSibling.getValue(leftSibling.getValues().size() - 1));

        //If the siblings aren't leaves, move the last child from the left to be the
        // first on the right
        if (!leftSibling.isLeaf()) {
            rightSibling.addFileId(0, leftSibling.getChild(leftSibling.getChildIds().size() - 1).getFileId());
        }

        //Remove the last entry of the left sibling (now moved to upper node)
        leftSibling.removeKeyValuePairAt(leftSibling.size() - 1);
        if (!leftSibling.isLeaf()) {
            leftSibling.getChildIds().removeElementAt(leftSibling.getChildIds().size() - 1);
        }
    }

    private final void borrowRight(int borrowLoc) throws IOException, ClassNotFoundException {
        ObjectBTree leftSibling = getChild(borrowLoc);
        ObjectBTree rightSibling = getChild(borrowLoc + 1);

        //Add the upper key to as the last entry of the left sibling
        leftSibling.addKeyValuePair(getKey(borrowLoc), getValue(borrowLoc));

        //Make the upper node's key the first key from the right sibling
        setKeyValuePairAt(borrowLoc, rightSibling.getKey(0), rightSibling.getValue(0));

        //If the siblings aren't leaves, move the first child from the right to be the
        // last on the left
        if (!leftSibling.isLeaf()) {
            leftSibling.addFileId(rightSibling.getChild(0).getFileId());
        }

        //Remove the first entry of the right sibling (now moved to upper node)
        rightSibling.removeKeyValuePairAt(0);
        if (!rightSibling.isLeaf()) {
            rightSibling.getChildIds().removeElementAt(0);
        }
    }

    /**
     * Compare the given objects via my comparator.
     */
    private final int compare(Object x, Object y) {
        if (x == getNullKey() && y == getNullKey()) {
            return 0;
        }
        if (x == getNullKey() && y != getNullKey()) {
            return -1;
        } else if (y == getNullKey() && x != getNullKey()) {
            return 1;
        }

        return _comparator.compare(x, y);
    }

    /**
     * Delete the given key and given rowid from the child in the specified position.
     */
    private final void deleteFromChild(int position, Object key, int rowid) throws IOException, ClassNotFoundException {
        ObjectBTree node = getChild(position);
        node.delete(key, rowid);
    }

    private final int findNearestKeyAbove(Object key) {
        int high = size();
        int low = 0;
        int cur = 0;

        //Short circuit
        if (size() == 0) {
            return 0;
        } else if (isLessThan(getKey(size() - 1), key)) {
            return size();
        } else if (isGreaterThanOrEqual(getKey(0), key)) {
            return 0;
        }

        while (low < high) {
            cur = (high + low) / 2;
            int comp = compare(key, getKey(cur));
            if (high == low) {
                cur = low;
                break;
            } else if (comp == 0) {
                //We found it now move to the first
                for (; (cur > 0) && (isEqual(key, getKey(cur))); cur--);
                break;
            } else if (high - low == 1) {
                cur = high;
                break;
            } else if (comp > 0) {
                if (low == cur) {
                    low++;
                } else {
                    low = cur;
                }
            } else { // comp < 0
                high = cur;
            }
        }

        //Now go to the nearest if there are multiple entries
        for (; (cur < size()) && isGreaterThan(key, getKey(cur)); cur++) {
        }

        return cur;
    }

    private final int findNearestKeyBelow(Object key) {
        int size = size();
        int high = size;
        int low = 0;
        int cur = 0;

        //Short circuit
        if (size == 0) {
            return -1;
        } else if (isLessThanOrEqual(getKey(size - 1), key)) {
            return size - 1;
        } else if (isGreaterThan(getKey(0), key)) {
            return -1;
        }

        while (low < high) {
            cur = (high + low) / 2;
            int comp = compare(key, getKey(cur));
            if (0 == comp) {
                //We found it now move to the last
                for (; (cur < size) && isEqual(key, getKey(cur)); cur++);
                break;
            } else if (comp > 0) {
                if (low == cur) {
                    low++;
                } else {
                    low = cur;
                }
            } else { // comp < 0
                high = cur;
            }
        }

        //Now go to the nearest if there are multiple entries
        for (; (cur >= 0) && (isLessThan(key, getKey(cur))); cur--);

        return cur;
    }

    /**
     * Find an index instance of specified key and specified rowid
     */
    private final int findNearestKeyBelow(Object key, int rowid) throws IOException {
        int size = size();
        int high = size;
        int low = 0;
        int cur = 0;

        //Short circuit
        if (size == 0) {
            return -1;
        } else if (isLessThan(getKey(size - 1), key)) {
            return size - 1;
        } else if (isGreaterThan(getKey(0), key)) {
            return -1;
        } else if (isEqual(getKey(size - 1), key)) {
            // Find a row
            cur = findNearestRowBelow(key, rowid, size - 1);
            return cur;
        }

        while (low < high) {
            cur = (high + low) / 2;
            int comp = compare(key, getKey(cur));
            if (0 == comp) {
                // Find a row
                cur = findNearestRowBelow(key, rowid, cur);
                break;
            } else if (comp > 0) {
                if (low == cur) {
                    low++;
                } else {
                    low = cur;
                }
            } else { // comp < 0
                high = cur;
            }
        }

        //Now go to the nearest if there are multiple entries
        for (; (cur >= 0) && (isLessThan(key, getKey(cur))); cur--);

        return cur;
    }

    /**
     * Find the row instsance of specified key and the specified row and the specified
     * index
     */
    private final int findNearestRowBelow(Object key, int rowid, int index) throws IOException {
        int cur = 0;
        int start = 0;
        int end = 0;
        boolean found = false;

        // Find the duplicated keys range
        if (index == (size() - 1) && index == 0) {
            //Case 1
            cur = 0;
            found = (rowid == getValue(cur));
            if (found) {
                cur = 0;
            } else if (getChildIds().size() > 0 && (rowid > getValue(cur))) {
                // the root is in the right-sub-tree
                cur++;
            } else {
                // the root is in the left-sub-tree
                cur = 0;
            }
            return cur;
        } else if (index == (size() - 1)) {
            //Case 2
            end = index;
            cur = index;
            // Find the first index which key is specified
            for (; (cur >= 0) && isEqual(key, getKey(cur)); cur--) {
                if (rowid == getValue(cur)) {
                    found = true;
                    break;
                }
            }
            start = found ? cur : ++cur;
        } else if (index == 0) {
            //Case 3
            start = index;
            cur = 0;
            // Find the last index which key is specified
            for (; (cur <= size()) && isEqual(key, getKey(cur)); cur++) {
                if (rowid == getValue(cur)) {
                    found = true;
                    break;
                }
            }
            end = found ? cur : --cur;
        } else {
            //Case 4
            cur = index;
            // Find the first index which key is specified
            for (; (cur >= 0) && isEqual(key, getKey(cur)); cur--) {
                if (rowid == getValue(cur)) {
                    found = true;
                    break;
                }
            }
            start = found ? cur : ++cur;
            if (!found) {
                cur = index;
                // Find the last index which key is specified
                for (; (cur < size()) && isEqual(key, getKey(cur)); cur++) {
                    if (rowid == getValue(cur)) {
                        found = true;
                        break;
                    }
                }
                end = found ? cur : --cur;
            }
        }

        if (!found) {
            if (rowid > getValue(end)) {
                //Case 1
                // the root is in the right-sub-tree
                cur = end;
            } else if (rowid < getValue(start)) {
                //Case 2
                // the root is in the left-sub-tree
                cur = start;
            } else {
                //Case 3
                // the root is in the middle-sub-tree
                for (int i = start; i < end; i++) {
                    if ((rowid > getValue(i)) && (rowid < getValue(i + 1))) {
                        cur = i;
                        break;
                    }
                }
            }
        }
        return cur;
    }

    private final void getAll(Object key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = findNearestKeyAbove(key);
        if (isLeaf()) {
            int stop;
            for (stop = start; stop < size() && isEqual(key, getKey(stop)); stop++) {
            }
            chain.addIterator(getValues().subList(start, stop).listIterator());
        } else {
            int i = start;
            for (; i < size() && isEqual(key, getKey(i)); i++) {
                getChild(i).getAll(key, chain);
                chain.addIterator(getValue(i));
            }
            getChild(i).getAll(key, chain);
        }
    }

    private final void getAllFrom(Object key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = findNearestKeyAbove(key);
        if (isLeaf()) {
            chain.addIterator(getValues().subList(start, size()).listIterator());
        } else {
            for (int i = start, size = size(); i < size + 1; i++) {
                getChild(i).getAllFrom(key, chain);
                if (i < size) {
                    chain.addIterator(getValue(i));
                } else {
                    break;
                }
            }
        }
    }

    private final void getAllTo(Object key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        if (isLeaf()) {
            int endpoint = getKeys().indexOf(key);
            if (-1 != endpoint) {
                chain.addIterator(getValues().subList(0, endpoint).listIterator());
            } else {
                chain.addIterator(getValues().listIterator());
            }
        } else {
            // else we need to interleave my child nodes as well
            int size = size();
            for (int i = 0; i < size + 1; i++) {
                getChild(i).getAllTo(key, chain);
                if (i < size && isGreaterThan(key, getKey(i))) {
                    chain.addIterator(getValue(i));
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Obtain the node with the specified file id, either from the cache or by loading it
     * from disk.
     */
    private final ObjectBTree getChildByFileId(int fileid) throws IOException, ClassNotFoundException {
        ObjectBTree child = (ObjectBTree) (getBTreeMetaData().getCachedNode(fileid));
        if (null == child) {
            child = loadNode(getBTreeMetaData(), _comparator, fileid);
            getBTreeMetaData().cacheNode(fileid, child);
        }
        return child;
    }

    /**
     * Obtain a reference to my list of keys.
     */
    private final List getKeys() {
        return _keys;
    }

    /**
     * Finds and deletes the left most value from this subtree. The key and value for the
     * node is returned in the parameters. This also does the replacement as it unwraps.
     */
    private final void getLeftMost(Object[] keyParam, int valueParam[]) throws IOException, ClassNotFoundException {
        if (isLeaf()) {
            keyParam[0] = getKey(0);
            valueParam[0] = getValue(0);
        } else {
            getChild(0).getLeftMost(keyParam, valueParam);
        }
    }

    /**
     * Finds and deletes the right most value from this subtree. The key and value for the
     * node is returned in the parameters. This also does the replacement as it unwraps.
     */
    private final void getRightMost(Object[] keyParam, int valueParam[]) throws IOException, ClassNotFoundException {
        if (isLeaf()) {
            int max = size() - 1;
            keyParam[0] = getKey(max);
            valueParam[0] = getValue(max);
        } else {
            int max = getChildIds().size() - 1;
            getChild(max).getRightMost(keyParam, valueParam);
        }
    }

    private void inorderIterator(IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = 0;//findNearestKeyAbove(key);
        if (isLeaf()) {
            int stop = size();
            chain.addIterator(getValues().subList(0, stop).listIterator());
        } else {
            int i = start;
            for (int size = size(); i < size; i++) {
                getChild(i).inorderIterator(chain);
                chain.addIterator(getValue(i));
            }
            getChild(i).inorderIterator(chain);
        }
    }

    /** Inserts the given key/value pair into this node, which is known to not be full */
    private final void insertNotfull(Object key, int value) throws IOException, ClassNotFoundException {
        // find the appropriate slot in me
        int i = findNearestKeyBelow(key);
        // if I'm a leaf...
        if (isLeaf()) {
            // ...just insert the key/value pair
            addKeyValuePair(i + 1, key, value);
            //...and we're done
        } else {
            // Else if I'm an internal node,
            // ...grab the child that should contain the key
            i++;
            ObjectBTree child = getChild(i);
            // if that child is full, split it
            if (child.isFull()) {
                // if full, split it
                subdivideChild(i, child);
                // (move forward one if needed)
                if (isGreaterThan(key, getKey(i))) {
                    i++;
                }
            }
            // then recurse
            getChild(i).insertNotfull(key, value);
        }
    }

    private final boolean isEqual(Object x, Object y) {
        return compare(x, y) == 0;
    }

    private final boolean isGreaterThan(Object x, Object y) {
        return compare(x, y) > 0;
    }

    private final boolean isGreaterThanOrEqual(Object x, Object y) {
        return compare(x, y) >= 0;
    }

    private final boolean isLessThan(Object x, Object y) {
        return compare(x, y) < 0;
    }

    private final boolean isLessThanOrEqual(Object x, Object y) {
        return compare(x, y) <= 0;
    }

    private final boolean isNotEqual(Object x, Object y) {
        return compare(x, y) != 0;
    }

    private final boolean isValid(boolean isRoot) throws IOException, ClassNotFoundException {
        //Check to make sure that the node isn't an empty branch
        int size = size();
        if (!isLeaf() && (size == 0)) {
            return false;
        }
        //Check to make sure that the node has enough children
        if (!isRoot && size < getMinimizationFactor() - 1) {
            return false;
        }
        //Check to make sure that there aren't too many children for the number of
        // entries
        if (!isLeaf() && (getChildIds().size() != size + 1 || size != getValues().size())) {
            return false;
        }
        //Check all of the children
        if (!isLeaf()) {
            for (int i = 0, I = getChildIds().size(); i < I; i++) {
                if (!getChild(i).isValid(false)) {
                    return false;
                }
            }
        }
        return true;
    }

    private final void maybeCollapseTree() throws IOException, ClassNotFoundException {
        if (!isLeaf() && size() <= 0) {
            ObjectBTree nodeToPromote = getChild(0);
            setTuples(nodeToPromote.getKeys(), nodeToPromote.getValues(), nodeToPromote.getChildIds());
        }
    }

    private final void mergeChildren(int mergeLoc, Object key) throws IOException, ClassNotFoundException {
        ObjectBTree leftChild = getChild(mergeLoc);
        ObjectBTree rightChild = getChild(mergeLoc + 1);

        //Move the key down to the left child
        leftChild.addKeyValuePair(key, getValue(mergeLoc));

        //Copy the keys and values from the right to the left
        leftChild.addTuples(rightChild.getKeys(), rightChild.getValues(), rightChild.getChildIds());

        //Now remove the item from the upper node (since it's been put in left child)
        removeKeyValuePairAt(mergeLoc);
        getChildIds().removeElementAt(mergeLoc + 1);

        rightChild.clearData();
    }

    /** Removes the specified given key/value pair. */
    private final void removeKeyValuePairAt(int index) {
        getKeys().remove(index);
        getValues().removeElementAt(index);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * {@link #replaceId Replace}the specified value within the child in the specified
     * position.
     */
    private final void replaceIdInChild(int position, Object key, int oldRowId, int newRowId) throws IOException, ClassNotFoundException {
        ObjectBTree node = getChild(position);
        node.replaceId(key, oldRowId, newRowId);
    }

    /** Sets my key list */
    private final void setKeys(List keys) {
        _keys = keys;
    }

    /** Sets the specified given key/value pair. */
    private final void setKeyValuePairAt(int index, Object key, int value) {
        getKeys().set(index, key);
        getValues().set(index, value);
        getBTreeMetaData().setDirty(this);
    }

    /** Sets the given given key/value/file id tuples. */
    private final void setTuples(List keys, IntList values, IntList fileIds) {
        setKeys(keys);
        setValues(values);
        if (null != fileIds) {
            setChildIds(fileIds);
        } else {
            getChildIds().clear();
        }
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Splits the given node into two nodes. After this method executes, the specified
     * child will contain the first {@link #getMinimzationFactor <i>t</i>}keys, and a new
     * node (stored at <i>pivot + 1 </i>) will contain the remaining keys. It is assumed
     * that child is full (child.size() == getKeyCapacity()) when this method is invoked.
     */
    private final void subdivideChild(int pivot, ObjectBTree child) throws IOException, ClassNotFoundException {
        // create the new child node, (a sibling to the specified child)
        ObjectBTree fetus = allocateNewNode();
        // insert said child into me (the parent)
        addFileId(pivot + 1, fetus.getFileId());

        // copy the tail key/value paris from child to the new node
        fetus.addKeyValuePairs(child.getKeys().subList(getMinimizationFactor(), getKeyCapacity()), child.getValues().subList(getMinimizationFactor(),
            getKeyCapacity()));

        // copy the children of child to the new node, if necessary
        int i = 0;
        if (!child.isLeaf()) {
            IntList sub = child.getChildIds().subList(getMinimizationFactor(), getKeyCapacity() + 1);
            fetus.addFileIds(sub);
            // remove the children from child
            for (i = getKeyCapacity(); i >= getMinimizationFactor(); i--) {
                child.getChildIds().removeElementAt(i);
            }
        }

        // add the median key (which now splits child from the new node)
        // here to the parent
        addKeyValuePair(pivot, child.getKey(getMinimizationFactor() - 1), child.getValue(getMinimizationFactor() - 1));

        // remove the key/value pairs from child
        for (i = (getKeyCapacity() - 1); i > (getMinimizationFactor() - 2); i--) {
            child.removeKeyValuePairAt(i);
        }
    }

    /**
     * Print this node, with every line prefixed by 2*space spaces.
     */
    private final String toString(int space) {
        StringBuffer buf = new StringBuffer();
        buf.append(space(space));
        buf.append(getFileId());
        buf.append(": ");
        buf.append(getKeys().toString());
        buf.append("/");
        buf.append(getValues().toString());
        buf.append("\n");
        if (!isLeaf()) {
            for (int i = 0, I = size() + 1; i < I; i++) {
                ObjectBTree child = null;
                try {
                    child = getChild(i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (null != child) {
                    buf.append(child.toString(space + 1));
                } else {
                    buf.append(space(space + 1));
                    buf.append("null");
                    buf.append("\n");
                }
            }
        }
        return buf.toString();
    }
    
    private final Comparator _comparator;
    private List _keys = null;
}
