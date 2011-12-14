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
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.IntListIterator;
import org.axiondb.io.AxionFileSystem;

/**
 * A B-Tree for integers, based on the implementation described in "Introduction to
 * Algorithms" by Cormen, Leiserson and Rivest (CLR).
 * 
 * @version  
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 * @author Rodney Waldhoff
 * @author Ritesh Adval
 * @author Charles Ye
 * @author Ahimanikya Satapathy
 */
public class IntBTree extends BaseBTree {

    // NB: We are aware that declaring methods as final doesn't improve performance in
    // general.The stuff declared final is declared final so that we know it's not
    // overriden in any subclass. This makes refactoring easier.

    /**
     * Create or load a new root node.
     */
    public IntBTree(File idxDir, String idxName, int minimizationFactor) throws IOException, ClassNotFoundException {
        super(idxDir, idxName, minimizationFactor);
        _keys = new ArrayIntList(getMinimizationFactor());
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
    protected IntBTree(BTreeMetaData meta) throws IOException, ClassNotFoundException {
        super(meta);
        _keys = new ArrayIntList(getMinimizationFactor());
        meta.assignFileId(this);
    }

    /**
     * Create a non-root node by reading it from disk.
     */
    protected IntBTree(BTreeMetaData meta, int fileId) throws IOException, ClassNotFoundException {
        super(meta);
        _keys = new ArrayIntList(getMinimizationFactor());
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
     * Delete an arbitrary instance of the specified key and the specified row
     */
    public final boolean delete(int key, int rowid) throws IOException, ClassNotFoundException {
        //Comments refer to the cases described in CLR (19.3)
        boolean deleted = false;
        int size = size();
        if (size <= 0) {
            return deleted;
        }
        int i = findNearestKeyBelow(key, rowid);
        if (i >= 0 && (isEqual(getKey(i), key) && isNotEqual(getValue(i), rowid))) {
            // Case 0
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
                    // Case 0b : if the key is on the far right,
                    // then we need to merge the last two nodes
                    int mergeLoc;
                    if (pivotLoc < size) {
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
            //Case 3
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
                    if (pivotLoc < size) {
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
                deleted = true;
            } else {
                // Case 2
                if (getChild(i).size() >= getMinimizationFactor()) {
                    // Case 2a, move predecessor up
                    int[] keyParam = new int[1];
                    int[] valueParam = new int[1];
                    getChild(i).getRightMost(keyParam, valueParam);
                    setKeyValuePairAt(i, keyParam[0], valueParam[0]);
                    deleteFromChild(i, keyParam[0], valueParam[0]);
                } else if (getChild(i + 1).size() >= getMinimizationFactor()) {
                    // Case 2b, move successor up
                    int[] keyParam = new int[1];
                    int[] valueParam = new int[1];
                    getChild(i + 1).getLeftMost(keyParam, valueParam);
                    setKeyValuePairAt(i, keyParam[0], valueParam[0]);
                    deleteFromChild(i + 1, keyParam[0], valueParam[0]);
                } else {
                    //Case 2c, merge nodes
                    mergeChildren(i, key);

                    //Now delete from the newly merged node
                    deleteFromChild(i, key, rowid);

                    maybeCollapseTree();
                }
            }
        }
        return deleted;
    }

    /**
     * Find some occurance of the given key.
     */
    public final Integer get(int key) throws IOException, ClassNotFoundException {
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
    public final IntListIterator getAll(int key) throws IOException, ClassNotFoundException {
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
    public final IntListIterator getAllFrom(int key) throws IOException, ClassNotFoundException {
        IntListIteratorChain chain = new IntListIteratorChain();
        getAllFrom(key, chain);
        return chain;
    }

    /**
     * Obtain an iterator over all values strictly less than the given key.
     */
    public final IntListIterator getAllTo(int key) throws IOException, ClassNotFoundException {
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
    public final void insert(int key, int value) throws IOException, ClassNotFoundException {
        // The general strategy here is to:
        //  (a) find the appropriate node
        //  (b) if it is not full, simply insert the key/value pair
        //  (c) else, split the node into two, pulling up the median key to the parent
        // Since "pulling up the median key" may require splitting the parent,
        // to avoid multiple passes we'll split each full node as we move down the tree.

        if (isFull()) {
            // if I'm full,
            //  create a new node
            IntBTree child = allocateNewNode();
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
    public final void replaceId(int key, int oldRowId, int newRowId) throws ClassNotFoundException, IOException {
        int i = findNearestKeyAbove(key);
        boolean valSet = false;
        int size = size();
        while ((i < size) && isEqual(key, getKey(i))) {
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

    /**
     * @see org.axiondb.util.BaseBTree#reset()
     */
    public void truncate() {
        getKeys().clear();
        super.truncate();
    }

    public IntListIterator valueIterator() throws IOException {
        return new IntIteratorIntListIterator(new BTreeValueIterator(this));
    }

    public IntListIterator valueIteratorGreaterThan(int fromkey) throws IOException, ClassNotFoundException {
        return valueIteratorGreaterThanOrEqualTo(fromkey + 1);
    }

    public IntListIterator valueIteratorGreaterThanOrEqualTo(int fromkey) throws IOException, ClassNotFoundException {
        StateStack stack = new StateStack();
        for (IntBTree node = this; null != node;) {
            int i = 0;
            for (int size = node.size();i < size && fromkey > node.getKey(i);) {
                i++;
            }
            stack.push(node, true, i);
            node = node.getChildOrNull(i);
        }
        return new IntIteratorIntListIterator(new BTreeValueIterator(stack));
    }

    final boolean isValid() throws IOException, ClassNotFoundException {
        return isValid(true);
    }

    protected final void addKeyValuePair(int key, int value, boolean setDirty) {
        getKeys().add(key);
        getValues().add(value);
        if (setDirty) {
            getBTreeMetaData().setDirty(this);
        }
    }

    /**
     * Create a new node.
     */
    protected IntBTree createNode(BTreeMetaData meta) throws IOException, ClassNotFoundException {
        return new IntBTree(meta);
    }

    /**
     * Obtain the key stored at the specified index.
     */
    protected final int getKey(int index) {
        return getKeys().get(index);
    }

    /**
     * Read the node with the specified fileId from disk.
     */
    protected IntBTree loadNode(BTreeMetaData meta, int fileId) throws IOException, ClassNotFoundException {
        return new IntBTree(meta, fileId);
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
                addKeyValuePair(in.readInt(), in.readInt(), false);
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
                out.writeInt(getKey(i));
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

    /** Adds the given key/value pair. Assumes I am not full. */
    private final void addKeyValuePair(int key, int value) {
        addKeyValuePair(key, value, true);
    }

    /** Adds the given key/value pair at the given index. Assumes I am not full. */
    private final void addKeyValuePair(int index, int key, int value) {
        getKeys().add(index, key);
        getValues().add(index, value);
        getBTreeMetaData().setDirty(this);
    }

    /** Adds the given key/value pair. Assumes I am not full. */
    private final void addKeyValuePairs(IntList keys, IntList values) {
        getKeys().addAll(keys);
        getValues().addAll(values);
        getBTreeMetaData().setDirty(this);
    }

    /** Adds the given key/value/fileId tuples. Assumes I am not full. */
    private final void addTuples(IntList keys, IntList values, IntList fileIds) {
        getKeys().addAll(keys);
        getValues().addAll(values);
        getChildIds().addAll(fileIds);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Create a new node and flag it as dirty.
     */
    private final IntBTree allocateNewNode() throws IOException, ClassNotFoundException {
        IntBTree tree = createNode(getBTreeMetaData());
        getBTreeMetaData().cacheNode(tree.getFileId(), tree);
        getBTreeMetaData().setDirty(tree);
        return tree;
    }

    private final void borrowLeft(int borrowLoc) throws IOException, ClassNotFoundException {
        IntBTree leftSibling = getChild(borrowLoc - 1);
        IntBTree rightSibling = getChild(borrowLoc);

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
        IntBTree leftSibling = getChild(borrowLoc);
        IntBTree rightSibling = getChild(borrowLoc + 1);

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
     * Compare the given ints via my comparator.
     */
    private final int compare(int x, int y) {
        if (x == NullObject.INSTANCE.intValue() && y == NullObject.INSTANCE.intValue()) {
            return 0;
        }
        if (x == NullObject.INSTANCE.intValue() && y != NullObject.INSTANCE.intValue()) {
            return -1;
        } else if (y == NullObject.INSTANCE.intValue() && x != NullObject.INSTANCE.intValue()) {
            return 1;
        }

        return (x > y) ? 1 : ((x < y) ? -1 : 0);
    }

    /**
     * Delete the given key from the child in the specified position.
     */
    private final boolean deleteFromChild(int position, int key, int rowid) throws IOException, ClassNotFoundException {
        IntBTree node = getChild(position);
        return node.delete(key, rowid);
    }

    private final int findNearestKeyAbove(int key) {
        int size = size();
        int high = size;
        int low = 0;
        int cur = 0;

        //Short circuit
        if (size == 0) {
            return 0;
        } else if (isLessThan(getKey(size - 1), key)) {
            return size;
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
        for (; (cur < size) && isGreaterThan(key, getKey(cur)); cur++) {
        }

        return cur;
    }

    private final int findNearestKeyBelow(int key) {
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
     * Find the index instsance of specified key and the specified row
     */
    private final int findNearestKeyBelow(int key, int rowid) throws IOException {
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
    private final int findNearestRowBelow(int key, int rowid, int index) throws IOException {
        int cur = 0;
        int start = 0;
        int end = 0;
        boolean found = false;
        int size = size();

        // Find the duplicated keys range
        if (index == (size - 1) && index == 0) {
            //Case 1
            cur = 0;
            found = isEqual(rowid, getValue(cur));
            if (found) {
                cur = 0;
            } else if (getChildIds().size() > 0 && isGreaterThan(rowid, getValue(cur))) {
                // the root is in the right-sub-tree
                cur++;
            } else {
                // the root is in the left-sub-tree
                cur = 0;
            }
            return cur;
        } else if (index == (size - 1)) {
            //Case 2
            end = index;
            cur = index;
            // Find the first index which key is specified
            for (; (cur >= 0) && isEqual(key, getKey(cur)); cur--) {
                if (isEqual(rowid, getValue(cur))) {
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
            for (; (cur <= size) && isEqual(key, getKey(cur)); cur++) {
                if (isEqual(rowid, getValue(cur))) {
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
                if (isEqual(rowid, getValue(cur))) {
                    found = true;
                    break;
                }
            }
            start = found ? cur : ++cur;
            if (!found) {
                cur = index;
                // Find the last index which key is specified
                for (; (cur < size) && isEqual(key, getKey(cur)); cur++) {
                    if (isEqual(rowid, getValue(cur))) {
                        found = true;
                        break;
                    }
                }
                end = found ? cur : --cur;
            }
        }

        if (!found) {
            if (isGreaterThan(rowid, getValue(end))) {
                //Case 1
                // the root is in the right-sub-tree
                cur = end;
            } else if (isLessThan(rowid, getValue(start))) {
                //Case 2
                // the root is in the left-sub-tree
                cur = start;
            } else {
                //Case 3
                // the root is in the middle-sub-tree
                for (int i = start; i < end; i++) {
                    if (isGreaterThan(rowid, getValue(i)) && isLessThan(rowid, getValue(i + 1))) {
                        cur = i;
                        break;
                    }
                }
            }
        }
        return cur;
    }

    private final void getAll(int key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = findNearestKeyAbove(key);
        int size = size();
        if (isLeaf()) {
            int stop;
            for (stop = start; stop < size && isEqual(key, getKey(stop)); stop++) {
            }
            chain.addIterator(getValues().subList(start, stop).listIterator());
        } else {
            int i = start;
            for (; i < size && isEqual(key, getKey(i)); i++) {
                getChild(i).getAll(key, chain);
                chain.addIterator(getValue(i));
            }
            getChild(i).getAll(key, chain);
        }
    }

    private final void getAllExcludingNull(IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = _keys.lastIndexOf(NullObject.INSTANCE.intValue());

        int size = size();
        if (isLeaf() && start != -1 && (start + 1) < size) {
            chain.addIterator(getValues().subList(start + 1, size).listIterator());
        } else {
            if (start == -1) {
                start = 0;
            }
            for (int i = start; i < size + 1; i++) {
                if (getChildIds().size() > 0) {
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

    private final void getAllFrom(int key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = findNearestKeyAbove(key);
        int size = size();
        if (isLeaf()) {
            chain.addIterator(getValues().subList(start, size).listIterator());
        } else {
            for (int i = start; i < size + 1; i++) {
                getChild(i).getAllFrom(key, chain);
                if (i < size) {
                    chain.addIterator(getValue(i));
                } else {
                    break;
                }
            }
        }
    }

    private final void getAllTo(int key, IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int size = size();
        if (isLeaf()) {
            int endpoint = getKeys().indexOf(key);
            if (-1 != endpoint) {
                chain.addIterator(getValues().subList(0, endpoint).listIterator());
            } else {
                chain.addIterator(getValues().listIterator());
            }
        } else {
            // else we need to interleave my child nodes as well
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
     * Return the child node at the given index, or throw an exception if no such row
     * exists.
     */
    private final IntBTree getChild(int index) throws IOException, ClassNotFoundException {
        IntBTree child = getChildOrNull(index);
        if (null == child) {
            throw new IOException("Node " + getFileId() + " has no child at index " + index);
        }
        return child;
    }

    /**
     * Obtain the node with the specified file id, either from the cache or by loading it
     * from disk.
     */
    private final IntBTree getChildByFileId(int fileid) throws IOException, ClassNotFoundException {
        IntBTree child = (IntBTree) (getBTreeMetaData().getCachedNode(fileid));
        if (null == child) {
            child = loadNode(getBTreeMetaData(), fileid);
            getBTreeMetaData().cacheNode(fileid, child);
        }
        return child;
    }

    private final IntBTree getChildOrNull(int index) throws IOException, ClassNotFoundException {
        if (index >= getChildIds().size()) {
            return null;
        }
        return getChildByFileId(getFileIdForIndex(index));
    }

    /**
     * Obtain a reference to my list of keys.
     */
    private final IntList getKeys() {
        return _keys;
    }

    /**
     * Finds and deletes the left most value from this subtree. The key and value for the
     * node is returned in the parameters. This also does the replacement as it unwraps.
     */
    private final void getLeftMost(int[] keyParam, int valueParam[]) throws IOException, ClassNotFoundException {
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
    private final void getRightMost(int[] keyParam, int valueParam[]) throws IOException, ClassNotFoundException {
        if (isLeaf()) {
            int max = size() - 1;
            keyParam[0] = getKey(max);
            valueParam[0] = getValue(max);
        } else {
            int max = getChildIds().size() - 1;
            getChild(max).getRightMost(keyParam, valueParam);
        }
    }

    private final boolean hasChild(int index) throws IOException, ClassNotFoundException {
        return (null != getChildOrNull(index));
    }

    private void inorderIterator(IntListIteratorChain chain) throws IOException, ClassNotFoundException {
        int start = 0;
        int size = size();
        if (isLeaf()) {
            int stop = size;
            chain.addIterator(getValues().subList(0, stop).listIterator());
        } else {
            int i = start;
            for (; i < size; i++) {
                getChild(i).inorderIterator(chain);
                chain.addIterator(getValue(i));
            }
            getChild(i).inorderIterator(chain);
        }
    }

    /** Inserts the given key/value pair into this node, which is known to not be full */
    private final void insertNotfull(int key, int value) throws IOException, ClassNotFoundException {
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
            IntBTree child = getChild(i);
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

    private final boolean isEqual(int x, int y) {
        return compare(x, y) == 0;
    }

    private final boolean isGreaterThan(int x, int y) {
        return compare(x, y) > 0;
    }

    private final boolean isGreaterThanOrEqual(int x, int y) {
        return compare(x, y) >= 0;
    }

    private final boolean isLessThan(int x, int y) {
        return compare(x, y) < 0;
    }

    private final boolean isLessThanOrEqual(int x, int y) {
        return compare(x, y) <= 0;
    }

    private final boolean isNotEqual(int x, int y) {
        return compare(x, y) != 0;
    }

    private final boolean isValid(boolean isRoot) throws IOException, ClassNotFoundException {
        int size = size();
        //Check to make sure that the node isn't an empty branch
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
            IntBTree nodeToPromote = getChild(0);
            setTuples(nodeToPromote.getKeys(), nodeToPromote.getValues(), nodeToPromote.getChildIds());
        }
    }

    private final void mergeChildren(int mergeLoc, int key) throws IOException, ClassNotFoundException {
        IntBTree leftChild = getChild(mergeLoc);
        IntBTree rightChild = getChild(mergeLoc + 1);

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
        getKeys().removeElementAt(index);
        getValues().removeElementAt(index);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * {@link #replaceId Replace}the specified value within the child in the specified
     * position.
     */
    private final void replaceIdInChild(int position, int key, int oldRowId, int newRowId) throws IOException, ClassNotFoundException {
        IntBTree node = getChild(position);
        node.replaceId(key, oldRowId, newRowId);
    }

    /** Sets my key list */
    private final void setKeys(IntList keys) {
        _keys = keys;
    }

    /** Sets the specified given key/value pair. */
    private final void setKeyValuePairAt(int index, int key, int value) {
        getKeys().set(index, key);
        getValues().set(index, value);
        getBTreeMetaData().setDirty(this);
    }

    /** Sets the given given key/value/file id tuples. */
    private final void setTuples(IntList keys, IntList values, IntList fileIds) {
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
    private final void subdivideChild(int pivot, IntBTree child) throws IOException, ClassNotFoundException {
        // create the new child node, (a sibling to the specified child)
        IntBTree fetus = allocateNewNode();
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
            for (int i = 0, size = size(); i < size + 1; i++) {
                IntBTree child = null;
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

    private static class BTreeValueIterator implements IntIterator {

        private StateStack _stack;

        BTreeValueIterator(IntBTree node) throws IOException {
            _stack = new StateStack();
            _stack.push(node, false, 0);
        }

        BTreeValueIterator(StateStack stack) throws IOException {
            _stack = stack;
        }

        public boolean hasNext() {
            while (true) {
                if (_stack.isEmpty()) {
                    return false;
                }
                State state = _stack.peek();
                if ((state.node.isLeaf() || state.visitedChildren) && state.position >= state.node.size()) {
                    _stack.pop();
                } else {
                    return true;
                }
            }
        }

        public int next() {
            try {
                while (true) {
                    if (_stack.isEmpty()) {
                        throw new NoSuchElementException();
                    }
                    State state = _stack.pop();
                    if (!state.visitedChildren) {
                        state.visitedChildren = true;
                        _stack.push(state);
                        if (state.node.hasChild(state.position)) {
                            _stack.push(state.node.getChild(state.position), false, 0);
                        }
                    } else if (state.position < state.node.size()) {
                        int value = state.node.getValue(state.position);
                        state.position++;
                        state.visitedChildren = false;
                        _stack.push(state);
                        return value;
                    } else {
                        // do nothing, I've already popped
                    }
                }
            } catch (Exception e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class State {

        IntBTree node;
        int position = 0;
        boolean visitedChildren = false;

        State(IntBTree n, boolean visited, int pos) {
            node = n;
            visitedChildren = visited;
            position = pos;
        }

        public String toString() {
            return "<" + node.getFileId() + "," + visitedChildren + "," + position + ">";
        }
    }

    private static class StateStack {

        private List _nodes = new ArrayList();

        StateStack() {
        }

        public String toString() {
            return _nodes.toString();
        }

        boolean isEmpty() {
            return _nodes.isEmpty();
        }

        State peek() {
            return (State) _nodes.get(_nodes.size() - 1);
        }

        State pop() {
            return (State) (_nodes.remove(_nodes.size() - 1));
        }

        void push(IntBTree tree, boolean visitedChildren, int position) {
            push(new State(tree, visitedChildren, position));
        }

        void push(State state) {
            _nodes.add(state);
        }

    }

    private IntList _keys;

}
