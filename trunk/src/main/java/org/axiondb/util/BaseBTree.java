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

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;
import org.axiondb.engine.rowcollection.IntHashMap;

/**
 * A b-tree is a balanced, multi-way search tree that assumes its data is stored on disk.
 * The b-tree structure is designed to minimize the number of disk accesses by storing
 * data in a broad, short tree and reading each node in one gulp.
 * <p />
 * Each b-tree node (excluding the root) contains at least <i>t </i>-1 and at most 2 <i>t
 * </i>-1 keys (and their associated values) (where <i>t </i> is the "minimization
 * factor") in non-decreasing order. Associated with each key <i>k(i) </i> is a reference
 * to a subtree containing all keys greater than <i>k(i-1) </i> and less than or equal to
 * <i>k(i) </i>. An "extra" subtree reference contains all keys greater than the maximum
 * key in this node.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Dave Pekarek Krohn
 * @author Rodney Waldhoff
 */
abstract class BaseBTree {
    // NB: We are aware that declaring methods as final doesn't improve performance in
    // general.The stuff declared final is declared final so that we know it's not
    // overriden in any subclass. This makes refactoring easier.
    
    protected BaseBTree(BTreeMetaData meta) throws IOException, ClassNotFoundException {
        _metaData = meta;
        setValues(new ArrayIntList(getMinimizationFactor() - 1));
        setChildIds(new ArrayIntList(getMinimizationFactor()));
    }

    /**
     * @param dir the directory to store my data files in
     * @param name the name of this btree structure (used to generate data file names)
     * @param minimizationFactor the minimization factor (often <i>t </i>). Each node will
     *        contain at most 2 <i>t </i>-1 keys, and 2 <i>t </i> children.
     * @param root the root of my tree (or null if this node is going to be the root).
     */
    protected BaseBTree(File dir, String name, int minimizationFactor) throws IOException, ClassNotFoundException {
        _metaData = new BTreeMetaData(dir, name, minimizationFactor, this);
        setValues(new ArrayIntList(getMinimizationFactor() - 1));
        setChildIds(new ArrayIntList(getMinimizationFactor()));
    }

    public abstract void save() throws IOException, ClassNotFoundException;

    /**
     * Saves the tree. It saves the counter file and {@link #write}s any dirty nodes.
     */
    public void save(File dataDirectory) throws IOException, ClassNotFoundException {
        getBTreeMetaData().setDataDirectory(dataDirectory);
        if (getBTreeMetaData().hasDirtyNodes()) {
            getBTreeMetaData().saveCounter();
        }
        for (IntHashMap.ValueIterator iter = getBTreeMetaData().getDirtyNodes(); iter.hasNext();) {
            BaseBTree tree = (BaseBTree) iter.next();
            tree.write();
            iter.remove();
        }
    }

    public void saveAfterTruncate() throws IOException, ClassNotFoundException {
        getBTreeMetaData().setAllClean();
        save();
    }

    public abstract int size();

    public void truncate() {
        getValues().clear();
        getChildIds().clear();
    }

    /**
     * Add a reference to the given file id. Flags me as dirty.
     */
    protected final void addFileId(int fileId) {
        getChildIds().add(fileId);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Store a reference to the given file id at the specifed index. Flags me as dirty.
     */
    protected final void addFileId(int index, int fileid) {
        getChildIds().add(index, fileid);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Add the given specified file ids. Flags me as dirty.
     */
    protected final void addFileIds(IntList fileIds) {
        getChildIds().addAll(fileIds);
        getBTreeMetaData().setDirty(this);
    }

    /**
     * Clear my values and file ids. Flags me as dirty.
     */
    protected void clearData() {
        getValues().clear();
        getChildIds().clear();
        getBTreeMetaData().setDirty(this);
    }

    protected final BTreeMetaData getBTreeMetaData() {
        return _metaData;
    }

    protected final IntList getChildIds() {
        return _childIds;
    }

    protected final int getFileId() {
        return _fileId;
    }

    /**
     * Get the file id for the specified index.
     */
    protected final int getFileIdForIndex(int index) {
        return getChildIds().get(index);
    }

    /**
     * Return the maximum number of keys I can contain (2*
     * {@link #getMinimizationFactor minimizationFactor}-1).
     */
    protected final int getKeyCapacity() {
        return (2 * getMinimizationFactor()) - 1;
    }

    protected final int getMinimizationFactor() {
        return _metaData.getMinimizationFactor();
    }

    protected final int getValue(int index) {
        return _vals.get(index);
    }

    protected final IntList getValues() {
        return _vals;
    }

    protected final boolean isFull() {
        return size() == getKeyCapacity();
    }

    /** Returns <code>true</code> iff I don't contain any child nodes. */
    protected final boolean isLeaf() {
        return (getChildIds().isEmpty());
    }

    /** Returns <code>true</code> iff I am the root node. */
    protected final boolean isRoot() {
        return _metaData.isRoot(this);
    }

    protected abstract void read() throws IOException, ClassNotFoundException;

    protected final void saveCounterIfRoot() throws IOException {
        if (isRoot()) {
            _metaData.saveCounter();
        }
    }

    protected final void setChildIds(IntList childIds) {
        _childIds = childIds;
    }

    protected final void setFileId(int fileId) {
        _fileId = fileId;
    }

    protected final void setValue(int index, int val) {
        _vals.set(index, val);
    }

    protected final void setValues(IntList vals) {
        _vals = vals;
    }

    /**
     * Return a String comprised of 2*n spaces.
     */
    protected final String space(int n) {
        StringBuffer buf = new StringBuffer(0);
        for (int i = 0; i < n; i++) {
            buf.append("  ");
        }
        return buf.toString();
    }

    protected abstract void write() throws IOException, ClassNotFoundException;

    /** The file ids of my children. */
    private IntList _childIds = null;
    /** The idenentifier for my data file. */
    private int _fileId = 0;

    private BTreeMetaData _metaData = null;
    /** The row ids corresponding to each of my keys. */
    private IntList _vals = null;
}
