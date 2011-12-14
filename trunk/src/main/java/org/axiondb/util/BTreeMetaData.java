/*
 * 
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import org.axiondb.engine.rowcollection.IntHashMap;

/**
 * Manages the meta-data for a BTree.
 * 
 * @version  
 * @author Rodney Waldhoff
 */
class BTreeMetaData {
    public BTreeMetaData(File dataDirectory, String name, int minimizationFactor, BaseBTree root)
            throws IOException, ClassNotFoundException {
        _dataDirectory = dataDirectory;
        _name = name.toUpperCase();
        _minimizationFactor = minimizationFactor;
        _root = root;
        _fileIdCounter = new CounterFile();
        _nodeCache = new IntHashMap();
        _dirtyNodes = new IntHashMap();
    }

    public boolean isRoot(BaseBTree node) {
        return node == _root;
    }

    public void assignFileId(BaseBTree node) {
        node.setFileId(_fileIdCounter.increment());
    }

    public File getDataDirectory() {
        return _dataDirectory;
    }

    public void setDataDirectory(File dir) {
        _dataDirectory = dir;
    }

    public String getName() {
        return _name;
    }

    public int getMinimizationFactor() {
        return _minimizationFactor;
    }

    public int incrementCounter() {
        return _fileIdCounter.increment();
    }

    public void saveCounter() throws IOException {
        _fileIdCounter.save(getCounterFile());
    }

    public void loadCounter() throws IOException {
        _fileIdCounter = CounterFile.load(getCounterFile());
    }

    public File getCounterFile() {
        return new File(getDataDirectory(), getName() + ".CTR");
    }

    public final File getFileById(int fileid) {
        return new File(getDataDirectory(), getName() + "." + fileid);
    }

    public void cacheNode(int fileId, BaseBTree tree) {
        _nodeCache.put(fileId, new SoftReference(tree));
    }

    public BaseBTree getCachedNode(int fileId) {
        Reference ref = (Reference) (_nodeCache.get(fileId));
        if (null != ref) {
            BaseBTree node = (BaseBTree) ref.get();
            if (null == node) {
                _nodeCache.remove(fileId);
            }
            return node;
        }
        return null;
    }

    public BaseBTree getCachedNode(Integer fileId) {
        return getCachedNode(fileId.intValue());
    }

    public void cacheNode(Integer fileId, BaseBTree tree) {
        cacheNode(fileId.intValue(), tree);
    }

    public void setDirty(BaseBTree tree) {
        setDirty(tree.getFileId(), tree);
    }

    public void setDirty(int fileId, BaseBTree tree) {
        _dirtyNodes.put(fileId, tree);
    }

    public void setDirty(Integer fileId, BaseBTree tree) {
        setDirty(fileId.intValue(), tree);
    }

    public void setAllClean() {
        _dirtyNodes.clear();
        _nodeCache.clear();

        // Delete each node file.
        for (int i = 0, I = _fileIdCounter.current(); i < I; i++) {
            File file = getFileById(i);
            if (file.exists()) {
                file.delete();
            }
        }
        
        _fileIdCounter = new CounterFile();
    }

    public IntHashMap.ValueIterator getDirtyNodes() {
        return _dirtyNodes.valueIterator();
    }

    public int getDirtyNodeCount() {
        return _dirtyNodes.size();
    }

    public boolean hasDirtyNodes() {
        return !_dirtyNodes.isEmpty();
    }

    private BaseBTree _root;
    private CounterFile _fileIdCounter;
    private String _name;
    private File _dataDirectory;
    private int _minimizationFactor = 1000;
    private IntHashMap _nodeCache;
    private IntHashMap _dirtyNodes;

}
