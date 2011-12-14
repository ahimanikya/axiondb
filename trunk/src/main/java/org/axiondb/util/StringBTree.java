/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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
import java.util.Comparator;

import org.axiondb.io.AxionFileSystem;

/**
 * An {@link ObjectBTree}optimized for reading and writing Strings.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class StringBTree extends ObjectBTree {

    // constructors
    //-------------------------------------------------------------------------

    /**
     * Create or load a new root node.
     */
    public StringBTree(File idxDir, String idxName, int minimizationFactor, Comparator comp)
            throws IOException, ClassNotFoundException {
        super(idxDir, idxName, minimizationFactor, comp);
    }

    /**
     * Create a new, non-root node.
     */
    private StringBTree(BTreeMetaData meta, Comparator comp) throws IOException,
            ClassNotFoundException {
        super(meta, comp);
    }

    /**
     * Create a non-root node by reading it from disk.
     */
    private StringBTree(BTreeMetaData meta, Comparator comp, int fileId) throws IOException,
            ClassNotFoundException {
        super(meta, comp, fileId);
    }

    // protected
    //-------------------------------------------------------------------------

    protected ObjectBTree createNode(BTreeMetaData meta, Comparator comp) throws IOException,
            ClassNotFoundException {
        return new StringBTree(meta, comp);
    }

    protected ObjectBTree loadNode(BTreeMetaData meta, Comparator comp, int fileId)
            throws IOException, ClassNotFoundException {
        return new StringBTree(meta, comp, fileId);
    }

    protected void read() throws IOException, ClassNotFoundException {
        ObjectInputStream in = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            in = fs.openObjectInputSteam(getBTreeMetaData().getFileById(getFileId()));
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                addKeyValuePair(in.readUTF(), in.readInt(), false);
            }
            size = in.readInt();
            for (int i = 0; i < size; i++) {
                getChildIds().add(in.readInt());
            }
        } finally {
            fs.closeInputStream(in);
        }
    }

    protected void write() throws IOException {
        ObjectOutputStream out = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            out = fs.createObjectOutputSteam(getBTreeMetaData().getFileById(getFileId()));
            int size = size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeUTF((String) getKey(i));
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

    protected Object getNullKey() {
        return NullObject.INSTANCE.toString();
    }

}

